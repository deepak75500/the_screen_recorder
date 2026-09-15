"""
app.py
------
Flask backend for browser-based screen recording -> Google Drive.

Key design point: Google Drive's resumable upload protocol requires every
INTERMEDIATE chunk you PUT to be a multiple of 256 KiB (262144 bytes) --
only the very last chunk may be an arbitrary size. Browser MediaRecorder
chunks (emitted every ~10s) are NOT guaranteed to land on 256 KiB
boundaries, so this backend buffers incoming bytes per recording session
and only forwards 256-KiB-aligned slices to Drive, keeping the remainder
buffered until either more data arrives or the recording is stopped
(at which point the remainder becomes the final, arbitrarily-sized chunk).

Sessions are kept in an in-memory dict for simplicity. This is fine for a
single-process dev/small deployment; see README "Limitations" for what
changes if you run multiple worker processes.
"""
import os
import re
import time
import uuid
import logging
import threading
from datetime import datetime

from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
import requests as http
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("screen-recorder")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration (from .env — never hard-code secrets)
# ---------------------------------------------------------------------------
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip()

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.file"
DRIVE_UPLOAD_INIT_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable"
DRIVE_TOKEN_URI = "https://oauth2.googleapis.com/token"

CHUNK_ALIGNMENT = 256 * 1024  # Google Drive resumable upload requirement
MAX_CHUNK_BYTES = 25 * 1024 * 1024  # reject absurdly large single chunks (abuse guard)
ALLOWED_CONTENT_TYPES = (
    "video/webm",
    "video/webm;codecs=vp9,opus",
    "video/webm;codecs=vp8,opus",
    "application/octet-stream",  # some browsers report Blob parts this way
)

SESSION_MAX_AGE_SEC = 12 * 3600  # 12h safety cap so abandoned sessions get reaped

# ---------------------------------------------------------------------------
# In-memory session store
#   recording_id -> RecordingSession
# ---------------------------------------------------------------------------
_sessions = {}
_sessions_lock = threading.Lock()


class RecordingSession:
    """Tracks one in-progress recording's Drive resumable-upload state."""

    def __init__(self, recording_id: str, filename: str, upload_url: str):
        self.recording_id = recording_id
        self.filename = filename
        self.upload_url = upload_url

        self.buffer = bytearray()        # bytes not yet sent to Drive
        self.bytes_sent_to_drive = 0     # bytes Drive has confirmed receiving
        self.next_chunk_number = 0       # next chunk_number we expect from the client
        self.chunks_received = 0

        self.finished = False
        self.file_id = None
        self.file_url = None
        self.error = None

        self.created_at = time.time()
        self.lock = threading.Lock()  # serializes all Drive calls for this session


def _get_access_token() -> str:
    """Exchanges the long-lived refresh token for a fresh short-lived access token."""
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        raise RuntimeError(
            "Google Drive credentials are not configured. Set GOOGLE_CLIENT_ID, "
            "GOOGLE_CLIENT_SECRET, and GOOGLE_REFRESH_TOKEN in .env."
        )
    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri=DRIVE_TOKEN_URI,
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=[DRIVE_SCOPE],
    )
    creds.refresh(GoogleAuthRequest())
    return creds.token


def _initiate_drive_resumable_session(filename: str) -> str:
    """Starts a Drive resumable upload session and returns its session URI."""
    access_token = _get_access_token()

    metadata = {"name": filename}
    if GOOGLE_DRIVE_FOLDER_ID:
        metadata["parents"] = [GOOGLE_DRIVE_FOLDER_ID]

    resp = http.post(
        DRIVE_UPLOAD_INIT_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=UTF-8",
        },
        json=metadata,
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to start Drive upload session: {resp.status_code} {resp.text}")

    session_uri = resp.headers.get("Location")
    if not session_uri:
        raise RuntimeError("Drive did not return a resumable session URI (Location header missing).")
    return session_uri


def _put_chunk_to_drive(session: RecordingSession, data: bytes, is_final: bool):
    """
    Sends one aligned chunk (or the final, arbitrarily-sized chunk) to Drive's
    resumable upload session. Returns (finished, file_json_or_None).
    """
    start = session.bytes_sent_to_drive
    length = len(data)
    access_token = _get_access_token()

    if is_final:
        total = start + length
        if length > 0:
            content_range = f"bytes {start}-{total - 1}/{total}"
        else:
            # We already uploaded everything in earlier chunks; this call just
            # tells Drive the final total size with a zero-length body.
            content_range = f"bytes */{total}"
    else:
        # Total size unknown while the recording is still in progress.
        content_range = f"bytes {start}-{start + length - 1}/*"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Length": str(length),
        "Content-Range": content_range,
    }

    resp = http.put(session.upload_url, headers=headers, data=bytes(data), timeout=120)

    if resp.status_code in (200, 201):
        # Upload complete — Drive returns the file resource.
        return True, resp.json()

    if resp.status_code == 308:
        # "Resume Incomplete" — expected for intermediate chunks.
        session.bytes_sent_to_drive = start + length
        return False, None

    raise RuntimeError(f"Drive chunk upload failed: {resp.status_code} {resp.text}")


def _flush_session(session: RecordingSession, final: bool):
    """
    Sends whatever is safely sendable from session.buffer to Drive.
    - Intermediate flush: only sends 256-KiB-aligned bytes, keeps the remainder.
    - Final flush: sends everything remaining, regardless of alignment, and
      finalizes the Drive file.
    """
    if final:
        chunk = bytes(session.buffer)
        session.buffer = bytearray()
        finished, file_json = _put_chunk_to_drive(session, chunk, is_final=True)
        if finished and file_json:
            session.finished = True
            session.file_id = file_json.get("id")
            session.file_url = f"https://drive.google.com/file/d/{session.file_id}/view"
        return

    aligned_len = (len(session.buffer) // CHUNK_ALIGNMENT) * CHUNK_ALIGNMENT
    if aligned_len == 0:
        return  # not enough buffered yet to send an aligned chunk

    chunk = bytes(session.buffer[:aligned_len])
    session.buffer = session.buffer[aligned_len:]
    _put_chunk_to_drive(session, chunk, is_final=False)


def _reap_stale_sessions():
    now = time.time()
    with _sessions_lock:
        stale = [rid for rid, s in _sessions.items() if now - s.created_at > SESSION_MAX_AGE_SEC]
        for rid in stale:
            log.warning("Reaping stale recording session %s", rid)
            del _sessions[rid]


def _safe_filename_component() -> str:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"Screen_Recording_{ts}.webm"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/recording/start", methods=["POST"])
def recording_start():
    _reap_stale_sessions()
    try:
        filename = _safe_filename_component()
        upload_url = _initiate_drive_resumable_session(filename)
    except Exception as exc:
        log.exception("Failed to start recording session")
        return jsonify({"success": False, "error": str(exc)}), 502

    recording_id = uuid.uuid4().hex
    session = RecordingSession(recording_id, filename, upload_url)
    with _sessions_lock:
        _sessions[recording_id] = session

    log.info("Started recording session %s (%s)", recording_id, filename)
    return jsonify({"success": True, "recording_id": recording_id, "filename": filename})


@app.route("/api/recording/chunk", methods=["POST"])
def recording_chunk():
    recording_id = request.form.get("recording_id", "")
    chunk_number_raw = request.form.get("chunk_number", "")
    uploaded_file = request.files.get("chunk")

    # --- Validate input; never trust the client ---
    with _sessions_lock:
        session = _sessions.get(recording_id)
    if session is None:
        return jsonify({"success": False, "error": "Unknown or expired recording_id"}), 404

    if uploaded_file is None:
        return jsonify({"success": False, "error": "Missing chunk file part"}), 400

    if uploaded_file.mimetype not in ALLOWED_CONTENT_TYPES:
        return jsonify({"success": False, "error": f"Unsupported content type: {uploaded_file.mimetype}"}), 400

    try:
        chunk_number = int(chunk_number_raw)
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "chunk_number must be an integer"}), 400

    data = uploaded_file.read()
    if len(data) == 0:
        return jsonify({"success": False, "error": "Empty chunk"}), 400
    if len(data) > MAX_CHUNK_BYTES:
        return jsonify({"success": False, "error": "Chunk exceeds maximum allowed size"}), 413

    with session.lock:
        if session.finished:
            return jsonify({"success": False, "error": "Recording already finalized"}), 409

        if chunk_number != session.next_chunk_number:
            # Reject out-of-order chunks: Drive's resumable upload is strictly
            # sequential, so accepting an out-of-order chunk would corrupt the file.
            return jsonify({
                "success": False,
                "error": f"Out-of-order chunk. Expected {session.next_chunk_number}, got {chunk_number}",
            }), 409

        try:
            session.buffer += data
            _flush_session(session, final=False)
        except Exception as exc:
            log.exception("Chunk upload to Drive failed for session %s", recording_id)
            session.error = str(exc)
            return jsonify({"success": False, "error": str(exc)}), 502

        session.next_chunk_number += 1
        session.chunks_received += 1

    return jsonify({"success": True, "chunk_number": chunk_number, "uploaded": True})


@app.route("/api/recording/stop", methods=["POST"])
def recording_stop():
    recording_id = (request.get_json(silent=True) or {}).get("recording_id") or request.form.get("recording_id", "")

    with _sessions_lock:
        session = _sessions.get(recording_id)
    if session is None:
        return jsonify({"success": False, "error": "Unknown or expired recording_id"}), 404

    with session.lock:
        if session.finished:
            return jsonify({
                "success": True,
                "file_id": session.file_id,
                "file_url": session.file_url,
            })
        try:
            _flush_session(session, final=True)
        except Exception as exc:
            log.exception("Failed to finalize Drive upload for session %s", recording_id)
            return jsonify({"success": False, "error": str(exc)}), 502

    if not session.finished:
        return jsonify({"success": False, "error": "Drive did not confirm the upload finished"}), 502

    log.info("Finished recording session %s -> %s", recording_id, session.file_url)

    with _sessions_lock:
        # Keep it briefly reachable for a status poll, but recording is done.
        pass

    return jsonify({"success": True, "file_id": session.file_id, "file_url": session.file_url})


@app.route("/api/recording/status", methods=["GET"])
def recording_status():
    recording_id = request.args.get("recording_id", "")
    with _sessions_lock:
        session = _sessions.get(recording_id)
    if session is None:
        return jsonify({"success": False, "error": "Unknown or expired recording_id"}), 404

    return jsonify({
        "success": True,
        "recording_id": recording_id,
        "filename": session.filename,
        "chunks_received": session.chunks_received,
        "bytes_uploaded": session.bytes_sent_to_drive,
        "finished": session.finished,
        "file_id": session.file_id,
        "file_url": session.file_url,
        "error": session.error,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # debug=False in anything resembling production; long-running dev server
    # with threaded=True lets multiple chunk requests interleave across
    # different recording sessions (each session serializes its own chunks
    # via session.lock).
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
