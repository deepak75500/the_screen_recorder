/*
 * app.js
 * ------
 * Browser-side screen recording and chunked upload.
 *
 * Important correctness detail: Google Drive's resumable upload (handled
 * server-side) is strictly sequential -- chunk N must arrive before chunk
 * N+1 is processed. So chunks are pushed into an in-memory queue and a
 * single worker uploads them ONE AT A TIME, in order, retrying with
 * exponential backoff on failure, before moving to the next chunk. This is
 * also the "pending chunk queue" that gives us basic network-drop recovery:
 * recording keeps running locally even if uploads are temporarily stuck.
 */

(() => {
  const startBtn = document.getElementById("start-btn");
  const pauseBtn = document.getElementById("pause-btn");
  const stopBtn = document.getElementById("stop-btn");
  const micToggle = document.getElementById("mic-toggle");
  const statusDot = document.getElementById("status-dot");
  const statusText = document.getElementById("status-text");
  const timerEl = document.getElementById("timer");
  const chunksCountEl = document.getElementById("chunks-count");
  const queueCountEl = document.getElementById("queue-count");
  const uploadBarTrack = document.getElementById("upload-bar-track");
  const uploadStatusEl = document.getElementById("upload-status");
  const driveLink = document.getElementById("drive-link");
  const drivePending = document.getElementById("drive-pending");
  const driveProcessing = document.getElementById("drive-processing");
  const errorBox = document.getElementById("error-box");

  const CHUNK_INTERVAL_MS = 10000; // "~every 10 seconds", per MediaRecorder.start(timeslice)
  const MAX_RETRIES = 6;

  let mediaRecorder = null;
  let displayStream = null;
  let micStream = null;
  let audioCtx = null;
  let combinedStream = null;

  let recordingId = null;
  let clientChunkNumber = 0;
  let chunksUploadedCount = 0;

  let uploadQueue = [];
  let queueWorkerRunning = false;

  let timerHandle = null;
  let recordingStartedAt = null;
  let accumulatedElapsedMs = 0;
  let driveLinkRevealTimer = null;

  // -------------------------------------------------------------------
  // UI helpers
  // -------------------------------------------------------------------
  function setHidden(el, value) {
    if (el) el.hidden = value;
  }

  function setStatus(kind, text) {
    if (statusDot) statusDot.className = "dot dot-" + kind;
    if (statusText) statusText.textContent = text;
  }

  function showError(message) {
    if (errorBox) {
      errorBox.hidden = false;
      errorBox.textContent = message;
    }
  }

  function clearError() {
    if (errorBox) {
      errorBox.hidden = true;
      errorBox.textContent = "";
    }
  }

  function formatElapsed(ms) {
    const totalSec = Math.floor(ms / 1000);
    const h = String(Math.floor(totalSec / 3600)).padStart(2, "0");
    const m = String(Math.floor((totalSec % 3600) / 60)).padStart(2, "0");
    const s = String(totalSec % 60).padStart(2, "0");
    return `${h}:${m}:${s}`;
  }

  function updateTimerDisplay() {
    const currentMs = accumulatedElapsedMs + (recordingStartedAt ? Date.now() - recordingStartedAt : 0);
    timerEl.textContent = formatElapsed(currentMs);
  }

  function startTimer() {
    accumulatedElapsedMs = 0;
    recordingStartedAt = Date.now();
    timerEl.textContent = "00:00:00";
    if (timerHandle) clearInterval(timerHandle);
    timerHandle = setInterval(updateTimerDisplay, 1000);
  }

  function pauseTimer() {
    if (recordingStartedAt !== null) {
      accumulatedElapsedMs += Date.now() - recordingStartedAt;
      recordingStartedAt = null;
    }
    if (timerHandle) clearInterval(timerHandle);
    timerHandle = null;
    updateTimerDisplay();
  }

  function resumeTimer() {
    recordingStartedAt = Date.now();
    if (timerHandle) clearInterval(timerHandle);
    timerHandle = setInterval(updateTimerDisplay, 1000);
    updateTimerDisplay();
  }

  function stopTimer() {
    if (timerHandle) clearInterval(timerHandle);
    timerHandle = null;
    recordingStartedAt = null;
    updateTimerDisplay();
  }

  function updateQueueUI() {
    if (chunksCountEl) chunksCountEl.textContent = String(chunksUploadedCount);
    if (queueCountEl) queueCountEl.textContent = String(uploadQueue.length);
  }

  // -------------------------------------------------------------------
  // Feature detection
  // -------------------------------------------------------------------
  function browserSupportsRecording() {
    return !!(navigator.mediaDevices &&
              navigator.mediaDevices.getDisplayMedia &&
              window.MediaRecorder);
  }

  function pickSupportedMimeType() {
    const candidates = [
      "video/webm;codecs=vp9,opus",
      "video/webm;codecs=vp8,opus",
      "video/webm",
    ];
    for (const type of candidates) {
      if (MediaRecorder.isTypeSupported(type)) return type;
    }
    return ""; // let the browser pick a default
  }

  // -------------------------------------------------------------------
  // Audio mixing: combine screen audio (if any) + mic audio (if enabled)
  // into a single track using the Web Audio API.
  // -------------------------------------------------------------------
  function buildCombinedStream(screenStream, microphoneStream) {
    const videoTrack = screenStream.getVideoTracks()[0];
    const screenAudioTracks = screenStream.getAudioTracks();
    const micAudioTracks = microphoneStream ? microphoneStream.getAudioTracks() : [];

    if (screenAudioTracks.length === 0 && micAudioTracks.length === 0) {
      // No audio available/selected at all -- video only.
      const videoOnly = new MediaStream([videoTrack]);
      return videoOnly;
    }

    if (screenAudioTracks.length > 0 && micAudioTracks.length === 0) {
      // Only screen/tab audio -- no mixing needed.
      return new MediaStream([videoTrack, ...screenAudioTracks]);
    }

    if (screenAudioTracks.length === 0 && micAudioTracks.length > 0) {
      // Only microphone -- no mixing needed.
      return new MediaStream([videoTrack, ...micAudioTracks]);
    }

    // Both present -- mix them into one track via Web Audio API.
    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const destination = audioCtx.createMediaStreamDestination();

    const screenAudioSource = audioCtx.createMediaStreamSource(new MediaStream(screenAudioTracks));
    screenAudioSource.connect(destination);

    const micAudioSource = audioCtx.createMediaStreamSource(new MediaStream(micAudioTracks));
    micAudioSource.connect(destination);

    return new MediaStream([videoTrack, ...destination.stream.getAudioTracks()]);
  }

  // -------------------------------------------------------------------
  // Upload queue: sequential, ordered, retried with exponential backoff
  // -------------------------------------------------------------------
  function enqueueChunk(blob, chunkNumber) {
    uploadQueue.push({ blob, chunkNumber, attempt: 0 });
    updateQueueUI();
    if (!queueWorkerRunning) {
      queueWorkerRunning = true;
      runQueueWorker();
    }
  }

  async function runQueueWorker() {
    while (uploadQueue.length > 0) {
      const item = uploadQueue[0]; // peek; only remove on success
      try {
        await uploadChunkWithRetry(item);
        uploadQueue.shift(); // success -> remove and move on
        chunksUploadedCount += 1;
        updateQueueUI();
      } catch (err) {
        // uploadChunkWithRetry already exhausted retries; surface the error
        // but keep the item at the front of the queue so a manual/next
        // network recovery can pick it back up (recording keeps running).
        showError(
          `Upload stalled on chunk ${item.chunkNumber}: ${err.message}. ` +
          `Recording continues locally; will keep retrying.`
        );
        await sleep(5000);
      }
    }
    queueWorkerRunning = false;
    uploadStatusEl.textContent = mediaRecorder && mediaRecorder.state === "recording"
      ? "Recording — chunks up to date"
      : "Idle";
  }

  async function uploadChunkWithRetry(item) {
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        uploadStatusEl.textContent = `Uploading chunk ${item.chunkNumber}` +
          (attempt > 0 ? ` (retry ${attempt})` : "");
        await postChunk(item.blob, item.chunkNumber);
        clearError();
        return;
      } catch (err) {
        if (attempt === MAX_RETRIES) throw err;
        const backoffMs = Math.min(1000 * 2 ** attempt, 30000);
        await sleep(backoffMs);
      }
    }
  }

  async function postChunk(blob, chunkNumber) {
    const form = new FormData();
    form.append("recording_id", recordingId);
    form.append("chunk_number", String(chunkNumber));
    form.append("chunk", blob, "chunk.webm");

    const resp = await fetch("/api/recording/chunk", { method: "POST", body: form });
    let payload = null;
    try { payload = await resp.json(); } catch (_) { /* ignore parse errors */ }

    if (!resp.ok || !payload || !payload.success) {
      const msg = (payload && payload.error) || `HTTP ${resp.status}`;
      throw new Error(msg);
    }
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function flushRecorderData() {
    if (!mediaRecorder) return;
    try {
      if (mediaRecorder.state === "recording" || mediaRecorder.state === "paused") {
        mediaRecorder.requestData();
      }
    } catch (_) {
      // Some browsers may throw if a recorder is in a transient state.
    }
  }

  function scheduleDriveLinkReveal(fileUrl) {
    if (driveLinkRevealTimer) clearTimeout(driveLinkRevealTimer);
    driveLinkRevealTimer = setTimeout(() => {
      setHidden(driveProcessing, true);
      setHidden(driveLink, false);
      if (driveLink) {
        driveLink.href = fileUrl;
        driveLink.textContent = "Open Recording";
      }
    }, 20000);
  }

  function waitForQueueToDrain() {
    return new Promise((resolve) => {
      const check = () => {
        if (uploadQueue.length === 0 && !queueWorkerRunning) resolve();
        else setTimeout(check, 300);
      };
      check();
    });
  }

  // -------------------------------------------------------------------
  // Start / Stop
  // -------------------------------------------------------------------
  async function startRecording() {
    clearError();

    if (mediaRecorder && mediaRecorder.state !== "inactive") {
      return;
    }

    if (!browserSupportsRecording()) {
      showError("Your browser does not support MediaRecorder. Please use a recent version of Chrome or Edge.");
      return;
    }

    try {
      displayStream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
    } catch (err) {
      if (err.name === "NotAllowedError") {
        showError("Screen sharing permission was denied.");
      } else {
        showError(`Could not start screen capture: ${err.message}`);
      }
      return;
    }

    if (micToggle.checked) {
      try {
        micStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      } catch (err) {
        // Non-fatal: proceed without mic if the user denies/has no mic.
        console.warn("Microphone unavailable:", err);
        micStream = null;
      }
    }

    // If the user stops sharing from the browser's own "Stop sharing" UI,
    // treat that the same as clicking our Stop button.
    displayStream.getVideoTracks()[0].addEventListener("ended", () => {
      if (mediaRecorder && mediaRecorder.state !== "inactive") stopRecording();
    });

    combinedStream = buildCombinedStream(displayStream, micStream);

    let startResp;
    try {
      const resp = await fetch("/api/recording/start", { method: "POST" });
      startResp = await resp.json();
      if (!resp.ok || !startResp.success) {
        throw new Error(startResp.error || `HTTP ${resp.status}`);
      }
    } catch (err) {
      showError(`Could not start a Google Drive upload session: ${err.message}`);
      cleanupStreams();
      return;
    }

    recordingId = startResp.recording_id;
    clientChunkNumber = 0;
    chunksUploadedCount = 0;
    uploadQueue = [];
    updateQueueUI();

    const mimeType = pickSupportedMimeType();
    const options = mimeType ? { mimeType, videoBitsPerSecond: 2_500_000 } : { videoBitsPerSecond: 2_500_000 };

    try {
      mediaRecorder = new MediaRecorder(combinedStream, options);
    } catch (err) {
      showError(`Could not create MediaRecorder: ${err.message}`);
      cleanupStreams();
      return;
    }

    mediaRecorder.addEventListener("dataavailable", (event) => {
      if (event.data && event.data.size > 0) {
        enqueueChunk(event.data, clientChunkNumber);
        clientChunkNumber += 1;
      }
    });

    mediaRecorder.addEventListener("error", (event) => {
      showError(`Recording error: ${event.error ? event.error.message : "unknown error"}`);
    });

    mediaRecorder.start(CHUNK_INTERVAL_MS);

    setStatus("recording", "RECORDING");
    startTimer();
    if (startBtn) startBtn.disabled = true;
    if (pauseBtn) {
      pauseBtn.disabled = false;
      pauseBtn.textContent = "Pause";
    }
    if (stopBtn) stopBtn.disabled = false;
    setHidden(uploadBarTrack, false);
    setHidden(driveLink, true);
    setHidden(drivePending, false);
    if (drivePending) drivePending.textContent = "Not uploaded yet";
    setHidden(driveProcessing, true);
    if (uploadStatusEl) uploadStatusEl.textContent = "Recording — uploading chunks as they're created";
  }

  function togglePauseRecording() {
    if (!mediaRecorder || mediaRecorder.state === "inactive") return;

    if (mediaRecorder.state === "recording") {
      flushRecorderData();
      mediaRecorder.pause();
      setStatus("paused", "PAUSED");
      if (pauseBtn) pauseBtn.textContent = "Resume";
      pauseTimer();
      if (uploadStatusEl) uploadStatusEl.textContent = "Recording paused.";
      return;
    }

    if (mediaRecorder.state === "paused") {
      mediaRecorder.resume();
      setStatus("recording", "RECORDING");
      if (pauseBtn) pauseBtn.textContent = "Pause";
      resumeTimer();
      if (uploadStatusEl) uploadStatusEl.textContent = "Recording — uploading chunks as they're created";
    }
  }

  async function stopRecording() {
    if (!mediaRecorder || mediaRecorder.state === "inactive") return;

    if (pauseBtn) {
      pauseBtn.disabled = true;
      pauseBtn.textContent = "Pause";
    }
    if (stopBtn) stopBtn.disabled = true;
    setStatus("uploading", "FINALIZING");

    flushRecorderData();

    const stopped = new Promise((resolve) => {
      mediaRecorder.addEventListener("stop", resolve, { once: true });
    });
    mediaRecorder.stop();
    await stopped;

    stopTimer();
    cleanupStreams();

    if (uploadStatusEl) uploadStatusEl.textContent = "Waiting for all chunks to finish uploading...";
    await waitForQueueToDrain();

    if (uploadStatusEl) uploadStatusEl.textContent = "Finalizing Google Drive file...";
    try {
      const resp = await fetch("/api/recording/stop", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ recording_id: recordingId }),
      });
      const payload = await resp.json();
      if (!resp.ok || !payload.success) {
        throw new Error(payload.error || `HTTP ${resp.status}`);
      }

      setStatus("done", "DONE");
      if (uploadStatusEl) uploadStatusEl.textContent = "Upload complete.";
      setHidden(drivePending, true);
      setHidden(driveProcessing, false);
      setHidden(driveLink, true);
      if (driveLink) {
        driveLink.href = payload.file_url;
        driveLink.textContent = "Open Recording";
      }
      scheduleDriveLinkReveal(payload.file_url);
    } catch (err) {
      setStatus("error", "ERROR");
      showError(`Could not finalize the Google Drive upload: ${err.message}`);
    } finally {
      if (startBtn) startBtn.disabled = false;
      if (pauseBtn) pauseBtn.disabled = true;
    }
  }

  function cleanupStreams() {
    if (driveLinkRevealTimer) {
      clearTimeout(driveLinkRevealTimer);
      driveLinkRevealTimer = null;
    }
    [displayStream, micStream].forEach((stream) => {
      if (stream) stream.getTracks().forEach((track) => track.stop());
    });
    setHidden(driveProcessing, true);
    setHidden(driveLink, true);
    displayStream = null;
    micStream = null;
    combinedStream = null;
    if (audioCtx) {
      audioCtx.close();
      audioCtx = null;
    }
  }

  // -------------------------------------------------------------------
  // Wire up buttons
  // -------------------------------------------------------------------
  if (startBtn) startBtn.addEventListener("click", startRecording);
  if (pauseBtn) pauseBtn.addEventListener("click", togglePauseRecording);
  if (stopBtn) stopBtn.addEventListener("click", stopRecording);

  if (!browserSupportsRecording()) {
    showError("Your browser does not support MediaRecorder. Please use a recent version of Chrome or Edge.");
    if (startBtn) startBtn.disabled = true;
  }
})();
