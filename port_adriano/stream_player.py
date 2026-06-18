import os
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

import numpy as np

from .config import MAX_BUFFER_FRAMES, MIN_BUFFER_FRAMES, PIPELINE_FPS
from .stream import resolve_stream, stream_frame_size, ytdlp_pipe_cmd

_gst_ready = False
# yt-dlp pipe handles cookies and segment auth; direct HLS often without it
_USE_YTDLP_PIPE = os.environ.get("STREAM_BACKEND", "ytdlp-pipe").strip().lower() != "gstreamer-hls"

def _ensure_gi():
    global _gst_ready
    if _gst_ready:
        return
    if "gi" not in sys.modules:
        for path in ("/usr/lib/python3/dist-packages", "/usr/local/lib/python3/dist-packages"):
            if Path(path).is_dir() and path not in sys.path:
                sys.path.insert(0, path)
    import gi
    gi.require_version("Gst", "1.0")
    from gi.repository import Gst
    Gst.init(None)
    _gst_ready = True

def _headers_to_gst_structure(gst, headers):
    if not headers:
        return None
    structure = gst.Structure.new_empty("http-extra-headers")
    for key, value in headers.items():
        structure.set_value(key, value)
    return structure

def _make_element(gst, factory, name):
    elem = gst.ElementFactory.make(factory, name)
    if elem is None:
        raise RuntimeError(f"GStreamer element not found: {factory}")
    return elem

def _build_output_chain(gst, pipeline):
    fps = int(PIPELINE_FPS)
    queue = _make_element(gst, "queue", "queue")
    convert = _make_element(gst, "videoconvert", "convert")
    rate = _make_element(gst, "videorate", "rate")
    rate.set_property("drop-only", True)
    capsfilter = _make_element(gst, "capsfilter", "caps")
    capsfilter.set_property(
        "caps",
        gst.Caps.from_string(f"video/x-raw,format=BGR,framerate={fps}/1"),
    )
    sink = _make_element(gst, "appsink", "sink")
    sink.set_property("emit-signals", False)
    sink.set_property("sync", False)
    sink.set_property("max-buffers", 32)
    sink.set_property("drop", False)

    for elem in (queue, convert, rate, capsfilter, sink):
        pipeline.add(elem)
    queue.link(convert)
    convert.link(rate)
    rate.link(capsfilter)
    capsfilter.link(sink)
    return queue

def _build_hls_pipeline(url, headers=None):
    from gi.repository import Gst

    pipeline = Gst.Pipeline.new("port-adriano-hls")
    src = _make_element(Gst, "souphttpsrc", "src")
    src.set_property("location", url)
    src.set_property("timeout", 0)
    extra = _headers_to_gst_structure(Gst, headers or {})
    if extra is not None:
        src.set_property("extra-headers", extra)

    queue = _build_output_chain(Gst, pipeline)
    is_hls = ".m3u8" in url or "manifest" in url or "/hls" in url.lower()

    if is_hls:
        src.set_property("is-live", True)
        demux = _make_element(Gst, "hlsdemux", "demux")
        decode = _make_element(Gst, "decodebin", "decode")
        for elem in (src, demux, decode):
            pipeline.add(elem)
        src.link(demux)
        demux.connect("pad-added", _on_hls_pad_added, decode)
        decode.connect("pad-added", _on_video_pad_added, queue)
    else:
        decode = _make_element(Gst, "decodebin", "decode")
        pipeline.add(src)
        pipeline.add(decode)
        src.link(decode)
        decode.connect("pad-added", _on_video_pad_added, queue)

    return pipeline

def _on_hls_pad_added(demux, pad, decode):
    caps = pad.get_current_caps() or pad.query_caps(None)
    if caps is None or not caps.to_string().startswith("video/"):
        return
    sink_pad = decode.get_static_pad("sink")
    if sink_pad and not sink_pad.is_linked():
        pad.link(sink_pad)

def _on_video_pad_added(element, pad, queue):
    caps = pad.get_current_caps() or pad.query_caps(None)
    if caps is None or not caps.to_string().startswith("video/"):
        return
    sink_pad = queue.get_static_pad("sink")
    if sink_pad and not sink_pad.is_linked():
        pad.link(sink_pad)

def _sample_to_frame(sample):
    from gi.repository import Gst
    buf = sample.get_buffer()
    caps = sample.get_caps().get_structure(0)
    width = caps.get_value("width")
    height = caps.get_value("height")
    ok, map_info = buf.map(Gst.MapFlags.READ)
    if not ok:
        return None
    try:
        if map_info.size < width * height * 3:
            return None
        frame = np.ndarray((height, width, 3), dtype=np.uint8, buffer=map_info.data)
        return frame.copy()
    finally:
        buf.unmap(map_info)

def _drain_stderr(proc, label):
    try:
        for line in proc.stderr:
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                print(f"[{label}] {text}", file=sys.stderr)
    except Exception:
        pass


class BufferedStreamPlayer:
    def __init__(self):
        self.buffer = deque(maxlen=MAX_BUFFER_FRAMES)
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.stop_event = threading.Event()
        self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.pipeline = None
        self._proc = None
        self._ytdlp_proc = None
        self.restart_count = 0
        self.total_frames = 0
        self.native_size = None

    def start(self):
        self.reader_thread.start()

    def stop(self):
        self.stop_event.set()
        self._stop_pipeline()
        self._stop_proc()

    def buffer_size(self):
        with self.lock:
            return len(self.buffer)

    def _stop_proc(self):
        for proc in (self._proc, self._ytdlp_proc):
            if proc is None:
                continue
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self._proc = None
        self._ytdlp_proc = None

    def _stop_pipeline(self):
        if self.pipeline is not None:
            try:
                from gi.repository import Gst
                self.pipeline.set_state(Gst.State.NULL)
            except Exception:
                pass
            self.pipeline = None

    def _reader_loop(self):
        _ensure_gi()
        from gi.repository import Gst

        while not self.stop_event.is_set():
            self.restart_count += 1
            backend = "yt-dlp pipe" if _USE_YTDLP_PIPE else "GStreamer HLS"
            print(
                f"{backend} started. Restart: {self.restart_count}",
                file=sys.stderr,
            )

            try:
                if _USE_YTDLP_PIPE:
                    self._run_ytdlp_pipe(Gst)
                else:
                    self._run_hls(Gst)
            except Exception as exc:
                print(f"Reader exception on yt-dlp pipe: {exc}", file=sys.stderr)
            finally:
                self._stop_pipeline()
                self._stop_proc()
                time.sleep(1.0)

    def _run_ytdlp_pipe(self, Gst):
        width, height = stream_frame_size()
        frame_bytes = width * height * 3
        if self.native_size is None:
            print(f"Stream resolution: {width}x{height}", file=sys.stderr)

        self._ytdlp_proc = subprocess.Popen(
            ytdlp_pipe_cmd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self._proc = subprocess.Popen(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                "pipe:0",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "bgr24",
                "-",
            ],
            stdin=self._ytdlp_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=frame_bytes * 4,
        )
        if self._ytdlp_proc.stdout is not None:
            self._ytdlp_proc.stdout.close()

        for proc, label in ((self._ytdlp_proc, "yt-dlp"), (self._proc, "ffmpeg")):
            threading.Thread(target=_drain_stderr, args=(proc, label), daemon=True).start()

        stdout = self._proc.stdout
        while not self.stop_event.is_set():
            if self._proc.poll() is not None or self._ytdlp_proc.poll() is not None:
                code = self._proc.returncode or self._ytdlp_proc.returncode
                print(f"[WARN] stream process exited ({code})", file=sys.stderr)
                break

            chunk = stdout.read(frame_bytes)
            if len(chunk) != frame_bytes:
                break

            frame = np.frombuffer(chunk, dtype=np.uint8).reshape(height, width, 3).copy()
            if self.native_size is None:
                self.native_size = (width, height)
                print(
                    f"Native stream {width}x{height} @ {PIPELINE_FPS}fps",
                    file=sys.stderr,
                )

            with self.cond:
                self.buffer.append(frame)
                self.total_frames += 1
                self.cond.notify_all()

    def _run_hls(self, Gst):
        spec = resolve_stream()
        if not spec:
            time.sleep(5.0)
            return
        try:
            self.pipeline = _build_hls_pipeline(spec["url"], spec.get("headers"))
        except Exception as exc:
            print(f"Pipeline build failed: {exc}", file=sys.stderr)
            return

        appsink = self.pipeline.get_by_name("sink")
        self.pipeline.set_state(Gst.State.PLAYING)
        self._pull_frames(Gst, appsink)

    def _pull_frames(self, Gst, appsink):
        while not self.stop_event.is_set():
            sample = appsink.emit("try-pull-sample", 500 * Gst.MSECOND)
            if sample is None:
                msg = self.pipeline.get_bus().pop_filtered(
                    Gst.MessageType.ERROR | Gst.MessageType.EOS,
                )
                if msg is not None:
                    if msg.type == Gst.MessageType.ERROR:
                        err, _ = msg.parse_error()
                        print(f"GStreamer error: {err}", file=sys.stderr)
                    else:
                        print("End of stream.", file=sys.stderr)
                    break
                continue

            frame = _sample_to_frame(sample)
            if frame is None:
                continue

            h, w = frame.shape[:2]
            if self.native_size is None:
                self.native_size = (w, h)
                print(
                    f"Native stream {w}x{h} @ {PIPELINE_FPS}fps",
                    file=sys.stderr,
                )

            with self.cond:
                self.buffer.append(frame)
                self.total_frames += 1
                self.cond.notify_all()

    def wait_for_initial_buffer(self):
        print(f"Buffer filling (target {MIN_BUFFER_FRAMES} frames)...", file=sys.stderr)
        while not self.stop_event.is_set():
            with self.lock:
                n = len(self.buffer)
            if n >= MIN_BUFFER_FRAMES:
                print(f"Buffer ready ({n} frames).", file=sys.stderr)
                return
            print(f"\rBuffer: {n}/{MIN_BUFFER_FRAMES}", end="", file=sys.stderr)
            time.sleep(0.1)
        print(file=sys.stderr)

    def get_next_frame(self):
        with self.lock:
            if not self.buffer:
                return None
            return self.buffer.popleft()
