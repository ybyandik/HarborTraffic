import sys
import threading
import time

import numpy as np
import torch

from ultralytics.engine.results import Boxes

from sahi import AutoDetectionModel
from ultralytics.trackers.byte_tracker import BYTETracker
from ultralytics.utils import IterableSimpleNamespace, yaml_load
from ultralytics.utils.checks import check_yaml


from port_adriano.config import CONF, MODEL, OVERLAP_RATIO, SLICE_SIZE, TARGET_CLASSES, TARGET_FPS

DEVICE = 0 if torch.cuda.is_available() else "cpu"
_SAHI_DEVICE = "cuda:0" if DEVICE != "cpu" else "cpu"


def _build_boxes(dets, shape):
    h, w = shape[:2]
    if not dets:
        return Boxes(np.zeros((0, 6), dtype=np.float32), orig_shape=(h, w))
    rows = [
        [x1, y1, x2, y2, d["score"], TARGET_CLASSES[0]]
        for d in dets
        for x1, y1, x2, y2 in [d["box"]]
    ]
    return Boxes(np.array(rows, dtype=np.float32), orig_shape=(h, w))


def _parse_tracks(tracks):
    if tracks is None or len(tracks) == 0:
        return []
    out = []
    for t in tracks:
        x1, y1, x2, y2 = map(int, t[:4])
        out.append({
            "box": (x1, y1, x2, y2),
            "track_id": int(t[4]),
            "score": float(t[5]),
            "label": "boat",
            "foot_x": (x1 + x2) / 2,
            "foot_y": float(y2),
        })
    return out


class ShipDetector:
    def __init__(self):
        print(f"{MODEL} loading on {_SAHI_DEVICE}", file=sys.stderr)
        self._model = AutoDetectionModel.from_pretrained(
            model_type="ultralytics",
            model_path=MODEL,
            confidence_threshold=CONF,
            device=_SAHI_DEVICE,
        )
        tracker_cfg = IterableSimpleNamespace(**yaml_load(check_yaml("bytetrack.yaml")))
        self._tracker = BYTETracker(args=tracker_cfg, frame_rate=int(TARGET_FPS))
        print("ByteTrack ready.", file=sys.stderr)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._pending = None
        self._dets = []
        self._inference_ms = 0.0
        self._busy = False
        self._enabled = True
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    @property
    def enabled(self):
        return self._enabled

    def toggle(self):
        self._enabled = not self._enabled
        if not self._enabled:
            self._tracker.reset()
            with self._lock:
                self._dets = []
        return self._enabled

    def submit(self, frame):
        if not self._enabled:
            return
        with self._lock:
            self._pending = frame.copy()

    def snapshot(self):
        with self._lock:
            return list(self._dets), self._inference_ms, self._busy

    def _detect(self, frame):
        from sahi.predict import get_sliced_prediction

        res = get_sliced_prediction(
            frame,
            self._model,
            slice_height=SLICE_SIZE,
            slice_width=SLICE_SIZE,
            overlap_height_ratio=OVERLAP_RATIO,
            overlap_width_ratio=OVERLAP_RATIO,
            verbose=0,
        )
        dets = []
        for obj in res.object_prediction_list:
            if obj.category.id not in TARGET_CLASSES:
                continue
            if obj.score.value < CONF:
                continue
            bb = obj.bbox
            dets.append({
                "box": (int(bb.minx), int(bb.miny), int(bb.maxx), int(bb.maxy)),
                "score": obj.score.value,
                "label": obj.category.name,
            })
        return dets

    def _track(self, frame, dets):
        boxes = _build_boxes(dets, frame.shape)
        tracks = self._tracker.update(boxes.cpu().numpy(), frame)
        return _parse_tracks(tracks)

    def _loop(self):
        while not self._stop.is_set():
            with self._lock:
                frame = self._pending
                self._pending = None

            if frame is None:
                time.sleep(0.02)
                continue

            self._busy = True
            t0 = time.perf_counter()
            try:
                raw_dets = self._detect(frame)
                dets = self._track(frame, raw_dets)
            except Exception as exc:
                print(f"Image processing failed: {exc}", file=sys.stderr)
                dets = []
            elapsed_ms = (time.perf_counter() - t0) * 1000

            with self._lock:
                self._dets = dets
                self._inference_ms = elapsed_ms
                self._busy = False
