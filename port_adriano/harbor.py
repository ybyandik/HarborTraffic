import sys
import cv2
import numpy as np

from .config import POLY_INNER, POLY_MOUTH, ZONE_INNER_REFS, ZONE_MOUTH_REFS

class MouthInnerHarborCounter:

    ZONE_SEA = 0
    ZONE_MOUTH = 1
    ZONE_INNER = 2
    ZONE_UNKNOWN = -1
    _ZONE_NAMES = ("sea", "mouth", "inner", "unknown")

    _PHASE_APPROACHING = "approaching"
    _PHASE_INSIDE = "inside"
    _PHASE_LEAVING = "leaving"

    def __init__(self, poly_mouth, poly_inner, *, mouth_refs, inner_refs):
        self.poly_mouth = np.array(poly_mouth, dtype=np.int32)
        self.poly_inner = np.array(poly_inner, dtype=np.int32)
        self.poly_sea = None
        self.enter = 0
        self.exit = 0
        self._zone = {}
        self._phase = {}
        self._pending_exit = set()
        self._validate(mouth_refs, inner_refs)

    def _in_poly(self, poly, x, y):
        return cv2.pointPolygonTest(poly, (float(x), float(y)), False) >= 0

    def _validate(self, mouth_refs, inner_refs):
        ok = True
        for p in mouth_refs:
            z = self._classify(float(p[0]), float(p[1]))
            if z != self.ZONE_MOUTH:
                print(
                    f"calib {p} expected mouth, got {self._ZONE_NAMES[z]}", file=sys.stderr)
                ok = False
        for p in inner_refs:
            z = self._classify(float(p[0]), float(p[1]))
            if z != self.ZONE_INNER:
                print(
                    f"calib {p} expected inner, got {self._ZONE_NAMES[z]}",file=sys.stderr)
                ok = False
        if ok:
            print("Harbor polygon calibration OK", file=sys.stderr)

    def _classify(self, x, y):
        if self._in_poly(self.poly_mouth, x, y):
            return self.ZONE_MOUTH
        if self._in_poly(self.poly_inner, x, y):
            return self.ZONE_INNER
        return self.ZONE_UNKNOWN

    def _init_phase(self, tid, zone):
        if zone == self.ZONE_MOUTH:
            self._phase[tid] = self._PHASE_APPROACHING
        elif zone == self.ZONE_INNER:
            self._phase[tid] = self._PHASE_INSIDE

    def _on_zone_change(self, tid, prev, zone):
        events = []
        phase = self._phase.get(tid)

        if prev == self.ZONE_MOUTH and zone == self.ZONE_INNER:
            if phase == self._PHASE_APPROACHING:
                self.enter += 1
                self._phase[tid] = self._PHASE_INSIDE
                self._pending_exit.discard(tid)
                events.append({"event": "enter", "track_id": tid, "from": prev, "to": zone})
            elif phase == self._PHASE_LEAVING:
                self._phase[tid] = self._PHASE_INSIDE
                self._pending_exit.discard(tid)

        elif prev == self.ZONE_INNER and zone == self.ZONE_MOUTH:
            if phase == self._PHASE_INSIDE:
                self._phase[tid] = self._PHASE_LEAVING
                self._pending_exit.add(tid)

        return events

    def update(self, tracks):
        seen = set()
        events = []

        for t in tracks:
            tid = int(t["id"])
            fx, fy = float(t["foot_x"]), float(t["foot_y"])
            seen.add(tid)

            zone = self._classify(fx, fy)
            if zone == self.ZONE_UNKNOWN:
                continue

            if tid not in self._phase:
                self._init_phase(tid, zone)

            prev = self._zone.get(tid)
            if prev is not None and prev != zone:
                events.extend(self._on_zone_change(tid, prev, zone))

            self._zone[tid] = zone

        for tid in list(self._zone):
            if tid not in seen:
                if tid in self._pending_exit:
                    self.exit += 1
                    events.append({
                        "event": "exit",
                        "track_id": tid,
                        "from": self.ZONE_INNER,
                        "to": self.ZONE_MOUTH,
                    })
                self._zone.pop(tid, None)
                self._phase.pop(tid, None)
                self._pending_exit.discard(tid)

        return events

def make_gate():
    return MouthInnerHarborCounter(
        POLY_MOUTH,
        POLY_INNER,
        mouth_refs=ZONE_MOUTH_REFS,
        inner_refs=ZONE_INNER_REFS,
    )
