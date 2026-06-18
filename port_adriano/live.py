import argparse
import sys
import time

import cv2

from image_processing.ship_detector import ShipDetector
from ingestion.ship_events import flush_ship_events, publish_ship_event

from .config import (
    DISPLAY_WIDTH,
    LOCATION,
    MIN_BUFFER_FRAMES,
    TARGET_FPS,
    WEBCAM_PAGE,
    WINDOW,
    YOUTUBE_URL,
)
from .harbor import MouthInnerHarborCounter, make_gate
from .overlay import draw_overlay, preview_zones, scale_for_display
from .stream_player import BufferedStreamPlayer


def run_live():
    print(f"Webcam page: {WEBCAM_PAGE}", file=sys.stderr)
    print(f"YouTube live: {YOUTUBE_URL}", file=sys.stderr)
    print("Stream backend: yt-dlp pipe to GStreamer", file=sys.stderr)
    print(f"Playback {TARGET_FPS} fps. Now, buffer pre-roll {MIN_BUFFER_FRAMES} frames", file=sys.stderr)

    player = BufferedStreamPlayer()
    detector = ShipDetector()
    gate = make_gate()

    player.start()
    detector.start()
    player.wait_for_initial_buffer()

    last_frame = None
    frame_interval = 1.0 / TARGET_FPS
    next_frame_time = time.perf_counter()

    print("Detecting ships. 'd' to toggle, 'q' to quit.", file=sys.stderr)

    try:
        while True:
            now = time.perf_counter()
            if now < next_frame_time:
                time.sleep(next_frame_time - now)
                now = time.perf_counter()
            next_frame_time = now + frame_interval

            frame = player.get_next_frame()
            if frame is not None:
                last_frame = frame
                detector.submit(frame)

            if last_frame is not None:
                dets, _, _ = detector.snapshot()
                show_frame, show_dets = scale_for_display(last_frame, dets)
                tracks = [
                    {"id": d["track_id"], "foot_x": d["foot_x"], "foot_y": d["foot_y"]}
                    for d in show_dets
                    if d.get("track_id") is not None
                ]
                for ev in gate.update(tracks):
                    zf = MouthInnerHarborCounter._ZONE_NAMES[ev["from"]]
                    zt = MouthInnerHarborCounter._ZONE_NAMES[ev["to"]]
                    publish_ship_event(
                        ev["event"],
                        ev["track_id"],
                        gate.enter,
                        gate.exit,
                        zf,
                        zt,
                        location=LOCATION,
                        harbor_id="port_adriano",
                    )
                cv2.imshow(WINDOW, draw_overlay(show_frame, show_dets, enter=gate.enter, exit_=gate.exit))

            key = cv2.waitKey(1) & 0xFF
            if key == ord("d"):
                on = detector.toggle()
                print(f"Detection {'ON' if on else 'OFF'}", file=sys.stderr)
            elif key == ord("q") or key == 27:
                break
    finally:
        detector.stop()
        player.stop()
        flush_ship_events()
        cv2.destroyAllWindows()

    return 0

def main():
    parser = argparse.ArgumentParser(description="ship detection")
    parser.add_argument(
        "--preview-zones",
        metavar="IMAGE",
        help="Draw zone polygons",
    )
    parser.add_argument(
        "-o", "--output",
        default="data/port_adriano_zones_preview.jpg",
        help=".",
    )
    args = parser.parse_args()

    if args.preview_zones:
        return preview_zones(args.preview_zones, args.output)
    return run_live()


if __name__ == "__main__":
    raise SystemExit(main())
