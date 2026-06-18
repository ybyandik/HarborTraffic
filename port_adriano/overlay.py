import sys
import cv2

from .config import DISPLAY_WIDTH, ZONE_ALPHA, ZONE_FILL
from .harbor import make_gate

def scale_for_display(frame, dets, display_width=DISPLAY_WIDTH):
    if frame is None:
        raise ValueError("frame is None")
    h, w = frame.shape[:2]
    if not display_width or w <= display_width:
        return frame, dets
    scale = display_width / w
    out = cv2.resize(frame, (display_width, int(h * scale)))
    scaled = []
    for d in dets:
        x1, y1, x2, y2 = d["box"]
        scaled.append({
            **d,
            "box": (
                int(x1 * scale), int(y1 * scale),
                int(x2 * scale), int(y2 * scale),
            ),
            "foot_x": d.get("foot_x", (x1 + x2) / 2) * scale,
            "foot_y": d.get("foot_y", y2) * scale,
        })
    return out, scaled


def draw_zone_polygons(frame, gate, *, show_sea=False):
    out = frame.copy()
    overlay = out.copy()
    zones = [
        ("mouth", gate.poly_mouth, ZONE_FILL["mouth"]),
        ("inner", gate.poly_inner, ZONE_FILL["inner"]),
    ]
    if show_sea:
        zones.insert(0, ("sea", gate.poly_sea, ZONE_FILL["sea"]))
    for _, poly, color in zones:
        cv2.fillPoly(overlay, [poly], color)
    cv2.addWeighted(overlay, ZONE_ALPHA, out, 1.0 - ZONE_ALPHA, 0, out)
    for name, poly, color in zones:
        cv2.polylines(out, [poly], isClosed=True, color=color, thickness=2)
        cx = int(poly[:, 0].mean())
        cy = int(poly[:, 1].mean())
        cv2.putText(out, name, (cx - 20, cy),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, color, 2, cv2.LINE_AA)
    return out


def draw_overlay(frame, dets, *, enter=0, exit_=0):
    out = frame.copy()
    for d in dets:
        x1, y1, x2, y2 = d["box"]
        color = (255, 200, 0) if d.get("track_id") is not None else (0, 255, 0)
        cv2.rectangle(out, (x1, y1), (x2, y2), color, 2)
        label = d.get("label", "boat")
        if d.get("track_id") is not None:
            label = f"#{d['track_id']} {label}"
        label += f" {d['score']:.2f}"
        cv2.putText(out, label, (x1, max(0, y1 - 5)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.45, color, 1, cv2.LINE_AA)
    cv2.putText(out, f"in: {enter}  out: {exit_}", (10, 28),
                cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2, cv2.LINE_AA)
    return out


def preview_zones(image_path, output_path="data/port_adriano_zones_preview.jpg"):
    frame = cv2.imread(image_path)
    if frame is None:
        print(f"Görüntü okunamadı: {image_path}", file=sys.stderr)
        return 1
    gate = make_gate()
    show, _ = scale_for_display(frame, [])
    out = draw_zone_polygons(show, gate)
    cv2.imwrite(output_path, out)
    print(f"Kaydedildi: {output_path} ({out.shape[1]}x{out.shape[0]})", file=sys.stderr)
    return 0
