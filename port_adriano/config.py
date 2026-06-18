from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

WEBCAM_PAGE = "https://www.webcamtaxi.com/en/spain/balearic-islands/port-adriano-cam.html"
YOUTUBE_URL = "https://www.youtube.com/watch?v=AeFSWxScJ8c"

LOCATION = "Port Adriano"

CHANNELS = 3
TARGET_FPS = 15.0
PIPELINE_FPS = 15
DISPLAY_WIDTH = 854
WINDOW = "Port Adriano Ship Detection"

MIN_BUFFER_FRAMES = 60
MAX_BUFFER_FRAMES = 120

# Zones in 854px display 
# inner
POLY_INNER = [
    (0, 280), (0, 480), (675, 480), (630, 295), (853, 255),
    (631, 159), (447, 96), (200, 96), (197, 264),
]
# mouth
POLY_MOUTH = [(631, 295), (676, 480), (853, 480), (853, 267)]

ZONE_INNER_REFS = [(320, 300)]
ZONE_MOUTH_REFS = [(740, 380)]

ZONE_FILL = {
    "sea": (180, 100, 0),
    "mouth": (0, 200, 255),
    "inner": (0, 160, 0),
}
ZONE_ALPHA = 0.30

MODEL = str(ROOT / "models" / "yolo11s.pt")
CONF = 0.35
SLICE_SIZE = 256
OVERLAP_RATIO = 0.5
TARGET_CLASSES = [8]
