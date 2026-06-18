import os
import sys
from pathlib import Path

from .config import YOUTUBE_URL

DEFAULT_FORMAT = os.environ.get("YTDLP_FORMAT", "94")

def _cookie_file():
    path = os.environ.get("YTDLP_COOKIES_FILE") or os.environ.get("YTDLP_COOKIES")
    if path and Path(path).is_file():
        return path
    return None

def _cookie_browser():
    return os.environ.get("YTDLP_COOKIES_BROWSER", "").strip()

def _uses_cookies():
    return bool(_cookie_file() or _cookie_browser())

def _ytdlp_opts():
    opts = {
        "format": DEFAULT_FORMAT,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
    }
    cookie = _cookie_file()
    if cookie:
        opts["cookiefile"] = cookie
    elif browser := _cookie_browser():
        parts = browser.split(":", 1)
        opts["cookiesfrombrowser"] = (parts[0],) if len(parts) == 1 else (parts[0], parts[1])
    else:
        opts["extractor_args"] = {"youtube": {"player_client": ["android_vr"]}}
    return opts


def ytdlp_pipe_cmd():
    override = os.environ.get("STREAM_URL", "").strip()
    if override:
        raise ValueError("STREAM_URL override cannot be piped; use a YouTube URL")

    cmd = [
        sys.executable,
        "-m",
        "yt_dlp",
        "-f",
        DEFAULT_FORMAT,
        "-o",
        "-",
        "--quiet",
        "--no-warnings",
        "--no-playlist",
        "--no-part",
        "--retries",
        "infinite",
        "--fragment-retries",
        "infinite",
    ]
    if cookie := _cookie_file():
        cmd += ["--cookies", cookie]
    elif browser := _cookie_browser():
        cmd += ["--cookies-from-browser", browser]
    else:
        cmd += ["--extractor-args", "youtube:player_client=android_vr"]

    cmd.append(YOUTUBE_URL)
    return cmd

def _extract_info():
    import yt_dlp

    with yt_dlp.YoutubeDL(_ytdlp_opts()) as ydl:
        return ydl.extract_info(YOUTUBE_URL, download=False)

def resolve_stream():
    override = os.environ.get("STREAM_URL", "").strip()
    if override:
        return {"url": override, "headers": {}, "width": 854, "height": 480}

    try:
        info = _extract_info()
    except ImportError:
        print("yt-dlp not installed", file=sys.stderr)
        return None
    except Exception as exc:
        print(f"Stream resolve failed: {exc}", file=sys.stderr)
        _print_cookie_hint(exc)
        return None

    url = info.get("url")
    if not url:
        print("yt-dlp returned no playable URL", file=sys.stderr)
        return None

    headers = dict(info.get("http_headers") or {})
    headers.setdefault("Referer", YOUTUBE_URL)
    return {
        "url": url,
        "headers": headers,
        "width": int(info.get("width") or 854),
        "height": int(info.get("height") or 480),
    }

def stream_frame_size():
    spec = resolve_stream()
    if spec:
        return spec["width"], spec["height"]
    return 854, 480

def resolve_m3u8_url():
    spec = resolve_stream()
    return spec["url"] if spec else None

def _print_cookie_hint(exc):
    msg = str(exc).lower()
    triggers = ("sign in", "bot", "cookies", "403", "429", "challenge", "no video formats")
    if not any(token in msg for token in triggers):
        return
    print("YouTube blocked due to cookies")
