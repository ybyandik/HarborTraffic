from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config" / "config.yaml"
HARBOR_DIR = ROOT / "config" / "harbors"

DEFAULT_HARBOR = "port_adriano"
WEATHER_HARBORS = {"port_adriano": HARBOR_DIR / "port_adriano.yaml"}


def _deep_merge(base, overlay):
    out = dict(base)
    for key, value in overlay.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out

def load_base_config():
    with open(DEFAULT_CONFIG, encoding="utf-8") as f:
        return yaml.safe_load(f)

def default_harbor_id():
    return load_base_config().get("default_harbor", DEFAULT_HARBOR)

def load_harbor_config(harbor_id=None, config_path=None):
    cfg = load_base_config()

    if config_path:
        overlay_path = Path(config_path)
    else:
        hid = harbor_id or cfg.get("default_harbor", DEFAULT_HARBOR)
        overlay_path = WEATHER_HARBORS.get(hid)
        if overlay_path is None:
            raise ValueError(f"Unknown harbor: {hid!r}")

    with open(overlay_path, encoding="utf-8") as f:
        overlay = yaml.safe_load(f)
    return _deep_merge(cfg, overlay)

def harbor_location_name(harbor_id):
    return load_harbor_config(harbor_id=harbor_id)["location"]["name"]
