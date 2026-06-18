import argparse
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, export_text

from spark.config import spark_settings

# Small-craft thresholds
MAX_WAVE_HEIGHT_M = 0.60
MAX_WIND_SPEED_MS = 10.0
MAX_WIND_GUSTS_MS = 15.0
MIN_VISIBILITY_M = 5000.0
MAX_PRECIPITATION_MM = 0.1
# Open-Meteo WMO codes: thunderstorm, freezing rain, heavy snow, dense fog.
SEVERE_WEATHER_CODES = {45, 48, 56, 57, 66, 67, 71, 73, 75, 77, 82, 85, 86, 95, 96, 99}

FEATURE_COLUMNS = [
    "wave_height",
    "wind_speed_10m",
    "wind_gusts_10m",
    "visibility",
    "precipitation",
    "temperature_2m",
    "weather_code",
    "is_day",
]

MODEL_FILENAME = "weather_suitability_tree.joblib"


def rule_based_suitable(row):
    if row["wave_height"] > MAX_WAVE_HEIGHT_M:
        return False
    if row["wind_speed_10m"] > MAX_WIND_SPEED_MS:
        return False
    if row["wind_gusts_10m"] > MAX_WIND_GUSTS_MS:
        return False
    if row["visibility"] < MIN_VISIBILITY_M:
        return False
    if row["precipitation"] > MAX_PRECIPITATION_MM:
        return False
    if int(row["weather_code"]) in SEVERE_WEATHER_CODES:
        return False
    return True


def load_weather_hourly(parquet_dir):
    path = parquet_dir / "weather"
    if not path.exists():
        raise FileNotFoundError(f"Weather Parquet not found: {path}")

    df = pd.read_parquet(path)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["hour"] = df["ts"].dt.floor("h")
    df = df.sort_values("ts").drop_duplicates(["hour", "location"], keep="last")
    df["is_day"] = (df["day_or_night"] == "day").astype(int)
    df["rule_suitable"] = df.apply(rule_based_suitable, axis=1).astype(int)
    return df.reset_index(drop=True)


def load_ship_events(parquet_dir):
    path = parquet_dir / "ship_weather"
    if not path.exists():
        path = parquet_dir / "ships"
    if not path.exists():
        raise FileNotFoundError(f"Ship Parquet not found under {parquet_dir}")

    df = pd.read_parquet(path)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["hour"] = df["ts"].dt.floor("h")
    return df


def prepare_features(df):
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")
    return df[FEATURE_COLUMNS].astype(float)


def train_model(weather, max_depth=4, test_size=0.25, random_state=42):
    X = prepare_features(weather)
    y = weather["rule_suitable"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y if len(np.unique(y)) > 1 else None,
    )

    clf = DecisionTreeClassifier(max_depth=max_depth, random_state=random_state)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
        "classification_report": classification_report(y_test, y_pred, zero_division=0),
        "feature_importances": dict(zip(FEATURE_COLUMNS, clf.feature_importances_.round(4))),
        "tree_rules": export_text(clf, feature_names=FEATURE_COLUMNS),
        "train_size": len(X_train),
        "test_size": len(X_test),
    }
    return clf, metrics


def predict_weather(clf, weather):
    X = prepare_features(weather)
    proba = clf.predict_proba(X)[:, 1]
    pred = clf.predict(X).astype(int)
    out = weather.copy()
    out["predicted_suitable"] = pred
    out["suitability_prob"] = proba.round(4)
    return out


def hourly_departures(ships):
    exits = ships[ships["event"] == "exit"]
    enters = ships[ships["event"] == "enter"]
    dep = exits.groupby(["hour", "location"], as_index=False).size().rename(columns={"size": "departures"})
    arr = enters.groupby(["hour", "location"], as_index=False).size().rename(columns={"size": "arrivals"})
    traffic = dep.merge(arr, on=["hour", "location"], how="outer").fillna(0)
    traffic["departures"] = traffic["departures"].astype(int)
    traffic["arrivals"] = traffic["arrivals"].astype(int)
    return traffic


def analyze_departures_in_suitable_weather(weather_pred, ships):
    traffic = hourly_departures(ships)
    merged = weather_pred.merge(traffic, on=["hour", "location"], how="left")
    merged["departures"] = merged["departures"].fillna(0).astype(int)
    merged["arrivals"] = merged["arrivals"].fillna(0).astype(int)
    return merged


def departing_ships_detail(weather_pred, ships):
    suitable_hours = weather_pred.loc[
        weather_pred["predicted_suitable"] == 1, ["hour", "location"]
    ]
    exits = ships[ships["event"] == "exit"].merge(suitable_hours, on=["hour", "location"], how="inner")
    cols = [
        "ts", "location", "track_id", "zone_from", "zone_to",
        "wave_height", "wind_speed_10m", "wind_gusts_10m", "visibility",
        "precipitation", "weather_code", "day_or_night",
    ]
    present = [c for c in cols if c in exits.columns]
    return exits[present].sort_values("ts").reset_index(drop=True)


def run(harbor_id=None, max_depth=4, save=True):
    cfg = spark_settings(harbor_id=harbor_id)
    parquet_dir = cfg["parquet_dir"]
    models_dir = Path(__file__).resolve().parent / "models"
    models_dir.mkdir(exist_ok=True)

    weather = load_weather_hourly(parquet_dir)
    ships = load_ship_events(parquet_dir)

    clf, metrics = train_model(weather, max_depth=max_depth)
    weather_pred = predict_weather(clf, weather)
    hourly = analyze_departures_in_suitable_weather(weather_pred, ships)
    departures = departing_ships_detail(weather_pred, ships)

    if save:
        out_dir = parquet_dir / "weather_suitability"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_cols = [
            "hour", "location", "wave_height", "wind_speed_10m", "wind_gusts_10m",
            "visibility", "precipitation", "temperature_2m", "weather_code", "day_or_night",
            "rule_suitable", "predicted_suitable", "suitability_prob",
            "departures", "arrivals",
        ]
        hourly[out_cols].to_parquet(out_dir / "hourly_suitability.parquet", index=False)
        if len(departures):
            departures.to_parquet(out_dir / "departures_suitable_weather.parquet", index=False)

        model_path = models_dir / MODEL_FILENAME
        joblib.dump({"model": clf, "features": FEATURE_COLUMNS, "thresholds": {
            "max_wave_height_m": MAX_WAVE_HEIGHT_M,
            "max_wind_speed_ms": MAX_WIND_SPEED_MS,
            "max_wind_gusts_ms": MAX_WIND_GUSTS_MS,
            "min_visibility_m": MIN_VISIBILITY_M,
        }}, model_path)
        print(
            f"accuracy={metrics['accuracy']:.2%} | "
            f"suitable hours={int(weather_pred['predicted_suitable'].sum())}/{len(weather_pred)} | "
            f"saved to {out_dir} and {model_path}"
        )

    return {
        "weather_pred": weather_pred,
        "hourly": hourly,
        "departures": departures,
        "metrics": metrics,
        "model": clf,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Train decision tree for maritime weather suitability on Parquet data",
    )
    parser.add_argument("--harbor", default=None, help="Harbor id (default: from config)")
    parser.add_argument("--max-depth", type=int, default=4, help="Decision tree max depth")
    parser.add_argument("--no-save", action="store_true", help="Skip writing Parquet + model")
    args = parser.parse_args(argv)
    run(harbor_id=args.harbor, max_depth=args.max_depth, save=not args.no_save)


if __name__ == "__main__":
    main()
