"""
Reconstruit clean/clean.csv a partir de tous les fichiers bruts dans raw/.
Ce script est idempotent : on peut le relancer autant de fois qu'on veut,
le fichier clean.csv est entierement recree a chaque execution.

Deduplication : meme ville + meme horodatage (heure) => une seule ligne.
"""
import os
import json
import glob
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parents[1]

load_dotenv(BASE_DIR / ".env", override=True)

RAW_DIR = Path(
    os.environ.get(
        "RAW_DIR",
        BASE_DIR / "raw"
    )
)

CLEAN_DIR = Path(
    os.environ.get(
        "CLEAN_DIR",
        BASE_DIR / "clean"
    )
)

CLEAN_FILE = CLEAN_DIR / "clean.csv"

# Composants de pollution renvoyes par OpenWeatherMap Air Pollution API
POLLUTANT_KEYS = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]


def parse_raw_file(filepath):
    """Un fichier raw peut contenir une seule mesure (extract courant)
    ou plusieurs mesures horaires (backfill history). On extrait toutes
    les lignes possibles."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    meta = data.get("_meta", {})
    ville = meta.get("ville")
    pays = meta.get("pays")
    lat = meta.get("lat")
    lon = meta.get("lon")

    rows = []
    for entry in data.get("list", []):
        dt_unix = entry.get("dt")
        if dt_unix is None:
            continue
        timestamp = datetime.fromtimestamp(dt_unix, tz=timezone.utc)

        components = entry.get("components", {})
        row = {
            "ville": ville,
            "pays": pays,
            "latitude": lat,
            "longitude": lon,
            "timestamp_utc": timestamp.isoformat(),
            "aqi": entry.get("main", {}).get("aqi"),
        }
        for key in POLLUTANT_KEYS:
            row[key] = components.get(key)

        rows.append(row)

    return rows


def build_clean():
    pattern = str(RAW_DIR / "*" / "*.json")
    raw_files = glob.glob(pattern)

    if not raw_files:
        print(f"Aucun fichier trouve dans {RAW_DIR}. Rien a faire.")
        return

    all_rows = []
    for filepath in raw_files:
        try:
            all_rows.extend(parse_raw_file(filepath))
        except Exception as e:
            print(f"[ERREUR] lecture {filepath} : {e}")

    if not all_rows:
        print("Aucune ligne extraite des fichiers raw.")
        return

    df = pd.DataFrame(all_rows)

    # Deduplication : meme ville + meme timestamp = une seule ligne
    # On garde la derniere valeur rencontree (en cas de recollecte)
    df = df.drop_duplicates(subset=["ville", "timestamp_utc"], keep="last")

    # Tri chronologique, puis par ville pour la lisibilite
    df = df.sort_values(by=["timestamp_utc", "ville"]).reset_index(drop=True)

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEAN_FILE, index=False)

    print(f"clean.csv reconstruit : {len(df)} lignes, {df['ville'].nunique()} villes.")
    print(f"Periode couverte : {df['timestamp_utc'].min()} -> {df['timestamp_utc'].max()}")
    print(f"Fichier ecrit : {CLEAN_FILE}")


if __name__ == "__main__":
    build_clean()
