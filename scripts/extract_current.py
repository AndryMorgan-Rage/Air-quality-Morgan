"""
Extraction horaire de la qualite de l'air (OpenWeatherMap Air Pollution API).
Ecrit un fichier JSON brut par ville et par appel dans raw/{ville}/.
Ce script ne modifie jamais un fichier existant : il ne fait qu'ajouter.
"""

import os
import json
import requests
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

CONFIG_PATH = Path(
    os.environ.get(
        "CITIES_CONFIG",
        BASE_DIR / "config" / "cities.json"
    )
)

API_KEY = os.environ.get("OWM_API_KEY")

BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution"


def load_cities():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Fichier cities.json introuvable : {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_city_aqi(ville, lat, lon):
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY
    }

    response = requests.get(BASE_URL, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def save_raw(ville, payload):
    ville_dir = RAW_DIR / ville.replace(" ", "_")
    ville_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    filename = f"{ville.replace(' ', '_')}_current_{now.strftime('%Y%m%dT%H%M%S')}Z.json"
    filepath = ville_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return filepath


def run_extract():
    if not API_KEY:
        raise RuntimeError("OWM_API_KEY n'est pas defini dans l'environnement")

    cities = load_cities()
    errors = []

    for city in cities:
        ville = city["ville"]

        try:
            data = fetch_city_aqi(ville, city["lat"], city["lon"])

            data["_meta"] = {
                "ville": ville,
                "pays": city["pays"],
                "lat": city["lat"],
                "lon": city["lon"],
                "collected_at": datetime.now(timezone.utc).isoformat(),
            }

            path = save_raw(ville, data)
            print(f"[OK] {ville} -> {path}")

        except Exception as e:
            print(f"[ERREUR] {ville} : {e}")
            errors.append((ville, str(e)))

    if errors:
        raise RuntimeError(f"Echecs lors de l'extraction : {errors}")


if __name__ == "__main__":
    run_extract()