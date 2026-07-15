"""
Backfill historique de la qualite de l'air (OpenWeatherMap Air Pollution History API).
Rejouable : peut etre relance sans creer de doublons (les fichiers sont nommes
par ville + jour, un nouvel appel ecrase juste le fichier du meme jour).

Usage:
    python backfill.py --months 3
    python backfill.py --start 2025-07-01 --end 2026-07-01
"""
import os
import json
import argparse
import requests
from datetime import datetime, timedelta, timezone

RAW_DIR = os.environ.get("RAW_DIR", "/opt/airflow/raw")
CONFIG_PATH = os.environ.get("CITIES_CONFIG", "/opt/airflow/config/cities.json")
API_KEY = os.environ.get("OWM_API_KEY")

BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"


def load_cities():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_day(lat, lon, day_start, day_end):
    params = {
        "lat": lat,
        "lon": lon,
        "start": int(day_start.timestamp()),
        "end": int(day_end.timestamp()),
        "appid": API_KEY,
    }
    response = requests.get(BASE_URL, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def save_raw(ville, day, payload):
    ville_dir = os.path.join(RAW_DIR, ville.replace(" ", "_"))
    os.makedirs(ville_dir, exist_ok=True)

    filename = f"{ville.replace(' ', '_')}_history_{day.strftime('%Y-%m-%d')}.json"
    filepath = os.path.join(ville_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return filepath


def daterange(start, end):
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)


def run_backfill(start_date, end_date):
    if not API_KEY:
        raise RuntimeError("OWM_API_KEY n'est pas defini dans l'environnement")

    cities = load_cities()
    total_calls = 0

    for city in cities:
        ville = city["ville"]
        for day in daterange(start_date, end_date):
            day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
            try:
                data = fetch_day(city["lat"], city["lon"], day_start, day_end)
                data["_meta"] = {
                    "ville": ville,
                    "pays": city["pays"],
                    "lat": city["lat"],
                    "lon": city["lon"],
                }
                path = save_raw(ville, day, data)
                total_calls += 1
                print(f"[OK] {ville} {day.strftime('%Y-%m-%d')} -> {path}")
            except Exception as e:
                print(f"[ERREUR] {ville} {day.strftime('%Y-%m-%d')} : {e}")

    print(f"\nBackfill termine : {total_calls} fichiers ecrits.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=3,
                         help="Nombre de mois d'historique a recuperer (depuis aujourd'hui)")
    parser.add_argument("--start", type=str, default=None,
                         help="Date de debut YYYY-MM-DD (prioritaire sur --months)")
    parser.add_argument("--end", type=str, default=None,
                         help="Date de fin YYYY-MM-DD (defaut: aujourd'hui)")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now(timezone.utc)
    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
    else:
        start_date = end_date - timedelta(days=30 * args.months)

    print(f"Backfill de {start_date.date()} a {end_date.date()}")
    run_backfill(start_date, end_date)
