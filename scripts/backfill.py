"""
Backfill historique de la qualite de l'air (OpenWeatherMap Air Pollution History API).
Rejouable : peut etre relance sans creer de doublons (les fichiers sont nommes
par ville + jour, un nouvel appel ecrase juste le fichier du meme jour).

Usage:
    python backfill.py --months 3
    python backfill.py --start 2025-07-01 --end 2026-07-01

Une pause (REQUEST_DELAY_SECONDS) est appliquee entre chaque appel API pour
rester sous la limite de 60 appels/minute du plan gratuit OpenWeatherMap.

RETRY : chaque appel est retente automatiquement (MAX_RETRIES fois, avec
un delai croissant) avant d'etre marque en erreur. Absorbe les timeouts et
coupures reseau ponctuelles (DNS, SSL) sans perdre de jours silencieusement.
"""
import os
import json
import time
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

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

BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"

# Plan gratuit OpenWeatherMap : 60 appels/minute max.
# 1.1s de pause -> ~54 appels/minute, marge de securite incluse.
REQUEST_DELAY_SECONDS = 1.1

# Retry sur erreurs reseau transitoires (timeout, DNS, SSL, connexion coupee)
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5  # 5s, puis 10s, puis 15s entre les tentatives


def load_cities():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_day(lat, lon, day_start, day_end):
    """Recupere les donnees d'un jour, avec retry automatique sur erreurs
    reseau transitoires. Leve la derniere exception si tous les essais echouent."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
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
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            # Erreurs reseau transitoires : ca vaut le coup de reessayer
            last_error = e
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SECONDS * attempt
                print(f"  [RETRY {attempt}/{MAX_RETRIES}] echec reseau, "
                      f"nouvelle tentative dans {wait}s : {e}")
                time.sleep(wait)
        except requests.exceptions.HTTPError:
            # Erreur HTTP (429 quota, 401 cle invalide, etc.) : pas la peine
            # de reessayer, c'est structurel, pas transitoire
            raise

    raise last_error


def save_raw(ville, day, payload):
    ville_dir = RAW_DIR / ville.replace(" ", "_")
    ville_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{ville.replace(' ', '_')}_history_{day.strftime('%Y-%m-%d')}.json"
    filepath = ville_dir / filename

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
    total_errors = 0
    failed_days = []

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
                total_errors += 1
                failed_days.append((ville, day.strftime('%Y-%m-%d')))
                print(f"[ERREUR] {ville} {day.strftime('%Y-%m-%d')} "
                      f"(apres {MAX_RETRIES} tentatives) : {e}")
            finally:
                # Pause systematique (succes ou erreur) pour respecter le quota API
                time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\nBackfill termine : {total_calls} fichiers ecrits, {total_errors} erreurs.")
    if failed_days:
        print("\nJours en echec (apres retries) :")
        for ville, jour in failed_days:
            print(f"  - {ville} {jour}")
        print("\nAstuce : relance le script avec les memes --start/--end, il est "
              "rejouable sans creer de doublons (chaque jour ecrase juste son "
              "propre fichier). Seuls les jours en echec ci-dessus referont un appel utile.")


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