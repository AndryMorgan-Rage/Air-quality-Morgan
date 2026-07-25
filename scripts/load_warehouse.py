"""
Charge clean/clean.csv dans le Data Warehouse (schema en etoile).
Rejouable : les dimensions sont chargees en "upsert", la table de faits
est reconstruite entierement a partir de clean.csv a chaque execution.

Necessite la variable d'environnement WAREHOUSE_DB_URL
(format: postgresql://user:password@host:port/dbname)

OPTIMISATION : tous les upserts sont faits en une seule requete groupee
(psycopg2.extras.execute_values) au lieu d'un aller-retour reseau par ligne.
Important car le warehouse est heberge a distance (Supabase, eu-west-1) :
avec des centaines/milliers de lignes, des inserts un par un font
exploser le temps d'execution au fil des runs (clean.csv grossit a
chaque extraction horaire).
"""
import os
from pathlib import Path
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parents[1]

CLEAN_DIR = Path(os.environ.get("CLEAN_DIR", BASE_DIR / "clean"))
CLEAN_FILE = CLEAN_DIR / "clean.csv"
DB_URL = os.environ.get("WAREHOUSE_DB_URL")

DDL = """
CREATE TABLE IF NOT EXISTS dim_ville (
    ville_id     SERIAL PRIMARY KEY,
    nom          TEXT NOT NULL,
    pays         TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    UNIQUE (nom, pays)
);

CREATE TABLE IF NOT EXISTS dim_temps (
    temps_id       SERIAL PRIMARY KEY,
    timestamp_utc  TIMESTAMPTZ NOT NULL UNIQUE,
    date           DATE NOT NULL,
    heure          INTEGER NOT NULL,
    jour_semaine   TEXT NOT NULL,
    est_weekend    BOOLEAN NOT NULL,
    mois           INTEGER NOT NULL,
    annee          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_aqi (
    fact_id      SERIAL PRIMARY KEY,
    ville_id     INTEGER NOT NULL REFERENCES dim_ville(ville_id),
    temps_id     INTEGER NOT NULL REFERENCES dim_temps(temps_id),
    aqi          INTEGER,
    co           DOUBLE PRECISION,
    no           DOUBLE PRECISION,
    no2          DOUBLE PRECISION,
    o3           DOUBLE PRECISION,
    so2          DOUBLE PRECISION,
    pm2_5        DOUBLE PRECISION,
    pm10         DOUBLE PRECISION,
    nh3          DOUBLE PRECISION,
    UNIQUE (ville_id, temps_id)
);
"""

JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]


def build_dim_temps(df):
    ts = pd.to_datetime(df["timestamp_utc"], utc=True)
    dim = pd.DataFrame({
        "timestamp_utc": ts,
        "date": ts.dt.date,
        "heure": ts.dt.hour,
        "jour_semaine": ts.dt.dayofweek.map(lambda d: JOURS_FR[d]),
        "est_weekend": ts.dt.dayofweek.isin([5, 6]),
        "mois": ts.dt.month,
        "annee": ts.dt.year,
    }).drop_duplicates(subset=["timestamp_utc"])
    return dim


def run_load():
    if not DB_URL:
        raise RuntimeError("WAREHOUSE_DB_URL n'est pas defini dans l'environnement")

    df = pd.read_csv(CLEAN_FILE)
    if df.empty:
        print("clean.csv est vide, rien a charger.")
        return

    # DDL via SQLAlchemy (une seule fois, pas de volume ici)
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        for statement in DDL.strip().split(";"):
            if statement.strip():
                conn.execute(text(statement))

    # Pour le reste, on passe par psycopg2 brut + execute_values :
    # un seul aller-retour reseau par table, quel que soit le nombre de lignes.
    raw_conn = psycopg2.connect(DB_URL)
    try:
        with raw_conn:
            with raw_conn.cursor() as cur:
                # --- dim_ville : upsert groupe ---
                villes = df[["ville", "pays", "latitude", "longitude"]].drop_duplicates()
                villes_values = list(villes.itertuples(index=False, name=None))
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dim_ville (nom, pays, latitude, longitude)
                    VALUES %s
                    ON CONFLICT (nom, pays) DO UPDATE
                    SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
                    """,
                    villes_values,
                )

                # --- dim_temps : upsert groupe ---
                dim_temps = build_dim_temps(df)
                temps_values = [
                    (
                        row.timestamp_utc.isoformat(),
                        str(row.date),
                        int(row.heure),
                        row.jour_semaine,
                        bool(row.est_weekend),
                        int(row.mois),
                        int(row.annee),
                    )
                    for row in dim_temps.itertuples(index=False)
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dim_temps
                        (timestamp_utc, date, heure, jour_semaine, est_weekend, mois, annee)
                    VALUES %s
                    ON CONFLICT (timestamp_utc) DO NOTHING
                    """,
                    temps_values,
                )

            # --- fact_aqi : recharge complete via jointures sur les dimensions ---
            ville_map = pd.read_sql("SELECT ville_id, nom, pays FROM dim_ville", engine)
            temps_map = pd.read_sql("SELECT temps_id, timestamp_utc FROM dim_temps", engine)

            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
            temps_map["timestamp_utc"] = pd.to_datetime(temps_map["timestamp_utc"], utc=True)

            merged = df.merge(ville_map, left_on=["ville", "pays"], right_on=["nom", "pays"])
            merged = merged.merge(temps_map, on="timestamp_utc")

            fact_cols = ["ville_id", "temps_id", "aqi", "co", "no", "no2",
                         "o3", "so2", "pm2_5", "pm10", "nh3"]
            fact_values = list(
                merged[fact_cols].itertuples(index=False, name=None)
            )

            with raw_conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO fact_aqi
                        (ville_id, temps_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                    VALUES %s
                    ON CONFLICT (ville_id, temps_id) DO UPDATE SET
                        aqi = EXCLUDED.aqi, co = EXCLUDED.co, no = EXCLUDED.no,
                        no2 = EXCLUDED.no2, o3 = EXCLUDED.o3, so2 = EXCLUDED.so2,
                        pm2_5 = EXCLUDED.pm2_5, pm10 = EXCLUDED.pm10, nh3 = EXCLUDED.nh3
                    """,
                    fact_values,
                )

        print(f"Warehouse charge : {len(merged)} lignes dans fact_aqi.")
    finally:
        raw_conn.close()


if __name__ == "__main__":
    run_load()