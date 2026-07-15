"""
Charge clean/clean.csv dans le Data Warehouse (schema en etoile).
Rejouable : les dimensions sont chargees en "upsert", la table de faits
est reconstruite entierement a partir de clean.csv a chaque execution.

Necessite la variable d'environnement WAREHOUSE_DB_URL
(format: postgresql://user:password@host:port/dbname)
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text

CLEAN_FILE = os.environ.get("CLEAN_FILE", "/opt/airflow/clean/clean.csv")
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

    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        for statement in DDL.strip().split(";"):
            if statement.strip():
                conn.execute(text(statement))

    # --- dim_ville : upsert ---
    villes = df[["ville", "pays", "latitude", "longitude"]].drop_duplicates()
    with engine.begin() as conn:
        for _, row in villes.iterrows():
            conn.execute(text("""
                INSERT INTO dim_ville (nom, pays, latitude, longitude)
                VALUES (:nom, :pays, :lat, :lon)
                ON CONFLICT (nom, pays) DO UPDATE
                SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
            """), {"nom": row["ville"], "pays": row["pays"],
                   "lat": row["latitude"], "lon": row["longitude"]})

    # --- dim_temps : upsert ---
    dim_temps = build_dim_temps(df)
    with engine.begin() as conn:
        for _, row in dim_temps.iterrows():
            conn.execute(text("""
                INSERT INTO dim_temps (timestamp_utc, date, heure, jour_semaine, est_weekend, mois, annee)
                VALUES (:ts, :date, :heure, :jour, :weekend, :mois, :annee)
                ON CONFLICT (timestamp_utc) DO NOTHING
            """), {"ts": row["timestamp_utc"].isoformat(), "date": str(row["date"]),
                   "heure": int(row["heure"]), "jour": row["jour_semaine"],
                   "weekend": bool(row["est_weekend"]), "mois": int(row["mois"]),
                   "annee": int(row["annee"])})

    # --- fact_aqi : recharge complete via jointures sur les dimensions ---
    ville_map = pd.read_sql("SELECT ville_id, nom, pays FROM dim_ville", engine)
    temps_map = pd.read_sql("SELECT temps_id, timestamp_utc FROM dim_temps", engine)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    temps_map["timestamp_utc"] = pd.to_datetime(temps_map["timestamp_utc"], utc=True)

    merged = df.merge(ville_map, left_on=["ville", "pays"], right_on=["nom", "pays"])
    merged = merged.merge(temps_map, on="timestamp_utc")

    with engine.begin() as conn:
        for _, row in merged.iterrows():
            conn.execute(text("""
                INSERT INTO fact_aqi (ville_id, temps_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                VALUES (:ville_id, :temps_id, :aqi, :co, :no, :no2, :o3, :so2, :pm2_5, :pm10, :nh3)
                ON CONFLICT (ville_id, temps_id) DO UPDATE SET
                    aqi = EXCLUDED.aqi, co = EXCLUDED.co, no = EXCLUDED.no, no2 = EXCLUDED.no2,
                    o3 = EXCLUDED.o3, so2 = EXCLUDED.so2, pm2_5 = EXCLUDED.pm2_5,
                    pm10 = EXCLUDED.pm10, nh3 = EXCLUDED.nh3
            """), {
                "ville_id": int(row["ville_id"]), "temps_id": int(row["temps_id"]),
                "aqi": row["aqi"], "co": row["co"], "no": row["no"], "no2": row["no2"],
                "o3": row["o3"], "so2": row["so2"], "pm2_5": row["pm2_5"],
                "pm10": row["pm10"], "nh3": row["nh3"],
            })

    print(f"Warehouse charge : {len(merged)} lignes dans fact_aqi.")


if __name__ == "__main__":
    run_load()
