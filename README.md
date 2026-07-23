# Air Quality ETL Pipeline

Pipeline ETL automatisant la collecte, le nettoyage et le chargement de donnees de qualite de l'air (AQI) pour 5 villes dans le monde.

## Stack

| Composant | Technologie | Version |
|---|---|---|
| Langage | Python | 3.13 |
| Orchestrateur | Apache Airflow | 3.2.2 |
| Executeur | CeleryExecutor | - |
| Broker | Redis | 7.2 |
| Base de metadonnees | PostgreSQL | 16 |
| Data Warehouse | PostgreSQL (Supabase) | - |
| Conteneurisation | Docker Compose | - |
| API externe | OpenWeatherMap Air Pollution API | - |
| Biblioteques Python | requests, pandas, sqlalchemy, psycopg2-binary, python-dotenv | - |

## Villes surveillees

| Ville | Pays | Latitude | Longitude |
|---|---|---|---|
| Antananarivo | MG | -18.8792 | 47.5079 |
| Paris | FR | 48.8566 | 2.3522 |
| New York | US | 40.7128 | -74.0060 |
| Tokyo | JP | 35.6762 | 139.6503 |
| Sydney | AU | -33.8688 | 151.2093 |

## Structure du projet

```
airflow-docker/
├── dags/
│   └── aqi_pipeline_dag.py       # DAG Airflow (extract >> transform >> load)
├── scripts/
│   ├── extract_current.py        # Extraction horaire via API current
│   ├── transform_clean.py        # Nettoyage + deduplication -> clean.csv
│   ├── load_warehouse.py         # Chargement en warehouse (schema en etoile)
│   └── backfill.py               # Backfill historique via API history
├── config/
│   ├── airflow.cfg               # Configuration Airflow
│   └── cities.json               # Liste des villes (coordonnees)
├── raw/                          # Donnees brutes JSON (par ville)
├── clean/                        # Donnees nettoyees (clean.csv)
├── docker-compose.yaml           # Orchestration Docker
├── .env                          # Secrets (non versionne)
├── .gitignore
└── ARCHITECTURE.md               # Documentation technique
```

## Demarrage

```bash
# Copier le fichier .env avec les cles
# Docker compose up
docker compose up -d

# Verifier que le DAG est actif dans l'UI Airflow (port 8080)
# User: airflow / Password: airflow
```

## Backfill historique

```bash
# 3 mois d'historique (defaut)
python scripts/backfill.py --months 3

# Periode personnalisee
python scripts/backfill.py --start 2025-07-01 --end 2026-07-01
```

## Donnees collectees

Pour chaque mesure horaire :
- **AQI** : Air Quality Index (1-5)
- **8 polluants** : CO, NO, NO2, O3, SO2, PM2.5, PM10, NH3
