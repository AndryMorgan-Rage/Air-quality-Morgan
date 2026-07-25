# Architecture

## Stack choisie

| Composant | Choix | Justification |
|---|---|---|
| API de donnees | OpenWeatherMap Air Pollution API (current + history) | Gratuite, simple d'acces, fournit AQI + 8 polluants (co, no, no2, o3, so2, pm2_5, pm10, nh3) pour n'importe quelle coordonnee |
| Orchestrateur | Apache Airflow 3.2.2 (CeleryExecutor) | Outil vu en cours, permet la planification horaire et le suivi visuel des executions |
| Deploiement | Docker Compose | Fait tourner tous les services (Airflow, Postgres, Redis) de facon reproductible sur n'importe quelle machine |
| Stockage raw/clean | Systeme de fichiers local, monte en volume Docker | Volume de donnees faible (5 villes x mesures horaires), pas besoin d'un data lake |
| Data Warehouse | PostgreSQL (Supabase) | Base gratuite et accessible publiquement, modele en etoile (fact_aqi + dim_ville + dim_temps) |
| Langage | Python 3.13 | Scripts ETL et DAG Airflow |

## Architecture du pipeline

```
OpenWeatherMap API (5 villes : Antananarivo, Paris, New York, Tokyo, Sydney)
        |
   extract_current.py  --> raw/{ville}/{ville}_current_{timestamp}.json
        |
   transform_clean.py  --> clean/clean.csv (reconstruit a chaque run, deduplique)
        |
   load_warehouse.py   --> PostgreSQL Supabase (fact_aqi, dim_ville, dim_temps)
```

Ces 3 etapes sont orchestrees par un DAG Airflow (`aqi_pipeline_dag.py`) qui s'execute automatiquement toutes les heures (`schedule="@hourly"`).

## Backfill historique

Script autonome (`scripts/backfill.py`) pour recuperer l'historique AQI via l'API History :

```
python backfill.py --months 3
python backfill.py --start 2025-07-01 --end 2026-07-01
```

- Appelle l'endpoint `/data/2.5/air_pollution/history` pour chaque ville et chaque jour
- Fichiers sauvegardes : `raw/{ville}/{ville}_history_{date}.json`
- Pause de 1.1s entre chaque appel API (limite gratuite : 60 req/min)
- Retry automatique (3 tentatives) sur erreurs reseau transitoires (timeout, DNS, SSL)
- Rejouable sans doublons : chaque jour ecrase son propre fichier

## Data Warehouse (schema en etoile)

```
dim_ville                    dim_temps
+-----------+               +------------+
| ville_id  |               | temps_id   |
| nom       |               | timestamp  |
| pays      |               | date       |
| latitude  |               | heure      |
| longitude |               | jour_sem   |
+-----------+               | weekend    |
       |                    | mois       |
       |    fact_aqi        | annee      |
       +----+-----------+---+------------+
            | fact_id   |
            | ville_id  |--> dim_ville
            | temps_id  |--> dim_temps
            | aqi       |
            | co, no, no2, o3, so2, pm2_5, pm10, nh3
            +-----------+
```

- `dim_ville` : upsert (insert + update si la ville existe deja)
- `dim_temps` : upsert (insert sans ecraser les timestamps existants)
- `fact_aqi` : recharge complete a chaque run avec upsert sur (ville_id, temps_id)

## Docker Compose (services)

| Service | Role |
|---|---|
| `postgres` | Base de donnees Airflow (metadonnees) |
| `redis` | Broker Celery pour l'execution distribuee |
| `airflow-apiserver` | API REST Airflow (port 8080) |
| `airflow-scheduler` | Planifie et declenche les DAGs |
| `airflow-dag-processor` | Charge et traite les fichiers DAG |
| `airflow-worker` | Execute les taches via Celery |
| `airflow-triggerer` | Gere les taches asynchrones |
| `airflow-init` | Initialisation (migrations DB, creation user) |
| `flower` | Monitoring Celery (port 5555, profile optionnel) |
| `airflow-cli` | CLI Airflow (profile debug) |


## Volumes Docker

| Volume local | Montage conteneur | Description |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | DAGs Airflow |
| `./logs` | `/opt/airflow/logs` | Logs Airflow |
| `./config` | `/opt/airflow/config` | Config Airflow + cities.json |
| `./plugins` | `/opt/airflow/plugins` | Plugins Airflow |
| `./scripts` | `/opt/airflow/scripts` | Scripts ETL Python |
| `./raw` | `/opt/airflow/raw` | Donnees brutes JSON |
| `./clean` | `/opt/airflow/clean` | Donnees nettoyees CSV |

## Erreurs rencontrees et resolutions

| Erreur | Cause | Resolution |
|---|---|---|
| `ModuleNotFoundError: No module named 'extract_current'` | Le dossier `scripts/` n'etait pas monte comme volume dans le conteneur Airflow | Ajout de `- ./scripts:/opt/airflow/scripts` (+ `raw/` et `clean/`) dans `docker-compose.yaml` |
| `NameError: name 'k' is not defined` | Caractere parasite tape par erreur dans le fichier DAG en l'editant avec Notepad | Suppression du caractere, ligne corrigee |
| Dependances Python manquantes (`requests`, `pandas`, `sqlalchemy`, `psycopg2-binary`) | Image Airflow de base ne contient pas ces librairies | Ajout dans `_PIP_ADDITIONAL_REQUIREMENTS` du `docker-compose.yaml` |
| Tache `extract` bloquee indefiniment en "En file" | Le conteneur `airflow-worker` etait reste au statut `Created` (jamais demarre), suite a une interruption (Ctrl+C) pendant `docker compose up -d` | `docker compose up -d airflow-worker` pour le forcer a demarrer ; eviter d'interrompre `docker compose up` en cours d'execution |
| Tache `load` en echec | `WAREHOUSE_DB_URL` pas encore defini, le warehouse Supabase n'etait pas encore cree par le groupe | En attente de la creation du warehouse par un membre du groupe ; le reste du pipeline (extract + transform) fonctionne deja correctement |

## Etat actuel

- Extraction horaire fonctionnelle (5 villes)
- Reconstruction de `clean.csv` fonctionnelle, dedupliquee et triee
- Chargement dans le warehouse fonctionnel (Supabase, schema en etoile)
- Backfill historique realise pour Antananarivo, Paris et New York (donnees disponibles dans `raw/`)
