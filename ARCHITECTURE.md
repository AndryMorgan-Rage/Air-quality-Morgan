# Architecture

## Stack choisie

| Composant | Choix | Justification |
|---|---|---|
| API de données | OpenWeatherMap Air Pollution API | Gratuite, simple d'accès, fournit AQI + 8 polluants (co, no, no2, o3, so2, pm2_5, pm10, nh3) pour n'importe quelle coordonnée |
| Orchestrateur | Apache Airflow 3.2.2 (CeleryExecutor) | Outil vu en cours, permet la planification horaire et le suivi visuel des exécutions |
| Déploiement | Docker Compose | Fait tourner tous les services (Airflow, Postgres, Redis) de façon reproductible sur n'importe quelle machine |
| Stockage raw/clean | Système de fichiers local, monté en volume Docker | Volume de données faible (5 villes x mesures horaires), pas besoin d'un data lake |
| Data Warehouse | PostgreSQL (Supabase) | Base gratuite et accessible publiquement, modèle en étoile (fact_aqi + dim_ville + dim_temps) |

## Architecture du pipeline

```
OpenWeatherMap API (5 villes)
        |
   extract_current.py  --> raw/{ville}/{ville}_current_{timestamp}.json
        |
   transform_clean.py  --> clean/clean.csv (reconstruit a chaque run, dedupliqué)
        |
   load_warehouse.py   --> PostgreSQL (fact_aqi, dim_ville, dim_temps)
```

Ces 3 étapes sont orchestrées par un DAG Airflow (`aqi_pipeline_dag.py`) qui s'exécute automatiquement toutes les heures (`schedule="@hourly"`).

## Secrets

La clé API OpenWeatherMap et l'URL de connexion au warehouse sont stockées dans un fichier `.env` (non versionné, ignoré par `.gitignore`), chargées automatiquement dans les conteneurs Airflow via `env_file` dans `docker-compose.yaml`.

## Erreurs rencontrées et résolutions

| Erreur | Cause | Résolution |
|---|---|---|
| `ModuleNotFoundError: No module named 'extract_current'` | Le dossier `scripts/` n'était pas monté comme volume dans le conteneur Airflow | Ajout de `- ./scripts:/opt/airflow/scripts` (+ `raw/` et `clean/`) dans `docker-compose.yaml` |
| `NameError: name 'k' is not defined` | Caractère parasite tapé par erreur dans le fichier DAG en l'éditant avec Notepad | Suppression du caractère, ligne corrigée |
| Dépendances Python manquantes (`requests`, `pandas`, `sqlalchemy`, `psycopg2-binary`) | Image Airflow de base ne contient pas ces librairies | Ajout dans `_PIP_ADDITIONAL_REQUIREMENTS` du `docker-compose.yaml` |
| Tâche `extract` bloquée indéfiniment en "En file" | Le conteneur `airflow-worker` était resté au statut `Created` (jamais démarré), suite à une interruption (Ctrl+C) pendant `docker compose up -d` | `docker compose up -d airflow-worker` pour le forcer à démarrer ; éviter d'interrompre `docker compose up` en cours d'exécution |
| Tâche `load` en échec | `WAREHOUSE_DB_URL` pas encore défini, le warehouse Supabase n'était pas encore créé par le groupe | En attente de la création du warehouse par un membre du groupe ; le reste du pipeline (extract + transform) fonctionne déjà correctement |

## État actuel

- ✅ Extraction horaire fonctionnelle (5 villes)
- ✅ Reconstruction de `clean.csv` fonctionnelle, dédupliquée et triée
- ⏳ Chargement dans le warehouse en attente de la création de la base Supabase
- ⏳ Backfill historique (3-12 mois) à lancer une fois l'accès à l'API historique validé
