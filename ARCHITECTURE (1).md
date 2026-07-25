# Architecture

## Stack choisie (version finale)

| Composant | Choix | Justification |
|---|---|---|
| API de données | OpenWeatherMap Air Pollution API (current + history) | Gratuite, simple d'accès, fournit AQI + 8 polluants (co, no, no2, o3, so2, pm2_5, pm10, nh3) pour n'importe quelle coordonnée |
| Orchestrateur | **GitHub Actions** (pivot depuis Apache Airflow) | Serveurs gratuits fournis par GitHub, planification via cron, aucune machine du groupe n'a besoin de tourner en continu |
| Stockage raw/clean | Fichiers versionnés dans le dépôt Git (`raw/`, `clean/`) | Le workflow committe lui-même les nouvelles données collectées à chaque exécution, ce qui sert aussi d'historique/preuve d'exécution |
| Data Warehouse | PostgreSQL (Supabase), connexion via Transaction Pooler | Base gratuite et accessible publiquement, modèle en étoile (fact_aqi + dim_ville + dim_temps) |
| Secrets | GitHub Secrets (Settings → Secrets and variables → Actions) | Clé API et URL de connexion chiffrées, jamais exposées dans le code ni les logs |

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

Ces 3 étapes sont regroupées dans un seul script `scripts/run_hourly.py`, exécuté par le workflow GitHub Actions `.github/workflows/air_quality_pipeline.yml`, planifié toutes les heures (`cron: "0 * * * *"`), avec un déclenchement manuel possible (`workflow_dispatch`).

À la fin de chaque exécution réussie, le workflow committe et pousse automatiquement les nouveaux fichiers `raw/` et `clean/clean.csv` dans le dépôt, avec l'auteur `github-actions[bot]` — ce qui constitue une preuve d'exécution autonome consultable directement dans l'historique Git et dans l'onglet Actions du repo.

## Pivot Airflow/Docker → GitHub Actions

### Contexte
Le pipeline a d'abord été développé et validé avec Apache Airflow 3.2.2 (CeleryExecutor) sous Docker Compose, en local. Cette architecture fonctionnait, mais présentait une contrainte forte pour un projet de groupe : elle nécessitait qu'une machine reste allumée en continu pour que le DAG s'exécute toutes les heures comme prévu.

### Décision
Le groupe a pivoté vers **GitHub Actions** comme orchestrateur, solution explicitement autorisée par l'énoncé ("l'orchestrateur au choix du groupe"). GitHub Actions fournit des runners gratuits, un vrai système de planification (`cron`), une interface de suivi des exécutions (`onglet Actions`), et ne dépend d'aucune machine du groupe.

### Mise en œuvre
1. **Workflow** : `.github/workflows/air_quality_pipeline.yml`, déclenché par `schedule` (cron horaire) et `workflow_dispatch` (test manuel).
2. **Script unique** : `scripts/run_hourly.py` regroupe extract → transform → load en une seule commande, appelée par le workflow.
3. **Secrets** : `OWM_API_KEY` et `WAREHOUSE_DB_URL` stockés dans GitHub Secrets, injectés en variables d'environnement au moment de l'exécution, jamais codés en dur.
4. **Persistance des données** : le workflow committe automatiquement `raw/` et `clean/` à la fin de chaque run réussi.

### Validation
- Premier test manuel (`workflow_dispatch`) : succès, visible dans l'onglet Actions avec badge vert.
- Vérification de l'exécution automatique (`schedule`) sur plusieurs jours : des commits signés `github-actions[bot]` apparaissent régulièrement dans l'historique du dépôt sans aucune intervention manuelle, confirmant que le cron se déclenche bien tout seul.
- `clean.csv` final : 4712 lignes, 0 valeur nulle, période couverte du 23/07/2025 au 23/07/2026 (backfill 12 mois + collecte horaire continue), 5 villes.

## Secrets

La clé API OpenWeatherMap et l'URL de connexion au warehouse sont stockées dans **GitHub Secrets** (`OWM_API_KEY`, `WAREHOUSE_DB_URL`), chiffrées et invisibles même dans les logs d'exécution. Elles sont injectées comme variables d'environnement (`env:`) dans l'étape du workflow qui lance `run_hourly.py`.

*(Historique : pendant la phase Airflow/Docker locale, ces mêmes secrets étaient stockés dans un fichier `.env` non versionné, chargé via `env_file` dans `docker-compose.yaml`. Cette approche a été conservée dans le code des scripts eux-mêmes — `load_dotenv()` — pour compatibilité, même si elle n'est plus utilisée en production sur GitHub Actions.)*

## Connexion au Data Warehouse : Transaction Pooler

`WAREHOUSE_DB_URL` utilise le **Transaction Pooler** Supabase (`aws-0-eu-west-1.pooler.supabase.com:6543`) plutôt que la connexion directe (`db.<projet>.supabase.co:5432`), qui ne résout qu'en IPv6 sur le plan gratuit Supabase et échoue depuis des environnements sans connectivité IPv6 sortante (Docker local, certains runners).

## Optimisation du chargement warehouse

`load_warehouse.py` utilise des upserts groupés (`psycopg2.extras.execute_values`) au lieu d'inserts ligne par ligne. Avec un warehouse hébergé à distance, des centaines/milliers de lignes insérées une par une auraient fait exploser le temps d'exécution au fil des runs (`clean.csv` grossit à chaque extraction).

## Résilience réseau du backfill

`backfill.py` inclut :
- un **throttling** (1.1s entre appels) pour rester sous la limite de 60 appels/minute du plan gratuit OpenWeatherMap ;
- un **retry automatique** (3 tentatives, délai croissant) sur les erreurs réseau transitoires (timeout, DNS, SSL), pour absorber les instabilités réseau ponctuelles sans perdre de jours de données silencieusement.

## Erreurs rencontrées et résolutions (phase Airflow/Docker locale)

| Erreur | Cause | Résolution |
|---|---|---|
| `ModuleNotFoundError: No module named 'extract_current'` | Le dossier `scripts/` n'était pas monté comme volume dans le conteneur Airflow | Ajout du volume dans `docker-compose.yaml` |
| Dépendances Python manquantes (`requests`, `pandas`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`) | Image Airflow de base ne les contient pas | Ajout dans `_PIP_ADDITIONAL_REQUIREMENTS` |
| `psycopg2.OperationalError: ... Network is unreachable` | Connexion directe Supabase résolue en IPv6 uniquement (plan gratuit), pas de connectivité IPv6 sortante | Passage au Transaction Pooler (IPv4) |
| `.env` modifié non pris en compte après `docker compose restart` | `restart` ne relit pas le `.env`, variables figées à la création du conteneur | Utiliser `docker compose up -d --force-recreate` |
| Erreurs réseau intermittentes (DNS, SSL, timeout) lors des extractions et chargements | Connexion internet locale instable (mesuré : 8% de perte de paquets, ~450ms de latence) | Retries au niveau Airflow (`retries`, `retry_delay`) puis, après le pivot, retries applicatifs dans `backfill.py` |
| Dépendance à une machine allumée en continu pour le `schedule="@hourly"` d'Airflow | Architecture Docker locale | **Pivot complet vers GitHub Actions** (voir section dédiée ci-dessus) |

## État actuel

- ✅ Extraction horaire fonctionnelle (5 villes), automatisée via GitHub Actions
- ✅ Reconstruction de `clean.csv` fonctionnelle, dédupliquée et triée
- ✅ Chargement dans le warehouse fonctionnel (upserts groupés, tables `dim_ville`, `dim_temps`, `fact_aqi` peuplées)
- ✅ Backfill historique 12 mois terminé (2025-07-23 → 2026-07-23)
- ✅ Pipeline entièrement autonome : s'exécute, se recharge et se committe tout seul, toutes les heures, sans dépendance à une machine du groupe — vérifié sur plusieurs jours d'exécution automatique
