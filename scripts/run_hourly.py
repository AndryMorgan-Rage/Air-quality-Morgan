"""
Pipeline AQI en un seul script, pense pour GitHub Actions.
Enchaine : extraction (extract_current) -> nettoyage (transform_clean) ->
chargement warehouse (load_warehouse).

Toutes les variables d'environnement (OWM_API_KEY, WAREHOUSE_DB_URL) sont
fournies par GitHub Secrets au moment de l'execution du workflow, jamais
codees en dur ici.

Si une etape echoue, le script s'arrete avec un code de sortie non-nul,
ce qui fait apparaitre le run en rouge dans l'onglet Actions de GitHub.
"""
import sys
from pathlib import Path

# Permet d'importer les modules extract_current / transform_clean / load_warehouse
# qui vivent dans le meme dossier scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent))

from extract_current import run_extract
from transform_clean import build_clean
from load_warehouse import run_load


def main():
    print("=" * 60)
    print("ETAPE 1/3 : extraction")
    print("=" * 60)
    run_extract()

    print()
    print("=" * 60)
    print("ETAPE 2/3 : transformation")
    print("=" * 60)
    build_clean()

    print()
    print("=" * 60)
    print("ETAPE 3/3 : chargement warehouse")
    print("=" * 60)
    run_load()

    print()
    print("Pipeline termine avec succes.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[ECHEC PIPELINE] {e}")
        sys.exit(1)
