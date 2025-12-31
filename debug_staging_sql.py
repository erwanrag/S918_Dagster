#!/usr/bin/env python
"""
Script de debug pour identifier l'erreur SQL exacte dans STAGING
"""

import os
import sys
import psycopg2

# Configuration
os.environ.setdefault("ETL_PG_HOST", "localhost")
os.environ.setdefault("ETL_PG_PORT", "5432")
os.environ.setdefault("ETL_PG_DATABASE", "etl_db")
os.environ.setdefault("ETL_PG_USER", "etl_user")

sys.path.insert(0, '/data/dagster')

from src.core.staging.transformer import build_select_with_extent_and_cast
from src.db.metadata import get_table_metadata

def test_sql_generation():
    """Tester la génération SQL pour focondi"""
    
    # Connexion
    conn = psycopg2.connect(
        host=os.getenv("ETL_PG_HOST"),
        port=int(os.getenv("ETL_PG_PORT")),
        database=os.getenv("ETL_PG_DATABASE"),
        user=os.getenv("ETL_PG_USER"),
        password=os.getenv("ETL_PG_PASSWORD")
    )
    
    try:
        # Récupérer métadonnées focondi
        metadata = get_table_metadata(conn, "focondi")
        if not metadata:
            print("❌ Metadata not found for focondi")
            return
        
        print("✅ Metadata loaded for focondi")
        print(f"   Columns: {len(metadata['columns'])}")
        
        # Générer SELECT expression
        select_expr = build_select_with_extent_and_cast(metadata["columns"], "src")
        
        # Construire requête complète
        full_query = f"""
INSERT INTO staging.focondi
SELECT 
    {select_expr},
    'test_run' AS "_etl_run_id",
    CURRENT_TIMESTAMP AS "_etl_valid_from"
FROM raw.raw_focondi src
LIMIT 1
"""
        
        print("\n" + "="*80)
        print("REQUÊTE SQL GÉNÉRÉE:")
        print("="*80)
        print(full_query)
        print("="*80)
        
        # Sauvegarder dans fichier
        with open("/tmp/staging_query_debug.sql", "w") as f:
            f.write(full_query)
        print("\n✅ Requête sauvegardée dans: /tmp/staging_query_debug.sql")
        
        # Tester l'exécution
        print("\n🔍 Test d'exécution de la requête...")
        with conn.cursor() as cur:
            try:
                # Tronquer d'abord
                cur.execute("TRUNCATE TABLE staging.focondi")
                
                # Puis insérer
                cur.execute(full_query)
                
                rows = cur.rowcount
                conn.rollback()  # Ne pas commiter le test
                
                print(f"✅ Requête exécutée avec succès ! ({rows} rows)")
                
            except Exception as e:
                conn.rollback()
                print(f"\n❌ ERREUR SQL:")
                print(f"   {str(e)}")
                print(f"\n📍 Détails de l'erreur:")
                print(f"   Type: {type(e).__name__}")
                
                # Extraire ligne problématique si possible
                error_msg = str(e)
                if "LIGNE" in error_msg or "LINE" in error_msg:
                    print(f"\n🔍 Message complet:")
                    print(error_msg)
    
    finally:
        conn.close()

if __name__ == "__main__":
    test_sql_generation()