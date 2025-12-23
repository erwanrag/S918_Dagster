"""
============================================================================
Dagster Assets - Ingestion Pipeline (SFTP → RAW → STAGING)
============================================================================
Réplique exactement les flows Prefect :
- sftp_to_raw_flow
- raw_to_staging_flow
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
)
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import json
import psycopg2

from shared.config import config, sftp_config

# Import des FONCTIONS Prefect (pas les @op, juste les fonctions)
from ETL.tasks.staging_tasks import (
    create_staging_table as prefect_create_staging_table,
    load_raw_to_staging as prefect_load_raw_to_staging
)


# ============================================================================
# HELPER FUNCTIONS (pas des @op, juste des fonctions)
# ============================================================================

def parse_filename(filename: str) -> Optional[Dict[str, Any]]:
    """Parser le nom de fichier parquet"""
    parts = filename.split('_')
    
    if len(parts) < 3:
        return None
    
    load_mode = parts[-1].upper()
    date_str = parts[-2]
    table_name = '_'.join(parts[:-2])
    
    return {
        'table_name': table_name,
        'date': date_str,
        'load_mode': load_mode,
        'filename': filename
    }


def read_metadata_json(parquet_path: Path) -> Optional[dict]:
    """Lire le fichier metadata.json"""
    metadata_path = parquet_path.with_suffix('.metadata.json')
    
    if not metadata_path.exists():
        return None
    
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return None


def log_file_to_monitoring(parquet_path: Path, metadata: Optional[dict], parsed: dict) -> int:
    """Logger dans sftp_monitoring"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        table_name = parsed['table_name']
        load_mode = parsed['load_mode']
        file_size = parquet_path.stat().st_size
        
        cur.execute("""
            INSERT INTO sftp_monitoring.sftp_file_log (
                file_name,
                file_path,
                table_name,
                load_mode,
                file_size_bytes,
                processing_status,
                detected_at
            ) VALUES (%s, %s, %s, %s, %s, 'PENDING', CURRENT_TIMESTAMP)
            RETURNING log_id
        """, (
            parquet_path.name,
            str(parquet_path),  # ← AJOUTER le chemin complet
            table_name,
            load_mode,
            file_size
        ))
        
        log_id = cur.fetchone()[0]
        conn.commit()
        
        return log_id
        
    finally:
        cur.close()
        conn.close()




def load_parquet_to_raw(parquet_path: Path, log_id: int, metadata: Optional[dict]) -> Dict[str, Any]:
    """Charger parquet dans RAW"""
    import pyarrow.parquet as pq
    from sqlalchemy import create_engine
    
    # Déterminer table name
    if metadata:
        table_name = metadata.get('config_name') or metadata.get('table_name')
        load_mode = metadata.get('load_mode', 'FULL')
    else:
        filename = parquet_path.stem
        parts = filename.split('_')
        table_name = '_'.join(parts[:-2]) if len(parts) >= 3 else filename
        load_mode = parts[-1] if len(parts) >= 3 else 'FULL'
    
    raw_table = f"raw.raw_{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Lire parquet
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        rows_count = len(df)
        
        # Créer schéma RAW
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.commit()
        
        # DROP/CREATE table
        cur.execute(f"DROP TABLE IF EXISTS {raw_table} CASCADE")
        conn.commit()
        
        # Construire SQLAlchemy URL
        sqlalchemy_url = f"postgresql://{config.user}:{config.password}@{config.host}:{config.port}/{config.database}"
        engine = create_engine(sqlalchemy_url)
        
        # Charger avec pandas
        df.to_sql(
            f"raw_{table_name.lower()}",
            engine,
            schema='raw',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=10000
        )
        
        # Update monitoring
        cur.execute("""
            UPDATE sftp_monitoring.sftp_file_log
            SET 
                processing_status = 'COMPLETED',
                row_count = %s,
                processed_at = CURRENT_TIMESTAMP
            WHERE log_id = %s
        """, (rows_count, log_id))
        conn.commit()
        
        return {
            'table_name': table_name,
            'rows_loaded': rows_count,
            'load_mode': load_mode,
            'log_id': log_id
        }
        
    finally:
        cur.close()
        conn.close()


# ============================================================================
# ASSET 1: SFTP File Discovery
# ============================================================================

@asset(
    name="sftp_discovered_files",
    group_name="ingestion",
    description="Scanner les fichiers parquet disponibles dans SFTP",
    compute_kind="sftp"
)
def sftp_discovered_files(context: AssetExecutionContext) -> List[Dict[str, Any]]:
    """
    Asset 1: Découvrir les fichiers parquet dans SFTP
    
    Équivalent Prefect: scan_sftp_directory() dans sftp_to_raw.py
    
    Returns:
        List[dict]: Fichiers découverts avec metadata
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: SFTP File Discovery")
    context.log.info("=" * 70)
    
    # Scanner SFTP
    parquet_dir = sftp_config.sftp_parquet_dir
    
    if not parquet_dir.exists():
        context.log.warning(f"SFTP directory not found: {parquet_dir}")
        return []
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    if not parquet_files:
        context.log.warning("No parquet files found in SFTP")
        return []
    
    discovered = []
    
    for parquet_path in parquet_files:
        # Parser filename (fonction helper, pas @op)
        parsed = parse_filename(parquet_path.stem + '.parquet')
        
        if not parsed:
            continue
        
        # Lire metadata.json (fonction helper, pas @op)
        metadata = read_metadata_json(parquet_path)
        
        file_info = {
            'parquet_path': str(parquet_path),
            'parsed': parsed,
            'metadata': metadata,
            'table_name': parsed['table_name'],
            'load_mode': parsed['load_mode']
        }
        
        discovered.append(file_info)
    
    context.log.info(f"Discovered {len(discovered)} files to process")
    
    # Metadata Dagster
    yield Output(
        discovered,
        metadata={
            "num_files": len(discovered),
            "tables": MetadataValue.text(
                ", ".join([f['table_name'] for f in discovered])
            )
        }
    )


# ============================================================================
# ASSET 2: RAW Loading (SFTP → RAW)
# ============================================================================

@asset(
    name="raw_tables_loaded",
    group_name="ingestion",
    description="Charger fichiers parquet dans schéma RAW",
    compute_kind="postgres",
    deps=["sftp_discovered_files"]
)
def raw_tables_loaded(
    context: AssetExecutionContext,
    sftp_discovered_files: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Asset 2: Charger tous les fichiers dans RAW
    
    Équivalent Prefect: sftp_to_raw_flow()
    
    Returns:
        dict: Résumé du chargement RAW
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: RAW Loading")
    context.log.info("=" * 70)
    
    if not sftp_discovered_files:
        context.log.warning("No files to load")
        return {'tables_loaded': 0, 'total_rows': 0}
    
    results = []
    total_rows = 0
    
    for file_info in sftp_discovered_files:
        try:
            parquet_path = Path(file_info['parquet_path'])
            metadata = file_info['metadata']
            parsed = file_info['parsed']
            
            context.log.info(f"\n[FILE] {parquet_path.name}")
            
            # Logger dans monitoring (fonction helper)
            log_id = log_file_to_monitoring(parquet_path, metadata, parsed)
            
            # Charger dans RAW (fonction helper)
            load_result = load_parquet_to_raw(parquet_path, log_id, metadata)
            
            rows = load_result['rows_loaded']
            total_rows += rows
            
            results.append({
                'table': load_result['table_name'],
                'rows': rows,
                'mode': load_result['load_mode']
            })
            
            context.log.info(f"[OK] {load_result['table_name']}: {rows:,} rows")
            
        except Exception as e:
            context.log.error(f"[ERROR] {file_info['table_name']}: {e}")
            continue
    
    context.log.info("=" * 70)
    context.log.info(f"RAW Loading Complete: {len(results)} tables, {total_rows:,} rows")
    context.log.info("=" * 70)
    
    # Metadata Dagster
    yield Output(
        {
            'tables_loaded': len(results),
            'total_rows': total_rows,
            'results': results
        },
        metadata={
            "tables_count": len(results),
            "total_rows": total_rows,
            "tables_list": MetadataValue.md(
                "\n".join([f"- **{r['table']}**: {r['rows']:,} rows ({r['mode']})" for r in results])
            )
        }
    )


# ============================================================================
# ASSET 3: STAGING Transformation (RAW → STAGING)
# ============================================================================

@asset(
    name="staging_tables_ready",
    group_name="ingestion",
    description="Transformer RAW → STAGING avec extent, hashdiff, déduplication",
    compute_kind="postgres",
    deps=["raw_tables_loaded"]
)
def staging_tables_ready(
    context: AssetExecutionContext,
    raw_tables_loaded: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Asset 3: Transformer toutes les tables RAW → STAGING
    
    Équivalent Prefect: raw_to_staging_flow_parallel()
    
    Features:
    - Éclatement colonnes EXTENT
    - Calcul _etl_hashdiff
    - Support FULL/INCREMENTAL/FULL_RESET
    
    Returns:
        dict: Résumé transformation STAGING
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: STAGING Transformation")
    context.log.info("=" * 70)
    
    if raw_tables_loaded['tables_loaded'] == 0:
        context.log.warning("No RAW tables to process")
        return {'tables_processed': 0, 'total_rows': 0}
    
    # Générer run_id
    run_id = f"dagster_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    results = []
    total_rows = 0
    
    for table_info in raw_tables_loaded['results']:
        table_name = table_info['table']
        load_mode = table_info['mode']
        
        try:
            context.log.info(f"\n[TABLE] {table_name}")
            context.log.info(f"[MODE] {load_mode}")
            
            # Appeler fonctions Prefect directement (via .fn)
            prefect_create_staging_table.fn(table_name, load_mode)
            rows = prefect_load_raw_to_staging.fn(table_name, run_id, load_mode)
            
            total_rows += rows
            
            results.append({
                'table': table_name,
                'rows': rows,
                'mode': load_mode,
                'verified': True
            })
            
            context.log.info(f"[OK] {table_name}: {rows:,} rows ({load_mode})")
            
        except Exception as e:
            context.log.error(f"[ERROR] {table_name}: {e}")
            continue
    
    context.log.info("=" * 70)
    context.log.info(f"STAGING Complete: {len(results)} tables, {total_rows:,} rows")
    context.log.info("=" * 70)
    
    # Metadata Dagster
    yield Output(
        {
            'tables_processed': len(results),
            'total_rows': total_rows,
            'results': results,
            'run_id': run_id
        },
        metadata={
            "tables_count": len(results),
            "total_rows": total_rows,
            "run_id": run_id,
            "tables_list": MetadataValue.md(
                "\n".join([
                    f"- **{r['table']}**: {r['rows']:,} rows ({r['mode']}) ✓" 
                    if r['verified'] else 
                    f"- **{r['table']}**: {r['rows']:,} rows ({r['mode']}) ✗"
                    for r in results
                ])
            )
        }
    )