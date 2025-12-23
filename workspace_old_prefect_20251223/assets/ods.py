"""
============================================================================
Dagster Assets - ODS Pipeline (STAGING → ODS)
============================================================================
Réplique exactement staging_to_ods_flow de Prefect
Support FULL / INCREMENTAL / FULL_RESET
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
)
from typing import Dict, Any
from datetime import datetime

from ..ops.ods_ops import (
    merge_staging_to_ods_op,
    verify_ods_table_op
)


# ============================================================================
# ASSET 4: ODS Merge (STAGING → ODS)
# ============================================================================

@asset(
    name="ods_tables_merged",
    group_name="ods",
    description="Merge STAGING → ODS avec support FULL/INCREMENTAL/FULL_RESET",
    compute_kind="postgres",
    deps=["staging_tables_ready"]
)
def ods_tables_merged(
    context: AssetExecutionContext,
    staging_tables_ready: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Asset 4: Merger toutes les tables STAGING → ODS
    
    Équivalent Prefect: staging_to_ods_flow()
    
    Modes supportés:
    - FULL: TRUNCATE + INSERT
    - INCREMENTAL: UPSERT (INSERT nouveaux + UPDATE modifiés)
    - FULL_RESET: DROP + CREATE + INSERT (reset complet)
    
    Détection automatique:
    1. Lire load_mode depuis sftp_monitoring
    2. Si force_full → FULL
    3. Si primary_keys → INCREMENTAL
    4. Sinon → FULL
    
    Returns:
        dict: Résumé des merges ODS
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: ODS Merge")
    context.log.info("=" * 70)
    
    if staging_tables_ready['tables_processed'] == 0:
        context.log.warning("No STAGING tables to merge")
        return {'tables_merged': 0, 'total_rows': 0}
    
    # Réutiliser run_id de STAGING
    run_id = staging_tables_ready.get('run_id', f"dagster_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    
    results = []
    total_rows = 0
    
    # Stats par mode
    mode_stats = {
        'FULL': 0,
        'INCREMENTAL': 0,
        'FULL_RESET': 0
    }
    
    for table_info in staging_tables_ready['results']:
        table_name = table_info['table']
        load_mode = table_info['mode']
        
        try:
            context.log.info(f"\n[TABLE] {table_name}")
            context.log.info(f"[MODE] {load_mode}")
            
            # Merge STAGING → ODS
            merge_result = merge_staging_to_ods_op(
                context,
                table_name,
                load_mode,
                run_id
            )
            
            actual_mode = merge_result['mode']
            rows_affected = merge_result['rows_affected']
            
            total_rows += rows_affected
            mode_stats[actual_mode] = mode_stats.get(actual_mode, 0) + 1
            
            # Vérifier ODS
            verify = verify_ods_table_op(context, table_name, run_id)
            
            results.append({
                'table': table_name,
                'mode': actual_mode,
                'rows_inserted': merge_result.get('rows_inserted', 0),
                'rows_updated': merge_result.get('rows_updated', 0),
                'rows_affected': rows_affected,
                'total_ods': verify.get('total_rows', 0),
                'verified': verify.get('exists', False)
            })
            
            # Log selon mode
            if actual_mode == 'INCREMENTAL':
                context.log.info(
                    f"[OK] {table_name}: "
                    f"+{merge_result['rows_inserted']} ~{merge_result['rows_updated']} "
                    f"(INCREMENTAL)"
                )
            elif actual_mode == 'FULL_RESET':
                context.log.info(
                    f"[OK] {table_name}: "
                    f"{rows_affected:,} rows (FULL_RESET - Table recréée)"
                )
            else:  # FULL
                context.log.info(
                    f"[OK] {table_name}: "
                    f"{rows_affected:,} rows (FULL)"
                )
            
        except Exception as e:
            context.log.error(f"[ERROR] {table_name}: {e}")
            continue
    
    context.log.info("=" * 70)
    context.log.info(f"ODS Merge Complete: {len(results)} tables")
    context.log.info(f"  FULL_RESET: {mode_stats.get('FULL_RESET', 0)} tables")
    context.log.info(f"  FULL: {mode_stats.get('FULL', 0)} tables")
    context.log.info(f"  INCREMENTAL: {mode_stats.get('INCREMENTAL', 0)} tables")
    context.log.info(f"  Total rows affected: {total_rows:,}")
    context.log.info("=" * 70)
    
    # Metadata Dagster
    yield Output(
        {
            'tables_merged': len(results),
            'total_rows': total_rows,
            'results': results,
            'mode_stats': mode_stats,
            'run_id': run_id
        },
        metadata={
            "tables_count": len(results),
            "total_rows_affected": total_rows,
            "run_id": run_id,
            "full_reset_count": mode_stats.get('FULL_RESET', 0),
            "full_count": mode_stats.get('FULL', 0),
            "incremental_count": mode_stats.get('INCREMENTAL', 0),
            "tables_detail": MetadataValue.md(
                "\n".join([
                    f"- **{r['table']}** ({r['mode']}): "
                    f"{r['rows_affected']:,} rows → "
                    f"ODS total: {r['total_ods']:,} "
                    f"{'✓' if r['verified'] else '✗'}"
                    for r in results
                ])
            )
        }
    )


# ============================================================================
# ASSET 5: ODS Summary (Vue d'ensemble)
# ============================================================================

@asset(
    name="ods_pipeline_summary",
    group_name="ods",
    description="Résumé complet du pipeline ODS",
    compute_kind="metadata",
    deps=["ods_tables_merged"]
)
def ods_pipeline_summary(
    context: AssetExecutionContext,
    ods_tables_merged: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Asset 5: Résumé complet du pipeline ODS
    
    Génère statistiques d'exécution pour monitoring
    
    Returns:
        dict: Résumé détaillé
    """
    context.log.info("=" * 70)
    context.log.info("PIPELINE ODS SUMMARY")
    context.log.info("=" * 70)
    
    import psycopg2
    from shared.config import config
    
    # Récupérer stats ODS depuis PostgreSQL
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter tables ODS
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'ods'
        """)
        total_ods_tables = cur.fetchone()[0]
        
        # Compter lignes totales ODS
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables 
            WHERE table_schema = 'ods'
        """)
        
        total_ods_rows = 0
        for (table,) in cur.fetchall():
            try:
                cur.execute(f"SELECT COUNT(*) FROM ods.{table}")
                total_ods_rows += cur.fetchone()[0]
            except:
                pass
        
        summary = {
            'run_id': ods_tables_merged.get('run_id'),
            'tables_merged': ods_tables_merged['tables_merged'],
            'total_ods_tables': total_ods_tables,
            'total_ods_rows': total_ods_rows,
            'mode_breakdown': ods_tables_merged.get('mode_stats', {}),
            'timestamp': datetime.now().isoformat()
        }
        
        context.log.info(f"Total ODS tables: {total_ods_tables}")
        context.log.info(f"Total ODS rows: {total_ods_rows:,}")
        context.log.info(f"Run ID: {summary['run_id']}")
        context.log.info("=" * 70)
        
        return summary
        
    finally:
        cur.close()
        conn.close()