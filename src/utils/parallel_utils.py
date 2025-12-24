"""
============================================================================
Parallel Processing Utils - Dagster ETL (16GB RAM optimized)
============================================================================
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, List, Optional, Tuple

logger = logging.getLogger(__name__)


def process_files_parallel(
    files: List[Path],
    process_func: Callable[[Path], Tuple[str, bool, Optional[str]]],
    max_workers: int = 2,
    description: str = "Processing files",
) -> dict:
    """
    Traite une liste de fichiers en parallèle avec ThreadPoolExecutor
    
    Args:
        files: Liste des fichiers à traiter
        process_func: Fonction de traitement (file_path) -> (table_name, success, error_msg)
        max_workers: Nombre de threads parallèles
        description: Description pour les logs
    
    Returns:
        dict avec 'success', 'failed', 'results'
    """
    logger.info(f"[PARALLEL] {description}: {len(files)} files avec {max_workers} workers")
    
    results = {
        "success": [],
        "failed": [],
        "total": len(files),
    }
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Soumettre tous les jobs
        future_to_file = {
            executor.submit(process_func, file_path): file_path 
            for file_path in files
        }
        
        # Collecter les résultats au fur et à mesure
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            
            try:
                table_name, success, error_msg = future.result()
                
                if success:
                    results["success"].append({
                        "file": file_path.name,
                        "table": table_name,
                    })
                    logger.info(f"[OK] {table_name} traité")
                else:
                    results["failed"].append({
                        "file": file_path.name,
                        "table": table_name,
                        "error": error_msg,
                    })
                    logger.error(f"[FAIL] {table_name}: {error_msg}")
                    
            except Exception as e:
                results["failed"].append({
                    "file": file_path.name,
                    "error": str(e),
                })
                logger.exception(f"[ERROR] Exception traitement {file_path.name}: {e}")
    
    logger.info(
        f"[PARALLEL DONE] {len(results['success'])} success, "
        f"{len(results['failed'])} failed sur {results['total']} total"
    )
    
    return results


def group_files_by_size(
    files: List[Path], 
    small_threshold_mb: float = 5.0,    # Petits fichiers <5MB
    large_threshold_mb: float = 50.0    # Gros fichiers >50MB
) -> dict:
    """
    Groupe les fichiers par taille pour optimiser la parallélisation
    
    Strategy (16GB RAM):
    - Small files (<5MB): Process with 4 workers
    - Medium files (5-50MB): Process with 2 workers
    - Large files (>50MB): Process sequentially
    
    Args:
        files: Liste des fichiers
        small_threshold_mb: Seuil petits fichiers (MB)
        large_threshold_mb: Seuil gros fichiers (MB)
    
    Returns:
        dict {"small": [], "medium": [], "large": []}
    """
    groups = {"small": [], "medium": [], "large": []}
    
    for file_path in files:
        size_mb = file_path.stat().st_size / (1024 * 1024)
        
        if size_mb < small_threshold_mb:
            groups["small"].append(file_path)
        elif size_mb < large_threshold_mb:
            groups["medium"].append(file_path)
        else:
            groups["large"].append(file_path)
    
    logger.info(
        f"[GROUPING] Small: {len(groups['small'])}, "
        f"Medium: {len(groups['medium'])}, Large: {len(groups['large'])}"
    )
    
    return groups


def process_files_by_size_strategy(
    files: List[Path],
    process_func: Callable[[Path], Tuple[str, bool, Optional[str]]],
) -> dict:
    """
    Traite les fichiers avec stratégie optimisée par taille (16GB RAM)
    
    Args:
        files: Liste des fichiers
        process_func: Fonction de traitement
    
    Returns:
        dict avec résultats agrégés
    """
    groups = group_files_by_size(files)
    
    all_results = {"success": [], "failed": [], "total": len(files)}
    
    # 1. Petits fichiers en parallèle (4 workers)
    if groups["small"]:
        logger.info(f"[STRATEGY] Processing {len(groups['small'])} small files (4 workers)")
        results = process_files_parallel(
            groups["small"],
            process_func,
            max_workers=4,
            description="Small files (<5MB)"
        )
        all_results["success"].extend(results["success"])
        all_results["failed"].extend(results["failed"])
    
    # 2. Fichiers moyens en parallèle (2 workers)
    if groups["medium"]:
        logger.info(f"[STRATEGY] Processing {len(groups['medium'])} medium files (2 workers)")
        results = process_files_parallel(
            groups["medium"],
            process_func,
            max_workers=2,
            description="Medium files (5-50MB)"
        )
        all_results["success"].extend(results["success"])
        all_results["failed"].extend(results["failed"])
    
    # 3. Gros fichiers séquentiels (1 worker)
    if groups["large"]:
        logger.info(f"[STRATEGY] Processing {len(groups['large'])} large files (sequential)")
        results = process_files_parallel(
            groups["large"],
            process_func,
            max_workers=1,
            description="Large files (>50MB)"
        )
        all_results["success"].extend(results["success"])
        all_results["failed"].extend(results["failed"])
    
    return all_results