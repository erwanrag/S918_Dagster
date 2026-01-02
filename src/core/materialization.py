"""
Module unifié pour AssetMaterializations riches
Utilisable dans tous les assets (ingestion, raw, staging, ods)
"""

from dagster import AssetMaterialization, MetadataValue, AssetKey
from typing import Dict, Any, Optional, List
import time
from datetime import datetime
from enum import Enum


class AssetLayer(str, Enum):
    """Couches du pipeline"""
    INGESTION = "ingestion"
    RAW = "raw"
    STAGING = "staging"
    ODS = "ods"
    PREP = "prep"
    MARTS = "marts"


class MaterializationBuilder:
    """
    Builder pour créer des AssetMaterializations riches
    
    Usage:
        builder = MaterializationBuilder(AssetLayer.RAW, "lisval1")
        builder.with_volumetry(rows_loaded=1000, rows_failed=5)
        builder.with_performance()
        yield builder.build()
    """
    
    def __init__(self, layer: AssetLayer, table_name: str):
        self.layer = layer
        self.table_name = table_name
        self.metadata: Dict[str, MetadataValue] = {}
        self.description = f"Processed {table_name}"
        self.start_time = time.time()
        
        # Tracking
        self.rows_loaded = 0
        self.rows_failed = 0
        self.source_rows = 0
    
    def with_volumetry(
        self, 
        rows_loaded: int, 
        rows_failed: int = 0, 
        source_rows: Optional[int] = None
    ):
        """Ajoute métriques volumétrie"""
        self.rows_loaded = rows_loaded
        self.rows_failed = rows_failed
        self.source_rows = source_rows or (rows_loaded + rows_failed)
        
        total = rows_loaded + rows_failed
        success_rate = (rows_loaded / total * 100) if total > 0 else 0.0  # ✅ 0.0 au lieu de 0
        
        self.metadata.update({
            "rows_loaded": MetadataValue.int(rows_loaded),
            "rows_failed": MetadataValue.int(rows_failed),
            "total_rows_processed": MetadataValue.int(total),
            "success_rate_pct": MetadataValue.float(float(round(success_rate, 2))),  # ✅ Forcer float()
        })
        
        if source_rows and source_rows != total:
            delta = rows_loaded - source_rows
            self.metadata["row_delta"] = MetadataValue.int(delta)
        
        return self
    
    def with_performance(self, duration_seconds: Optional[float] = None):
        """Ajoute métriques performance"""
        duration = duration_seconds or (time.time() - self.start_time)
        throughput = self.rows_loaded / duration if duration > 0 else 0
        
        self.metadata.update({
            "duration_seconds": MetadataValue.float(round(duration, 2)),
            "throughput_rows_per_sec": MetadataValue.int(int(throughput)),
        })
        return self
    
    def with_source(self, source_path: str, file_size_mb: Optional[float] = None):
        """Ajoute info source"""
        self.metadata["source_file"] = MetadataValue.path(source_path)
        if file_size_mb:
            self.metadata["file_size_mb"] = MetadataValue.float(round(file_size_mb, 2))
        return self
    
    def with_quality(
        self, 
        quality_score: Optional[float] = None,
        quality_passed: bool = True,
        quality_issues: int = 0,
        quality_report_path: Optional[str] = None
    ):
        """Ajoute métriques qualité"""
        if quality_score is not None:
            emoji = "🟢" if quality_score > 0.9 else "🟡" if quality_score > 0.7 else "🔴"
            self.metadata.update({
                "quality_score": MetadataValue.float(round(quality_score, 3)),
                "quality_status": MetadataValue.text(f"{emoji} {quality_score:.1%}"),
            })
        
        self.metadata.update({
            "quality_passed": MetadataValue.bool(quality_passed),
            "quality_issues_count": MetadataValue.int(quality_issues),
        })
        
        if quality_report_path:
            self.metadata["quality_report"] = MetadataValue.url(
                f"file://{quality_report_path}"
            )
        
        return self
    
    def with_load_mode(self, load_mode: str):
        """Ajoute mode de chargement"""
        self.metadata["load_mode"] = MetadataValue.text(load_mode)
        return self
    
    def with_extent_columns(self, extent_count: int):
        """Ajoute nombre de colonnes extent (spécifique Progress)"""
        if extent_count > 0:
            self.metadata["extent_columns_count"] = MetadataValue.int(extent_count)
        return self
    
    def with_consolidation_info(
        self, 
        is_consolidated: bool,
        original_files_count: Optional[int] = None
    ):
        """Indique si fichier consolidé"""
        if is_consolidated:
            self.metadata["consolidated"] = MetadataValue.bool(True)
            if original_files_count:
                self.metadata["original_files_count"] = MetadataValue.int(original_files_count)
        return self
    
    def with_error(self, error_message: str):
        """Ajoute info erreur"""
        self.metadata["error"] = MetadataValue.text(error_message[:500])  # Limite 500 chars
        self.metadata["status"] = MetadataValue.text("❌ FAILED")
        return self
    
    def build(self) -> AssetMaterialization:
        """Construit l'AssetMaterialization finale"""
        
        # Auto-add timestamp
        self.metadata["load_timestamp"] = MetadataValue.text(
            datetime.now().isoformat()
        )
        
        # Auto-build summary
        summary = self._build_summary()
        self.metadata["summary"] = MetadataValue.md(summary)
        
        return AssetMaterialization(
            asset_key=AssetKey([self.layer.value, self.table_name]),
            description=self.description,
            metadata=self.metadata
        )
    
    def _build_summary(self) -> str:
        """Construit tableau récapitulatif Markdown"""
        total = self.rows_loaded + self.rows_failed
        success_rate = (self.rows_loaded / total * 100) if total > 0 else 0
        duration = self.metadata.get("duration_seconds", MetadataValue.float(0.0)).value
        throughput = self.metadata.get("throughput_rows_per_sec", MetadataValue.int(0)).value
        
        lines = [
            f"### 📊 {self.layer.value.upper()} - {self.table_name}",
            "",
            "| Métrique | Valeur |",
            "|----------|--------|",
            f"| ✅ Lignes chargées | {self.rows_loaded:,} |",
        ]
        
        if self.rows_failed > 0:
            lines.append(f"| ❌ Lignes en erreur | {self.rows_failed:,} |")
        
        lines.extend([
            f"| 📊 Total traité | {total:,} |",
            f"| 🎯 Taux de succès | {success_rate:.2f}% |",
        ])
        
        if duration > 0:
            lines.append(f"| ⏱️ Durée | {duration:.2f}s |")
            lines.append(f"| ⚡ Throughput | {int(throughput):,} rows/sec |")
        
        # Qualité
        if "quality_passed" in self.metadata:
            quality_emoji = "✅" if self.metadata["quality_passed"].value else "⚠️"
            issues = self.metadata.get("quality_issues_count", MetadataValue.int(0)).value
            lines.append(f"| 🔍 Qualité | {quality_emoji} ({issues} issues) |")
        
        # Consolidation
        if "consolidated" in self.metadata:
            orig_count = self.metadata.get("original_files_count", MetadataValue.int(0)).value
            lines.append(f"| 📦 Consolidé | Oui (depuis {orig_count} fichiers) |")
        
        return "\n".join(lines)


# ========== SHORTCUTS ==========

def create_raw_materialization(
    table_name: str,
    rows_loaded: int,
    rows_failed: int = 0,
    source_path: str = None,
    load_mode: str = "FULL",
    extent_columns: int = 0,
    quality_passed: bool = True,
    quality_issues: int = 0,
    is_consolidated: bool = False,
    original_files_count: int = 0,
    **kwargs
) -> AssetMaterialization:
    """
    Shortcut pour créer materialization RAW layer
    
    Usage:
        mat = create_raw_materialization(
            table_name="lisval1",
            rows_loaded=1000,
            source_path="/sftp/lisval1.parquet",
            load_mode="FULL"
        )
        yield mat
    """
    builder = MaterializationBuilder(AssetLayer.RAW, table_name)
    
    builder.with_volumetry(rows_loaded, rows_failed)
    builder.with_performance()
    builder.with_load_mode(load_mode)
    
    if source_path:
        builder.with_source(source_path)
    
    if extent_columns > 0:
        builder.with_extent_columns(extent_columns)
    
    if quality_issues > 0 or not quality_passed:
        builder.with_quality(
            quality_passed=quality_passed,
            quality_issues=quality_issues
        )
    
    if is_consolidated:
        builder.with_consolidation_info(
            is_consolidated=True,
            original_files_count=original_files_count
        )
    
    return builder.build()