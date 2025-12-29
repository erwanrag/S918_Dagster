"""Ops pour importer métadonnées Progress"""
from dagster import op, Out, DynamicOut, DynamicOutput, In  # ← Ajouter In
from pathlib import Path
import json

@op(
    name="scan_metadata_files",
    out=DynamicOut(dict),  # ← Changé de Path à dict
    description="Scanner fichiers metadata.json"
)
def scan_metadata_files_op(context):
    """Scan répertoire metadata et yield chaque fichier"""
    from src.config.settings import get_settings
    settings = get_settings()
    
    metadata_dir = settings.sftp_metadata_dir  # ← Utiliser settings
    
    if not metadata_dir.exists():
        context.log.warning(f"Metadata dir not found: {metadata_dir}")
        return
    
    json_files = list(metadata_dir.glob("*.metadata.json"))
    context.log.info(f"Found {len(json_files)} metadata files")
    
    for file_path in json_files:
        yield DynamicOutput(
            {"path": str(file_path)},  # ← Dict au lieu de Path
            mapping_key=file_path.stem
        )


@op(
    name="load_metadata_file",
    ins={"file_info": In(dict)},  # ← Changé de file_path
    out=Out(dict),
    required_resource_keys={"postgres"},
    description="Charger un fichier metadata dans PostgreSQL"
)
def load_metadata_file_op(context, file_info: dict) -> dict:
    """Charge metadata.json dans metadata.proginovcolumns"""
    file_path = Path(file_info["path"])
    
    with open(file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    table_name = metadata.get("table_name")
    columns = metadata.get("columns", [])
    
    with context.resources.postgres.get_connection() as conn:
        cur = conn.cursor()
        
        for col in columns:
            cur.execute("""
                INSERT INTO metadata.proginovcolumns (
                    "TableName", "ColumnName", "DataType",
                    "IsMandatory", "Width", "Scale", "ProgressOrder"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("TableName", "ColumnName")
                DO UPDATE SET
                    "DataType" = EXCLUDED."DataType",
                    "IsMandatory" = EXCLUDED."IsMandatory",
                    "Width" = EXCLUDED."Width",
                    "Scale" = EXCLUDED."Scale"
            """, (
                table_name,
                col["name"],
                col["type"],
                col.get("mandatory", False),
                col.get("width"),
                col.get("scale"),
                col.get("order", 0)
            ))
        
        conn.commit()
    
    context.log.info(f"Loaded metadata: {table_name} ({len(columns)} cols)")
    return {"table": table_name, "columns": len(columns), "path": str(file_path)}


@op(
    name="archive_metadata_file",
    ins={"result": In(dict)},  # ← Un seul input
    description="Archiver fichier metadata traité"
)
def archive_metadata_file_op(context, result: dict):
    """Déplace fichier metadata dans /processed/"""
    file_path = Path(result["path"])
    archive_dir = file_path.parent / "processed"
    archive_dir.mkdir(exist_ok=True)
    
    archive_path = archive_dir / file_path.name
    file_path.rename(archive_path)
    
    context.log.info(f"Archived: {file_path.name}")