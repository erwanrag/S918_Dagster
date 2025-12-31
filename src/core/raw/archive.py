from pathlib import Path
from datetime import datetime
from typing import Dict
import shutil
import logging


def archive_and_cleanup(
    base_filename: str,
    archive_root: Path,
    incoming_data_dir: Path,
    logger: logging.Logger,
) -> Dict[str, Path]:
    """
    Archive les fichiers traités depuis Incoming/data vers Processed/YYYY-MM-DD

    Incoming/data :
        parquet/{base}.parquet
        metadata/{base}_metadata.json
        status/{base}_status.json

    Processed/YYYY-MM-DD :
        {base}.parquet
        {base}_metadata.json
        {base}_status.json
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    archive_dir = archive_root / date_str
    archive_dir.mkdir(parents=True, exist_ok=True)

    source_paths = {
        "parquet": incoming_data_dir / "parquet" / f"{base_filename}.parquet",
        "metadata": incoming_data_dir / "metadata" / f"{base_filename}_metadata.json",
        "status": incoming_data_dir / "status" / f"{base_filename}_status.json",
    }

    archived_paths: Dict[str, Path] = {}

    for file_type, source_path in source_paths.items():
        if not source_path.exists():
            logger.debug(f"⏭️  {file_type}: {source_path.name} not found")
            continue

        dest_path = archive_dir / source_path.name

        if dest_path.exists():
            logger.warning(
                f"⚠️  {dest_path.name} already archived in {archive_dir}, skipping move"
            )
            archived_paths[file_type] = dest_path
            continue

        shutil.move(str(source_path), str(dest_path))
        archived_paths[file_type] = dest_path

        size_mb = round(dest_path.stat().st_size / 1024 / 1024, 2)
        logger.info(
            f"📦 {file_type:10s}: {source_path.name:40s} → "
            f"Processed/{date_str}/{dest_path.name} ({size_mb:>6.2f} MB)"
        )

    return archived_paths
