"""
============================================================================
Metadata Utils - Utilitaires pour métadonnées colonnes enrichies
============================================================================
"""

from typing import List, Dict, Any


def get_extent_columns(columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extraire les colonnes avec extent > 0
    
    Args:
        columns: Liste des métadonnées colonnes
    
    Returns:
        Liste des colonnes avec extent
    """
    return [col for col in columns if col.get("extent", 0) > 0]


def generate_extent_columns_list(column_name: str, extent: int) -> List[str]:
    """
    Générer liste des colonnes éclatées pour une colonne EXTENT
    
    Args:
        column_name: Nom de la colonne (ex: "zal")
        extent: Nombre d'éléments (ex: 5)
    
    Returns:
        Liste des colonnes éclatées (ex: ["zal_1", "zal_2", ..., "zal_5"])
    """
    return [f"{column_name}_{i+1}" for i in range(extent)]


def get_column_by_name(columns: List[Dict[str, Any]], column_name: str) -> Dict[str, Any] | None:
    """
    Récupérer métadonnées d'une colonne par son nom
    
    Args:
        columns: Liste des métadonnées colonnes
        column_name: Nom de la colonne
    
    Returns:
        Dict métadonnées ou None
    """
    for col in columns:
        if col.get("column_name") == column_name:
            return col
    return None


def build_create_table_sql(
    schema: str,
    table_name: str,
    columns: List[Dict[str, Any]],
    include_etl_columns: bool = True
) -> str:
    """
    Construire SQL CREATE TABLE depuis métadonnées colonnes
    
    Args:
        schema: Nom du schéma (ex: "raw")
        table_name: Nom de la table (ex: "raw_client")
        columns: Liste des métadonnées colonnes
        include_etl_columns: Si True, ajoute colonnes ETL (_loaded_at, etc.)
    
    Returns:
        SQL CREATE TABLE complet
    """
    col_definitions = []
    
    for col in columns:
        col_name = col["column_name"]
        data_type = col.get("data_type", "TEXT")
        
        # Colonnes EXTENT → VARCHAR (seront éclatées en ODS)
        if col.get("extent", 0) > 0:
            data_type = "TEXT"
        
        col_definitions.append(f'"{col_name}" {data_type}')
    
    # Ajouter colonnes ETL
    if include_etl_columns:
        col_definitions.extend([
            '"_loaded_at" TIMESTAMP DEFAULT NOW()',
            '"_source_file" TEXT',
            '"_sftp_log_id" INTEGER'
        ])
    
    columns_sql = ",\n    ".join(col_definitions)
    
    return f"""
CREATE TABLE {schema}.{table_name} (
    {columns_sql}
);
""".strip()


def get_postgres_type_from_progress(
    progress_type: str,
    width: int | None = None,
    scale: int | None = None,
    extent: int = 0
) -> str:
    """
    Mapper type Progress → PostgreSQL
    
    Args:
        progress_type: Type Progress (ex: "character", "integer", "decimal")
        width: Largeur colonne
        scale: Précision décimale
        extent: Si > 0, force VARCHAR
    
    Returns:
        Type PostgreSQL (ex: "VARCHAR(50)", "INTEGER", "NUMERIC(10,2)")
    """
    # EXTENT → VARCHAR
    if extent > 0:
        return "TEXT"
    
    # Mapping standard
    type_map = {
        "character": f"VARCHAR({width})" if width and width < 10000 else "TEXT",
        "clob": "TEXT",
        "integer": "INTEGER",
        "int64": "BIGINT",
        "decimal": f"NUMERIC({width},{scale})" if width and scale else "NUMERIC",
        "logical": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "datetime-tz": "TIMESTAMP WITH TIME ZONE",
        "recid": "BIGINT",
    }
    
    return type_map.get(progress_type.lower(), "TEXT")


def count_extent_columns(columns: List[Dict[str, Any]]) -> int:
    """
    Compter le nombre de colonnes EXTENT
    
    Args:
        columns: Liste des métadonnées colonnes
    
    Returns:
        Nombre de colonnes avec extent > 0
    """
    return sum(1 for col in columns if col.get("extent", 0) > 0)


def calculate_total_expanded_columns(columns: List[Dict[str, Any]]) -> int:
    """
    Calculer le nombre total de colonnes après éclatement EXTENT
    
    Args:
        columns: Liste des métadonnées colonnes
    
    Returns:
        Nombre total de colonnes (normales + éclatées)
    
    Example:
        Input: 220 colonnes dont 35 avec extent (total 160 éléments)
        Output: 220 - 35 + 160 = 345 colonnes
    """
    normal_cols = sum(1 for col in columns if col.get("extent", 0) == 0)
    extent_elements = sum(col.get("extent", 0) for col in columns)
    
    return normal_cols + extent_elements


def get_mandatory_columns(columns: List[Dict[str, Any]]) -> List[str]:
    """
    Extraire les colonnes obligatoires (NOT NULL)
    
    Args:
        columns: Liste des métadonnées colonnes
    
    Returns:
        Liste des noms de colonnes obligatoires
    """
    return [
        col["column_name"] 
        for col in columns 
        if col.get("is_mandatory", False)
    ]


def validate_columns_metadata(columns: List[Dict[str, Any]]) -> tuple[bool, list[str]]:
    """
    Valider la structure des métadonnées colonnes
    
    Args:
        columns: Liste des métadonnées colonnes
    
    Returns:
        (is_valid, list_of_errors)
    """
    errors = []
    
    if not columns:
        errors.append("Empty columns list")
        return (False, errors)
    
    required_fields = ["column_name", "data_type", "progress_type"]
    
    for i, col in enumerate(columns):
        for field in required_fields:
            if field not in col:
                errors.append(f"Column {i}: missing field '{field}'")
    
    return (len(errors) == 0, errors)