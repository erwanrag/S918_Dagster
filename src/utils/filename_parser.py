def parse_and_resolve(
    file_name: str,
    metadata: dict,
    strict: bool = False
) -> dict:
    """
    Parse nom fichier et résout TableName vs ConfigName
    
    Returns:
        {
            "table_identifier": str,
            "table_name": str,
            "config_name": str | None,
            "physical_name": str,  # config_name si existe, sinon table_name
            "is_valid": bool,
            "validation_message": str | None
        }
    """
    table_identifier = file_name.split('_')[0]
    table_name = metadata.get('table_name', table_identifier)
    config_name = metadata.get('config_name')
    
    # ✅ Logique physique
    physical_name = config_name if config_name else table_name
    
    return {
        "table_identifier": table_identifier,
        "table_name": table_name,
        "config_name": config_name,
        "physical_name": physical_name,
        "is_valid": True,
        "validation_message": None
    }