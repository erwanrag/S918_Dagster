def detect_anomalies(stat):
    anomalies = []

    if stat.rows_source > 0 and stat.rows_loaded == 0:
        anomalies.append({
            "level": "ERROR",
            "metric": "rows_loaded",
            "message": "Source has rows but nothing loaded",
        })

    if stat.rows_loaded > stat.rows_source * 1.2:
        anomalies.append({
            "level": "WARNING",
            "metric": "volume_spike",
            "message": "Loaded rows unusually high",
        })

    if stat.duration_sec > 600:
        anomalies.append({
            "level": "WARNING",
            "metric": "duration",
            "message": "Table processing very slow",
        })

    return anomalies
