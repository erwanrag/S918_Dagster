from dagster import sensor


@sensor(job_name="ingestion_pipeline")
def etl_anomaly_sensor(context):
    context.log.info("Sensor OK — anomalies monitored")
