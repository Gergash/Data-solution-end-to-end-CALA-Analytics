-- Log de eventos de la aplicación. Particionamiento por fecha_evento es vital (volumen alto, ahorro y rendimiento).
CREATE OR REPLACE TABLE `{{ params.bq_project }}.{{ params.bq_dataset }}.eventos_app` (
    event_id STRING,
    id_cliente STRING,
    event_name STRING,
    plataforma STRING,
    fecha_evento TIMESTAMP,
    version_app STRING,
    ip_origen STRING,
    latencia_ms FLOAT64
)
PARTITION BY DATE(fecha_evento)
CLUSTER BY event_name, id_cliente
OPTIONS(
    description = 'Log de eventos de la aplicación particionado por día'
);
