-- BigQuery Standard SQL. Tabla de atenciones particionada por día y clusterizada.
-- Ejecutar con BigQueryInsertJobOperator; el DAG debe pasar params: bq_project, bq_dataset.
CREATE OR REPLACE TABLE `{{ params.bq_project }}.{{ params.bq_dataset }}.atenciones` (
  id_atencion STRING,
  id_cliente STRING,
  documento_cliente STRING,
  fecha_atencion TIMESTAMP,
  fecha_proceso TIMESTAMP,
  valor_facturado FLOAT64,
  estado STRING,
  codigo_cups STRING,
  canal_ingreso STRING,
  json_detalle JSON
)
PARTITION BY DATE(fecha_atencion)
CLUSTER BY id_cliente, estado
OPTIONS(
  description = 'Atenciones: partición por fecha_atencion, cluster por id_cliente y estado'
);
