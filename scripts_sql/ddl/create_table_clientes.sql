-- BigQuery Standard SQL. Tabla de clientes clusterizada.
-- Ejecutar con BigQueryInsertJobOperator; el DAG debe pasar params: bq_project, bq_dataset.
CREATE OR REPLACE TABLE `{{ params.bq_project }}.{{ params.bq_dataset }}.clientes` (
  id_cliente STRING,
  documento STRING,
  fecha_registro TIMESTAMP,
  segmento STRING,
  ciudad STRING,
  score_crediticio INT64
)
CLUSTER BY ciudad, segmento
OPTIONS(
  description = 'Clientes: cluster por ciudad y segmento'
);
