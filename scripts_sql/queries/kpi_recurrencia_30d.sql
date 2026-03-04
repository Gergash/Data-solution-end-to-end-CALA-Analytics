-- Vista KPI: clientes recurrentes (≥2 atenciones en ventana 30d).
-- Corrección: Uso de UNIX_DATE para que RANGE funcione (BigQuery exige tipo numérico en ORDER BY para RANGE).
CREATE OR REPLACE VIEW `{{ params.bq_project }}.{{ params.bq_dataset }}.v_kpi_recurrencia_30d` AS
WITH ventana AS (
  SELECT id_cliente, fecha_proceso,
    COUNT(*) OVER (
      PARTITION BY id_cliente
      ORDER BY UNIX_DATE(DATE(fecha_proceso))
      RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
    ) AS atenciones_30d
  FROM `{{ params.bq_project }}.{{ params.bq_dataset }}.atenciones`
  WHERE fecha_proceso IS NOT NULL
)
SELECT fecha_proceso, COUNT(DISTINCT id_cliente) AS clientes_recurrentes_30d
FROM ventana WHERE atenciones_30d >= 2
GROUP BY fecha_proceso ORDER BY fecha_proceso;
