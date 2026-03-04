-- Vista KPI: clientes recurrentes (≥2 atenciones en ventana 30d). Sin SQL embebido en Python.
CREATE OR REPLACE VIEW `{{ params.bq_project }}.{{ params.bq_dataset }}.v_kpi_recurrencia_30d` AS
WITH ventana AS (
  SELECT id_cliente, fecha_proceso,
    COUNT(*) OVER (
      PARTITION BY id_cliente
      ORDER BY DATE(fecha_proceso)
      RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) AS atenciones_30d
  FROM `{{ params.bq_project }}.{{ params.bq_dataset }}.atenciones`
  WHERE fecha_proceso IS NOT NULL
)
SELECT fecha_proceso, COUNT(DISTINCT id_cliente) AS clientes_recurrentes_30d
FROM ventana WHERE atenciones_30d >= 2
GROUP BY fecha_proceso ORDER BY fecha_proceso;
