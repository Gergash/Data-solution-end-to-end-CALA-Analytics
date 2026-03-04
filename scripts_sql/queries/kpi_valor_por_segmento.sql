-- KPI: total facturado y promedio de score crediticio por Segmento y Ciudad.
-- Solo atenciones con estado 'Completado'. Crea la vista para consultas posteriores.
CREATE OR REPLACE VIEW `{{ params.bq_project }}.{{ params.bq_dataset }}.v_kpi_valor_por_segmento` AS
SELECT
  c.segmento,
  c.ciudad,
  SUM(a.valor_facturado) AS total_facturado,
  AVG(c.score_crediticio) AS promedio_score_crediticio
FROM `{{ params.bq_project }}.{{ params.bq_dataset }}.atenciones` AS a
INNER JOIN `{{ params.bq_project }}.{{ params.bq_dataset }}.clientes` AS c
  ON a.id_cliente = c.id_cliente
WHERE UPPER(TRIM(a.estado)) = 'COMPLETADO'
GROUP BY c.segmento, c.ciudad
ORDER BY c.segmento, c.ciudad;
