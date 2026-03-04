-- KPI: latencia promedio por plataforma y tipo de evento (iOS/Android/Web).
CREATE OR REPLACE VIEW `{{ params.bq_project }}.{{ params.bq_dataset }}.v_kpi_latencia_plataforma` AS
SELECT
    plataforma,
    event_name,
    AVG(latencia_ms) AS latencia_promedio,
    COUNT(*) AS volumen_eventos
FROM `{{ params.bq_project }}.{{ params.bq_dataset }}.eventos_app`
GROUP BY 1, 2
ORDER BY latencia_promedio DESC;
