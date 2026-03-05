# Glosario de Eventos y Telemetría: CALA Analytics

## 1. Estructura del Evento JSON
Cada registro en la tabla `eventos_app` representa una acción del usuario en la interfaz o una respuesta del servidor, capturada en formato semi-estructurado.

### Campos Principales:
* **event_id (STRING):** Identificador único del evento de telemetría.
* **timestamp (DATETIME):** Momento exacto de la ocurrencia (UTC).
* **tipo_evento (STRING):** Categoría de la acción (Ej: 'login', 'search_query', 'api_call', 'error_report').
* **plataforma (STRING):** Origen del evento (Ej: 'Web', 'Android', 'iOS').

## 2. Definición del Campo `json_detalle`
Este campo contiene un objeto anidado con métricas técnicas específicas. La vista de latencia extrae datos de aquí.

### Atributos dentro del JSON:
* **latencia_ms (NUMBER):** Tiempo de respuesta del servidor en milisegundos. 
    * *Cálculo de KPI:* Se promedia este valor para medir la salud de la plataforma.
* **status_code (NUMBER):** Código de respuesta HTTP (Ej: 200 para éxito, 500 para error).
* **user_agent (STRING):** Información del navegador o dispositivo del usuario.
* **version_app (STRING):** Versión del software que generó el evento.

## 3. Lógica de Extracción en BigQuery
Para transformar estos datos en métricas legibles, se utiliza la función `JSON_EXTRACT_SCALAR`.

**Ejemplo de Consulta Técnica:**
```sql
SELECT 
    plataforma, 
    AVG(SAFE_CAST(JSON_EXTRACT_SCALAR(json_detalle, '$.latencia_ms') AS FLOAT64)) as latencia_promedio
FROM `data-solution-end-to-end-21.cala_analytics.eventos_app`
GROUP BY 1;