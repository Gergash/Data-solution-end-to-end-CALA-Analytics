# Diccionario de Datos y Definiciones de Negocio: CALA Analytics

## 1. Entidades Principales (Tablas Base)

### Tabla: atenciones
Registro de cada interacción de servicio con un cliente.
* **id_atencion (STRING):** Identificador único del registro (Convertido de int64 para compatibilidad).
* **id_cliente (STRING):** Llave foránea que conecta con la tabla de clientes (Clustered).
* **fecha_atencion (DATE):** Campo de particionamiento diario para optimización de costos.
* **valor_facturado (FLOAT):** Monto económico de la atención.
* **json_detalle (JSON):** Campo semi-estructurado con telemetría técnica de la plataforma.

### Tabla: clientes
Maestro de usuarios de la plataforma.
* **id_cliente (STRING):** Identificador único del cliente.
* **segmento (STRING):** Categorización comercial (Ej: 'Particular', 'Convenio', 'EPS').
* **ciudad (STRING):** Ubicación geográfica para análisis de densidad.

## 2. Lógica de KPIs (Vistas de Negocio)

### KPI: Valor por Segmento (`v_kpi_valor_por_segmento`)
* **Definición:** Sumatoria del `valor_facturado` agrupado por la ciudad y el segmento del cliente.
* **Utilidad:** Identificar qué regiones y qué tipos de clientes generan el mayor ingreso bruto.
* **Filtro Técnico:** Requiere un `INNER JOIN` exitoso entre `atenciones` y `clientes`.

### KPI: Recurrencia a 30 Días (`v_kpi_recurrencia_30d`)
* **Definición:** Porcentaje de clientes que tienen más de una atención en un periodo de 30 días corridos.
* **Cálculo:** Utiliza funciones de ventana `COUNT(id_atencion) OVER(PARTITION BY id_cliente)` filtrando por fechas relativas.

### KPI: Latencia de Plataforma (`v_kpi_latencia_plataforma`)
* **Definición:** Tiempo promedio de respuesta extraído del campo `json_detalle`.
* **Procesamiento:** Utiliza la función `JSON_EXTRACT_SCALAR` de BigQuery para convertir datos anidados en métricas de rendimiento.

## 3. Glosario Técnico de la Solución
* **Full Table Scan:** Error de eficiencia donde se lee toda la tabla. Se evita usando `WHERE fecha_atencion = 'YYYY-MM-DD'`.
* **Embeddings:** Representación numérica de estas definiciones que permite a la IA realizar búsquedas semánticas.
* **Clustering:** Organización de datos por `id_cliente` para acelerar búsquedas de historial médico/atenciones.