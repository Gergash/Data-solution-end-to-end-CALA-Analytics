# Arquitectura del Sistema: CALA Analytics (Ecosistema Data & IA)

## 1. Resumen Ejecutivo
La solución "CALA Analytics" es una plataforma integral de datos diseñada para la ingesta, procesamiento, análisis y consulta inteligente de información transaccional y operativa. Combina un robusto Data Warehouse en la nube con un motor de Inteligencia Artificial (RAG) para ofrecer respuestas contextuales basadas en documentación técnica.

## 2. Componentes de la Infraestructura

### A. Orquestación (Apache Airflow)
El sistema utiliza Airflow para gestionar la dependencia de tareas mediante tres DAGs principales:
* **DAG 1 (Ingesta Transaccional):** Extrae datos de archivos locales (`atenciones.csv`, `clientes.csv`), realiza limpieza de tipos de datos con Pandas (conversión de IDs a String y manejo de JSON) y los carga en BigQuery.
* **DAG 2 (Preproceso de Conocimiento):** Escanea la carpeta `kb/` en busca de archivos Markdown y PDF, fragmentando el texto en "chunks" optimizados para la IA.
* **DAG 3 (Actualización RAG):** Genera embeddings vectoriales y actualiza el índice FAISS para asegurar que el motor de búsqueda tenga la información más reciente.

### B. Almacenamiento y Cómputo (Google BigQuery)
Los datos se almacenan en un dataset denominado `cala_analytics` bajo el proyecto `data-solution-end-to-end-21`.
* **Optimización de Costos:** Se utiliza **Particionamiento por Día** en el campo `fecha_atencion` y **Clustering** por `id_cliente` y `estado` para evitar escaneos completos de tablas (Full Table Scans).
* **Capa de Negocio:** Se implementaron Vistas SQL para calcular KPIs de latencia, recurrencia de 30 días y valor facturado por segmento.

### C. Motor de IA (Retrieval Augmented Generation - RAG)
El sistema de consultas utiliza una arquitectura de búsqueda semántica:
* **Vector Store:** FAISS (Facebook AI Similarity Search) para indexación de alta velocidad.
* **Embeddings:** Modelos de lenguaje para convertir texto en vectores numéricos.
* **API de Consulta:** Desarrollada en FastAPI, recibe preguntas del usuario, busca el contexto más relevante en los documentos y genera una respuesta precisa citando la fuente.

## 3. Flujo de Datos (Data Pipeline)
1. **Source:** Archivos CSV/JSON en el volumen montado de Docker.
2. **Processing:** Transformación de tipos de datos `int64` a `string` en Python para compatibilidad con BigQuery.
3. **Load:** Carga mediante `load_table_from_dataframe` con configuraciones de `job_config` que respetan el particionamiento.
4. **Insights:** Consultas sobre las vistas de KPI para el Dashboard.

## 4. Decisiones de Escalabilidad
Si el volumen de datos creciera 10x, el sistema está preparado para migrar de Docker local a **Cloud Composer** (GCP) y utilizar **Vistas Materializadas** en BigQuery para reducir el tiempo de respuesta y los costos de procesamiento.