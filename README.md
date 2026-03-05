# CALA Analytics — Solución de Datos End-to-End

**Autor:** Gerónimo Saldaña Espinal
**Tecnologías:** GCP · BigQuery · Apache Airflow · Docker · Python · FastAPI · RAG (FAISS)
**Estado:** Producción / MVP Concluido

---

## Visión General

CALA Analytics es una plataforma integral que combina Ingeniería de Datos tradicional con Inteligencia Artificial Generativa. Su propósito es escalar, automatizar y extraer valor tanto de datos transaccionales como de documentación técnica, ofreciendo una solución 360° que abarca desde la ingesta de datos hasta un asistente conversacional basado en RAG (Retrieval Augmented Generation).

La solución se sustenta en tres capas: una capa de **orquestación** gestionada con Apache Airflow, un **Data Warehouse** en Google BigQuery y un **motor de IA** construido con FastAPI y FAISS como base de datos vectorial. Juntas, estas capas garantizan la integridad, disponibilidad y capacidad de razonamiento sobre los datos.

---

## Decisiones Técnicas Clave

La elección de **Google Cloud Platform (GCP)** responde a su capacidad nativa de escalar BigQuery de forma serverless y a su robusta gestión de identidades mediante IAM. La infraestructura local se contenerizó con **Docker Compose**, levantando servicios independientes para Airflow (Scheduler, Worker, Webserver, Postgres y Redis) y asegurando paridad total entre los entornos de desarrollo y producción.

Para el formato de datos, se optó por **CSV** en datos estructurados y **JSON** para telemetría; esto permite a BigQuery aprovechar sus funciones nativas de `JSON_EXTRACT` para analítica de eventos sin pre-procesamiento costoso. En la capa de IA, **FAISS** fue seleccionado por su bajísima latencia en búsquedas de similitud (~770 ms), lo que habilita respuestas casi instantáneas desde el Dashboard.

---

## Arquitectura del Sistema

### Orquestación de Datos (Apache Airflow)

Airflow gestiona los pipelines de datos mediante DAGs que coordinan desde la carga inicial de archivos hasta el cálculo de KPIs complejos. Tres DAGs componen el flujo:

- **`dag_1_ingestion_transaccional.py`** — Pipeline principal de ETL. Limpia los datos de origen, crea la infraestructura necesaria en BigQuery y carga los registros transaccionales.
- **`dag_2_preprocesamiento_ia.py`** — Prepara los documentos de la base de conocimiento (`kb/`) para que el motor de IA pueda procesarlos.
- **`dag_3_actualizacion_rag.py`** — Genera los embeddings vectoriales y actualiza el índice FAISS, garantizando que la API siempre opere con información fresca.

La lógica pesada reside en módulos de soporte dentro de `utils/`:

- **`ingestion.py`** contiene las transformaciones de Python (conversión de IDs a string, manejo de formato JSON) que previenen errores de carga en BigQuery.
- **`rag.py`** gestiona la búsqueda semántica y la interacción con el modelo de lenguaje.

### Data Warehouse (Google BigQuery)

BigQuery actúa como el núcleo de verdad para los datos estructurados. La tabla principal de `atenciones` está optimizada mediante:

- **Particionamiento por día** sobre el campo `fecha_atencion`, de modo que una consulta sobre una semana específica solo escanea los fragmentos correspondientes a esos 7 días.
- **Clustering** por `id_cliente` y `estado`, que organiza físicamente los datos dentro de cada partición y permite a BigQuery localizar bloques exactos al buscar la recurrencia de un cliente, reduciendo drásticamente los bytes procesados.

Los scripts SQL se organizan en dos carpetas:

- **`ddl/create_table_atenciones.sql`** — Define la estructura física, incluyendo particionamiento y clustering.
- **`queries/kpi_recurrencia_30d.sql`** — Lógica SQL avanzada con funciones de ventana para detectar clientes recurrentes.

### Motor de IA (RAG)

El servidor FastAPI (`main.py`) expone el endpoint `/ask`, que recibe preguntas en lenguaje natural. Al recibir una consulta, el sistema busca en el índice vectorial (`faiss.index`) los fragmentos más relevantes de los documentos Markdown almacenados en `kb/` (`arquitectura_actual.md` y `faq_operativa.md`), y devuelve una respuesta con citas bibliográficas en aproximadamente 770 ms.

---

## Flujo de Datos

1. **Ingesta:** Airflow toma los archivos `atenciones.csv` y `eventos_app.json`, los transforma mediante `ingestion.py` y los carga en BigQuery.
2. **Visualización:** Consultas en BigQuery extraen KPIs a partir de las vistas generadas, alimentando dashboards de seguimiento.
3. **Razonamiento:** Ante una pregunta del usuario, la API consulta el `faiss.index`, extrae fragmentos de la base de conocimiento y devuelve una respuesta fundamentada con una latencia de ~770 ms.

---

## Estructura del Repositorio

```
CALA ANALYTICS/
│
├── config/
│   └── .gitkeep
│
├── dags/
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── github_commit.py
│   │   ├── ia.py
│   │   ├── ingestion.py
│   │   └── rag.py
│   ├── .gitkeep
│   ├── dag_1_ingestion_transaccional.py
│   ├── dag_2_preprocesamiento_ia.py
│   ├── dag_3_actualizacion_rag.py
│   ├── pipeline_common.cpython...
│   └── pipeline_entrenamiento_ia.py
│
├── Dataset_Prueba_Tecnica_GCP.../
│   ├── kb/
│   │   ├── arquitectura_actual.md
│   │   ├── definiciones.md
│   │   ├── faq_operativa.md
│   │   ├── glosario_eventos.md
│   │   └── politicas.md
│   ├── atenciones.csv
│   ├── clientes.csv
│   └── eventos_app.json
│
├── embeddings/
│   ├── .gitkeep
│   ├── faiss.index
│   └── metadata.json
│
├── gcp-credentials/
│   ├── .gitkeep
│   └── key.json
│
├── kb/                              # Base de conocimiento activa
│
├── logs/
│   ├── cleaned/
│   │   ├── atenciones.csv
│   │   └── clientes.csv
│   ├── dag_id=dag_1_ingestion_tra.../
│   │   └── run_id=manual__2026-03-.../
│   │       ├── task_id=cargar_bigquery_.../
│   │       ├── task_id=ejecutar_sql_kpi_.../
│   │       └── task_id=limpiar_y_estand.../
│   ├── dag_id=dag_2_preprocesam.../
│   │   └── run_id=manual__2026-03-.../
│   ├── dag_id=dag_3_actualizacion.../
│   │   └── run_id=manual__2026-03-.../
│   ├── dag_processor_manager/
│   │   └── dag_processor_manager.log
│   └── scheduler/
│       ├── 2026-03-03/
│       ├── 2026-03-04/
│       └── 2026-03-05/
│
├── pipeline_ia_artefactos/
│   ├── indices.pt
│   └── vocab.pt
│
├── plugins/
│   └── .gitkeep
│
├── scripts_sql/
│   ├── ddl/
│   │   ├── create_table_atenciones.sql
│   │   ├── create_table_clientes.sql
│   │   └── create_table_eventos_app.sql
│   └── queries/
│       ├── kpi_recurrencia_30d.sql
│       ├── kpi_rendimiento_plataforma.sql
│       └── kpi_valor_por_segmento.sql
│
├── .env
├── .env.example
├── .gitignore
├── .gitkeep
├── airflow.cfg
├── airflow-webserver.pid
├── ask_audit.log
├── copy_embeddings_from_cont...
├── docker-compose.yaml
├── Dockerfile
├── gcp-key.json
├── index.html
├── kb_chunks.json
├── main.py
├── metrics.json
├── razonamiento.json
├── README.md
├── requirements.txt
└── webserver_config.py
```

---

## Despliegue

### Despliegue en GCP (VM + Docker)

**1. Habilitación de APIs.** Se activan los servicios esenciales de Google: Compute Engine API (host del backend y Airflow), BigQuery API (almacenamiento y procesamiento) y Secret Manager (gestión segura de `key.json` y API Keys de OpenAI/Gemini).

**2. Gestión de Identidades (IAM).** Se configura una Cuenta de Servicio con el principio de mínimo privilegio, asignándole los roles de *Administrador de BigQuery* (crear tablas, vistas y ejecutar jobs) y *Usuario de Storage* (para una eventual escalada hacia carga desde buckets de GCS). Como nota de seguridad: se deshabilitó la política de organización `iam.disableServiceAccountKeyCreation` a nivel de proyecto para permitir la generación de llaves JSON necesarias para la autenticación de contenedores externos.

**3. Conectividad y Red.** El stack se despliega en una instancia de VM en Compute Engine (e2-medium o superior):

1. Instalar Docker y Docker Compose en la instancia.
2. Clonar el repositorio: `git clone https://github.com/Gergash/Data-solution-end-to-end-CALA-Analytics`.
3. Crear el archivo `.env` con las credenciales de GCP y las llaves de IA.
4. Levantar el stack: `docker compose up -d`.

**4. Acceso.** Se configuran reglas de Firewall en la consola de GCP para permitir tráfico entrante en los puertos **8080** (interfaz web de Airflow), **8000** (endpoint de la API FastAPI) y, de forma opcional, el dashboard de visualización en GitHub Pages.

### Despliegue en Cloud Composer (Airflow Administrado)

Para escenarios que requieran alta disponibilidad y escalabilidad automática sin gestionar la infraestructura de Docker, la alternativa es **Cloud Composer**, el servicio administrado de Airflow en GCP.

**Preparación del entorno.** Se crea un entorno de Cloud Composer (v2 o v3) desde la consola de GCP, lo que levanta automáticamente un clúster de GKE donde corre Airflow. Se dimensionan los recursos del Web Server, Scheduler y Workers según la carga esperada.

**Migración de DAGs y dependencias.** Al crear el entorno, Google genera un bucket de Cloud Storage. Los archivos `.py` de los DAGs se suben a la carpeta `/dags` de ese bucket; las carpetas de soporte (`utils/`, `scripts_sql/`) se replican en la misma estructura. Las dependencias de Python (`pandas`, `google-cloud-bigquery`, `openai`, `faiss-cpu`) se especifican en `requirements.txt` dentro de la configuración del entorno.

**Datos y credenciales.** Los archivos de datos locales se migran a un bucket de GCS y los DAGs se actualizan para leer desde `gs://nombre-bucket/` en lugar de `/opt/airflow/`. No es necesario usar un archivo `key.json`: el entorno de Composer utiliza una Cuenta de Servicio de Usuario vinculada a la instancia de GKE, con permisos de `BigQuery Admin` y `Storage Admin` asignados de forma nativa.

---

## Optimización de Costos y Rendimiento en BigQuery

En BigQuery, el costo se determina principalmente por la cantidad de datos escaneados en cada consulta. Un diseño ineficiente que provoque *Full Table Scans* puede disparar los costos de forma exponencial conforme el volumen de datos crece.

Las estrategias implementadas para evitarlo son el **particionamiento por día** y el **clustering por cliente y estado**, descritos anteriormente. Ante un escenario de crecimiento **10x** (de GB a TB), se contemplan medidas adicionales:

- **Vistas Materializadas** para KPIs frecuentes (como el valor por segmento), almacenando resultados pre-calculados y evitando reprocesar las tablas base en cada consulta.
- **Filtros obligatorios** por la columna de partición (`fecha_atencion`), implementados en el código de la aplicación o vía políticas de BigQuery.
- **Tipos de datos eficientes:** usar `DATE` nativo en lugar de `STRING` para fechas, reduciendo el tamaño de almacenamiento y procesamiento.
- **Almacenamiento a largo plazo:** BigQuery reduce automáticamente el costo de almacenamiento en un 50% para datos sin modificaciones en 90 días, lo cual favorece la retención histórica del proyecto.

---

## Resultados

La integración de estas tres capas permite una solución que:

- **Reduce la latencia** en la toma de decisiones mediante KPIs automáticos calculados por Airflow y servidos desde BigQuery.
- **Optimiza costos operativos** gracias a un diseño de base de datos consciente del consumo (particionamiento, clustering y estrategias de escalabilidad).
- **Habilita el autoservicio de información técnica** a través de un asistente de IA confiable, capaz de responder preguntas en lenguaje natural con citas bibliográficas y una latencia inferior a un segundo.