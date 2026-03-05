# FAQ Operativa y Guía de Solución de Problemas (Troubleshooting)

## 1. Preguntas sobre Carga de Datos (ETL)

### ¿Por qué el conteo de filas en BigQuery es 0 después de correr el DAG?
* **Causa 1:** Las variables `GCP_PROJECT_ID` o `BQ_PROJECT_ID` no están definidas en Airflow (Admin -> Variables). El script saltará la carga con un mensaje `[SKIP]`.
* **Causa 2:** Error de tipos de datos. Pandas lee los IDs como `int64` pero BigQuery espera `STRING`. 
* **Solución:** Asegurar que en `utils/ingestion.py` se use `dtype={'id_atencion': str}` al leer el CSV.

### ¿Cómo manejo el error "Unsupported field type: JSON"?
* **Causa:** La librería `load_table_from_dataframe` no mapea directamente objetos complejos a JSON nativo de BQ.
* **Solución:** Convertir la columna a string usando `json.dumps()` en Python y definir el campo como `STRING` en el `job_config`. BigQuery hará el cast automático al insertar.

### ¿Qué hago si sale un error de "Incompatible table partitioning"?
* **Causa:** La tabla en BigQuery fue creada con Clustering o Particionamiento, pero el Job de carga no incluye esa especificación.
* **Solución:** Actualizar el `LoadJobConfig` en el script de carga para incluir `clustering_fields=['id_cliente', 'estado']`.

## 2. Preguntas sobre Infraestructura y GCP

### ¿Cómo se autentica el sistema con Google Cloud?
El sistema utiliza una **Service Account** (Cuenta de Servicio) con roles de `BigQuery Admin`. La llave privada se almacena en `gcp-credentials/key.json`. Si el archivo no existe, las tareas de Airflow fallarán por falta de permisos.

### ¿Por qué falló la creación de la Service Account al inicio?
Debido a una política de organización de GCP llamada `iam.disableServiceAccountKeyCreation`. Se debió desactivar esta restricción a nivel de proyecto para permitir la generación del archivo JSON necesario para Docker.

## 3. Consultas y KPIs

### ¿Por qué la vista de KPIs no muestra resultados si hay datos en 'atenciones'?
Probablemente la tabla `clientes` está vacía o los IDs no coinciden. La vista usa un `INNER JOIN`, por lo que si el `id_cliente` no existe en ambas tablas, el registro desaparece del reporte. Verifica que ambos archivos CSV tengan la misma codificación de IDs.

### ¿Cómo evito costos altos al consultar BigQuery?
* Siempre usa filtros por fecha (`fecha_atencion`) para activar el particionamiento.
* Evita usar `SELECT *`. Selecciona solo las columnas necesarias.
* El sistema usa Clustering, lo que significa que filtrar por `id_cliente` es extremadamente eficiente y barato.

## 4. Errores de Conectividad y Configuración de Airflow

### ¿Por qué aparece el error "Unexpected keyword END" en BigQuery?
* **Causa:** El ID del proyecto (`data-solution-end-to-end-21`) contiene guiones. SQL interpreta los guiones como operadores de resta o confunde partes del nombre con palabras reservadas.
* **Solución:** Envolver siempre el nombre de la tabla/proyecto en comillas invertidas (backticks): `` `proyecto.dataset.tabla` ``.

### ¿Por qué la variable de entorno no se refleja en el DAG?
* **Causa:** Airflow no siempre toma los cambios del archivo `.env` o de la terminal del host de forma inmediata dentro de los contenedores Workers.
* **Solución:** Utilizar `Variable.get("GCP_PROJECT_ID")` desde el menú **Admin -> Variables** de la UI de Airflow. Es más persistente y fácil de auditar que las variables de entorno de sistema.

## 5. Errores de Transformación con Pandas y PyArrow

### ¿Por qué falla la carga con el error "Expected a string or bytes dtype, got int64"?
* **Causa:** Pandas infiere que columnas como `id_atencion` son números enteros. Sin embargo, el esquema (DDL) de BigQuery define estas columnas como `STRING`. PyArrow (el motor de carga) bloquea la operación por incompatibilidad de tipos.
* **Solución:** Forzar el tipo de datos al leer el CSV: `pd.read_csv(path, dtype={'id_atencion': str})` o convertir la columna antes de la carga: `df['id_atencion'] = df['id_atencion'].astype(str)`.

### ¿Por qué el log muestra "Done. Returned value was: None" pero no hay datos?
* **Causa:** El script de Python tiene una validación de seguridad que retorna `None` si no encuentra las credenciales o el ID del proyecto, sin lanzar una excepción (Silent Failure).
* **Solución:** Revisar los logs de Airflow buscando el mensaje `[SKIP]`. Si aparece, significa que la lógica de carga nunca se ejecutó realmente por falta de parámetros.

## 6. Errores de la API RAG y Embeddings

### ¿Por qué la API responde "No tengo información suficiente"?
* **Causa:** El archivo de base de conocimientos (`.md`) existe, pero no se ha actualizado el índice vectorial.
* **Solución:** Se deben ejecutar en orden el **DAG 2** (para procesar el texto) y el **DAG 3** (para generar los vectores en FAISS). Sin el archivo `faiss.index` actualizado, la API solo tiene memoria de los documentos viejos.

### ¿Cómo se explica la latencia de ~770ms en las respuestas?
* **Optimización:** El uso de **FAISS** (Facebook AI Similarity Search) permite realizar búsquedas en milisegundos incluso con miles de documentos. La mayor parte del tiempo de respuesta se debe a la generación de la respuesta final por el LLM, no a la búsqueda del contexto.