# Políticas de Seguridad, Privacidad y Gobernanza: CALA Analytics

## 1. Protección de Datos y Privacidad (GDPR / Ley 1581)
La plataforma "CALA Analytics" sigue los principios de privacidad por diseño en la infraestructura de Google Cloud.

* **Encriptación en Reposo:** Todos los datos almacenados en BigQuery (`atenciones`, `clientes`, `eventos_app`) están encriptados automáticamente por Google utilizando AES-256.
* **Encriptación en Tránsito:** La comunicación entre Airflow, la API FastAPI y BigQuery se realiza exclusivamente a través de túneles TLS/SSL (HTTPS).
* **Anonimización:** Para análisis de segmentación, se recomienda el uso de `documento_cliente` de forma truncada o hasheada en las vistas públicas para proteger la identidad del usuario.

## 2. Gestión de Accesos (IAM - Identity and Access Management)
El acceso a los recursos de GCP está estrictamente controlado mediante Roles de Mínimo Privilegio:
* **Service Account (SA):** El sistema utiliza una cuenta de servicio única para los DAGs de Airflow.
* **Roles Asignados:** `roles/bigquery.dataEditor` (para cargar datos) y `roles/bigquery.jobUser` (para ejecutar consultas).
* **Restricción de Llaves:** Se prohíbe el uso de llaves JSON en repositorios públicos. El archivo `key.json` debe estar incluido en el `.gitignore`.

## 3. Gobernanza de BigQuery
Para mantener la salud del Data Warehouse, se aplican las siguientes políticas operativas:
* **Retención de Datos:** Los datos en tablas particionadas tienen una política de expiración sugerida de 2 años para optimizar costos de almacenamiento a largo plazo.
* **Cuotas de Consulta:** Se prohíbe el uso de `SELECT *` en entornos de producción para evitar el consumo innecesario de cuota de escaneo de bytes.
* **Clustering Obligatorio:** Toda tabla con más de 10 millones de registros debe implementar Clustering por IDs de búsqueda frecuente para garantizar el rendimiento.

## 4. Ética y Uso de la IA (RAG)
* **Veracidad:** El motor RAG tiene prohibido "alucinar". Si la información no se encuentra en los documentos de la carpeta `kb/`, la API debe responder: "No tengo información suficiente en los manuales técnicos para responder esto".
* **Filtro de Contenido:** La API no procesa ni almacena datos sensibles de salud (PHI) en los logs de la IA para cumplir con normativas de seguridad de la información médica.