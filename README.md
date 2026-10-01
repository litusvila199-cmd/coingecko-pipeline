# 🚀 CoinGecko Data Engineering Pipeline

Pipeline de **Data Engineering end-to-end** construido para extraer datos de mercado de criptomonedas desde la API de CoinGecko, almacenarlos en un Data Lake en AWS, transformarlos con Apache Spark mediante AWS Glue, consultarlos con Amazon Athena y cargarlos también en PostgreSQL como base de datos relacional.

El proyecto integra además **Apache Airflow para la orquestación**, **Docker para la ejecución local de los servicios** y **GitHub Actions para integración continua (CI)**.

El objetivo es reproducir una arquitectura de datos cercana a un entorno profesional, combinando procesamiento, almacenamiento, orquestación, analítica y buenas prácticas de desarrollo.

---

## 📌 Descripción general

El pipeline sigue este flujo:

```text
                    ┌──────────────────┐
                    │   CoinGecko API  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Python - Extract │
                    └────────┬─────────┘
                             │
                             ▼
                 ┌────────────────────────┐
                 │     Amazon S3 - RAW    │
                 │        / BRONZE        │
                 └────────────┬───────────┘
                              │
                              ▼
                 ┌────────────────────────┐
                 │      AWS Glue +        │
                 │   Apache Spark         │
                 │     Transformación     │
                 └────────────┬───────────┘
                              │
                              ▼
                 ┌────────────────────────┐
                 │   Amazon S3 - CURATED  │
                 │        / SILVER        │
                 └──────────┬───────┬─────┘
                            │       │
                  ┌─────────┘       └──────────────┐
                  ▼                                ▼
        ┌──────────────────┐              ┌─────────────────┐
        │  Amazon Athena   │              │   PostgreSQL    │
        │   SQL / análisis │              │ Base relacional │
        └──────────────────┘              └─────────────────┘


                    ┌──────────────────────┐
                    │     Apache Airflow   │
                    │      Orquestación    │
                    └──────────────────────┘
```

Airflow coordina las diferentes etapas:

```text
Extract → Transform → Load
```

---

# 🏗️ Arquitectura

La arquitectura combina componentes **cloud** y **locales**:

### AWS

- **Amazon S3** → Data Lake para almacenar los datos RAW y CURATED.
- **AWS Glue** → ejecución del proceso de transformación utilizando Apache Spark.
- **AWS Glue Data Catalog** → catálogo de metadatos de los datos almacenados en S3.
- **AWS Glue Crawler** → descubrimiento automático del esquema de los datos CURATED.
- **Amazon Athena** → consultas SQL directamente sobre los datos almacenados en S3.
- **IAM** → gestión de permisos para que Glue pueda acceder a los recursos necesarios.
- **Amazon CloudWatch** → logs generados durante la ejecución de los servicios AWS.

### Local / Docker

- **Python** → extracción y carga de datos.
- **Apache Airflow 3.0.6** → orquestación del pipeline.
- **PostgreSQL 16** → almacenamiento relacional.
- **Apache Spark** → entorno local para desarrollo y validación del procesamiento.
- **Docker / Docker Compose** → contenedorización y ejecución reproducible del entorno.

### Desarrollo y CI/CD

- **Git**
- **GitHub**
- **GitHub Actions**
- **pytest**
- Variables de entorno mediante `.env`.

---

# 🔄 Flujo del pipeline

## 1. Extract — Python + CoinGecko API

El pipeline comienza realizando una petición a la API de CoinGecko.

Se consultan datos de varias criptomonedas:

- Bitcoin
- Ethereum
- Solana
- Cardano

La extracción utiliza Python y `requests`.

Los datos obtenidos se almacenan inicialmente en formato JSON.

El proceso registra eventos importantes mediante logging y controla los errores HTTP y de conexión.

### Ejemplo conceptual

```text
CoinGecko API
      ↓
Python requests
      ↓
JSON
```

---

# 2. RAW / BRONZE — Amazon S3

Los datos extraídos se almacenan en Amazon S3 como datos RAW.

La estructura utilizada es:

```text
s3://carles-data-lake/coingecko/raw/
```

Los archivos se organizan utilizando una estructura temporal:

```text
coingecko/
└── raw/
    └── YYYY-MM-DD/
        └── HH-MM-SS/
            └── prices.json
```

Esta capa conserva los datos originales antes de aplicar transformaciones.

El objetivo de esta separación es mantener una copia de los datos originales y permitir volver a procesarlos posteriormente si fuese necesario.

---

# 3. Transform — AWS Glue + Apache Spark

La transformación se ejecuta mediante **AWS Glue**, utilizando Apache Spark.

El Job de Glue lee los datos RAW desde S3:

```text
S3 RAW
   ↓
AWS Glue
   ↓
Apache Spark
```

Durante la transformación se realizan operaciones como:

- Selección de columnas relevantes.
- Conversión de tipos de datos.
- Tratamiento de valores numéricos.
- Creación de nuevas columnas derivadas.
- Preparación de los datos para consumo analítico.

Entre las columnas utilizadas se encuentran:

```text
id
symbol
name
current_price
market_cap
market_cap_rank
total_volume
high_24h
low_24h
price_change_24h
price_change_percentage_24h
circulating_supply
total_supply
last_updated
```

También se calcula:

```text
previous_price_24h
```

a partir del precio actual y la variación de precio en las últimas 24 horas.

---

# 4. CURATED / SILVER — Amazon S3

Una vez transformados, los datos se almacenan en formato **Parquet** en una segunda capa de S3:

```text
s3://carles-data-lake/coingecko/curated/
```

El formato Parquet permite almacenar los datos de forma columnar y resulta adecuado para posteriores consultas analíticas.

La separación entre:

```text
RAW → CURATED
```

permite diferenciar los datos originales de los datos preparados para consumo.

---

# 5. AWS Glue Data Catalog

El proyecto utiliza **AWS Glue Data Catalog** como catálogo de metadatos.

El Data Catalog permite registrar información sobre los datasets almacenados en S3, como:

- estructura
- columnas
- tipos de datos
- ubicación de los archivos

Esto permite que otros servicios de AWS puedan descubrir y utilizar los datos sin tener que definir manualmente toda su estructura cada vez.

---

# 6. AWS Glue Crawler

Para automatizar el descubrimiento del esquema se utilizó un **AWS Glue Crawler**.

El crawler analiza los datos CURATED almacenados en S3 y genera los metadatos correspondientes en Glue Data Catalog.

Flujo:

```text
S3 CURATED
     ↓
Glue Crawler
     ↓
Glue Data Catalog
     ↓
Metadata / Schema
```

Esto facilita posteriormente el acceso mediante Amazon Athena.

---

# 7. Amazon Athena

**Amazon Athena** permite realizar consultas SQL directamente sobre los datos almacenados en S3, sin necesidad de cargar previamente todo el dataset en una base de datos tradicional.

La arquitectura queda:

```text
S3 CURATED
     ↓
Glue Data Catalog
     ↓
Amazon Athena
     ↓
SQL
```

Se validó el dataset mediante consultas SQL y se comprobó que Athena podía consultar correctamente los datos transformados.

Ejemplo:

```sql
SELECT *
FROM curated
LIMIT 10;
```

Esto permite utilizar S3 como Data Lake y Athena como capa de consulta analítica serverless.

---

# 8. PostgreSQL — Base de datos relacional

Además del Data Lake, el proyecto incorpora **PostgreSQL** como destino relacional.

Esto permite demostrar dos formas diferentes de consumir los mismos datos:

```text
                 S3 CURATED
                    │
             ┌──────┴──────┐
             │             │
             ▼             ▼
         Athena        PostgreSQL
        Data Lake      Relacional
```

La tabla principal es:

```text
crypto_prices
```

Incluye información como:

- identificación de la criptomoneda
- símbolo
- nombre
- precio actual
- capitalización de mercado
- ranking
- volumen
- máximos y mínimos de 24 horas
- variaciones de precio
- supply
- timestamp
- precio anterior calculado
- timestamp de registro

La tabla utiliza una clave primaria compuesta:

```text
(id, recorded_at)
```

Esto permite conservar diferentes registros de una misma criptomoneda a lo largo del tiempo.

PostgreSQL aporta una perspectiva diferente a la del Data Lake: mientras S3 conserva los datos para procesamiento y análisis, PostgreSQL proporciona un modelo relacional estructurado para consultas y consumo desde aplicaciones o herramientas que trabajan con bases de datos SQL.

---

# 9. Apache Airflow — Orquestación

Apache Airflow se utiliza como **orquestador del pipeline**.

La DAG principal es:

```text
coingecko_pipeline
```

La dependencia entre tareas es:

```text
Extract
   ↓
Transform
   ↓
Load
```

Airflow se encarga de coordinar:

1. Extracción desde CoinGecko.
2. Lanzamiento del Job de AWS Glue.
3. Carga de los datos CURATED en PostgreSQL.

La tarea de transformación utiliza `GlueJobOperator` para ejecutar el Job de AWS Glue.

De esta forma, Airflow actúa como capa de orquestación mientras AWS Glue se encarga del procesamiento distribuido.

### Arquitectura de orquestación

```text
                 Apache Airflow
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
          Extract   Transform    Load
             │         │         │
             ▼         ▼         ▼
         CoinGecko   AWS Glue   PostgreSQL
                      + Spark
```

---

# 🐳 Docker

Todo el entorno local está contenedorizado utilizando Docker Compose.

Los principales servicios son:

```text
python
postgres
spark
airflow-apiserver
airflow-scheduler
airflow-dag-processor
```

Esto permite levantar el entorno de desarrollo de forma reproducible sin instalar individualmente todos los servicios en el sistema operativo.

### PostgreSQL

PostgreSQL se ejecuta mediante:

```text
postgres:16
```

y utiliza un volumen Docker para conservar los datos entre reinicios de los contenedores.

### Airflow

Airflow utiliza varios componentes separados:

```text
Airflow API Server
       │
       ├── Scheduler
       │
       └── DAG Processor
```

El **DAG Processor** es especialmente importante en Airflow 3, ya que se encarga del procesamiento de los archivos DAG.

Los logs de Airflow también utilizan un volumen Docker compartido.

---

# 🔐 Configuración y seguridad

La configuración sensible no está almacenada directamente en el código.

Se utiliza un archivo:

```text
.env
```

para variables como:

```text
S3_BUCKET
API_URL
CRYPTO_IDS
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

El archivo `.env` está incluido en `.gitignore` y **no se sube a GitHub**.

El repositorio incluye:

```text
.env.example
```

como plantilla para reproducir la configuración.

Las credenciales AWS utilizadas durante el desarrollo local se proporcionan mediante la configuración local de AWS y no se incluyen en el repositorio.

---

# 🧪 Testing

El proyecto incluye tests automatizados utilizando **pytest**.

Se han desarrollado pruebas para las partes principales de extracción y carga.

Durante la validación final:

```text
4 tests passed
```

También se utiliza:

```bash
python -m compileall src
```

para comprobar que el código Python mantiene una sintaxis válida.

---

# ⚙️ GitHub Actions — CI

El repositorio incorpora **GitHub Actions** para integración continua.

El workflow se ejecuta en:

```text
push → main
pull_request → main
```

El proceso realiza:

```text
Checkout
   ↓
Setup Python
   ↓
Install dependencies
   ↓
Python syntax check
   ↓
pytest
```

Esto permite detectar errores de sintaxis y fallos en los tests antes de integrar cambios en `main`.

Actualmente el proyecto utiliza **CI**, no despliegue automático (CD).

---

# 📁 Estructura del proyecto

```text
coingecko-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── dags/
│   └── coingecko_pipeline.py
│
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   │
│   ├── extract/
│   │   ├── __init__.py
│   │   └── extract.py
│   │
│   ├── transform/
│   │   ├── __init__.py
│   │   └── transform.py
│   │
│   ├── load/
│   │   ├── __init__.py
│   │   └── load.py
│   │
│   └── utils/
│       ├── __init__.py
│       └── logger.py
│
├── tests/
│   ├── test_extract.py
│   └── test_load.py
│
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

La estructura sigue una separación de responsabilidades:

```text
extract  → extracción
transform → transformación
load     → carga
config   → configuración
utils    → utilidades compartidas
dags     → orquestación
tests    → pruebas
```

---

# 🛠️ Tecnologías utilizadas

| Tecnología | Uso |
|---|---|
| Python | Extracción y carga |
| CoinGecko API | Fuente de datos |
| Pandas | Lectura y preparación de datos para PostgreSQL |
| Apache Spark | Procesamiento distribuido |
| AWS Glue | Ejecución del proceso Spark en AWS |
| Amazon S3 | Data Lake |
| AWS Glue Data Catalog | Catálogo de metadatos |
| AWS Glue Crawler | Descubrimiento del esquema |
| Amazon Athena | Consultas SQL sobre S3 |
| PostgreSQL | Base de datos relacional |
| Apache Airflow | Orquestación |
| Docker | Contenedorización |
| Docker Compose | Orquestación local de contenedores |
| Git | Control de versiones |
| GitHub | Repositorio |
| GitHub Actions | Integración continua |
| pytest | Testing |

---

# 🎯 Objetivos de aprendizaje

Este proyecto fue diseñado para practicar conceptos fundamentales de **Data Engineering y Cloud**:

- Construcción de pipelines ETL.
- Consumo de APIs.
- Data Lakes.
- Arquitecturas RAW / CURATED.
- Amazon S3.
- Apache Spark.
- AWS Glue.
- Glue Data Catalog.
- Glue Crawlers.
- Amazon Athena.
- Bases de datos relacionales.
- PostgreSQL.
- Apache Airflow.
- Docker y Docker Compose.
- CI con GitHub Actions.
- Gestión de configuración mediante variables de entorno.
- Logging.
- Testing.
- Control de versiones con Git.
- Integración entre servicios locales y cloud.

---

# ☁️ Arquitectura Cloud

La arquitectura cloud utilizada durante la implementación fue:

```text
                    ┌─────────────────┐
                    │  CoinGecko API  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Python      │
                    │     Extract     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Amazon S3    │
                    │       RAW       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    AWS Glue     │
                    │ Apache Spark    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Amazon S3    │
                    │     CURATED     │
                    └───────┬─┬───────┘
                            │ │
                 ┌──────────┘ └──────────┐
                 ▼                      ▼
        ┌─────────────────┐     ┌─────────────────┐
        │    Athena       │     │   PostgreSQL    │
        │ SQL Analytics   │     │ Relational DB   │
        └─────────────────┘     └─────────────────┘

                 ▲
                 │
        ┌─────────────────┐
        │ Glue Data       │
        │ Catalog         │
        │ + Crawler       │
        └─────────────────┘
```

Airflow actúa como capa de orquestación sobre el flujo:

```text
Airflow
   │
   ├── Extract
   │
   ├── AWS Glue
   │
   └── Load → PostgreSQL
```

---

# ▶️ Ejecución local

Clonar el repositorio:

```bash
git clone <URL_DEL_REPOSITORIO>
cd coingecko-pipeline
```

Crear el archivo de configuración:

```bash
cp .env.example .env
```

Configurar las variables necesarias en `.env`.

Levantar los servicios:

```bash
docker compose up -d
```

Comprobar el estado:

```bash
docker compose ps
```

Airflow estará disponible en:

```text
http://localhost:8080
```

PostgreSQL estará disponible en:

```text
localhost:5432
```

Para detener los servicios:

```bash
docker compose stop
```

---

# ☁️ Recursos AWS

Durante el desarrollo y validación se utilizaron los siguientes recursos:

```text
Amazon S3
AWS Glue Job
AWS Glue Crawler
AWS Glue Data Catalog
Amazon Athena
IAM Role
Amazon CloudWatch
```

Los recursos específicos utilizados para la demostración del pipeline fueron eliminados posteriormente para evitar mantener servicios innecesarios activos y consumir créditos de AWS.

El bucket S3 `carles-data-lake` se conserva vacío para poder reutilizarlo en futuros proyectos.

La arquitectura y el código permanecen en el repositorio para que el proyecto pueda reproducirse nuevamente creando los recursos AWS necesarios.

---

# 🔎 Resultado

El pipeline fue ejecutado de extremo a extremo:

```text
CoinGecko API
      ↓
Python
      ↓
S3 RAW
      ↓
AWS Glue + Spark
      ↓
S3 CURATED
      ↓
   ┌──┴───────────┐
   ▼              ▼
Athena        PostgreSQL
```

La ejecución permitió validar:

- extracción correcta desde la API;
- almacenamiento RAW en S3;
- transformación con AWS Glue y Spark;
- generación de datos CURATED en Parquet;
- descubrimiento de metadatos mediante Glue Crawler;
- consultas mediante Athena;
- carga de datos en PostgreSQL;
- coordinación de las etapas mediante Airflow;
- ejecución reproducible mediante Docker;
- validación automática mediante GitHub Actions.

---

# 👨‍💻 Autor

Proyecto desarrollado como parte de mi portfolio de **Data Engineering**, con foco en Python, SQL, AWS, procesamiento de datos, orquestación y automatización.

El objetivo del proyecto es demostrar la capacidad de diseñar y construir un pipeline de datos completo, desde la extracción hasta el consumo analítico, utilizando servicios cloud y herramientas habituales en entornos profesionales.