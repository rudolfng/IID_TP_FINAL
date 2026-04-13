# 🔭 Astronomy Observation ELT Pipeline

Pipeline de datos automatizado (ELT) diseñado para la determinación de ventanas óptimas de observación astronómica en Paraguay, integrando pronósticos meteorológicos y posiciones celestes en tiempo real.

## 🚀 Arquitectura Técnica (Modern Data Stack)

El proyecto implementa el paradigma **ELT** desacoplando la ingesta de la transformación:

1.  **Ingesta (Extract & Load):** [Airbyte](https://airbyte.com/) extrae datos de **OpenWeather API** (clima) y **AstronomyAPI** (cuerpos celestes).
2.  **Almacenamiento (DWH):** [MotherDuck](https://motherduck.com/) (DuckDB en la nube) como motor analítico columnar.
3.  **Transformación (Transform):** [dbt Core](https://www.getdbt.com/) para el modelado en capas (`staging`, `intermediate`, `marts`) y lógica de Scoring.
4.  **Orquestación:** [Prefect](https://www.prefect.io/) para la gobernanza del flujo, reintentos y logging de valor de negocio.
5.  **Visualización (BI):** [Metabase](https://www.metabase.com/) para el consumo de la **OBT (One Big Table)** final.

## 🛠️ Configuración e Instalación

### 1. Entorno de Python
```bash
# Crear entorno virtual
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Variables de Entorno
Copia el archivo de ejemplo y configura tus credenciales:
```bash
cp .env.example .env
# Edita .env con tus tokens de MotherDuck y IDs de conexión de Airbyte
```

### 3. Configuración de dbt
Asegúrate de tener configurado tu `profiles.yml` (usualmente en `~/.dbt/`):
```yaml
astronomy_project:
  outputs:
    dev:
      type: duckdb
      path: "md:tp_final"
      motherduck_token: "{{ env_var('MOTHERDUCK_TOKEN') }}"
  target: dev
```

## ⚙️ Ejecución del Pipeline

El proceso completo está orquestado por **Prefect**. Para ejecutar el ciclo de ingesta, transformación y validación:

```bash
python prefect/astronomy_pipeline.py
```

### Tareas de dbt manuales
Si deseas ejecutar transformaciones de forma aislada:
```bash
cd dbt
dbt deps    # Instalar paquetes (dbt-expectations)
dbt run     # Ejecutar modelos
dbt test    # Ejecutar pruebas de calidad
```

## 📊 Capas de Datos y Lógica de Negocio

### Modelado dbt
- **Staging (`stg_`):** Aplanamiento de JSONs complejos (`->>`), normalización de nombres de localidades y ajuste de zonas horarias (`TIMESTAMPTZ` a `America/Asuncion`).
- **Marts (`obt_`):** Implementación de la **One Big Table** `obt_observation_windows` para maximizar el rendimiento en Metabase.

### Algoritmo de Scoring Astronómico
El `observation_score` (0-100) se calcula en la OBT bajo las siguientes reglas:
- **Hard Rules (Filtros a 0):** 
    - Altitud solar > -12° (Luz diurna).
    - Altitud del astro < 10° sobre el horizonte.
    - Nubosidad > 80%.
- **Ponderación:** Nubes (40%), Visibilidad (30%) e Altitud (30%).

## ✅ Calidad de Datos (Testing)
Se utilizan pruebas de `dbt-expectations` para garantizar:
- **Integridad Física:** Altitud entre -90 y 90, nubosidad/humedad entre 0 y 100.
- **Unicidad Compuesta:** Evitar *fan-out* mediante llave única `[planet_id, location_name, observation_time]`.
- **Volumetría:** Validación de filas mínimas para detectar fallos en las APIs de origen.

## 📈 Visualización en Metabase
El dashboard "TP_FINAL STARGAZING" ofrece:
- **Ranking de Observación:** Mejores astros por hora y lugar.
- **Logística Meteorológica:** Gráficos de nubosidad y temperatura esperada.
- **Trayectoria Celeste:** Trayectoria de los astros proyectada sobre el horizonte.
