"""
Pipeline ELT para Ventanas de Observación Astronómica en Paraguay
OpenWeather/AstronomyAPI -> Airbyte -> MotherDuck -> dbt
"""

import os
import time
from pathlib import Path
from typing import Optional, List
from dotenv import load_dotenv
import httpx
import duckdb
from prefect import flow, task, get_run_logger
from prefect.client.schemas.schedules import CronSchedule
from prefect_dbt.cli.commands import DbtCoreOperation

# Cargar variables de entorno
load_dotenv()

# Configuración de Airbyte
AIRBYTE_HOST = os.getenv("AIRBYTE_HOST", "localhost")
AIRBYTE_PORT = int(os.getenv("AIRBYTE_PORT", 8000))
AIRBYTE_USERNAME = os.getenv("AIRBYTE_USERNAME", "airbyte")
AIRBYTE_PASSWORD = os.getenv("AIRBYTE_PASSWORD", "password")

# IDs de Conexión
AIRBYTE_CONNECTION_ID_FORECAST = os.getenv("AIRBYTE_CONNECTION_ID_FORECAST")
AIRBYTE_CONNECTION_ID_ASTRONOMY = os.getenv("AIRBYTE_CONNECTION_ID_ASTRONOMY")

# Configuración dbt
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

@task(name="Airbyte Sync Task", retries=3, retry_delay_seconds=60)
def sync_airbyte_connection(connection_id: str, connection_name: str):
    """Sincroniza una conexión específica de Airbyte y espera a que termine"""
    logger = get_run_logger()
    base_url = f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/v1"

    if not connection_id:
        logger.error(f"ERROR: ID de conexión para {connection_name} no definido.")
        return False

    logger.info(f"Iniciando sincronización para {connection_name} ({connection_id})...")

    try:
        with httpx.Client(timeout=30, auth=(AIRBYTE_USERNAME, AIRBYTE_PASSWORD)) as client:
            sync_response = client.post(
                f"{base_url}/connections/sync", 
                json={"connectionId": connection_id}
            )
            
            if sync_response.status_code == 409:
                logger.warning(f"Ya hay un proceso activo para {connection_name}. Esperando...")
                return "running"
            
            sync_response.raise_for_status()
            job_id = sync_response.json()["job"]["id"]

            while True:
                status_response = client.post(f"{base_url}/jobs/get", json={"id": job_id})
                status_response.raise_for_status()
                job_data = status_response.json()["job"]
                status = job_data["status"]
                
                if status == "succeeded":
                    logger.info(f"✅ Sincronización de {connection_name} exitosa.")
                    return True
                elif status in ("failed", "cancelled"):
                    raise RuntimeError(f"Airbyte job {job_id} ({connection_name}) falló.")
                
                time.sleep(20)
    except Exception as e:
        logger.error(f"Falla crítica en Airbyte Sync ({connection_name}): {str(e)}")
        raise

@task(name="dbt Transform Task", retries=3)
def run_dbt(commands: List[str]):
    """Ejecuta comandos de dbt Core"""
    logger = get_run_logger()
    try:
        result = DbtCoreOperation(
            commands=commands,
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        ).run()
        logger.info("✅ Transformaciones dbt finalizadas.")
        return result
    except Exception as e:
        logger.error(f"❌ Error en la ejecución de dbt: {str(e)}")
        raise

@task(name="Log High Score Windows", retries=2)
def log_high_score_windows():
    """Consulta a MotherDuck para registrar cuántas ventanas de alta calidad se encontraron"""
    logger = get_run_logger()
    token = os.getenv("MOTHERDUCK_TOKEN")
    
    if not token:
        logger.warning("MOTHERDUCK_TOKEN no definido, saltando logging de valor.")
        return

    try:
        con = duckdb.connect(f"md:tp_final?motherduck_token={token}")
        
        # Consulta para Logging de Valor (Score > 75)
        count_high_score = con.execute("""
            SELECT count(*) 
            FROM main.obt_observation_windows 
            WHERE observation_score > 75
        """).fetchone()[0]
        
        logger.info(f"✨ VALOR GENERADO: {count_high_score} registros con Score > 75 (Calidad Excelente).")

    except Exception as e:
        logger.error(f"Error al consultar ventanas de alta calidad: {str(e)}")

@flow(name="Astronomy Observatory ETL", 
      description="Pipeline ELT para monitoreo astronómico en Paraguay")
def astronomy_observatory_flow(
    run_extract: bool = True,
    run_transform: bool = True,
    run_tests: bool = True
):
    """Flujo principal con reportes de calidad astronómica"""
    logger = get_run_logger()
    logger.info("🔭 Iniciando Ciclo de Datos Astronómicos...")

    # 1. Extracción (Airbyte)
    if run_extract:
        sync_airbyte_connection(AIRBYTE_CONNECTION_ID_FORECAST, "OpenWeather Forecast")
        sync_airbyte_connection(AIRBYTE_CONNECTION_ID_ASTRONOMY, "AstronomyAPI Positions")

    # 2. Transformación y Calidad (dbt)
    if run_transform or run_tests:
        dbt_commands = ["dbt deps"]
        if run_transform: dbt_commands.append("dbt run")
        if run_tests: dbt_commands.append("dbt test")
        run_dbt(dbt_commands)
        
        # 3. Logging de Valor (Score > 75)
        if run_transform:
            log_high_score_windows()

    logger.info("🚀 Pipeline finalizado con éxito.")

@flow(name="dbt Only Astronomy Pipeline")
def dbt_only_astronomy_pipeline(run_tests: bool = True):
    """Pipeline que solo ejecuta dbt para astronomía"""
    run_dbt(["dbt deps", "dbt run"])
    if run_tests:
        run_dbt(["dbt test"])

if __name__ == "__main__":
    # Opción 1: Ejecutar una vez
    # astronomy_observatory_flow()

    # Opción 2: Ejecutar solo dbt
    # dbt_only_astronomy_pipeline()

    # Opción 3: Servir con schedule (3:30am, 12:00pm, 5:00pm)
    astronomy_observatory_flow.serve(
        name="astronomy-scheduled",
        schedules=[
            CronSchedule(cron="30 3 * * *"),
            CronSchedule(cron="0 12 * * *"),
            CronSchedule(cron="0 17 * * *")
        ],
        parameters={
            "run_extract": True,
            "run_transform": True,
            "run_tests": True
        },
    )
