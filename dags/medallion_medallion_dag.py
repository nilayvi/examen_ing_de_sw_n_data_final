"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException

# pylint: disable=import-error,wrong-import-position
from airflow.providers.standard.operators.python import PythonOperator

from include.transformations import (
    clean_daily_transactions
)


# Carpeta base del proyecto (un nivel por encima de /dags)
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))


# Rutas usadas en el pipeline
RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def run_dbt_tests(ti=None, **context):
    """Run dbt tests and save results to quality directory."""
    ds_nodash = _resolve_ds_nodash(context)

    print(f"Running dbt tests for ds_nodash: {ds_nodash}")

    result = _run_dbt_command("test", ds_nodash)
    test_status = "passed" if result.returncode == 0 else "failed"

    results = {
        "ds_nodash": ds_nodash,
        "status": test_status,
        "return_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "timestamp": pendulum.now("UTC").isoformat(),
    }

    quality_file = QUALITY_DIR / f"dq_results_{ds_nodash}.json"
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    with open(quality_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"Test results saved to: {quality_file}")
    print(f"Overall status: {test_status}")

    if test_status == "failed":
        raise AirflowException(
            "dbt tests failed. Check the JSON result file for details."
        )

    return results


def run_dbt_silver(ti=None, **context):
    """Run dbt silver layer models."""
    print(f"Available context keys: {list(context.keys())}")

    ds_nodash = _resolve_ds_nodash(context)

    print(f"Using ds_nodash: {ds_nodash}")

    result = _run_dbt_command("run", ds_nodash)

    if result.returncode != 0:
        raise AirflowException(f"dbt silver failed: {result.stderr}")

    return result.stdout


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Arma las variables de entorno necesarias para ejecutar dbt."""
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """Ejecuta un comando de dbt (run o test) y devuelve el proceso."""
    env = _build_env(ds_nodash)
    # Split command into individual arguments
    cmd_parts = command.split()
    return subprocess.run(
        ["dbt"] + cmd_parts + ["--project-dir", str(DBT_DIR), "--no-send-anonymous-usage-stats"],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _bronze_clean_task(ds_nodash: str, **_) -> None:
    """
    Lee el CSV del día desde data/raw, lo limpia con pandas
    y guarda un parquet en data/clean.
    ds_nodash viene en formato YYYYMMDD ({{ ds_nodash }}).
    """
    exec_date = datetime.strptime(ds_nodash, "%Y%m%d").date()

    print(f"[bronze_clean] Ejecutando limpieza para fecha: {exec_date.isoformat()}")
    print(f"[bronze_clean] RAW_DIR: {RAW_DIR}")
    print(f"[bronze_clean] CLEAN_DIR: {CLEAN_DIR}")

    output_path = clean_daily_transactions(
        execution_date=exec_date,
        raw_dir=RAW_DIR,
        clean_dir=CLEAN_DIR,
    )

    print(f"[bronze_clean] Archivo limpio generado en: {output_path}")




def _gold_dbt_tests(ds_nodash: str, **_) -> None:
    """
    Ejecuta dbt test sobre las tablas finales y genera un archivo JSON
    en data/quality indicando si se pasaron los tests.
    """
    print(f"[gold_dbt_tests] Ejecutando dbt test para ds_nodash={ds_nodash}")
    result = _run_dbt_command("test", ds_nodash)

    print("[gold_dbt_tests] STDOUT:\n", result.stdout)
    print("[gold_dbt_tests] STDERR:\n", result.stderr)

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    output_path = QUALITY_DIR / f"gold_tests_{ds_nodash}.json"

    status = {
        "ds_nodash": ds_nodash,
        "success": result.returncode == 0,
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(f"[gold_dbt_tests] Resultado de tests guardado en: {output_path}")

    if result.returncode != 0:
        raise AirflowException("dbt test falló. Ver archivo de resultados y logs.")


def _resolve_ds_nodash(context: dict) -> str:
    """Obtain the execution date (YYYYMMDD) from the Airflow context."""
    ds_nodash = context.get("ds_nodash")
    if ds_nodash:
        return str(ds_nodash)

    logical_date = (
        context.get("data_interval_start")
        or context.get("logical_date")
        or context.get("execution_date")
    )

    if logical_date:
        if hasattr(logical_date, "strftime"):
            return logical_date.strftime("%Y%m%d")
        if hasattr(logical_date, "format"):
            return logical_date.format("YYYYMMDD")

    return pendulum.now("UTC").strftime("%Y%m%d")


def build_dag() -> DAG:
    """Construye el DAG de la pipeline medallion (bronze → silver → gold)."""
    with DAG(
        dag_id="medallion_pipeline",
        description="Bronze Silver Gold",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=True,        # ✅ solo corre las fechas que dispares, no todo el histórico
        max_active_runs=1,
    ) as medallion_dag:
        # TODO:
        # * Agregar las tasks necesarias del pipeline para completar lo pedido por el enunciado.
        # * Usar PythonOperator con el argumento op_kwargs para pasar ds_nodash a las funciones.
        #   De modo que cada task pueda trabajar con la fecha de ejecución correspondiente.
        # Recomendaciones:
        #  * Pasar el argumento ds_nodash a las funciones definidas arriba.
        #    ds_nodash contiene la fecha de ejecución en formato YYYYMMDD sin guiones.
        #    Utilizarlo para que cada task procese los datos del dia correcto y los archivos
        #    de salida tengan nombres únicos por fecha.
        #  * Asegurarse de que los paths usados en las funciones sean relativos a BASE_DIR.
        #  * Usar las funciones definidas arriba para cada etapa del pipeline.

        # * No se pueden usar las recomendaciones porque no son compatibles con las versiones de Airflow
        #   usadas en este entorno de evaluación

        ## Esta es la capa bronze
        bronze_clean = PythonOperator(
            task_id="clean_daily_transactions",
            python_callable=clean_daily_transactions,
            op_kwargs={
                "raw_dir": RAW_DIR,
                "clean_dir": CLEAN_DIR,
                "ds_nodash": "{{ ds_nodash }}"
            },
        )

        ## Esta es la capa silver donde se guarda en DuckDB via dbt
        silver_dbt_run = PythonOperator(
            task_id="run_dbt_silver",
            python_callable=run_dbt_silver,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},

        )

        ## Esta es la capa gold donde se ejecutan los tests de calidad
        gold_dbt_tests = PythonOperator(
            task_id="gold_dbt_tests",
            python_callable=run_dbt_tests,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},

        )

        # Define task dependencies
        bronze_clean >> silver_dbt_run >> gold_dbt_tests

        return medallion_dag


dag = build_dag()
