import time

import pendulum
from airflow.sdk import dag, task

from Bronze import executar_bronze
from Prata import executar_silver
from ouro import executar_crawler_ouro, executar_ouro


START_DATE = pendulum.datetime(
    2026,
    7,
    29,
    0,
    0,
    0,
    tz="America/Sao_Paulo",
)

SYMBOLS = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "GOOGL",  # Alphabet / Google
    "AMZN",   # Amazon
    "NVDA",   # Nvidia
]


@task()
def task_bronze(symbol: str) -> str:
    time.sleep(1)
    bronze_key = executar_bronze(symbol)
    return bronze_key


@task()
def task_prata(bronze_key: str, symbol: str) -> str:
    prata_key = executar_silver(bronze_key, symbol)
    return prata_key


@task()
def task_ouro(silver_key: str, symbol: str) -> str:
    return executar_ouro(silver_key, symbol)


@task()
def task_crawler(ouro_keys: list[str]) -> list[str]:
    return executar_crawler_ouro(ouro_keys)


@dag(
    dag_id="marketpulse_daily",
    schedule="0 0 * * *",
    start_date=START_DATE,
    catchup=False,
    tags=["marketpulse", "daily"],
)
def marketpulse_daily():
    ouro_keys = []
    previous_bronze_key = None

    for symbol in SYMBOLS:
        task_suffix = symbol.lower()

        bronze_key = task_bronze.override(
            task_id=f"task_bronze_{task_suffix}",
        )(symbol)

        if previous_bronze_key is not None:
            previous_bronze_key >> bronze_key

        previous_bronze_key = bronze_key

        prata_key = task_prata.override(
            task_id=f"task_prata_{task_suffix}",
        )(bronze_key, symbol)

        ouro_key = task_ouro.override(
            task_id=f"task_ouro_{task_suffix}",
        )(prata_key, symbol)

        ouro_keys.append(ouro_key)

    task_crawler(ouro_keys)


marketpulse_daily_dag = marketpulse_daily()
