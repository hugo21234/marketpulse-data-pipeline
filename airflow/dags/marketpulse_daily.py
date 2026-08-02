import pendulum
from airflow.sdk import dag, task

from Bronze import executar_bronze
from Prata import executar_silver


start_date = pendulum.datetime(
    2026,
    8,
    1,
    tz="America/Sao_Paulo",
)


@task()
def task_bronze(symbol: str):
    bronze_key = executar_bronze(symbol)
    return bronze_key


@task()
def task_prata(bronze_key: str, symbol: str):
    prata_key = executar_silver(bronze_key, symbol)
    return prata_key


@dag(
    dag_id="marketpulse_daily",
    schedule="0 22 * * 1-5",
    start_date=start_date,
    catchup=False,
    tags=["marketpulse", "daily"],
)
def marketpulse_daily():
    symbol = "AAPL"

    bronze_key = task_bronze(symbol)
    task_prata(bronze_key, symbol)


marketpulse_daily_dag = marketpulse_daily()
