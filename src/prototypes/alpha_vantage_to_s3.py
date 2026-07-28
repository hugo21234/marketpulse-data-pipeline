import json
import os
from datetime import datetime

import boto3
import requests
from dotenv import load_dotenv


class AlphaVantageResponseError(Exception):
    """Erro retornado no conteúdo da resposta da Alpha Vantage."""


load_dotenv()

ALPHAVANTAGE_URL = os.getenv("ALPHAVANTAGE_URL")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_KEY_PREFIX = os.getenv(
    "S3_KEY_PREFIX",
    "bronze/alphavantage/time_series_daily",
)


def buscar_dados_daily(symbol: str, api_key: str) -> dict:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }

    try:
        response = requests.get(
            ALPHAVANTAGE_URL,
            params=params,
            timeout=10,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        codigo = error.response.status_code
        print(f"Erro HTTP na extração: {codigo}")
        raise
    except requests.exceptions.Timeout:
        print("A requisição excedeu o tempo limite.")
        raise
    except requests.exceptions.ConnectionError as error:
        print(f"Erro de conexão: {error}")
        raise
    except requests.exceptions.RequestException as error:
        print(f"Ocorreu um erro na requisição: {error}")
        raise

    try:
        payload = response.json()
    except requests.exceptions.JSONDecodeError as error:
        print(f"Erro ao decodificar a resposta JSON: {error}")
        raise

    mensagem_api = (
        payload.get("Error Message")
        or payload.get("Information")
        or payload.get("Note")
    )

    if mensagem_api:
        raise AlphaVantageResponseError(mensagem_api)

    if "Time Series (Daily)" not in payload:
        raise AlphaVantageResponseError(
            "A série temporal diária não foi encontrada."
        )

    return payload


def salvar_json_no_s3(payload: dict, symbol: str) -> str:
    extraction_date = datetime.now().strftime("%Y-%m-%d")
    object_key = (
        f"{S3_KEY_PREFIX}/"
        f"symbol={symbol}/"
        f"extraction_date={extraction_date}/"
        "data.json"
    )
    serialized_payload = json.dumps(payload).encode("utf-8")

    s3_client = boto3.client("s3", region_name="sa-east-1")

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=object_key,
            Body=serialized_payload,
            ContentType="application/json",
        )
    except Exception as error:
        print(f"Erro ao enviar para o S3: {error}")
        raise

    return f"s3://{S3_BUCKET_NAME}/{object_key}"


def main() -> None:
    symbol = "AAPL"

    try:
        payload = buscar_dados_daily(symbol, ALPHAVANTAGE_API_KEY)
        s3_uri = salvar_json_no_s3(payload, symbol)
        print(f"Carga concluída: {s3_uri}")
    except AlphaVantageResponseError as error:
        print(f"Erro de conteúdo da Alpha Vantage: {error}")
        raise


if __name__ == "__main__":
    main()
