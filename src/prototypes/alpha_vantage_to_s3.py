import argparse
import os
from datetime import datetime, timezone

import boto3
import requests
from dotenv import load_dotenv

import ExPersonalizad


load_dotenv()

AWS_REGION = "sa-east-1"
ALPHAVANTAGE_URL = os.getenv("ALPHAVANTAGE_URL")
S3_BUCKET_NAME = (
    os.getenv("S3_BUCKET_NAME")
    or os.getenv("S3-BUCKET_NAME")
    or os.getenv("S3-BUCKET-NAME")
)
S3_KEY_PREFIX = (
    os.getenv("S3_KEY_PREFIX")
    or os.getenv("key_Prefix")
    or "bronze/alphavantage/time_series_daily"
)
SSM_PARAMETER_NAME = "/marketpulse/alphavantage/api_key"


def validar_configuracoes() -> None:
    if not ALPHAVANTAGE_URL:
        raise ValueError("A variável ALPHAVANTAGE_URL não foi encontrada.")

    if not S3_BUCKET_NAME:
        raise ValueError("A variável S3_BUCKET_NAME não foi encontrada.")


def buscar_parametro_api_key() -> str:
    ssm_client = boto3.client("ssm", region_name=AWS_REGION)

    response = ssm_client.get_parameter(
        Name=SSM_PARAMETER_NAME,
        WithDecryption=True,
    )

    return response["Parameter"]["Value"]


def buscar_dados_daily(symbol: str, api_key: str) -> str:
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
        raise ExPersonalizad.AlphaVantageResponseError(mensagem_api)

    if "Time Series (Daily)" not in payload:
        raise ExPersonalizad.AlphaVantageResponseError(
            "A série temporal diária não foi encontrada."
        )

    # A Bronze preserva exatamente o texto devolvido pela fonte.
    return response.text


def construir_chave_s3(symbol: str, instante_utc: datetime) -> str:
    timestamp = instante_utc.strftime("%Y%m%dT%H%M%SZ")
    data_ingestao = instante_utc.strftime("%Y-%m-%d")

    return (
        f"{S3_KEY_PREFIX}/"
        f"symbol={symbol}/"
        f"ingestion_date={data_ingestao}/"
        f"{timestamp}.json"
    )


def salvar_json_bruto_no_s3(conteudo_bruto: str, object_key: str) -> str:
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=object_key,
            Body=conteudo_bruto.encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as error:
        print(f"Erro ao enviar para o S3: {error}")
        raise

    # O contrato entre as tarefas é a object key, não a URI completa.
    return object_key


def executar_bronze(symbol: str) -> str:
    validar_configuracoes()

    api_key = buscar_parametro_api_key()
    conteudo_bruto = buscar_dados_daily(symbol, api_key)
    instante_utc = datetime.now(timezone.utc)
    object_key = construir_chave_s3(symbol, instante_utc)

    return salvar_json_bruto_no_s3(conteudo_bruto, object_key)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extrai preços diários da Alpha Vantage e salva o JSON na Bronze."
    )
    parser.add_argument("--symbol", default="AAPL")
    args = parser.parse_args()

    try:
        object_key = executar_bronze(args.symbol)
        print("Carga Bronze concluída.")
        print(f"Key: {object_key}")
        print(f"URI: s3://{S3_BUCKET_NAME}/{object_key}")
    except ExPersonalizad.AlphaVantageResponseError as error:
        print(f"Erro de conteúdo da Alpha Vantage: {error}")
        raise


if __name__ == "__main__":
    main()
