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
)

S3_KEY_PREFIX = (
    os.getenv("S3_KEY_PREFIX")
    or os.getenv("key_Prefix")
    or "bronze/alphavantage/time_series_daily"
)

SSM_PARAMETER_NAME = "/marketpulse/alphavantage/api_key"


def validar_configuracoes():
    if not ALPHAVANTAGE_URL:
        raise ValueError("A variável ALPHAVANTAGE_URL não foi encontrada.")

    if not S3_BUCKET_NAME:
        raise ValueError("A variável do bucket S3 não foi encontrada.")


def construir_key(symbol, instant_utc):
    if instant_utc.tzinfo is None:
        raise ValueError(
            "O instante da ingestão deve possuir timezone."
        )

    instant_utc = instant_utc.astimezone(timezone.utc)

    data_ingestao = instant_utc.strftime("%Y-%m-%d")

    object_key = (
        f"{S3_KEY_PREFIX}/"
        f"symbol={symbol}/"
        f"ingestion_date={data_ingestao}/"
        "daily.json"
    )

    return object_key

def buscar_parametro_api_key():
    ssm_client = boto3.client(
        "ssm",
        region_name=AWS_REGION
    )

    response = ssm_client.get_parameter(
        Name=SSM_PARAMETER_NAME,
        WithDecryption=True
    )

    return response["Parameter"]["Value"]


def buscar_dados_daily(symbol, api_key):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key
    }

    try:
        response = requests.get(
            ALPHAVANTAGE_URL,
            params=params,
            timeout=10
        )

        response.raise_for_status()

    except requests.exceptions.HTTPError as error:
        codigo = error.response.status_code
        print(f"Erro HTTP na requisição: {codigo}")
        raise

    except requests.exceptions.Timeout:
        print("A requisição excedeu o tempo limite.")
        raise

    except requests.exceptions.ConnectionError as error:
        print(f"Erro de conexão: {error}")
        raise

    except requests.exceptions.RequestException as error:
        print(f"Erro durante a requisição: {error}")
        raise

    try:
        response_json = response.json()

    except requests.exceptions.JSONDecodeError as error:
        print(f"Erro ao decodificar a resposta JSON: {error}")
        raise

    mensagem_erro = (
        response_json.get("Error Message")
        or response_json.get("Information")
        or response_json.get("Note")
    )

    if mensagem_erro:
        raise ExPersonalizad.AlphaVantageResponseError(
            mensagem_erro
        )

    if "Time Series (Daily)" not in response_json:
        raise ExPersonalizad.AlphaVantageResponseError(
            "A série temporal diária não foi encontrada."
        )

    # Mantém exatamente o texto retornado pela fonte.
    return response.text    

def normalizar_symbol(symbol) -> str:
    if not isinstance(symbol, str):
        raise TypeError("O símbolo deve ser uma string.")

    symbol_normalizado = symbol.strip().upper()

    if not symbol_normalizado:
        raise ValueError("O símbolo não pode estar vazio.")

    return symbol_normalizado


def salvar_bronze(dados_brutos, object_key):
    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=object_key,
            Body=dados_brutos.encode("utf-8"),
            ContentType="application/json"
        )

    except Exception as error:
        print(f"Erro ao enviar os dados para o S3: {error}")
        raise

    return object_key


def executar_bronze(symbol, instant_utc=None):
    if instant_utc is None:
        instant_utc = datetime.now(timezone.utc)

    validar_configuracoes()

    symbol = normalizar_symbol(symbol)

    object_key = construir_key(
        symbol=symbol,
        instant_utc=instant_utc
    )

    api_key = buscar_parametro_api_key()

    dados_brutos = buscar_dados_daily(
        symbol=symbol,
        api_key=api_key
    )

    chave_salva = salvar_bronze(
        dados_brutos=dados_brutos,
        object_key=object_key
    )

    return {'chave_salva': chave_salva, 'bucket': S3_BUCKET_NAME, 'symbol': symbol, 'ingestion_date' : instant_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}



