import io
import json
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv


load_dotenv()


AWS_REGION = "sa-east-1"

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

SILVER_PREFIX = "silver/alphavantage/daily_prices"

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)


def pegar_body(bronze_key):
    response = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=bronze_key
    )

    return response["Body"].read().decode("utf-8")


def transformar_dados(body):
    data = json.loads(body)

    if "Time Series (Daily)" not in data:
        raise ValueError(
            "A chave 'Time Series (Daily)' não foi encontrada."
        )

    time_series = data["Time Series (Daily)"]
    registros = []

    for reference_date, values in time_series.items():
        registros.append(
            {
                "reference_date": reference_date,
                "open_price": values.get("1. open"),
                "high_price": values.get("2. high"),
                "low_price": values.get("3. low"),
                "close_price": values.get("4. close"),
                "volume": values.get("5. volume"),
            }
        )

    return registros


def criar_dataframe(registros, symbol):
    df = pd.DataFrame(registros)

    if df.empty:
        raise ValueError("O DataFrame está vazio.")

    df["reference_date"] = pd.to_datetime(
        df["reference_date"],
        errors="coerce"
    )

    colunas_precos = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
    ]

    for coluna in colunas_precos:
        df[coluna] = pd.to_numeric(
            df[coluna],
            errors="coerce"
        )

    df["volume"] = pd.to_numeric(
        df["volume"],
        errors="coerce"
    )

    df["symbol"] = symbol
    df["source"] = "alphavantage"
    df["extracted_at"] = datetime.now(timezone.utc)

    return df


def validar_dataframe(df):
    if df.isna().any().any():
        print("Valores nulos por coluna:")
        print(df.isna().sum())

        raise ValueError(
            "Existem valores nulos ou inválidos no DataFrame."
        )

    if df.duplicated(
        subset=["symbol", "reference_date"]
    ).any():
        raise ValueError(
            "Existem registros duplicados para symbol + reference_date."
        )

    colunas_precos = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
    ]

    if (df[colunas_precos] <= 0).any().any():
        raise ValueError(
            "Existem preços menores ou iguais a zero."
        )

    if (df["volume"] < 0).any():
        raise ValueError(
            "Existem volumes negativos."
        )

    if (df["high_price"] < df["low_price"]).any():
        raise ValueError(
            "Existem registros em que high_price é menor que low_price."
        )

    if (df["high_price"] < df["open_price"]).any():
        raise ValueError(
            "Existem registros em que high_price é menor que open_price."
        )

    if (df["high_price"] < df["close_price"]).any():
        raise ValueError(
            "Existem registros em que high_price é menor que close_price."
        )

    if (df["low_price"] > df["open_price"]).any():
        raise ValueError(
            "Existem registros em que low_price é maior que open_price."
        )

    if (df["low_price"] > df["close_price"]).any():
        raise ValueError(
            "Existem registros em que low_price é maior que close_price."
        )

    df["volume"] = df["volume"].astype("int64")

    return df


def construir_silver_key(symbol, instante_utc):
    ingestion_date = instante_utc.strftime("%Y-%m-%d")
    timestamp = instante_utc.strftime("%Y%m%dT%H%M%SZ")
    prataKey =  (
        f"{SILVER_PREFIX}/"
        f"symbol={symbol}/"
        f"ingestion_date={ingestion_date}/"
        f"{timestamp}.parquet"
    )

    return prataKey



def salvar_silver(df, silver_key):
    parquet_buffer = io.BytesIO()

    df.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        index=False
    )

    parquet_buffer.seek(0)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=silver_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/vnd.apache.parquet"
    )

    return silver_key


def executar_silver(bronze_key, symbol):
    body = pegar_body(bronze_key)

    registros = transformar_dados(body)

    df = criar_dataframe(
        registros=registros,
        symbol=symbol
    )

    df = validar_dataframe(df)

    instante_utc = datetime.now(timezone.utc)

    silver_key = construir_silver_key(
        symbol=symbol,
        instante_utc=instante_utc
    )

    chave_salva = salvar_silver(
        df=df,
        silver_key=silver_key
    )

    print("Carga Silver concluída.")
    print(df.dtypes)
    print(f"s3://{BUCKET_NAME}/{chave_salva}")

    return chave_salva

