import argparse
import io
import json
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

AWS_REGION = "sa-east-1"
S3_BUCKET_NAME = (
    os.getenv("S3_BUCKET_NAME")
    or os.getenv("S3-BUCKET_NAME")
    or os.getenv("S3-BUCKET-NAME")
)
SILVER_KEY_PREFIX = (
    os.getenv("SILVER_KEY_PREFIX")
    or "silver/alphavantage/daily_prices"
)


def validar_configuracoes() -> None:
    if not S3_BUCKET_NAME:
        raise ValueError("A variável S3_BUCKET_NAME não foi encontrada.")


def pegar_body(bronze_key: str) -> str:
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    response = s3_client.get_object(
        Bucket=S3_BUCKET_NAME,
        Key=bronze_key,
    )

    return response["Body"].read().decode("utf-8")


def transformar_dados(body: str) -> list[dict]:
    payload = json.loads(body)

    if "Time Series (Daily)" not in payload:
        raise ValueError(
            "A chave 'Time Series (Daily)' não foi encontrada no JSON Bronze."
        )

    time_series = payload["Time Series (Daily)"]
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


def criar_dataframe(registros: list[dict], symbol: str) -> pd.DataFrame:
    df = pd.DataFrame(registros)

    if df.empty:
        raise ValueError("O DataFrame Silver está vazio.")

    df["reference_date"] = pd.to_datetime(
        df["reference_date"],
        errors="coerce",
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
            errors="coerce",
        )

    df["volume"] = pd.to_numeric(
        df["volume"],
        errors="coerce",
    )

    instante_transformacao = datetime.now(timezone.utc)
    df["symbol"] = symbol.upper()
    df["source"] = "alpha_vantage"
    df["extracted_at"] = instante_transformacao

    return df


def validar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.isna().any().any():
        print("Valores nulos ou inválidos por coluna:")
        print(df.isna().sum())
        raise ValueError(
            "Existem valores nulos ou inválidos no DataFrame Silver."
        )

    if df.duplicated(subset=["symbol", "reference_date"]).any():
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
        raise ValueError("Existem preços menores ou iguais a zero.")

    if (df["volume"] < 0).any():
        raise ValueError("Existem volumes negativos.")

    if (df["high_price"] < df["low_price"]).any():
        raise ValueError("Existem registros com high_price menor que low_price.")

    if (df["high_price"] < df["open_price"]).any():
        raise ValueError("Existem registros com high_price menor que open_price.")

    if (df["high_price"] < df["close_price"]).any():
        raise ValueError("Existem registros com high_price menor que close_price.")

    if (df["low_price"] > df["open_price"]).any():
        raise ValueError("Existem registros com low_price maior que open_price.")

    if (df["low_price"] > df["close_price"]).any():
        raise ValueError("Existem registros com low_price maior que close_price.")

    hoje_utc = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
    if (df["reference_date"] > hoje_utc).any():
        raise ValueError("Existem datas de referência no futuro.")

    df["volume"] = df["volume"].astype("int64")
    df["reference_date"] = df["reference_date"].dt.date

    colunas_ordenadas = [
        "symbol",
        "reference_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "source",
        "extracted_at",
    ]

    return df[colunas_ordenadas]


def construir_silver_key(bronze_key: str, symbol: str) -> str:
    partes = bronze_key.split("/")

    particao_ingestao = next(
        (
            parte
            for parte in partes
            if parte.startswith("ingestion_date=")
        ),
        None,
    )

    if not particao_ingestao:
        raise ValueError(
            "A chave Bronze não contém a partição ingestion_date."
        )

    nome_arquivo = partes[-1]

    if not nome_arquivo.endswith(".json"):
        raise ValueError("A chave Bronze não aponta para um arquivo JSON.")

    timestamp = nome_arquivo.removesuffix(".json")

    # O timestamp da Bronze é reutilizado para tornar retries idempotentes.
    return (
        f"{SILVER_KEY_PREFIX}/"
        f"symbol={symbol.upper()}/"
        f"{particao_ingestao}/"
        f"{timestamp}.parquet"
    )


def salvar_silver(df: pd.DataFrame, silver_key: str) -> str:
    parquet_buffer = io.BytesIO()

    df.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        index=False,
    )

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=silver_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )

    return silver_key


def executar_silver(bronze_key: str, symbol: str) -> str:
    validar_configuracoes()

    body = pegar_body(bronze_key)
    registros = transformar_dados(body)
    df = criar_dataframe(registros, symbol)
    df_validado = validar_dataframe(df)
    silver_key = construir_silver_key(bronze_key, symbol)

    return salvar_silver(df_validado, silver_key)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transforma um JSON Bronze em Parquet Silver."
    )
    parser.add_argument(
        "--bronze-key",
        required=True,
        help="Object key do JSON Bronze, iniciando em bronze/.",
    )
    parser.add_argument("--symbol", default="AAPL")
    args = parser.parse_args()

    silver_key = executar_silver(
        bronze_key=args.bronze_key,
        symbol=args.symbol,
    )

    print("Carga Silver concluída.")
    print(f"Key: {silver_key}")
    print(f"URI: s3://{S3_BUCKET_NAME}/{silver_key}")


if __name__ == "__main__":
    main()
