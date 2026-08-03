import os
from io import BytesIO

import pandas as pd
from pandas.api.types import is_numeric_dtype

import boto3
from dotenv import load_dotenv


load_dotenv()

AWS_REGION = "sa-east-1"

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

SILVER_PREFIX = "silver/alphavantage/daily_prices"

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)


def GetParquet(silver_key) -> pd.DataFrame:
    response = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=silver_key
    )
    dados = response["Body"].read()

    return pd.read_parquet(BytesIO(dados), engine="pyarrow")


if BUCKET_NAME is None:
    raise ValueError("A variável de ambiente 'S3_BUCKET_NAME' não está definida.")


def validar_dados(df: pd.DataFrame):
    erros = []

    if df.empty:
        raise ValueError("O DataFrame está vazio.")

    required_columns = [
        "symbol",
        "reference_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]

    numeric_columns = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]

    price_columns = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
    ]

    # Existência das colunas
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(
                f"A coluna obrigatória '{column}' não foi encontrada."
            )

    # Tipos numéricos
    for column in numeric_columns:
        if not is_numeric_dtype(df[column]):
            raise ValueError(
                f"A coluna '{column}' precisa ser numérica."
            )

    # Nulos em colunas obrigatórias
    for column in required_columns:
        if df[column].isnull().any():
            erros.append(
                f"Valores nulos encontrados na coluna '{column}'."
            )

    # Preços precisam ser maiores que zero
    for column in price_columns:
        if (df[column] <= 0).any():
            erros.append(
                f"Valores menores ou iguais a zero em '{column}'."
            )

    # Volume pode ser zero, mas não negativo
    if (df["volume"] < 0).any():
        erros.append("Valores negativos encontrados em 'volume'.")

    # Duplicidade da chave do registro
    if df.duplicated(
        subset=["symbol", "reference_date"]
    ).any():
        erros.append(
            "Registros duplicados para symbol + reference_date."
        )

    if erros:
        raise ValueError(
            "Erros encontrados na validação:\n" + "\n".join(erros)
        )


def Analisando_Dados(df: pd.DataFrame):
    df = df.sort_values(by=["reference_date"])

    variaçãoPercentual = (df["close_price"].pct_change() * 100).round(2)
    df["variação_percentual"] = variaçãoPercentual

    media_movel_7d = df["close_price"].rolling(window=7).mean()
    df["media_movel_7d"] = media_movel_7d

    retorno_diario = df["close_price"] - df["open_price"]
    df["retorno_diario"] = retorno_diario

    retorno_diarioPCT = (
        (df["close_price"] - df["open_price"])
        / df["open_price"]
        * 100
    )
    df["retorno_diario_percentual"] = retorno_diarioPCT.round(2)

    df["volatilidade_7d_percentual"] = (
        retorno_diarioPCT.rolling(window=7)
        .std()
        .mul(100)
        .round(2)
    )
    df["symbol"] = df["symbol"].astype(str)

    df["reference_date"] = pd.to_datetime(
        df["reference_date"],
        errors="raise"
    )

    df = df.sort_values("reference_date").reset_index(drop=True)

    df["ingestion_date"] = pd.Timestamp.now(tz="UTC")

    return df


if __name__ == "__main__":
    silver_key = (
        "silver/alphavantage/daily_prices/"
        "symbol=AAPL/ingestion_date=2026-08-03/"
        "20260803T130916Z.parquet"
    )

    df = GetParquet(silver_key)
    dadosOrdenados = df.sort_values(by=["reference_date"])

    print(df.head())
    print(validar_dados(df))
    print(dadosOrdenados.head)
    print(Analisando_Dados(df))
