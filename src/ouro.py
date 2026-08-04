from datetime import datetime, timezone
import time

import io
import os
from io import BytesIO

import pandas as pd
from pandas.api.types import is_numeric_dtype

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = "sa-east-1"

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

OURO_PREFIX = "gold/daily_indicators"

GLUE_CRAWLER_NAME = os.getenv("GLUE_CRAWLER_NAME")
if GLUE_CRAWLER_NAME is None:
    raise ValueError(
        "A variável de ambiente 'GLUE_CRAWLER_NAME' não está definida."
    )

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)


def GetParquet(silver_key ) -> pd.DataFrame:
    response = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=silver_key
    )
    dados = response["Body"].read()

    return pd.read_parquet(BytesIO(dados), engine="pyarrow")

if BUCKET_NAME is None:
    raise ValueError("A variável de ambiente 'S3_BUCKET_NAME' não está definida.")
        
# df = pd.DataFrame(GetParquet(silver_key))

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
    
    variacaoPercentual = (df["close_price"].pct_change() * 100).round(2)
    df["variacao_percentual"] = variacaoPercentual

    df["media_movel_7d"] = (
    df["close_price"]
    .rolling(window=7, min_periods=7)
    .mean()
    .round(2)
    .astype("Float64"))
    

    retorno_diario = df['close_price'] - df['open_price']
    df['retorno_diario'] = retorno_diario.round(2)

    df["retorno_diario_percentual"] = (
    (df["close_price"] - df["open_price"])
    .div(df["open_price"])
    .mul(100)
    .round(2))

    retorno_fechamento = df["close_price"].pct_change()

    df["volatilidade_7d_percentual"] = (
    retorno_fechamento
    .rolling(window=7, min_periods=7)
    .std()
    .mul(100)
    .round(2)
    .astype("Float64"))
    df['symbol'] = df['symbol'].astype(str)

    df["reference_date"] = pd.to_datetime(df["reference_date"], errors='raise')

    df = df.sort_values("reference_date").reset_index(drop=True)

    ingestion_date = pd.Timestamp.now(tz="UTC")
    df["ingestion_date"] = ingestion_date

    return df

def construir_ouro_key(symbol, instante_utc):
    ingestion_date = instante_utc.strftime("%Y-%m-%d")
    timestamp = instante_utc.strftime("%Y%m%dT%H%M%SZ")
    ouroKey =  (
        f"{OURO_PREFIX}/"
        f"symbol={symbol}/"
        f"processing_date={ingestion_date}/"
        f"{timestamp}.parquet"
    )

    return ouroKey

def salvar_ouro(df, ouro_key):
    parquet_buffer = io.BytesIO()

    df_para_salvar = df.drop(columns=["symbol"]).copy()
    df_para_salvar["reference_date"] = (
        pd.to_datetime(df_para_salvar["reference_date"], errors="raise").dt.date
    )

    df_para_salvar.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        index=False
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=ouro_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/vnd.apache.parquet"
    )

    return ouro_key

def executar_ouro(silver_key, symbol):
    df = GetParquet(silver_key)

    validar_dados(df)

    dfOuro = Analisando_Dados(df)

    instante_utc = datetime.now(timezone.utc)

    ouro_key = construir_ouro_key(
        symbol=symbol,
        instante_utc=instante_utc
    )

    chave_salva = salvar_ouro(
        df=dfOuro,
        ouro_key=ouro_key
    )

    print("Carga ouro concluída.")
    print(dfOuro.dtypes)
    print(f"s3://{BUCKET_NAME}/{chave_salva}")

    return ouro_key

def executar_crawler_ouro(ouro_keys):
    if isinstance(ouro_keys, str):
        chaves_para_validar = [ouro_keys]
    else:
        chaves_para_validar = list(ouro_keys)

    if not chaves_para_validar:
        raise ValueError("Nenhuma chave Gold foi informada para o crawler.")

    glue_client = boto3.client("glue", region_name=AWS_REGION)

    for ouro_key in chaves_para_validar:
        s3_client.head_object(
            Bucket=BUCKET_NAME,
            Key=ouro_key
        )

    glue_client.start_crawler(
        Name=GLUE_CRAWLER_NAME
    )

    inicio = time.monotonic()
    limite_segundos = 900

    while True:
        response = glue_client.get_crawler(
            Name=GLUE_CRAWLER_NAME
        )

        crawler = response["Crawler"]
        estado = crawler["State"]

        if estado == "READY":
            ultima_execucao = crawler.get("LastCrawl", {})
            status = ultima_execucao.get("Status")

            if status != "SUCCEEDED":
                erro = ultima_execucao.get(
                    "ErrorMessage",
                    "Erro não informado pelo Glue."
                )

                raise RuntimeError(
                    f"Crawler terminou com status '{status}': {erro}"
                )

            print("Crawler concluído com sucesso.")
            return ouro_keys

        tempo_decorrido = time.monotonic() - inicio

        if tempo_decorrido >= limite_segundos:
            raise TimeoutError(
                f"O crawler não terminou em {limite_segundos} segundos."
            )

        time.sleep(2)

    

