from datetime import datetime, timezone
import io
import os
import time
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv
from pandas.api.types import is_numeric_dtype

load_dotenv()

AWS_REGION = "sa-east-1"
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
OURO_PREFIX = "gold/daily_indicators"
GLUE_CRAWLER_NAME = os.getenv("GLUE_CRAWLER_NAME")

if BUCKET_NAME is None:
    raise ValueError("A variável de ambiente 'S3_BUCKET_NAME' não está definida.")

if GLUE_CRAWLER_NAME is None:
    raise ValueError(
        "A variável de ambiente 'GLUE_CRAWLER_NAME' não está definida."
    )

s3_client = boto3.client("s3", region_name=AWS_REGION)


def GetParquet(silver_key: str) -> pd.DataFrame:
    response = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=silver_key,
    )
    dados = response["Body"].read()
    return pd.read_parquet(BytesIO(dados), engine="pyarrow")


def validar_dados(df: pd.DataFrame) -> None:
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

    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"A coluna obrigatória '{column}' não foi encontrada.")

    for column in numeric_columns:
        if not is_numeric_dtype(df[column]):
            raise ValueError(f"A coluna '{column}' precisa ser numérica.")

    for column in required_columns:
        if df[column].isnull().any():
            erros.append(f"Valores nulos encontrados na coluna '{column}'.")

    for column in price_columns:
        if (df[column] <= 0).any():
            erros.append(f"Valores menores ou iguais a zero em '{column}'.")

    if (df["volume"] < 0).any():
        erros.append("Valores negativos encontrados em 'volume'.")

    if df.duplicated(subset=["symbol", "reference_date"]).any():
        erros.append("Registros duplicados para symbol + reference_date.")

    if erros:
        raise ValueError("Erros encontrados na validação:\n" + "\n".join(erros))


def analisar_dados(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["reference_date"] = pd.to_datetime(df["reference_date"], errors="raise")
    df["symbol"] = df["symbol"].astype(str)
    df = df.sort_values(by=["symbol", "reference_date"]).reset_index(drop=True)

    df["variacao_percentual"] = (df["close_price"].pct_change() * 100).round(2)
    df["media_movel_7d"] = (
        df["close_price"].rolling(window=7, min_periods=7).mean().round(2).astype("Float64")
    )
    df["retorno_diario"] = (df["close_price"] - df["open_price"]).round(2)
    df["retorno_diario_percentual"] = (
        (df["close_price"] - df["open_price"])
        .div(df["open_price"])
        .mul(100)
        .round(2)
    )

    retorno_fechamento = df["close_price"].pct_change()
    df["volatilidade_7d_percentual"] = (
        retorno_fechamento.rolling(window=7, min_periods=7).std().mul(100).round(2).astype("Float64")
    )
    df["ingestion_date"] = pd.Timestamp.now(tz="UTC")

    return df


Analisando_Dados = analisar_dados


def buscar_keys_existentes_ouro(symbol: str) -> list[str]:
    prefix = f"{OURO_PREFIX}/symbol={symbol}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    ouro_keys: list[str] = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                ouro_keys.append(key)

    return ouro_keys


def buscar_datas_gold_existentes(symbol: str) -> set[pd.Timestamp]:
    datas_existentes: set[pd.Timestamp] = set()

    for ouro_key in buscar_keys_existentes_ouro(symbol):
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=ouro_key)
        parquet_data = response["Body"].read()

        df_existente = pd.read_parquet(
            BytesIO(parquet_data),
            engine="pyarrow",
            columns=["reference_date"],
        )

        if df_existente.empty:
            continue

        reference_dates = pd.to_datetime(
            df_existente["reference_date"],
            errors="raise",
        ).dt.normalize()
        datas_existentes.update(reference_dates.tolist())

    return datas_existentes


buscarDadosExistentes_ouro = buscar_datas_gold_existentes


def construir_ouro_key(symbol: str, instante_utc: datetime) -> str:
    processing_date = instante_utc.strftime("%Y-%m-%d")
    return (
        f"{OURO_PREFIX}/"
        f"symbol={symbol}/"
        f"processing_date={processing_date}/"
        "daily.parquet"
    )


def salvar_ouro(df: pd.DataFrame, ouro_key: str) -> str:
    parquet_buffer = io.BytesIO()
    df_para_salvar = df.copy()

    df_para_salvar["reference_date"] = pd.to_datetime(
        df_para_salvar["reference_date"],
        errors="raise",
    ).dt.date

    df_para_salvar.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        index=False,
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=ouro_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )

    return ouro_key


def executar_ouro(silver_key: str, symbol: str) -> str | None:
    df_silver = GetParquet(silver_key)
    validar_dados(df_silver)

    symbols_encontrados = df_silver["symbol"].dropna().astype(str).unique()
    if len(symbols_encontrados) != 1:
        raise ValueError("O arquivo Silver deve conter exatamente um símbolo.")

    if symbols_encontrados[0] != symbol:
        raise ValueError(
            f"A task esperava '{symbol}', mas encontrou '{symbols_encontrados[0]}'."
        )

    df_ouro = analisar_dados(df_silver)
    df_ouro["reference_date"] = pd.to_datetime(
        df_ouro["reference_date"],
        errors="raise",
    ).dt.normalize()

    datas_existentes = buscar_datas_gold_existentes(symbol)
    df_novo = df_ouro[~df_ouro["reference_date"].isin(datas_existentes)].copy()

    if df_novo.empty:
        print(f"Nenhum dado Gold novo para {symbol}. Nenhum arquivo será gravado.")
        return None

    instante_utc = datetime.now(timezone.utc)
    ouro_key = construir_ouro_key(symbol=symbol, instante_utc=instante_utc)
    chave_salva = salvar_ouro(df=df_novo, ouro_key=ouro_key)

    print("Carga ouro concluída.")
    print(f"Símbolo: {symbol}")
    print(f"Registros gravados: {len(df_novo)}")
    print(f"s3://{BUCKET_NAME}/{chave_salva}")

    return chave_salva


def executar_crawler_ouro(ouro_keys: list[str | None] | str | None) -> list[str]:
    if isinstance(ouro_keys, str):
        chaves_para_validar = [ouro_keys]
    elif ouro_keys is None:
        chaves_para_validar = []
    else:
        chaves_para_validar = [ouro_key for ouro_key in ouro_keys if ouro_key is not None]

    if not chaves_para_validar:
        print("Nenhum arquivo Gold novo. O Glue Crawler não será executado.")
        return []

    glue_client = boto3.client("glue", region_name=AWS_REGION)

    for ouro_key in chaves_para_validar:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=ouro_key)

    glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)

    inicio = time.monotonic()
    limite_segundos = 900

    while True:
        response = glue_client.get_crawler(Name=GLUE_CRAWLER_NAME)
        crawler = response["Crawler"]
        estado = crawler["State"]

        if estado == "READY":
            ultima_execucao = crawler.get("LastCrawl", {})
            status = ultima_execucao.get("Status")

            if status != "SUCCEEDED":
                erro = ultima_execucao.get("ErrorMessage", "Erro não informado pelo Glue.")
                raise RuntimeError(f"Crawler terminou com status '{status}': {erro}")

            print("Crawler concluído com sucesso.")
            return chaves_para_validar

        if time.monotonic() - inicio >= limite_segundos:
            raise TimeoutError(f"O crawler não terminou em {limite_segundos} segundos.")

        time.sleep(2)