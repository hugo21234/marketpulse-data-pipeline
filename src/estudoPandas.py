import os

import pandas as pd



dados = pd.read_parquet('daily_prices.parquet')
df = pd.DataFrame(dados)
df.shape
df.dtypes
df.info()

print(dados,df.shape,
df.dtypes,
df.info()
)