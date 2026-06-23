#%%
from pathlib import Path
import os



BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "data" / "raw"

INTERIM_DIR = BASE_DIR / "data" / "interim"

PROCESSED_DIR = BASE_DIR / "data" / "processed"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://airflow:airflow@127.0.0.1:5433/airflow"
)

TABLE_NOME = "crypto_prices"

COINS = "bitcoin,ethereum,solana"

BASE_CURRENCY = "usd"
# %%
import config

print(dir(config)) # ----- Para testar 
# %%

# %%
