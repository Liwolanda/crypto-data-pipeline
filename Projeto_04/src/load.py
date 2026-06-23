#%%
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from config import PROCESSED_DIR, DATABASE_URL, TABLE_NOME
import logging


logging.basicConfig(
     level=logging.INFO,
     format='%(asctime)s - %(levelname)s - %(message)s',
     force=True
     )

# ----- criando extracao

def load():

    try:
        
        arquivos = list(Path(PROCESSED_DIR).glob("*.parquet")
                        )
        
        ultimo_arquivo = max(
            arquivos,
            key=lambda arquivo: arquivo.stat().st_mtime
        )

        df = pd.read_parquet(ultimo_arquivo)


        motor = create_engine(DATABASE_URL)

        df.to_sql(
            TABLE_NOME,
            con=motor,
            if_exists="append",
            index=False
            )
        
    
    
    except Exception as e:
        logging.error(f"erro ao carregar {ultimo_arquivo} para o banco de dados: {e}")
        
        raise
# %%
# %%
from sqlalchemy import create_engine

engine = create_engine(
    DATABASE_URL
)

with engine.connect() as conn:
    print("Conectado!")
# %%
