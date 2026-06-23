#%%
import pandas as pd
import logging
from config import INTERIM_DIR, PROCESSED_DIR
import numpy as np
from pathlib import Path
from datetime import datetime


# ----- pegando data
def analytics():

    try:
                # ----- usado pra cria o arquivo

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                nome_arquivo = f"crypto_analytcs_{timestamp}.parquet"

                caminho_processed = f"{PROCESSED_DIR}/{nome_arquivo}"
                
                logging.info("pegando informacoes!!")
                
                arquivos = list(Path(INTERIM_DIR).glob("*.parquet"))

                ultimo_arquivo = max(
                        arquivos,
                        key=lambda arquivo: arquivo.stat().st_mtime
                        
                )
                
                df = pd.read_parquet(ultimo_arquivo)
                
                logging.info(f"arquivo {df} pego com sucesso ")

                df = df.sort_values(by=["coin_id", "extracted_at"])
            
                df['previous_price_usd'] = (
                                   df.groupby('coin_id')['price_usd']
                    .shift(1)
                )

                df['price_diff_usd'] = (
                    df['price_usd'] - df['previous_price_usd']
                )

                df['price_diff_percent'] = (
                    (df['price_usd'] - df['previous_price_usd']) / df['previous_price_usd']  * 100
                )

                df.to_parquet(
                       caminho_processed,
                       index=False
                )
                
                
                
                return caminho_processed
    
    except Exception as e:
        logging.error(f"error {e}")

        raise

# %%




