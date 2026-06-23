#%%
from pathlib import Path
from config import RAW_DIR, INTERIM_DIR
import pandas as pd
import logging
from datetime import datetime


logging.basicConfig(
     level=logging.INFO,
     format='%(asctime)s - %(levelname)s - %(message)s',
     force=True
)

def extracao():

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        nome_arquivo = f"crypto_prices_interim_{timestamp}.parquet"

        caminho_interim = f"{INTERIM_DIR}/{nome_arquivo}"
        
        logging.info("arquivos sendo puxados!!")
        
        arquivos = list(Path(RAW_DIR).glob("*.parquet"))

        logging.info(f"{len(arquivos)}puxados com sucesso")

        listas_df = []

        for arquivo in arquivos:

            df = pd.read_parquet(arquivo)
            print("ARQUIVO:", arquivo.name)
            print("SHAPE:", df.shape)
            print("COLUNAS:", df.columns.tolist())

            listas_df.append(df)


        df_final = pd.concat(listas_df, ignore_index=True)

        df_final.to_parquet(
            caminho_interim,
            index=False
        )
        
        return caminho_interim
        
        

    except Exception as e:
        
        logging.error(f"erro ao puxar {arquivos}: {e}")
        
        raise



    
        

            

# %%
# %%
