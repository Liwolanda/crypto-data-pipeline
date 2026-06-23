#%%
import requests
import pandas as pd
import logging
from datetime import datetime
from config import *
from airflow.models import Variable


def extracao_coins():

    try:    
        
        
        registros = []
        
        horario_coleta = datetime.now()

        timestamp = horario_coleta.strftime("%Y%m%d_%H%M")
        
        nome_arquivo = f"crypto_prices_{timestamp}.parquet"

        

        caminho_arquivo = f"{RAW_DIR}/{nome_arquivo}"

        print(caminho_arquivo)

        url = "https://api.coingecko.com/api/v3/simple/price"

        
        coins = Variable.get(
            "crypto_coins",
            default_var=COINS
        )
        
        
        params = {      
            "ids" : coins,
            "vs_currencies": BASE_CURRENCY

}

        reposta = requests.get(
                   url,
            params=params
        )

        dados = reposta.json()

        
        for coin_id, values in dados.items():

            
            registro = {
                "coin_id": coin_id,
                "price_usd": values[BASE_CURRENCY],
                "extracted_at": horario_coleta
            }

            

        
            registros.append(registro)

        df = pd.DataFrame(registros)

        df.to_parquet(
            caminho_arquivo,
            index=False
        )

        print("retornando", caminho_arquivo)

        return caminho_arquivo

          
    except Exception as e:

        logging.error(f"erro {e}")
    
        raise
    
   
# %%
