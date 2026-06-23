#%%
from extract import extracao_coins
import logging
from transform import extracao
from analytics import analytics
import pandas as pd
from load import load

logging.basicConfig(
     level=logging.INFO,
     format='%(asctime)s - %(levelname)s - %(message)s',
     force=True
)


def main():
    arquivo = extracao_coins()
    print(arquivo)
    
    
    arquivo_interim = extracao()
    print(arquivo_interim)


    arquivo_processed = analytics()
    print(arquivo_processed)

    load()


if __name__ == "__main__":

    main()
# %%
main()
# %%


