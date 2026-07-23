#%%

from pathlib import Path
import pandas as pd
import logging
## Function to inspect the data




def inspect_dataframe(df, dataframe_name="dataframe"):

    try:
        
        
        logging.info(f"Inspecionando DataFrame: {dataframe_name}")

        print("=" * 80)
        print(f"DataFrame: {dataframe_name}")

        print("\nSchema:")
        df.printSchema()

        print("\nColunas:")
        print(df.columns)

        print("\nQuantidade de linhas:")
        print(df.count())

        print("\nAmostra dos dados:")
        df.show(5, truncate=False)

        logging.info(f"Inspeção finalizada: {dataframe_name}")

    except Exception as e:
        logging.error(f"Erro ao inspecionar DataFrame {dataframe_name}: {e}")
        raise

    
# %%
# %%

# %%
