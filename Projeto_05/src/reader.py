#%%
import logging
from pathlib import Path
# ----- funcao

def read(spark, filepath):

    try:

        SEPARADORES = {
            "Branches.csv": ",",
            "Categories.csv": ",",
            "Customers.csv": ";",
            "Orders.csv": ",",
            "Order_Details.csv": ","
        }

        sepador = SEPARADORES.get(Path(filepath).name, ",")
        
        
        df = spark.read.csv(
            filepath,
            header=True,
            inferSchema=True,
            sep=sepador
        )

        logging.info(f"arquivo lido com sucesso: {filepath}")

        return df
    
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo: {filepath}. Detalhes: {e}")
        raise
# %%
