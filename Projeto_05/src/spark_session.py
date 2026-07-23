#%%

from pyspark.sql import SparkSession

# ----- cd C:\Users\leodo\Projetos\Projeto_curriculo\Projeto_05 
# ----- .\.venv\Scripts\Activate.ps1
# ----- para sair deactivate

    
from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession
        .builder
        .appName("Projeto_05_Sales_Analytics")
        .master("local[*]")
        .getOrCreate()
    )

    return spark


# %%

# %%
