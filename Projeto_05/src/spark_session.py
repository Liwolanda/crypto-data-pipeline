#%%

from pyspark.sql import SparkSession

# ----- cd C:\Users\leodo\Projetos\Projeto_curriculo\Projeto_05 
# ----- .\.venv\Scripts\Activate.ps1
# ----- para sair deactivate






def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Projeto_05_Sales_Analytics")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    return spark


# %%

# %%
