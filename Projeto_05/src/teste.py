#%%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Teste")
    .master("local[*]")
    .getOrCreate()
)

df = spark.createDataFrame([
    (1, "João"),
    (2, "Maria")
], ["id", "nome"])

df.write.mode("overwrite").parquet("../data/teste")

print("SALVOU!")

spark.stop()
# %%
