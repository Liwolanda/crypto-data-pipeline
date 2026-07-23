#%%
from silver import silver_save
from spark_session import create_spark_session
from reader import read
from inspect_data import inspect_dataframe
import logging
from bronze import save_bronze
from pathlib import Path


FILES = {
    "branches": "../data/raw/Branches.csv"
    
    }


def main():
    try:
        logging.info("Criando Spark Session...")

        spark = create_spark_session()

        print("SPARK:", spark)
        print("TIPO:", type(spark))

        for name, path in FILES.items():
            logging.info(f"Lendo arquivo: {path}")

            df = read(spark, path)

            inspect_dataframe(df, name)

            bronze_path = f"../data/bronze/{name}"
            silver_path = f"../data/silver/{name}"

            print("=" * 60)
            print(f"Arquivo atual : {name}")
            print(f"Bronze path   : {bronze_path}")
            print(f"Path absoluto : {Path(bronze_path).resolve()}")
            print("=" * 60)

            save_bronze(df, bronze_path)

            silver_save(
                spark,
                bronze_path,
                silver_path
                )



        spark.stop()

        logging.info("Pipeline Bronze finalizado com sucesso.")

    except Exception as e:
        logging.error(f"Pipeline falhou: {e}")
        raise


if __name__ == "__main__":
    main()


# %%
import os
print(os.environ.get("HADOOP_HOME"))
# %%
