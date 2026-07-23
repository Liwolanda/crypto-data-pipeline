import logging

from bronze import save_bronze
from inspect_data import inspect_dataframe
from reader import read
from silver import process_silver
from spark_session import create_spark_session


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


FILES = {
    "branches": "../data/raw/Branches.csv",
    "categories": "../data/raw/Categories.csv",
    "customers": "../data/raw/Customers.csv",
    "orders": "../data/raw/Orders.csv",
    "order_details": "../data/raw/Order_Details.csv",
    }


SMALL_TABLES = {
    "branches",
    "categories",
    "customers",
}


def preview_dataframe(
    df,
    dataframe_name: str
) -> None:
    """
    Mostra apenas schema e cinco linhas.

    Não executa count(), evitando uma leitura completa
    em tabelas grandes.
    """

    print("=" * 80)
    print(f"DataFrame: {dataframe_name}")

    print("\nSchema:")
    df.printSchema()

    print("\nAmostra:")
    df.show(5, truncate=False)


def main() -> None:
    spark = None

    try:
        logging.info("Criando SparkSession...")

        spark = create_spark_session()

        for name, raw_path in FILES.items():
            logging.info(f"Iniciando tabela: {name}")
            logging.info(f"Lendo arquivo Raw: {raw_path}")

            raw_df = read(
                spark,
                raw_path
            )

            if name in SMALL_TABLES:
                inspect_dataframe(
                    raw_df,
                    f"{name}_raw"
                )
            else:
                preview_dataframe(
                    raw_df,
                    f"{name}_raw"
                )

            bronze_path = f"../data/bronze/{name}"

            save_bronze(
                raw_df,
                bronze_path
            )

            silver_path = f"../data/silver/{name}"

            silver_df = process_silver(
                spark=spark,
                table_name=name,
                bronze_path=bronze_path,
                silver_path=silver_path
            )

            if name in SMALL_TABLES:
                inspect_dataframe(
                    silver_df,
                    f"{name}_silver"
                )
            else:
                preview_dataframe(
                    silver_df,
                    f"{name}_silver"
                )

            logging.info(f"Tabela finalizada: {name}")

        logging.info(
            "Pipeline Bronze e Silver executado com sucesso."
        )

    except Exception:
        logging.exception(
            "Erro durante a execução do pipeline."
        )
        raise

    finally:
        if spark is not None:
            logging.info("Encerrando SparkSession...")
            spark.stop()


if __name__ == "__main__":
    main()
