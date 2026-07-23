import logging

from pyspark.sql import DataFrame, SparkSession


PRIMARY_KEYS = {
    "branches": ["branch_id"],
    "categories": ["itemid"],
    "customers": ["userid"],
    "orders": ["orderid"],
    "order_details": ["orderdetailid"],
}


def read_bronze(
    spark: SparkSession,
    bronze_path: str
) -> DataFrame:
    """Lê uma tabela Parquet da camada Bronze."""

    logging.info(f"Lendo Bronze: {bronze_path}")

    bronze_df = spark.read.parquet(bronze_path)

    return bronze_df


def standardize_column_names(
    df: DataFrame
) -> DataFrame:
    """Padroniza nomes das colunas para minúsculas e snake_case."""

    standardized_names = [
        column_name
        .strip()
        .lower()
        .replace(" ", "_")
        for column_name in df.columns
    ]

    standardized_df = df.toDF(*standardized_names)

    return standardized_df


def remove_duplicates(
    df: DataFrame,
    primary_keys: list[str]
) -> DataFrame:
    """Remove registros duplicados usando as chaves da tabela."""

    deduplicated_df = df.dropDuplicates(primary_keys)

    return deduplicated_df


def remove_null_primary_keys(
    df: DataFrame,
    primary_keys: list[str]
) -> DataFrame:
    """Remove registros cuja chave principal esteja nula."""

    valid_df = df.dropna(subset=primary_keys)

    return valid_df


def save_silver(
    df: DataFrame,
    silver_path: str
) -> None:
    """Salva o DataFrame tratado em Parquet na camada Silver."""

    logging.info(f"Salvando Silver: {silver_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    logging.info(f"Silver salva com sucesso: {silver_path}")


def process_silver(
    spark: SparkSession,
    table_name: str,
    bronze_path: str,
    silver_path: str
) -> DataFrame:
    """Orquestra todas as etapas da camada Silver."""

    try:
        primary_keys = PRIMARY_KEYS[table_name]

        bronze_df = read_bronze(
            spark,
            bronze_path
        )

        silver_df = standardize_column_names(
            bronze_df
        )

        silver_df = remove_duplicates(
            silver_df,
            primary_keys
        )

        silver_df = remove_null_primary_keys(
            silver_df,
            primary_keys
        )

        save_silver(
            silver_df,
            silver_path
        )

        return silver_df

    except KeyError:
        logging.exception(
            f"Não existe chave configurada para a tabela: {table_name}"
        )
        raise

    except Exception:
        logging.exception(
            f"Erro ao processar a tabela Silver: {table_name}"
        )
        raise