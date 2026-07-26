import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


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


def transform_order_details(
    df: DataFrame
) -> DataFrame:
    """Converte as colunas monetárias de order_details para double."""

    monetary_columns = {
        "unitprice",
        "totalprice",
    }

    transformed_df = df.select(
        *[
            (
                F.regexp_replace(
                    F.col(column_name),
                    ",",
                    "."
                )
                .cast("double")
                .alias(column_name)
            )
            if column_name in monetary_columns
            else F.col(column_name)
            for column_name in df.columns
        ]
    )

    return transformed_df



def transform_orders(
    df: DataFrame
) -> DataFrame:
    """Trata as colunas numéricas e temporais da tabela orders."""

    transformed_df = df.withColumn(
        "totalbasket",
        F.regexp_replace(
            F.col("totalbasket"),
            ",",
            "."
        ).cast("double")
    )

    transformed_df = transformed_df.withColumn(
        "date_",
        F.to_date(
            F.col("date_"),
            "dd/MM/yyyy"
        )
    )

    return transformed_df

def transform_customers(
    df:DataFrame
) -> DataFrame:

    ## transformacao birthdate = date

    transformed_df = df.withColumn(
        "birthdate",
        F.to_date(
            F.col("birthdate"),
            "dd/MM/yyyy"
        )
    )

    return transformed_df

def transform_branches(
        df:DataFrame
) -> DataFrame:

    ## Transformacao do lat e log = colocar ponto

    transformed_df = df.withColumn(
        "lat",
        F.col("lat").cast("double") / 100000000
    ).withColum(
        F.col("lon").cast("double") / 100000000
    )
    return transformed_df
    


def apply_table_rules(
    df: DataFrame,
    table_name: str
) -> DataFrame:

    if table_name == "orders":
        return transform_orders(df)

    if table_name == "order_details":
        return transform_order_details(df)

    if table_name == "birthdate":
        return transform_customers(df)

    if table_name == "branch_id"
        return transform_branches(df) 

    return df




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

        silver_df = apply_table_rules(
            silver_df,
            table_name
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