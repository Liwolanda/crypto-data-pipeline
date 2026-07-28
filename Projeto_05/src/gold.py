#%%
import logging

from pyspark.sql import DataFrame, SparkSession


def read_silver(
    spark: SparkSession,
    silver_path: str,
    table_name: str
) -> DataFrame:
    """Lê uma tabela Parquet da camada Silver."""

    logging.info(
        f"Lendo tabela Silver: {table_name} em {silver_path}"
    )

    silver_df = spark.read.parquet(silver_path)

    return silver_df


def create_gold_sales(
    orders_df: DataFrame,
    order_details_df: DataFrame
) -> DataFrame:
    """Une pedidos e itens dos pedidos pelo campo orderid."""

    gold_df = orders_df.join(
        order_details_df,
        on="orderid",
        how="inner"
    )

    return gold_df

def join_customers (
        gold_df: DataFrame,
        customers_df:DataFrame
) -> DataFrame:

    #Unindo gold_df no Customers.csv pelo USERID

    joined_df = gold_df.join(
        customers_df,
        on="userid",
        how="inner"
    )

    return joined_df

def join_branches (
    joined_df:DataFrame,
    branches_df:DataFrame

) -> DataFrame :

    #Unindo joined_df no branches pelo Branch_id

    joined_df = joined_df.join(
        branches_df,
        on="branch_id",
        how="inner"
    )

    return joined_df


def save_gold(
    df: DataFrame,
    gold_path: str
) -> None:
    """Salva um DataFrame na camada Gold em formato Parquet."""

    logging.info(f"Salvando Gold em: {gold_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(gold_path)
    )

    logging.info(
        f"Gold salva com sucesso em: {gold_path}"
    )


def process_gold(
    spark: SparkSession,
    orders_silver_path: str,
    order_details_silver_path: str,
    gold_path: str
) -> DataFrame:
    """Orquestra a criação da tabela gold_sales."""

    try:
        orders_df = read_silver(
            spark=spark,
            silver_path=orders_silver_path,
            table_name="orders"
        )

        order_details_df = read_silver(
            spark=spark,
            silver_path=order_details_silver_path,
            table_name="order_details"
        )

        gold_df = create_gold_sales(
            orders_df=orders_df,
            order_details_df=order_details_df
        )

        save_gold(
            df=gold_df,
            gold_path=gold_path
        )

        return gold_df

    except Exception:
        logging.exception(
            "Erro ao processar a tabela gold_sales."
        )
    raise
# %%
