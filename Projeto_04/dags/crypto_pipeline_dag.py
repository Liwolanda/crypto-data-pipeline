#%%
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/src")

from extract import extracao_coins
from transform import extracao
from analytics import analytics
from load import load

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 6, 1),
    schedule="0/5 * * * *", # Runs every 5 minutes
    catchup=False,
    default_args=default_args,
    description="Pipeline de ETL para dados de criptomoedas",
    tags=["crypto", "etl", "parquet", "airflow"]
) as dag:
    
    extracao_task = PythonOperator(
        task_id="extract_crypto",
        python_callable=extracao_coins
    )

    transformacao_task = PythonOperator(
        task_id="transform_crypto",
        python_callable=extracao
    )

    analise_task = PythonOperator(
        task_id="analytics_crypto",
        python_callable=analytics
    )

    load_task = PythonOperator(
        task_id="load_crypto",
        python_callable=load
        )

    extracao_task >> transformacao_task >> analise_task >> load_task
# %%
