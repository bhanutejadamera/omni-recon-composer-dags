from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='omni_recon_etl_dag_v2',
    default_args=default_args,
    description='ETL DAG for Sales Reconciliation: PySpark + BigQuery',
    schedule_interval=None,
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['sales', 'reconciliation', 'pyspark', 'bigquery']
) as dag:

    run_pyspark_job = BashOperator(
        task_id='run_pyspark_job',
        bash_command="""
        gcloud dataproc jobs submit pyspark \
        --cluster=omni-dataproc-cluster \
        --region=us-central1 \
        gs://omni-recon-data/scripts/sales_reconciliation_etl.py \
        -- gs://omni-recon-data/raw gs://omni-recon-data/processed
        """
    )

    load_parquet_to_bigquery = BashOperator(
        task_id='load_parquet_to_bigquery',
        bash_command="""
        bq load \
        --source_format=PARQUET \
        --autodetect \
        --replace \
        --time_partitioning_field=transaction_date \
        omni_recon.sales_reconciliation_fact \
        gs://omni-recon-data/processed/sales_reconciliation_fact/year=2025/month=1/*.parquet,gs://omni-recon-data/processed/sales_reconciliation_fact/year=2025/month=2/*.parquet,gs://omni-recon-data/processed/sales_reconciliation_fact/year=2025/month=3/*.parquet,gs://omni-recon-data/processed/sales_reconciliation_fact/year=2025/month=4/*.parquet
        """
    )

    run_pyspark_job >> load_parquet_to_bigquery
