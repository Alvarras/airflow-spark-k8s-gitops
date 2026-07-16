from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

# ---------------------------------------------------------------------------
# Default arguments — mengikuti konvensi yang sama dengan spark_pi DAG
# ---------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0,
}

# ---------------------------------------------------------------------------
# Helper tasks
# ---------------------------------------------------------------------------
def startBatch():
    print('##### [Ecommerce Analytics] Pipeline Started #####')
    print(f'Run time: {datetime.now().isoformat()}')
    print('Analyses: Funnel | RFM Segmentation | Brand/Category Insight | Time-Series')


def done():
    print('##### [Ecommerce Analytics] Pipeline Completed #####')
    print(f'Finish time: {datetime.now().isoformat()}')
    print('Results loaded to PostgreSQL tables:')
    print('  - tbl_funnel_analysis')
    print('  - tbl_rfm_segments')
    print('  - tbl_brand_category_insight')
    print('  - tbl_price_sensitivity')
    print('  - tbl_hourly_activity')
    print('  - tbl_daily_activity')
    print('  - tbl_daily_revenue')


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id='spark_ecommerce_analytics',
    start_date=datetime.now() - timedelta(days=1),
    default_args=default_args,
    schedule=None,      # Manual trigger only
    catchup=False,
    tags=['ecommerce', 'pyspark', 'analytics', 'rfm', 'funnel'],
    description=(
        'Analisis data ecommerce (170k+ baris) dari S3: '
        'Funnel, RFM, Brand/Category Insight, dan Time-Series. '
        'Hasil dimuat ke PostgreSQL.'
    ),
) as dag:

    start_batch_task = PythonOperator(
        task_id='start_batch',
        python_callable=startBatch,
    )

    spark_ecommerce_task = SparkKubernetesOperator(
        task_id='spark_ecommerce_etl',
        namespace='airflow',
        # Path relatif terhadap folder dags/ — diikuti oleh GitSync
        application_file='spark-apps/spark-ecommerce-analytics.yaml',
        kubernetes_conn_id='kubernetes_default',
        # Polling interval & timeout untuk job yang memproses 170k+ baris
        poll_interval=10,
        startup_timeout_seconds=300,
    )

    done_task = PythonOperator(
        task_id='done',
        python_callable=done,
    )

    start_batch_task >> spark_ecommerce_task >> done_task
