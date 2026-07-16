import os
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

# Dapatkan directory tempat file DAG ini berada agar path YAML selalu absolut
DAG_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Default arguments — mengikuti konvensi yang sama dengan spark_pi DAG
# ---------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 15),
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
    start_date=datetime(2026, 7, 15),
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

    def run_spark_operator(**kwargs):
        import traceback
        try:
            from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
            op = SparkKubernetesOperator(
                task_id='spark_ecommerce_etl_inner',
                namespace='airflow',
                application_file=os.path.join(DAG_DIR, 'spark-apps', 'spark-ecommerce-analytics.yaml'),
                kubernetes_conn_id='kubernetes_default',
                poll_interval=10,
                startup_timeout_seconds=300,
            )
            # Jalankan operator secara terprogram
            return op.execute(context=kwargs)
        except Exception as e:
            err_msg = traceback.format_exc()
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host='10.5.0.35',
                    database='dbadmin',
                    user='dbadmin',
                    password='P@ssw0rd123',
                    port='5432'
                )
                cur = conn.cursor()
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS etl_error_log (
                        id SERIAL PRIMARY KEY,
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("INSERT INTO etl_error_log (error_message) VALUES (%s);", (err_msg,))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as db_err:
                print(f"Failed to log exception to Postgres: {db_err}")
            raise e

    spark_ecommerce_task = PythonOperator(
        task_id='spark_ecommerce_etl',
        python_callable=run_spark_operator,
    )

    done_task = PythonOperator(
        task_id='done',
        python_callable=done,
    )

    start_batch_task >> spark_ecommerce_task >> done_task
