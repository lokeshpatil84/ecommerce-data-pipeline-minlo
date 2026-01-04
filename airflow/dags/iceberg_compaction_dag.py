from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
import boto3

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'iceberg_maintenance',
    default_args=default_args,
    description='Iceberg table maintenance and compaction',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['iceberg', 'maintenance'],
)

def run_iceberg_compaction(**context):
    """Run Iceberg table compaction using Athena"""
    client = boto3.client('athena')
    
    queries = [
        "OPTIMIZE glue_catalog.ecommerce_production.orders REWRITE DATA USING BIN_PACK",
        "VACUUM glue_catalog.ecommerce_production.orders",
        "ANALYZE TABLE glue_catalog.ecommerce_production.orders COMPUTE STATISTICS"
    ]
    
    for query in queries:
        try:
            response = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': 'ecommerce_production'},
                ResultConfiguration={'OutputLocation': 's3://ecommerce-pipeline-glue-temp-production/athena-results/'}
            )
            print(f"Started query: {query}")
            print(f"Execution ID: {response['QueryExecutionId']}")
        except Exception as e:
            print(f"Error running query {query}: {e}")
            raise

compaction_task = PythonOperator(
    task_id='compact_iceberg_tables',
    python_callable=run_iceberg_compaction,
    dag=dag,
)

compaction_task