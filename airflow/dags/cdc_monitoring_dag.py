from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
import boto3
import requests

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cdc_monitoring_pipeline',
    default_args=default_args,
    description='Monitor CDC pipeline and trigger Glue jobs',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['cdc', 'kafka', 'glue', 'iceberg'],
)

def check_debezium_connector(**context):
    """Check if Debezium connector is running"""
    try:
        response = requests.get('http://debezium:8083/connectors/postgres-connector/status')
        status = response.json()
        
        if status['connector']['state'] != 'RUNNING':
            raise Exception(f"Debezium connector not running: {status}")
        
        print(f"Debezium connector status: {status['connector']['state']}")
        return True
    except Exception as e:
        print(f"Error checking Debezium: {e}")
        raise

def check_kafka_lag(**context):
    """Check Kafka consumer lag"""
    # This is simplified - in production use kafka-python or confluent-kafka
    print("Checking Kafka consumer lag...")
    # Add actual lag checking logic here
    return True

def validate_iceberg_data(**context):
    """Validate data in Iceberg tables"""
    client = boto3.client('glue')
    
    try:
        # Get table statistics
        response = client.get_table(
            DatabaseName='ecommerce_production',
            Name='orders'
        )
        
        print(f"Table location: {response['Table']['StorageDescriptor']['Location']}")
        print(f"Last updated: {response['Table']['UpdateTime']}")
        return True
    except Exception as e:
        print(f"Error validating Iceberg data: {e}")
        raise

check_debezium = PythonOperator(
    task_id='check_debezium_connector',
    python_callable=check_debezium_connector,
    dag=dag,
)

check_lag = PythonOperator(
    task_id='check_kafka_lag',
    python_callable=check_kafka_lag,
    dag=dag,
)

run_aggregation = GlueJobOperator(
    task_id='run_iceberg_aggregation',
    job_name='ecommerce-pipeline-iceberg-aggregation',
    script_location='s3://ecommerce-pipeline-glue-scripts-production/iceberg_aggregation.py',
    aws_conn_id='aws_default',
    region_name='us-east-1',
    iam_role_name='ecommerce-pipeline-glue-role',
    dag=dag,
)

wait_for_aggregation = GlueJobSensor(
    task_id='wait_for_aggregation',
    job_name='ecommerce-pipeline-iceberg-aggregation',
    run_id='{{ task_instance.xcom_pull(task_ids="run_iceberg_aggregation", key="return_value") }}',
    aws_conn_id='aws_default',
    dag=dag,
)

validate_data = PythonOperator(
    task_id='validate_iceberg_data',
    python_callable=validate_iceberg_data,
    dag=dag,
)

check_debezium >> check_lag >> run_aggregation >> wait_for_aggregation >> validate_data
