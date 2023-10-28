from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaCreateFunctionOperator,
    LambdaInvokeFunctionOperator,
)

import boto3, json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "jagos",
    "depends_on_past": False,
    "email": ["jagacnoob2@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="beer-flow",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
) as dag:
    invoke_lambda_function_1 = LambdaInvokeFunctionOperator(
        task_id="scrapy", function_name="scrapy-lambda", invocation_type="Event"
    )

    invoke_lambda_function_2 = LambdaInvokeFunctionOperator(
        task_id="process", function_name="preprocess-lambda", invocation_type="Event"
    )

    delay_bash_task: BashOperator = BashOperator(
        task_id="delay_bash_task", bash_command="sleep 15s"
    )

    invoke_lambda_function_1 >> delay_bash_task >> invoke_lambda_function_2
