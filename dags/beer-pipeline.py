from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaCreateFunctionOperator,
    LambdaInvokeFunctionOperator,
)

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import boto3, json
import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args = {
    "start_date": pendulum.datetime(2023, 11, 2, tz="UTC"),
    "owner": "jagos",
    "depends_on_past": False,
    "email": ["jagacnoob2@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": "@daily",
}

with DAG(
    dag_id="beer-flow",
    default_args=default_args,
) as dag:
    # start scrapy
    invoke_lambda_function_1: LambdaInvokeFunctionOperator = (
        LambdaInvokeFunctionOperator(
            task_id="scrapy", function_name="scrapy-lambda", invocation_type="Event"
        )
    )
    # making sure that the file have time to be uploaded to s3
    delay_bash_task_1: BashOperator = BashOperator(
        task_id="delay_bash_task_1", bash_command="sleep 15s"
    )

    delay_bash_task_2: BashOperator = BashOperator(
        task_id="delay_bash_task_2", bash_command="sleep 15s"
    )

    # clean and load
    invoke_lambda_function_2: LambdaInvokeFunctionOperator = (
        LambdaInvokeFunctionOperator(
            task_id="process",
            function_name="preprocess-lambda",
            invocation_type="Event",
        )
    )

    create_final_table = SQLExecuteQueryOperator(
        task_id="final_dataset",
        sql="""create table if not exists final_table as (
                select t1.last_updated,
                    t1.beer_name,
                    t1.brewer_name,
                    t1.beer_style,
                    t1.location,
                    t1.percent_alcohol,
                    t1.beer_avg_score,
                    t1.avg_across_all,
                    t1.beer_number_of_reviews,
                    t1.number_of_ratings,
                    t1.date_added,
                    t1.number_of_wants,
                    t1.number_of_gots,
                    t1.status,
                    t2.brewer_avg_beer_score,
                    t2.number_of_beers,
                    t2.avg_brewer_score,
                    t2.brewer_number_of_reviews,
                    t2.city,
                    t2.establishment_type

                    from  beers t1
                    left join brewers t2
                        on t1.brewer_name = t2.brewer_name);""",
        conn_id="postgres_default",
        autocommit=True,
    )

    (
        invoke_lambda_function_1
        >> delay_bash_task_1
        >> invoke_lambda_function_2
        >> delay_bash_task_2
        >> create_final_table
    )
