from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from functions import get_last_date_from_table, extract_csv_orders, extract_csv_stocks, upload_to_gcs, upload_to_bq_orders, upload_to_bq_stocks, get_last_orderheader, headers_pipeline


default_args = {
    "owner":"airflow",
    "email_on_failure":False,
    "email_on_retry": False,
    "email": "mabdkarim@outlook.fr",
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}
with DAG("retail_dag", start_date=datetime(2023,1,1), 
         schedule_interval="0 7 * * *", default_args=default_args,
         catchup=False
        ) as dag:
    
    get_last_date = PythonOperator(
        task_id = "get_last_date",
        python_callable=get_last_date_from_table
    )

    extract_orders_csv = PythonOperator(
        task_id = "extract_orders_csv",
        python_callable=extract_csv_orders,
        op_args=[get_last_date.output]
    )

    extract_stocks_csv = PythonOperator(
        task_id = "extract_stocks_csv",
        python_callable=extract_csv_stocks,
        op_args=[get_last_date.output]
    )

    upload_orders_to_gcs = PythonOperator(
        task_id = "upload_orders_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[extract_orders_csv.output]
    )

    upload_stocks_to_gcs = PythonOperator(
        task_id = "upload_stocks_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[extract_stocks_csv.output]
    )

    upload_to_bq_orders = PythonOperator(
        task_id = "upload_to_bq_orders",
        python_callable=upload_to_bq_orders,
        op_args=[upload_orders_to_gcs.output]
    )

    upload_to_bq_stocks = PythonOperator(
        task_id = "upload_to_bq_stocks",
        python_callable=upload_to_bq_stocks,
        op_args=[upload_stocks_to_gcs.output]
    )

    get_last_header = PythonOperator(
        task_id = "get_last_header",
        python_callable=get_last_orderheader
    )

    headers_beam_pipeline = PythonOperator(
        task_id = "beam_headers_pipeline",
        python_callable=headers_pipeline,
        op_args=[upload_orders_to_gcs.output, get_last_header.output]
    )

    get_last_header >> get_last_date >> extract_orders_csv >> upload_orders_to_gcs >> upload_to_bq_orders >> headers_beam_pipeline
    get_last_date >> extract_stocks_csv >> upload_stocks_to_gcs >> upload_to_bq_stocks

