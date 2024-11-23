from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from books import books_etl, transferToS3, s3ToMongoDB

with DAG(
    dag_id='books',
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    description="Book Project",
    catchup=False
) as dag:
    
    books_etl = PythonOperator(
        task_id = 'books_etl',
        python_callable = books_etl,
    )

    transferToS3 = PythonOperator(
        task_id = 'transferToS3',
        python_callable = transferToS3
    )

    s3ToMongoDB = PythonOperator(
        task_id = 's3ToMongoDB',
        python_callable = s3ToMongoDB
    )

    books_etl >> transferToS3 >> s3ToMongoDB