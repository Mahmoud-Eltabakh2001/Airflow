from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
     dag_id="firstdag_id",
     description="This is my first dag about sales",
     tags=["sales"],

     start_date=datetime(2025,4,28,4,5),
     schedule_interval="*/15 * * * *",
     dagrun_timeout=timedelta(minutes=45),
  
     catchup=False,
) as dag: 
    create_table=PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_conn",
        sql='''
             CREATE TABLE IF NOT EXISTS customers(
             customer_id varchar(50) not null,
             customer_name varchar not null,
             address varchar not null,
             birth_date date not null
             )
            '''
    )

    insert_values=PostgresOperator(
        task_id="insert_values",
        postgres_conn_id="postgres_conn",
        sql='''
             insert into customers values('2','Sama','Benha','2001-06-04'),
                                         ('3','Ahmed','Benha','2003-06-04'),
                                         ('4','Ali','Behera','2001-12-3'),
                                         ('5','Eslam','Qalub','1992-03-30'),
                                         ('6','Esraa','Qalub','1996-02-06');
            '''
    )

    create_table >> insert_values