#%%
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch


default_args = {
    'owner': 'roman_bauer',
    'start_date': dt.datetime(2021, 3, 20),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


home = '/home/roman_bauer/Documents/'


def queryPostgresql():
    conn_string = "dbname='dataengineering' host='192.168.31.44' user='postgres' password='hinSo3Be'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select name, city from users", conn)
    print(df)
    df.to_csv(home + 'postgres_samples/postgresqldata.csv')
    print("-------Data Saved------")


def insertElasticsearch():
    es = Elasticsearch([{'host': '192.168.31.44', 'port': '9200'}]) 
    df = pd.read_csv(home + 'postgres_samples/postgresqldata.csv')
    for i,r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="frompostgresql",
                    doc_type = "doc", body = doc)
        print(res)	


with DAG('postgres_to_ES',
        default_args=default_args,
        schedule_interval=timedelta(minutes=5),
        ) as dag:
    getData = PythonOperator(task_id='QueryPostgreSQL',
                            python_callable=queryPostgresql)
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                python_callable=insertElasticsearch)


getData >> insertData