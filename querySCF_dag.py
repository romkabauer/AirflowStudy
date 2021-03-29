import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import urllib
import json
import glob
import re
import os
from elasticsearch import Elasticsearch, helpers


default_args = {
    'owner': 'roman_bauer',
    'start_date': dt.datetime(2021, 3, 21, 15, 0, 0, 0),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


def querySCF():
    params = {'place_url':'bernalillo-county','per_page':'100'}
    url = 'https://seeclickfix.com/api/v2/issues?' + urllib.parse.urlencode(params)
    rawreply = urllib.request.urlopen(url).read()
    reply = json.loads(rawreply)
    
    with open(f'/home/roman_bauer/Documents/airflow_temp/data_page1.json', 'w', encoding='utf-8') as f:
        json.dump(reply, f, ensure_ascii=False, indent=4)
    
    while reply['metadata']['pagination']['page'] < reply['metadata']['pagination']['pages']:
        url = reply['metadata']['pagination']['next_page_url']
        rawreply = urllib.request.urlopen(url).read()
        reply = json.loads(rawreply)
        with open('/home/roman_bauer/Documents/airflow_temp/data_page' + str(reply['metadata']['pagination']['page']) + '.json', 'w', encoding='utf-8') as f:
            json.dump(reply, f, ensure_ascii=False, indent=4)


def querySCF_archive():
    params = {'place_url':'bernalillo-county','per_page':'100', 'status':'Archived'}
    url = 'https://seeclickfix.com/api/v2/issues?' + urllib.parse.urlencode(params)
    rawreply = urllib.request.urlopen(url).read()
    reply = json.loads(rawreply)
    
    with open(f'/home/roman_bauer/Documents/airflow_temp/data_page1.json', 'w', encoding='utf-8') as f:
        json.dump(reply, f, ensure_ascii=False, indent=4)
    
    while reply['metadata']['pagination']['page'] < reply['metadata']['pagination']['pages']:
        url = reply['metadata']['pagination']['next_page_url']
        rawreply = urllib.request.urlopen(url).read()
        reply = json.loads(rawreply)
        with open('/home/roman_bauer/Documents/airflow_temp/arch_data_page' + str(reply['metadata']['pagination']['page']) + '.json', 'w', encoding='utf-8') as f:
            json.dump(reply, f, ensure_ascii=False, indent=4)


def prepareElastcsearchMessage():
    for js_file in [f for f in os.listdir('/home/roman_bauer/Documents/airflow_temp') if re.match(r'(arch_)?data_page[0-9]+\.json', f)]:
        with open('/home/roman_bauer/Documents/airflow_temp/' + js_file, 'r+', encoding='utf-8') as f:
            json_obj = json.load(f)
            for ind in json_obj['issues']:
                ind['location'] = ','.join([str(ind['lat']), str(ind['lng'])])
                date_cr = ind['created_at'].split('T')
                ind['opendate'] = date_cr[0]
            f.seek(0)
            json.dump(json_obj, f)
            f.truncate()


def putIntoES():
    es = Elasticsearch({'192.168.31.44'})

    for js_file in [f for f in os.listdir('/home/roman_bauer/Documents/airflow_temp') if re.match(r'(arch_)?data_page[0-9]+\.json', f)]:
        with open('/home/roman_bauer/Documents/airflow_temp/' + js_file, 'r', encoding='utf-8') as f:
            json_obj = json.loads(f.read())
            for ind in json_obj['issues']:
                es.update(index='scf', doc_type='_doc', id=ind['id'], body={ "doc": ind, "doc_as_upsert": True})
            

with DAG('queryDataSCF',
        default_args=default_args,
        schedule_interval=timedelta(hours=8),
        ) as dag:
    queryData = PythonOperator(task_id='querySCF',
                               python_callable=querySCF)
    prepareData = PythonOperator(task_id='prepareSCFData',
                                python_callable=prepareElastcsearchMessage)
    putData = PythonOperator(task_id='putIntoElastic',
                            python_callable=putIntoES)
    queryArchiveData = PythonOperator(task_id='querySCF_arch',
                                    python_callable=querySCF_archive)


queryData >> prepareData >> putData
queryArchiveData >> prepareData >> putData