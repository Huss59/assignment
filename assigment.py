from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago






default_args = {
    'owner': 'linux_work',
    # 'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['hussain.nazir1998@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
# 'queue': 'bash_queue',
# 'pool': 'backfill',
# 'priority_weight': 10,
# 'end_date': datetime(2016, 1, 1),
}




dag = DAG(
    dag_id='Assignment',
    default_args=default_args,
    description='Web Scrapping with Airflow',
    schedule_interval=timedelta(days=1)
)




def webscraping():
    from selectorlib import Extractor #pip install selectorlib
    import requests
    import pandas as pd 
    r = requests.get('https://www.lushusa.com/face/cleansers-scrubs/?cgid=cleansers-scrubs&start=0&sz=28')
    yml_text = """
    product_data:
        css: 'div.d-flex div.product-tile'
        xpath: null
        multiple: true
        type: Text
        children:
    image:
            css: 'img.product-tile-image'
            xpath: null
            multiple: false
            type: Image
    name:
            css: 'h3.product-tile-name'
            xpath: null
            multiple: false
            type: Text
    price:
            css: 'div.tile-price-size span.tile-price'
            xpath: null
            multiple: false
            type: Text"""
    e = Extractor.from_yaml_string(yml_text)
    data = e.extract(r.text)
# print(data['product_data'])
    df = pd.DataFrame(data['product_data'])
    df.to_excel('hussain.xlsx', index=False)

assignment = PythonOperator(
    task_id = 'webscraping' ,
    python_callable = webscraping ,
    dag = dag

)

assignment