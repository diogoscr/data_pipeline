from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import  PythonOperator
import pendulum
from os.path import join
import pandas as pd

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2023, 12, 16, tz="UTC"),
    schedule_interval='0 0 * * 1', #executar toda segunda feira
) as dag:
    
    tarefa1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/diogoaraujo/Desktop/data_pipeline/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados():
        city = 'Boston'
        key = 'KEY'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        file_path = f'/Users/diogoaraujo/Desktop/data_pipeline/semana={data_interval_end}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime','tempmin','temp','tempmax']].to_csv(file_path + 'temperatura.csv')
        dados[['datetime','description','icon']].to_csv(file_path + 'condicoes.csv')


    tarefa2 = PythonOperator(
        task_id="extrai_dados",
        python_callable=extrai_dados,
        op_kwargs= {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None

    )

    tarefa1 >> tarefa2