from airflow.models import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
    'meu primeiro dag',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:

    tarefa1 = EmptyOperator(task_id = 'tarefa1')
    tarefa2 = EmptyOperator(task_id = 'tarefa2')
    tarefa3 = EmptyOperator(task_id = 'tarefa3')
    tarefa4 = BashOperator(
    task_id = 'cria_pasta',
    bash_command = 'mkdir -p "/diogoaraujo/Desktop/test'
    )

    tarefa1 >> [tarefa2, tarefa3]
    tarefa3 >> tarefa4