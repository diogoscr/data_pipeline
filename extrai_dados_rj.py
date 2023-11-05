import os
from os.path import join
import pandas as pd  
from datetime import datetime, timedelta

#intervalo de datas
data_inicio = "2023-01-01"
data_fim = "2023-02-01"

city = 'RiodeJaneiro'
key = '9VQPZTQ59D5L5ZP9K5VT558EJ'

URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
    f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')

dados = pd.read_csv(URL)
print(dados.head())

file_path = f'/Users/diogoaraujo/Desktop/data_pipeline/semana={data_inicio}'
os.mkdir(file_path)

dados.to_csv(file_path + 'dados_brutos_rj.csv')
dados[['datetime','tempmin','temp','tempmax']].to_csv(file_path + 'temperatura_rj.csv')
dados[['datetime','description','icon']].to_csv(file_path + 'condicoes_rj.csv')
