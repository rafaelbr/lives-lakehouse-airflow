import json
import os
import shutil
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook

codigo_ibge = 5300108
mes_ano = 202301

with DAG(
    dag_id='beneficio_auxilio_emergencial',
    schedule_interval=None,
    start_date=datetime(2023, 9, 13),
    params={'codigo_ibge': codigo_ibge, 'mes_ano': mes_ano}
) as dag:
    @task
    def ler_api(**context):
        hook = HttpHook(method='GET', http_conn_id='API_TRANSPARENCIA')
        pagina = 1
        codigo = context['params']['codigo_ibge']
        mesano = context['params']['mes_ano']
        os.mkdir(f'/tmp/auxilio_emergencial')
        while True:
            response = hook.run(
                endpoint=f'/api-de-dados/auxilio-emergencial-beneficiario-por-municipio?'
                         f'codigoIbge={codigo}&mesAno={mesano}&pagina={pagina}'
            )
            if response.status_code != 200:
                raise Exception('Erro ao ler API')
            data = response.json()
            if len(data) == 0:
                break
            json.dump(data, open(f'/tmp/auxilio_emergencial/auxilio_emergencial_{pagina}.json', 'x'))
            pagina += 1


    @task
    def envia_s3():
        hook = S3Hook(aws_conn_id='AWS-LIVES')
        files = os.listdir(f'/tmp/auxilio_emergencial')
        for file in files:
            print(f'Enviando arquivo {file} para o S3')
            hook.load_file(
                filename=f'/tmp/auxilio_emergencial/{file}',
                key=f'auxilio_emergencial/json/codigo_ibge={codigo_ibge}/mes_ano={mes_ano}/{file}',
                bucket_name='geekfox-live-bronze'
            )


    @task(trigger_rule='all_done')
    def limpa_arquivos():
        shutil.rmtree(f'/tmp/auxilio_emergencial', ignore_errors=True)


    ler_api() >> envia_s3() >> limpa_arquivos()

#5300108
