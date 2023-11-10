import json
import os
import shutil
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.http.hooks.http import HttpHook

with DAG(
    dag_id="orgao_dag",
    schedule_interval=None,
    start_date=datetime(2023, 8, 20),
    catchup=False,
    tags=['lakehouse']
) as dag:
    @task
    def ler_api():
        hook = HttpHook(method='GET', http_conn_id='API_TRANSPARENCIA')
        pagina = 1
        os.mkdir(f'/opt/airflow/temp/orgaos')
        while True:
            response = hook.run(endpoint=f'/api-de-dados/orgaos-siape?pagina={pagina}')
            if response.status_code != 200:
                raise Exception('Erro ao ler API')
            data = response.json()
            if len(data) == 0:
                break
            json.dump(data, open(f'//opt/airflow/temp/orgaos/orgao_{pagina}.json', 'x'))
            pagina += 1

    @task
    def envia_s3():
        hook = S3Hook(aws_conn_id='AWS-LIVES')
        files = os.listdir(f'/tmp/orgaos')
        for file in files:
            print(f'Enviando arquivo {file} para o S3')
            hook.load_file(
                filename=f'/opt/airflow/temp/orgaos/{file}',
                key=f'orgaos_siapi/json/{file}',
                bucket_name='geekfox-live-bronze'
            )

    @task(trigger_rule='all_done')
    def limpa_arquivos():
        shutil.rmtree(f'/opt/airflow/temp/orgaos', ignore_errors=True)

    ler_api() >> envia_s3() >> limpa_arquivos()







