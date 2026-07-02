from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id="dbt_build_models",
    description='Tratamento dos dados com dbt e gravação no MinIO',
    schedule=None,
    start_date=pendulum.datetime(2026, 6, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["transformacao", "dbt"],
)
def dbt_build_models():

    ano_mes = '2026_06'
    #ano_mes = pendulum.now("America/Sao_Paulo").format("YYYY_MM")

    @task.bash
    def dbt_build_stg(ano_mes: str) -> str:      

        return f'cd /opt/airflow/dbt && dbt build --select staging --vars \'{{"ano_mes": "{ano_mes}"}}\''
    
    @task.bash
    def dbt_build_dim(ano_mes: str) -> str:
        return f'cd /opt/airflow/dbt && dbt build --select marts --vars \'{{\"ano_mes\": \"{ano_mes}\"}}\''     
    
    dbt_build_stg(ano_mes) >> dbt_build_dim(ano_mes)

dag_execucao = dbt_build_models()