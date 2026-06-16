from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id="dbt_silver",
    description='Tratamento dos dados com dbt e gravação no MinIO',
    schedule=None,
    start_date=pendulum.datetime(2026, 5, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["transformacao", "dbt"],
)
def dbt_silver_dag():

    @task.bash
    def dbt_build() -> str:
        return 'cd /opt/airflow/dbt && dbt build && rm -f /opt/airflow/temp/cnpj_data_lakehouse.db'   
    
    dbt_build()

dag_execucao = dbt_silver_dag()