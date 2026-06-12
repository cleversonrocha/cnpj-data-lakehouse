from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id='carregamento_gold',    
    description='DAG dbt + DuckDB',
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 6, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['dbt', 'duckdb', 'gold', 'taskflow'],
)
def dbt_duckdb_pipeline():
    
    @task.bash
    def dbt_run() -> str:
        """Roda os modelos dbt (DuckDB em memória -> grava Parquet no MinIO)"""
        return (
            'dbt run '
            '--project-dir /opt/airflow/dbt/camada_gold '
            '--profiles-dir /opt/airflow/dbt/camada_gold'
        )
    
    run_exec = dbt_run()
    
    run_exec

# Instancia a DAG para o Airflow localizá-la
dbt_duckdb_pipeline()