from airflow.sdk import dag, task
import pendulum
import os

@dag(
    dag_id="teste_variaveis_ambiente",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["teste", "infraestrutura"],
)
def teste_env_vars():

    @task
    def verificar_variaveis():
        print("INICIANDO TESTE DE VARIÁVEIS DE AMBIENTE...")        
        
        endpoint = os.getenv("MINIO_ENDPOINT")
        user = os.getenv("MINIO_ROOT_USER")
        password = os.getenv("MINIO_ROOT_PASSWORD")
        bucket = os.getenv("MINIO_BUCKET_BRONZE")        
        
        print(f"-> MINIO_ENDPOINT: {endpoint}")
        print(f"-> MINIO_ROOT_USER: {user}")
        print(f"-> MINIO_BUCKET_BRONZE: {bucket}")
                
        if password:            
            senha_mascarada = f"{password[:2]}******{password[-2:]}" if len(password) > 4 else "****"
            print(f"-> MINIO_ROOT_PASSWORD: {senha_mascarada} (Lida com sucesso!)")
        else:
            print("-> MINIO_ROOT_PASSWORD: None (Variável NÃO encontrada!)")      
        
        if None in [endpoint, user, password, bucket]:
            raise ValueError("FALHA: Pelo menos uma variável retornou None. Verifique o docker-compose.yaml!")
        else:
            print("SUCESSO ABSOLUTO: O Airflow está a ler perfeitamente o seu ficheiro .env!")

    verificar_variaveis()

# Instanciação da DAG
dag_execucao = teste_env_vars()