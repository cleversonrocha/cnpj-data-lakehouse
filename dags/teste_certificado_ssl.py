from airflow.sdk import dag, task
import pendulum
import requests

@dag(
    dag_id="teste_conexao_ssl_profissional",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["teste", "seguranca", "ssl"],
)
def teste_ssl_pipeline():

    @task
    def testar_conexao_estrita():
        
        url_teste = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/"                
        caminho_certificado = "/opt/airflow/certs/arquivos.receitafederal.gov.br.crt"
        
        print(f"Iniciando teste de conexão estrita...")
                
        try:
            #requests.head apenas testa a ligação e as credenciais de segurança, sem baixar dados
            response = requests.head(url_teste, verify=caminho_certificado, timeout=15)                       
            print("-> O Airflow conseguiu validar a identidade do servidor do governo.")
            print(f"-> Código de Resposta HTTP: {response.status_code}")                        
            
        except requests.exceptions.SSLError as ssl_err:
            print("ERRO DE VALIDAÇÃO SSL!")
            print(f"Detalhes técnicos: {ssl_err}")            
            raise ssl_err
            
        except Exception as e:
            print(f"Ocorreu um erro inesperado de rede: {e}")
            raise e

    testar_conexao_estrita()

# Instanciação da DAG
dag_execucao = teste_ssl_pipeline()