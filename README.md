# 🏢 Projeto de Estudos: Data Lakehouse com Dados do Cadastro Nacional da Pessoa Jurídica - CNPJ

Olá! Meu nome é Cleverson. Sou um profissional com mais de 15 anos de experiência em análise e desenvolvimento de sistemas (PHP, PL/SQL, bancos relacionais) e atualmente estou em **transição de carreira para a Engenharia de Dados**.

Este repositório contém o meu principal projeto prático. O objetivo aqui foi sair da teoria e construir um pipeline de **Big Data** do zero, processando os dados públicos da Receita Federal (que somam dezenas de gigabytes) usando uma infraestrutura local que simula o ambiente de nuvem.

## 🎯 O Desafio
Os dados públicos do CNPJ são pesados, divididos em múltiplos arquivos `.zip` e difíceis de manipular em ferramentas tradicionais (como o Excel ou bancos transacionais comuns). 

Meu desafio foi criar um processo automatizado para ingerir, limpar, transformar e disponibilizar esses dados para análises rápidas, aplicando o conceito de **Arquitetura Medalhão**.

### 🗂️ Fonte dos Dados
Os dados brutos processados neste pipeline são públicos, disponibilizados pela Receita Federal do Brasil e atualizados mensalmente.
- **Portal de Dados Abertos:** [Cadastro Nacional da Pessoa Jurídica - CNPJ](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- **Formato de Origem:** Arquivos `.zip` contendo tabelas `.csv` com o cadastro completo de empresas, estabelecimentos, sócios e tabelas de domínio.

## 🛠️ Stack Tecnológico que estou estudando/aplicando

Como minha máquina local atua como o servidor, decidi usar o **Docker** para isolar e orquestrar os seguintes serviços:

* **Apache Airflow:** Para agendar e automatizar o download, extração e transformação dos dados.
* **MinIO:** Para simular o Amazon S3 localmente (criando as camadas de armazenamento separadas do processamento).
* **DuckDB:** Para o processamento, lendo os CSVs e convertendo para o formato colunar Parquet.
* **DBT:** Para tratar e modelar os dados. Documentação: 👉 **[https://cleversonrocha.github.io/cnpj-data-lakehouse](https://cleversonrocha.github.io/cnpj-data-lakehouse)**
* **Databricks:** Para armazenar os dados em nuvem e apresentar com Power BI.

**Docker Containers:**
![Docker Containers](imagens/docker_containers.png)

**Docker Images:**
![Docker Images](imagens/docker_images.png)

## 🏗️ Como estruturei os dados (Arquitetura Medalhão)

- 🥉 **Bronze:** O Airflow baixa um arquivo compactado no formato "ZIP" do [Portal de Dados Abertos do Governo Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj), e extrai outros arquivos "ZIP" no bucket "silver/raw" do MinIO exatamente como vieram (dados brutos).
- 🥈 **Silver:** O DuckDB utilizando a extensão "zipfs" lê os arquivos "CSV" de dentro dos arquivos "ZIP" da camada Bronze, remove os Null Bytes e converte para o formato parquet, salvando no bucket "silver/raw" do MinIO, e depois com DBT, renomeia as colunas, tipa os dados e grava na mesma camada silver no bucket "silver/cleaned".
- 🥇 **Gold:** Com DBT e DuckDB, são lidos os dados do bucket MinIO "silver/cleaned" e feita as agregações e cruzamentos (ex: Estabelecimentos + Municípios + CNAEs) para criar um modelo pronto para ferramentas de BI, como o Power BI, e salvos no bucket "gold/bridge","gold/dim","gold/fact","gold/int" e "gold/agg" do MinIO.

Obs.: Uma cópia de toda a estrutura do armazenamento local é gravada também no Databricks.

**MinIO Local**

- Camada Bronze:
![MinIO Bucket Bronze](imagens/minio_bucket_bronze.png)

- Camada Silver:
![MinIO Bucket Silver Raw](imagens/minio_bucket_silver_raw.png)
![MinIO Bucket Silver Cleaned](imagens/minio_bucket_silver_cleaned.png)

- Camada Gold:
![MinIO Bucket Gold Bridge](imagens/minio_bucket_gold_bridge.png)
![MinIO Bucket Gold Dim](imagens/minio_bucket_gold_dim.png)
![MinIO Bucket Gold Fact](imagens/minio_bucket_gold_fact.png)
![MinIO Bucket Gold Int](imagens/minio_bucket_gold_int.png)
![MinIO Bucket Gold Agg](imagens/minio_bucket_gold_agg.png)

**Databricks:**

- Camada Bronze:
![Databricks Bucket Bronze](imagens/databricks_bronze_raw_1.png)
![Databricks Bucket Bronze](imagens/databricks_bronze_raw_2.png)

- Camada Silver:
![Databricks Bucket Silver](imagens/databricks_silver_raw.png)
![Databricks Bucket Silver](imagens/databricks_silver_cleanead.png)

- Camada Gold:
![Databricks Bucket Gold](imagens/databricks_gold_agg.png)
![Databricks Bucket Gold](imagens/databricks_gold_bridge.png)
![Databricks Bucket Gold](imagens/databricks_gold_dim.png)
![Databricks Bucket Gold](imagens/databricks_gold_fact.png)
![Databricks Bucket Gold](imagens/databricks_gold_int_1.png)
![Databricks Bucket Gold](imagens/databricks_gold_int_2.png)

**Modelo Star Schema:**
![Modelo Star Schema](imagens/star_schema.png)

**Modelo Star Schema no Power BI com Agregações e Medidas:**
![Modelo Star Schema Power BI](imagens/power_bi_star_schema.jpg)


### 🗺️ Diagrama da Arquitetura

![Pipeline de Dados - RFB](imagens/pipeline_rfb.gif)

## ⚙️ Como rodar na sua máquina

1. Crie na sua conta do Databricks o catálogo, schemas,volumes e delta tables utilizando o  [notebook](databricks/notebooks/create_catalog_schemas_volumes_tables.ipynb).
- **Execute o bloco de código abaixo para criar o catálogo, schemas, volumes:**
```
%sql
DROP SCHEMA IF EXISTS cnpj_data_lakehouse.bronze CASCADE;
DROP SCHEMA IF EXISTS cnpj_data_lakehouse.silver CASCADE;
DROP SCHEMA IF EXISTS cnpj_data_lakehouse.gold CASCADE;
CREATE CATALOG IF NOT EXISTS cnpj_data_lakehouse;
CREATE SCHEMA IF NOT EXISTS cnpj_data_lakehouse.bronze;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.bronze.raw;
CREATE SCHEMA IF NOT EXISTS cnpj_data_lakehouse.silver;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.silver.raw;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.silver.cleaned;
CREATE SCHEMA IF NOT EXISTS cnpj_data_lakehouse.gold;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.gold.bridge;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.gold.dim;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.gold.fact;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.gold.int;
CREATE VOLUME IF NOT EXISTS cnpj_data_lakehouse.gold.agg;
```

![CELL 1](imagens/cell_1.png)

2. Clone o repositório: `git clone https://github.com/cleversonrocha/cnpj-data-lakehouse.git`
3. Crie um arquivo `.env` na pasta raiz com o conteúdo abaixo alterando os valores das variáveis conforme necessário:
```
AIRFLOW_UID=50000
FERNET_KEY=#Execute o comando -> python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__API__SECRET_KEY=#Execute o comando -> python -c "import secrets; print(secrets.token_urlsafe(16))"

_AIRFLOW_WWW_USER_CREATE=true
_AIRFLOW_WWW_USER_USERNAME=<seu_usuario>
_AIRFLOW_WWW_USER_PASSWORD=<sua_senha>
_AIRFLOW_WWW_USER_ROLE=Admin
_AIRFLOW_WWW_USER_FIRSTNAME=<seu_nome>
_AIRFLOW_WWW_USER_LASTNAME=<seu_sobrenome>
_AIRFLOW_WWW_USER_EMAIL=<seu_email>

MINIO_ROOT_USER=<seu_usuario>
MINIO_ROOT_PASSWORD=<sua_senha>
MINIO_ENDPOINT="http://minio:9000"

DATABRICKS_HOST=<host_databricks>
DATABRICKS_TOKEN=<databricks_token> #No menu principal do Databricks -> SQL Warehouse -> Python -> Create a personal access token - scope(all-apis) Ex.: dapi...
```
4. Suba os containers com o Docker: `docker-compose up -d`
5. Acesse o MinIO em `localhost:9001` e crie os buckets `bronze`, `silver` e `gold`.
6. Acesse o Airflow em `localhost:8080`.
7. A DAG "ingestao_bronze" vai iniciar e baixar automaticamente os dados do Portal Dados Abertos do Governo Federal, e após terminar será feita a chamada para a DAG "ingestao_silver", onde os arquivos "zip" são descompactados e o DuckDB lê os arquivos "csv" e converte para o formato "parquet" salvando os arquivos no bucket local "silver", ao finalizar, será iniciada a DAG "dbt_build_models" onde os dados serão tratados, modelados e salvos no bucket "gold" e subir todos os arquivos dos buckets do MinIO local para o Databricks.

**DAG ingestao_bronze:**
![DAG ingestao_bronze](imagens/ingestao_bronze.png)

**DAG ingestao_silver:**
![DAG ingestao_silver](imagens/ingestao_silver.png)

**DAG dbt_build_models:**
![DAG dbt_build_models](imagens/dbt_build_models.png)

**DAG auditoria_bronze_para_silver:**
Obs.: Deve ser iniciada manualmente.
![DAG auditoria_bronze_para_silver](imagens/auditoria_bronze_para_silver.png)

8. Após os arquivos ja estiverem no Databricks:
- **Execute o 2º Bloco de código abaixo para criar as tabelas:**
```
import os
import datetime as dt

ano_mes = dt.datetime.now().strftime("%Y_%m")
volumes = os.listdir('/Volumes/cnpj_data_lakehouse/gold') #bridge, dim, fact, int, agg

for volume in volumes:
  if(volume.startswith('int')): continue 
      
  files = os.listdir(f"/Volumes/cnpj_data_lakehouse/gold/{volume}/{ano_mes}")       
      
  for file in files:
    table_name = file.replace('.parquet','')        

    spark.sql(f"DROP TABLE IF EXISTS cnpj_data_lakehouse.gold.{table_name}")    
    
    spark.sql(f"CREATE OR REPLACE TABLE cnpj_data_lakehouse.gold.{table_name} USING DELTA AS SELECT * FROM parquet.`/Volumes/cnpj_data_lakehouse/gold/{volume}/{ano_mes}/{file}`")
    
    if(table_name.startswith('agg_')): continue

    sql_alter_table = f"ALTER TABLE cnpj_data_lakehouse.gold.{table_name}"

    match table_name:
        case 'bridge_estabelecimentos_socios':
            spark.sql(f"{sql_alter_table} ALTER COLUMN sk_estabelecimento SET NOT NULL")
            spark.sql(f"{sql_alter_table} ALTER COLUMN sk_socio SET NOT NULL")
            spark.sql(f"{sql_alter_table} ADD CONSTRAINT pk_{table_name}_sk_id PRIMARY KEY (sk_estabelecimento, sk_socio)")

        case 'bridge_estabelecimentos_cnaes':
            spark.sql(f"{sql_alter_table} ALTER COLUMN sk_estabelecimento SET NOT NULL")
            spark.sql(f"{sql_alter_table} ALTER COLUMN sk_cnae SET NOT NULL")
            spark.sql(f"{sql_alter_table} ADD CONSTRAINT pk_{table_name}_sk_id PRIMARY KEY (sk_estabelecimento, sk_cnae)")
        
        case 'fact_estabelecimentos':
            spark.sql(f"{sql_alter_table} ALTER COLUMN sk_estabelecimento SET NOT NULL")
            spark.sql(f"{sql_alter_table} ADD CONSTRAINT pk_{table_name}_sk_estabelecimento PRIMARY KEY (sk_estabelecimento)")        

        case _:    
            spark.sql(f"{sql_alter_table} ALTER COLUMN sk_id SET NOT NULL")
            spark.sql(f"{sql_alter_table} ADD CONSTRAINT pk_{table_name}_sk_id PRIMARY KEY (sk_id)")
```

![CELL 2](imagens/cell_2.png)

- **Execute o 3º Bloco de código abaixo para criar os relacionamentos entre as tabelas e otimizações:**
```
%sql
--agg_fact_estabelecimentos_cnaes
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes DROP CONSTRAINT IF EXISTS fk_agg_fact_estabelecimentos_cnaes_dim_cnaes; 
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes ADD CONSTRAINT fk_agg_fact_estabelecimentos_cnaes_dim_cnaes FOREIGN KEY (sk_cnae) REFERENCES cnpj_data_lakehouse.gold.dim_cnaes(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes DROP CONSTRAINT IF EXISTS fk_agg_fact_estabelecimentos_cnaes_dim_tempo_inicio_atividades; 
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes ADD CONSTRAINT fk_agg_fact_estabelecimentos_cnaes_dim_tempo_inicio_atividades FOREIGN KEY (sk_tempo_inicio_atividade) REFERENCES cnpj_data_lakehouse.gold.dim_tempo_inicio_atividades(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes DROP CONSTRAINT IF EXISTS fk_agg_fact_estabelecimentos_cnaes_dim_situacoes; 
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes ADD CONSTRAINT fk_agg_fact_estabelecimentos_cnaes_dim_situacoes FOREIGN KEY (sk_situacoes) REFERENCES cnpj_data_lakehouse.gold.dim_situacoes(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes DROP CONSTRAINT IF EXISTS fk_agg_fact_estabelecimentos_cnaes_dim_localidades; 
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes ADD CONSTRAINT fk_agg_fact_estabelecimentos_cnaes_dim_localidades FOREIGN KEY (sk_localidades) REFERENCES cnpj_data_lakehouse.gold.dim_localidades(sk_id);

--agg_fact_estabelecimentos
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_agg_fact_estabelecimentos_dim_tempo_inicio_atividades; 
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos ADD CONSTRAINT fk_agg_fact_estabelecimentos_dim_tempo_inicio_atividades FOREIGN KEY (sk_tempo_inicio_atividade) REFERENCES cnpj_data_lakehouse.gold.dim_tempo_inicio_atividades(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_agg_fact_estabelecimentos_dim_situacoes; 
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos ADD CONSTRAINT fk_agg_fact_estabelecimentos_dim_situacoes FOREIGN KEY (sk_situacoes) REFERENCES cnpj_data_lakehouse.gold.dim_situacoes(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_agg_fact_estabelecimentos_dim_localidades; 
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos ADD CONSTRAINT fk_agg_fact_estabelecimentos_dim_localidades FOREIGN KEY (sk_localidades) REFERENCES cnpj_data_lakehouse.gold.dim_localidades(sk_id);

--bridge_estabelecimentos_cnaes
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_cnaes DROP CONSTRAINT IF EXISTS fk_bridge_estabelecimentos_cnaes_fact_estabelecimentos; 
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_cnaes ADD CONSTRAINT fk_bridge_estabelecimentos_cnaes_fact_estabelecimentos FOREIGN KEY (sk_estabelecimento) REFERENCES cnpj_data_lakehouse.gold.fact_estabelecimentos(sk_estabelecimento);
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_cnaes DROP CONSTRAINT IF EXISTS fk_bridge_estabelecimentos_cnaes_dim_cnaes; 
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_cnaes ADD CONSTRAINT fk_bridge_estabelecimentos_cnaes_dim_cnaes FOREIGN KEY (sk_cnae) REFERENCES cnpj_data_lakehouse.gold.dim_cnaes(sk_id);

--bridge_estabelecimentos_socios
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_socios DROP CONSTRAINT IF EXISTS fk_bridge_estabelecimentos_socios_fact_estabelecimentos; 
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_socios ADD CONSTRAINT fk_bridge_estabelecimentos_socios_fact_estabelecimentos FOREIGN KEY (sk_estabelecimento) REFERENCES cnpj_data_lakehouse.gold.fact_estabelecimentos(sk_estabelecimento);
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_socios DROP CONSTRAINT IF EXISTS fk_bridge_estabelecimentos_socios_dim_socios;
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_socios ADD CONSTRAINT fk_bridge_estabelecimentos_socios_dim_socios FOREIGN KEY (sk_socio) REFERENCES cnpj_data_lakehouse.gold.dim_socios(sk_id);

--fact_estabelecimentos
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_fact_estabelecimentos_dim_tempo_inicio_atividades; 
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos ADD CONSTRAINT fk_fact_estabelecimentos_dim_tempo_inicio_atividades FOREIGN KEY (sk_tempo_inicio_atividade) REFERENCES cnpj_data_lakehouse.gold.dim_tempo_inicio_atividades(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_fact_estabelecimentos_dim_tempo_situacao_cadastral; 
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos ADD CONSTRAINT fk_fact_estabelecimentos_dim_tempo_situacao_cadastral FOREIGN KEY (sk_tempo_situacoes_cadastrais) REFERENCES cnpj_data_lakehouse.gold.dim_tempo_situacoes_cadastrais(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_fact_estabelecimentos_dim_tempo_situacao_especial; 
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos ADD CONSTRAINT fk_fact_estabelecimentos_dim_tempo_situacao_especial FOREIGN KEY (sk_tempo_situacoes_especiais) REFERENCES cnpj_data_lakehouse.gold.dim_tempo_situacoes_especiais(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_fact_estabelecimentos_dim_situacoes;
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos ADD CONSTRAINT fk_fact_estabelecimentos_dim_situacoes FOREIGN KEY (sk_situacoes) REFERENCES cnpj_data_lakehouse.gold.dim_situacoes(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_fact_estabelecimentos_dim_localidades; 
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos ADD CONSTRAINT fk_fact_estabelecimentos_dim_localidades FOREIGN KEY (sk_localidades) REFERENCES cnpj_data_lakehouse.gold.dim_localidades(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_fact_estabelecimentos_dim_naturezas_juridicas; 
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos ADD CONSTRAINT fk_fact_estabelecimentos_dim_naturezas_juridicas FOREIGN KEY (sk_naturezas_juridicas) REFERENCES cnpj_data_lakehouse.gold.dim_naturezas_juridicas(sk_id);
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos DROP CONSTRAINT IF EXISTS fk_fact_estabelecimentos_dim_cadastro; 
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos ADD CONSTRAINT fk_fact_estabelecimentos_dim_cadastro FOREIGN KEY (sk_estabelecimento) REFERENCES cnpj_data_lakehouse.gold.dim_cadastro(sk_id);

-- Ativando o Liquid Clustering nas tabelas
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes CLUSTER BY (sk_cnae, sk_localidades, sk_tempo_inicio_atividade, sk_situacoes);
ALTER TABLE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos CLUSTER BY (sk_tempo_inicio_atividade, sk_situacoes, sk_localidades);
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_cnaes CLUSTER BY (sk_estabelecimento, sk_cnae);
ALTER TABLE cnpj_data_lakehouse.gold.bridge_estabelecimentos_socios CLUSTER BY (sk_estabelecimento, sk_socio);
ALTER TABLE cnpj_data_lakehouse.gold.dim_cadastro CLUSTER BY (cnpj_completo);
ALTER TABLE cnpj_data_lakehouse.gold.fact_estabelecimentos CLUSTER BY (sk_tempo_inicio_atividade, sk_situacoes, sk_localidades, sk_naturezas_juridicas);
OPTIMIZE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos_cnaes FULL;
OPTIMIZE cnpj_data_lakehouse.gold.agg_fact_estabelecimentos FULL;
OPTIMIZE cnpj_data_lakehouse.gold.bridge_estabelecimentos_cnaes FULL;
OPTIMIZE cnpj_data_lakehouse.gold.bridge_estabelecimentos_socios FULL;
OPTIMIZE cnpj_data_lakehouse.gold.dim_cadastro FULL;
OPTIMIZE cnpj_data_lakehouse.gold.fact_estabelecimentos FULL;
```

![CELL 3](imagens/cell_3.png)

9. Configure a conexão do [relatório do Power BI](power_bi/cnpj_data_lakehouse_databricks_direct_query.pbix) com seus dados de conta do Databricks no modo direct_query.

- Acesse o relatório deste projeto clicando [aqui](https://app.powerbi.com/view?r=eyJrIjoiNmJkYmUwY2ItYmI0NC00NGVkLThmMTgtZTAxNzQ0YWY0NDMxIiwidCI6IjY1OWNlMmI4LTA3MTQtNDE5OC04YzM4LWRjOWI2MGFhYmI1NyJ9).

- Arquivo do relatório com toda a base importada (12GB) clique [aqui](https://drive.google.com/file/d/1MRnuA1L4UtRmidtiJYJLeGosOVAQZBDX/view?usp=sharing).
* Necessária a instalação do Power BI Desktop.

![Capa do relatório do Power BI](imagens/pagina1.png)

![Página 1 do relatório do Power BI](imagens/pagina2.png)

![Página 2 do relatório do Power BI](imagens/pagina3.png)

![Página 3 do relatório do Power BI](imagens/pagina4.png)

![Página 4 do relatório do Power BI](imagens/pagina5.png)

![Página 5 do relatório do Power BI](imagens/pagina6.png)

## 💻 Ambiente de Processamento Local (Hardware e Software)

Para suportar o processamento de dezenas de gigabytes de arquivos em uma arquitetura de Big Data local (simulando a nuvem), o projeto foi desenvolvido e executado na seguinte infraestrutura:

### Hardware Utilizado
- **Processador:** Intel Core i7-12700
- **Memória RAM:** 32GB (2x 16GB Kingston DDR4 3200MHz CL16)
- **Armazenamento:** SSD Kingston 1TB M.2 NVMe Gen4x4 (Leitura de 3500MB/s e Gravação de 2100MB/s)
- **GPU (Opcional para Analytics):** Placa de Vídeo Galax RTX 4060 8GB GDDR6
- **Placa Mãe:** Gigabyte Z690 UD M.2 DDR4
- **Refrigeração e Energia:** Water Cooler Corsair H100 240mm e Fonte Corsair CV750W 80Plus Bronze

![Informações do Sistema](imagens/informacoes_sistema.png)

Sinta-se à vontade para entrar em contato para eventuais dúvidas e dar feedbacks sobre o código!