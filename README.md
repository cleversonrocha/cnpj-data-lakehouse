🚧 Aviso: Este projeto está atualmente em desenvolvimento ativo. A infraestrutura base já foi configurada e estou construindo os pipelines de ingestão e transformação etapa por etapa.

# 🏢 Projeto de Estudos: Data Lakehouse com Dados do CNPJ

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

* **Apache Airflow:** Para agendar e automatizar o download e a extração dos arquivos.
* **MinIO:** Para simular o Amazon S3 localmente (criando as camadas de armazenamento separadas do processamento).
* **Apache Spark (PySpark):** Para o processamento pesado e distribuído, lendo os CSVs e convertendo para o formato colunar Parquet.
* **DuckDB:** Para rodar consultas SQL analíticas em cima dos arquivos finais de forma quase instantânea.

## 🏗️ Como estruturei os dados (Arquitetura Medalhão)

- 🥉 **Bronze:** O Airflow baixa os Zips da Receita, extrai os CSVs e salva no MinIO exatamente como vieram (dados brutos).
- 🥈 **Silver:** O PySpark lê a Bronze, adiciona os cabeçalhos, tipa as colunas, trata nulos. Os dados são salvos em `.parquet`.
- 🥇 **Gold:** Agregações e cruzamentos (ex: Estabelecimentos + Municípios + CNAEs) para criar um modelo pronto para ferramentas de BI, como o Power BI.

### 🗺️ Diagrama da Arquitetura

```mermaid
graph TD
    %% Cores e Estilos
    classDef source fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef orquestrador fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef storage fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef processing fill:#ffebee,stroke:#b71c1c,stroke-width:2px;
    classDef analytics fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;

    %% Nós do Diagrama
    RFB[🌐 Portal Receita Federal<br/>Arquivos .zip]:::source
    Airflow[⚙️ Apache Airflow<br/>Download e Extração]:::orquestrador
    Bronze[(🥉 MinIO Bronze<br/>Raw .csv)]:::storage
    Spark1[⚡ PySpark<br/>Limpeza e Tipagem]:::processing
    Silver[(🥈 MinIO Silver<br/>Clean .parquet)]:::storage
    Spark2[⚡ PySpark<br/>Agregação Gold]:::processing
    Gold[(🥇 MinIO Gold<br/>Business .parquet)]:::storage
    DuckDB[🦆 DuckDB<br/>Consultas SQL]:::analytics
    PBI[📊 Power BI<br/>Dashboards]:::analytics

    %% Conexões
    RFB -->|Ingestão| Airflow
    Airflow -->|Upload| Bronze
    Bronze -->|Leitura| Spark1
    Spark1 -->|Escrita| Silver
    Silver -->|Leitura| Spark2
    Spark2 -->|Escrita| Gold
    Gold -->|Conexão httpfs| DuckDB
    Gold -->|Conexão Direta| PBI
```

## ⚙️ Como rodar na sua máquina

1. Clone o repositório: `git clone https://github.com/cleversonrocha/cnpj-data-lakehouse.git`
2. Configure as permissões criando um arquivo `.env` com a variável `AIRFLOW_UID=50000`.
3. Suba os containers com o Docker: `docker-compose up -d`
4. Acesse o MinIO em `localhost:9001` e crie os buckets `bronze`, `silver` e `gold`.
5. Acesse o Airflow em `localhost:8080` para iniciar as DAGs.

## 💻 Ambiente de Processamento Local (Hardware e Software)

Para suportar o processamento de dezenas de gigabytes de arquivos em uma arquitetura de Big Data local (simulando a nuvem), o projeto foi desenvolvido e executado na seguinte infraestrutura:

### Hardware Utilizado
- **Processador:** Intel Core i7-12700
- **Memória RAM:** 32GB (2x 16GB Kingston DDR4 3200MHz CL16)
- **Armazenamento:** SSD Kingston 1TB M.2 NVMe Gen4x4 (Leitura de 3500MB/s e Gravação de 2100MB/s)
- **GPU (Opcional para Analytics):** Placa de Vídeo Galax RTX 4060 8GB GDDR6
- **Placa Mãe:** Gigabyte Z690 UD M.2 DDR4
- **Refrigeração e Energia:** Water Cooler Corsair H100 240mm e Fonte Corsair CV750W 80Plus Bronze

Sinta-se à vontade para me dar feedbacks sobre o código!