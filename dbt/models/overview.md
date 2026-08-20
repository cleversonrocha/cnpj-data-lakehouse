{% docs __overview__ %}

# CNPJ Data Lakehouse

Pipeline de engenharia de dados que processa a base pública de CNPJ da
Receita Federal (~72 milhões de registros), estruturando-a em um data
lakehouse com arquitetura em camadas (bronze → silver → gold), orquestrado
via Apache Airflow e transformado com dbt.

---

## 🏗️ Arquitetura

| Componente | Papel |
|---|---|
| **Apache Airflow 3.2.1** | Orquestração das DAGs (`ingestao_bronze`, `ingestao_silver`, `dbt_build_models`) |
| **MinIO** | Object storage intermediário (bronze/silver/gold) |
| **DuckDB** | Engine de transformação local/dev |
| **Databricks (Unity Catalog)** | Warehouse serverless para produção, consumido via Power BI |
| **dbt** | Transformação, testes de dados e modelagem dimensional |
| **Power BI** | Camada de consumo (DirectQuery + Import composite model) |

---

## 📐 Modelo Dimensional

Modelo Kimball clássico com uma tabela fato e nove dimensões compartilhadas, além de duas pontes para resolver relacionamentos muitos para muitos e duas agregações pré-calculadas para otimizar performance no Power BI.

| Tipo | Tabela | Descrição | Quantidade de Registros |
|---|---|---| ---: |
| Fato | `fact_estabelecimentos` | Estabelecimentos (matriz/filiais) | 72.789.638 |
| Dimensão | `dim_cadastro` | Dados cadastrais | 72.789.638 |
| Ponte | `bridge_estabelecimentos_cnaes` | Resolve relacionamento muitos para muitos com dim_cnaes | 196.759.105 |
| Dimensão | `dim_cnaes` | Atividades econômicas dos estabelecimentos | 1.361 |
| Dimensão | `dim_localidades` | Localização geográfica dos estabelecimentos | 12.611 |
| Dimensão | `dim_naturezas_juridicas` | Natureza Jurídica das empresas | 93 |
| Dimensão | `dim_situacoes` | Identificação Matriz/Filial, Situação Cadastral e Especial dos estabelecimentos | 251 |
| Ponte | `bridge_estabelecimentos_socios` | Resolve relacionamentos muitos para muitos com dim_socios | 37.952.710 |
| Dimensão | `dim_socios` | Histórico societário das empresas | 28.146.721 |
| Dimensão | `dim_tempo_inicio_atividades` | Calendário com datas de início das atividades dos estabelecimentos | 49.235 |
| Dimensão | `dim_tempo_situacoes_cadastrais` | Calendário com datas das situações cadastrais dos estabelecimentos | 46.320 |
| Dimensão | `dim_tempo_situacoes_especiais` | Calendário com datas das situações especiais dos estabelecimentos | 21.892 |
| Agregação | `agg_fact_estabelecimentos` | Pré-agregação com tipos dos estabelecimentos, situações cadastrais, portes por início da atividade, situação, localização e natureza jurídica dos estabelecimentos  | 32.630.634 |
| Agregação | `agg_fact_estabelecimentos_cnaes` | Pré-agregação com as atividades econômicas dos estabelecimentos, por data de início da atividade, situações, localização e tipo do CNAE | 166.468.071 |

---

## ⚙️ Camadas do Pipeline

1. **Bronze** — ingestão raw dos arquivos da Receita Federal (layout fixo, sem transformação)
2. **Silver** — limpeza, tipagem, padronização (ex: formatação de telefone via `REGEXP_REPLACE`)
3. **Gold** — star schema final, particionado por competência (`ano_mes`)

---

## 🧩 Macros principais

| Macro | Função |
|---|---|
| `get_ano_mes()` | Resolve a competência da execução (`--vars` ou fallback no mês corrente) |
| `get_s3_path(base_path, file_name)` | Monta o caminho S3/MinIO particionado |
| `export_to_s3(bucket_path, file_name)` | Gera `COPY ... TO parquet` como post-hook |

---

## 🔗 Links úteis

- [Repositório no GitHub](https://github.com/cleversonrocha/cnpj-data-lakehouse)
- [Dashboard Power BI](https://app.powerbi.com/view?r=eyJrIjoiNmJkYmUwY2ItYmI0NC00NGVkLThmMTgtZTAxNzQ0YWY0NDMxIiwidCI6IjY1OWNlMmI4LTA3MTQtNDE5OC04YzM4LWRjOWI2MGFhYmI1NyJ9)

{% enddocs %}