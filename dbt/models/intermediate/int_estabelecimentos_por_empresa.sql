{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_estabelecimentos_por_empresa')
    ]
) }}

SELECT
    cnpj_basico,
    CAST(COUNT(sk_id) AS SMALLINT) AS qtd_estabelecimentos,
    CAST(SUM(CASE WHEN identificador_matriz_filial = 1 THEN 1 ELSE 0 END)  AS TINYINT) AS qtd_matrizes,
    CAST(SUM(CASE WHEN identificador_matriz_filial = 2 THEN 1 ELSE 0 END)  AS SMALLINT) AS qtd_filiais,        
    CAST(SUM(CASE WHEN situacao_cadastral = 1 THEN 1 ELSE 0 END)  AS SMALLINT) AS qtd_situacao_nulas,
    CAST(SUM(CASE WHEN situacao_cadastral = 2 THEN 1 ELSE 0 END)  AS SMALLINT) AS qtd_situacao_ativas,
    CAST(SUM(CASE WHEN situacao_cadastral = 3 THEN 1 ELSE 0 END)  AS SMALLINT) AS qtd_situacao_suspensas,
    CAST(SUM(CASE WHEN situacao_cadastral = 4 THEN 1 ELSE 0 END)  AS SMALLINT) AS qtd_situacao_inaptas,
    CAST(SUM(CASE WHEN situacao_cadastral = 8 THEN 1 ELSE 0 END)  AS SMALLINT) AS qtd_situacao_baixadas
FROM {{ ref('int_estabelecimentos') }}
GROUP BY cnpj_basico