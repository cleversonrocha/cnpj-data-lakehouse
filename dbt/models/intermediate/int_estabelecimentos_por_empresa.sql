-- {{ ref('fact_estabelecimentos') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_estabelecimentos_por_empresa')
    ]
) }}

SELECT
    sk_empresas,
    CAST(COUNT(sk_id) AS SMALLINT) AS qtd_estabelecimentos,
    CAST(SUM(qtd_matrizes) AS SMALLINT) AS  qtd_matriz,
    CAST(SUM(qtd_filiais) AS SMALLINT) AS  qtd_filiais,
    CAST(SUM(qtd_nulas) AS SMALLINT) AS  qtd_nulas,
    CAST(SUM(qtd_ativas) AS SMALLINT) AS  qtd_ativas,
    CAST(SUM(qtd_suspensas) AS SMALLINT) AS  qtd_suspensas,
    CAST(SUM(qtd_inaptas) AS SMALLINT) AS  qtd_inaptas,
    CAST(SUM(qtd_baixadas) AS SMALLINT) AS  qtd_baixadas
FROM {{ ref('fact_estabelecimentos') }}
GROUP BY sk_empresas