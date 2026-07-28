{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_socios_por_empresa')
    ]
) }}

SELECT
    cnpj_basico,
    CAST(COUNT(cnpj_basico) AS SMALLINT) AS qtd_socios,
    CAST(SUM(CASE WHEN identificador = 1 THEN 1 ELSE 0 END) AS SMALLINT) AS qtd_socios_pf,
    CAST(SUM(CASE WHEN identificador = 2 THEN 1 ELSE 0 END) AS SMALLINT) AS qtd_socios_pj,
    CAST(SUM(CASE WHEN identificador = 3 THEN 1 ELSE 0 END) AS SMALLINT) AS qtd_socios_estrangeiro
FROM {{ ref('int_socios') }}
GROUP BY cnpj_basico