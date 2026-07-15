-- {{ ref('bridge_empresas_socios') }} {{ ref('dim_tipos_pessoas') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_socios_por_empresa')
    ]
) }}

SELECT
    es.sk_empresa_id,
    CAST(COUNT(es.sk_socio_id) AS SMALLINT) AS qtd_socios,
    CAST(COUNT(CASE WHEN tp.codigo = 1 THEN 1 END) AS SMALLINT) AS qtd_socios_pf,
    CAST(COUNT(CASE WHEN tp.codigo = 2 THEN 1 END) AS SMALLINT) AS qtd_socios_pj,
    CAST(COUNT(CASE WHEN tp.codigo = 3 THEN 1 END) AS SMALLINT) AS qtd_socios_estrangeiro
FROM {{ ref('bridge_empresas_socios') }} es
JOIN {{ ref('dim_tipos_pessoas') }} tp ON tp.sk_id = es.sk_tipos_pessoas
GROUP BY es.sk_empresa_id