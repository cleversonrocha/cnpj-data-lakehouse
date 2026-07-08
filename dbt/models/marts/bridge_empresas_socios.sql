-- {{ ref('dim_estabelecimentos_empresas') }} {{ ref('stg_socios') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_empresas_socios')
    ]
) }}

SELECT
    es.sk_id AS sk_estabelecimento_id,
    qs.sk_id AS sk_socio_id,
    COALESCE(CAST(STRFTIME(data_entrada_sociedade, '%Y%m%d') AS INTEGER), 0) AS sk_data_entrada_sociedade
FROM {{ ref('dim_estabelecimentos_empresas') }} es
JOIN {{ ref('stg_socios') }} qs ON qs.cnpj_basico = es.cnpj_basico