{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_empresas_socios')
    ]
) }}

SELECT
    e.sk_id AS sk_empresa,
    s.sk_id AS sk_socio,
    NOW() AS data_processamento
FROM {{ ref('dim_empresas') }} e
JOIN {{ ref('dim_socios') }} s ON s.cnpj_basico = e.cnpj_basico