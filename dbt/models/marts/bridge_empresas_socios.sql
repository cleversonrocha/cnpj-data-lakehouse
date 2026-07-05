-- {{ ref('dim_quadro_societario') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_empresas_socios')
    ]
) }}

SELECT
    DISTINCT cnpj_basico
FROM {{ ref('dim_quadro_societario') }}