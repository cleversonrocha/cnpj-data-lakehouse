-- {{ ref('stg_empresas') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_empresas')
    ]
) }}

SELECT sk_id,cnpj_basico,razao_social,capital_social FROM {{ ref('stg_empresas') }}