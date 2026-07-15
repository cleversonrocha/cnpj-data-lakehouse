-- {{ ref('stg_municipios') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_municipios')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_municipios') }}