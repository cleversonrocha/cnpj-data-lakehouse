-- {{ ref('stg_faixas_etarias') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_faixas_etarias')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_faixas_etarias')}}