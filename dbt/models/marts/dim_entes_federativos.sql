-- {{ ref('stg_entes_federativos') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_entes_federativos')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_entes_federativos') }}