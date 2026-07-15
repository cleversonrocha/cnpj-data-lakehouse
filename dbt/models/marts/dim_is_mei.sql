-- {{ ref('stg_is_mei') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_is_mei')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_is_mei') }}