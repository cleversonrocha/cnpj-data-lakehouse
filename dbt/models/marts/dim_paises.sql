-- {{ ref('stg_paises') }} ← comentário que força dependência

{{ config(
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_paises')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_paises') }}