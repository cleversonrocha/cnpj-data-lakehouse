-- {{ ref('stg_municipios_exterior') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_municipios_exterior')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_municipios_exterior') }}