-- {{ ref('stg_socios') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_socios')
    ]
) }}

SELECT sk_id,nome_razao_social,cpf_cnpj,representante_legal,nome_do_representante FROM {{ ref('stg_socios') }}