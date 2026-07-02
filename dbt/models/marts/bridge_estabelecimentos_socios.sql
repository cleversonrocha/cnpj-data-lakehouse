-- {{ ref('dim_estabelecimentos') }} {{ ref('dim_quadro_societario') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_estabelecimentos_socios')
    ]
) }}

SELECT    
    e.sk_id AS sk_estabelecimento_id,    
    s.sk_id AS sk_socio_id  
FROM {{ ref('dim_estabelecimentos') }} e
JOIN {{ ref('dim_quadro_societario') }} s ON s.cnpj_basico = e.cnpj_basico