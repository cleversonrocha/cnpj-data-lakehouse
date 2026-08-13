{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_estabelecimentos_socios')
    ]
) }}

SELECT        
    e.sk_id AS sk_estabelecimento,
    s.sk_id AS sk_socio,        
    NOW() AS data_processamento
FROM {{ ref('int_estabelecimentos') }} e
JOIN {{ ref('int_empresas') }} em ON em.cnpj_basico = e.cnpj_basico
JOIN {{ ref('int_socios') }} s ON s.cnpj_basico = em.cnpj_basico