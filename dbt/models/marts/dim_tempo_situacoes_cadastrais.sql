{{ config(    
    post_hook=[        
        export_to_s3(bucket_path='gold/dim', file_name='dim_tempo_situacoes_cadastrais')
    ]
) }}

SELECT 
    sk_id,
    data_referencia,
    ano,
    mes,
    nome_mes,
    NOW() AS data_processamento 
FROM {{ ref('int_tempo_situacoes_cadastrais') }}