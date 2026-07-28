{{ config(
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_simples')
    ]
) }}

SELECT    
    cnpj_basico,        
    opcao_simples,        
    data_opcao_simples,
    data_exclusao_simples,
    opcao_mei,
    data_opcao_mei,
    data_exclusao_mei,
    NOW() AS data_processamento
FROM {{ ref('stg_simples') }}
