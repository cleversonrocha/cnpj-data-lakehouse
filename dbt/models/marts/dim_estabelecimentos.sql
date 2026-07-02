{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_estabelecimentos')
    ]
) }}

SELECT
    sk_id,
    cnpj_basico, 
    cnpj_ordem,
    cnpj_dv,        
    CASE identificador_matriz_filial
        WHEN 1 THEN 'MATRIZ'
        WHEN 2 THEN 'FILIAL'        
    END AS tipo_estabelecimento,
    nome_fantasia,
    data_inicio_atividade,    
    CASE situacao_cadastral
    	WHEN 1 THEN 'NULA'
    	WHEN 2 THEN 'ATIVA'
    	WHEN 3 THEN 'SUSPENSA'
    	WHEN 4 THEN 'INAPTA'
    	WHEN 8 THEN 'BAIXADA'    	
    END AS situacao_cadastral,    
    data_situacao_cadastral,    
    mo.descricao AS motivo_situacao_cadastral,
    data_situacao_especial,
    situacao_especial
FROM {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }} es
LEFT JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='motivos') }} mo ON mo.codigo = es.motivo_situacao_cadastral