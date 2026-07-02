{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_empresas')
    ]
) }}

SELECT    
    em.cnpj_basico as sk_id,    
    razao_social,
    CASE porte_empresa
    	WHEN 0 THEN 'NÃO INFORMADO'
    	WHEN 1 THEN 'MICRO EMPRESA'
    	WHEN 3 THEN 'EMPRESA DE PEQUENO PORTE'
    	WHEN 5 THEN 'DEMAIS'    	
    END AS porte,
    ente_federativo_responsavel,         
    capital_social,
    n.descricao AS natureza_juridica,
    q.descricao AS qualificacao_responsavel,
    CASE
        -- É MEI Ativo: Optou pelo MEI e nunca foi excluído (ou a exclusão é futura/nula)
        WHEN s.opcao_mei = 'S' AND s.data_exclusao_mei IS NULL THEN 'SIM'        
        -- Ex-MEI: Já optou no passado, mas foi excluído
        WHEN s.opcao_mei = 'S' AND s.data_exclusao_mei IS NOT NULL THEN 'NÃO (EX-MEI)'         
        -- Nunca foi MEI
        WHEN s.opcao_mei = 'N' THEN 'NÃO'        
        -- Casos onde não há informação no bloco do Simples
        ELSE 'NÃO INFORMADO'
    END AS is_mei   
FROM {{ get_s3_path(base_path='silver/cleaned', file_name='empresas') }} em
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='naturezas') }} n ON n.codigo = em.natureza_juridica    
LEFT JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='qualificacoes') }} q ON q.codigo = em.qualificacao_responsavel
LEFT JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='simples') }} s ON s.cnpj_basico = em.cnpj_basico