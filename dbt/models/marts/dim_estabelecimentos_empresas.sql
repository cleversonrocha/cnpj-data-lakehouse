{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_estabelecimentos_empresas')
    ]
) }}

SELECT
    sk_id,
    es.cnpj_basico, 
    cnpj_ordem,
    cnpj_dv,        
    CASE identificador_matriz_filial
        WHEN 1 THEN 'MATRIZ'
        WHEN 2 THEN 'FILIAL'        
    END AS tipo_estabelecimento,
    nome_fantasia,    
    CASE situacao_cadastral
    	WHEN 1 THEN 'NULA'
    	WHEN 2 THEN 'ATIVA'
    	WHEN 3 THEN 'SUSPENSA'
    	WHEN 4 THEN 'INAPTA'
    	WHEN 8 THEN 'BAIXADA'    	
    END AS situacao_cadastral,        
    mo.descricao AS motivo_situacao_cadastral,    
    situacao_especial,
    uf,        
    desc_municipio,    
    desc_pais,
    tipo_logradouro,
    logradouro,    
    numero,    
    complemento,
    bairro,
    cep,
    ddd_1,
    telefone_1,
    ddd_2,
    telefone_2,
    ddd_fax,
    fax,
    email,     
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
FROM {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }} es
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='empresas') }} em ON em.cnpj_basico = es.cnpj_basico
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='naturezas') }} n ON n.codigo = em.natureza_juridica    
LEFT JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='motivos') }} mo ON mo.codigo = es.motivo_situacao_cadastral
LEFT JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='qualificacoes') }} q ON q.codigo = em.qualificacao_responsavel
LEFT JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='simples') }} s ON s.cnpj_basico = em.cnpj_basico
