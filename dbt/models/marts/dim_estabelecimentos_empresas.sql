{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_estabelecimentos_empresas')
    ]
) }}

SELECT
    sk_id,
    es.cnpj_basico,    
    SUBSTR(es.cnpj_basico, 1, 2) || '.' || SUBSTR(es.cnpj_basico, 3, 3) || '.' || SUBSTR(es.cnpj_basico, 6, 3) || '/' || cnpj_ordem || '-' || cnpj_dv AS cnpj_completo,
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
    tipo_logradouro || ' ' || logradouro AS logradouro,    
    numero,    
    complemento,
    bairro,
    CASE 
        WHEN cep IS NULL THEN cep
        ELSE SUBSTR(cep, 1, 5) || '-' || SUBSTR(cep, 6, 3)
    END AS cep,
    CASE 
        WHEN ddd_1 IS NULL OR telefone_1 IS NULL THEN NULL
        ELSE '(' || ddd_1 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(telefone_1, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_1, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(telefone_1, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_1, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE telefone_1
            END
    END AS telefone_1,
    CASE 
        WHEN ddd_2 IS NULL OR telefone_2 IS NULL THEN NULL
        ELSE '(' || ddd_2 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(telefone_2, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_2, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(telefone_2, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_2, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE telefone_2
            END
    END AS telefone_2,
    CASE 
        WHEN ddd_fax IS NULL OR fax IS NULL THEN NULL
        ELSE '(' || ddd_fax || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(fax, '\D', '', 'g')) = 9             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(fax, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(fax, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(fax, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE fax
            END
    END AS fax,  
    email,     
    razao_social,
    CASE porte_empresa
    	WHEN 0 THEN 'NÃO INFORMADO'
    	WHEN 1 THEN 'MICRO EMPRESA'
    	WHEN 3 THEN 'EMPRESA DE PEQUENO PORTE'
    	WHEN 5 THEN 'DEMAIS'    	
    END AS porte,
    CASE porte_empresa
    	WHEN 0 THEN 'NÃO INFORMADO'
    	WHEN 1 THEN 'ME'
    	WHEN 3 THEN 'EPP'
    	WHEN 5 THEN 'DEMAIS'
    END AS porte_sigla,    
    ente_federativo_responsavel,         
    capital_social,
    n.codigo_formatado AS codigo_natureza_juridica,
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
FROM {{ ref('stg_estabelecimentos') }} es
JOIN {{ ref('stg_empresas') }} em ON em.cnpj_basico = es.cnpj_basico
JOIN {{ ref('stg_naturezas') }} n ON n.codigo = em.natureza_juridica    
LEFT JOIN {{ ref('stg_motivos') }} mo ON mo.codigo = es.motivo_situacao_cadastral
LEFT JOIN {{ ref('stg_qualificacoes') }} q ON q.codigo = em.qualificacao_responsavel
LEFT JOIN {{ ref('stg_simples') }} s ON s.cnpj_basico = em.cnpj_basico
