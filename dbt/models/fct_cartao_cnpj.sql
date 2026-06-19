{{ config(
    materialized='external',    
    location=get_s3_path(base_path='gold/facts', file_name='fct_cartao_cnpj')
) }}

SELECT
    REGEXP_REPLACE(
        CONCAT(em.cnpj_basico, es.cnpj_ordem, es.cnpj_dv), 
        '(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})', 
        '\1.\2.\3/\4-\5'
    ) AS numero_inscricao,
    CASE es.identificador_matriz_filial
        WHEN 1 THEN 'MATRIZ'
        WHEN 2 THEN 'FILIAL'
        ELSE null
    END AS tipo_estabelecimento,    
    es.data_inicio_atividade,
    em.razao_social AS nome_empresarial,
    es.nome_fantasia AS titulo_estabelecimento,    
    CASE em.porte_empresa
    	WHEN 0 THEN 'NÃO INFORMADO'
    	WHEN 1 THEN 'MICRO EMPRESA'
    	WHEN 3 THEN 'EMPRESA DE PEQUENO PORTE'
    	WHEN 5 THEN 'DEMAIS'
    	ELSE null
    END AS porte,        
    CONCAT(es.cnae_fiscal_principal, ' - ', cfp.descricao) AS cnae_principal,    
	(
        SELECT STRING_AGG(CONCAT(c.codigo, ' - ', c.descricao), ' | ')
        FROM {{ get_s3_path(base_path='silver/cleaned', file_name='cnaes') }} c
        WHERE c.codigo IN (
            SELECT TRIM(unnest(string_split(es.cnae_fiscal_secundaria, ',')))::INTEGER
        )
    ) AS cnae_secundarias,        
    CONCAT(em.natureza_juridica, ' - ', n.descricao) AS natureza_juridica,    
    es.logradouro,
    es.numero,
    es.complemento,    
    regexp_replace(es.cep, '^(\d{5})(\d{3})$', '\1-\2') AS cep,
    es.bairro,
    mu.descricao as municipio,
    es.uf,
    es.email AS endereco_eletronico,
    CONCAT('(',es.ddd_1,') ',
        CASE 
            WHEN length(es.telefone_1) = 8 THEN 
                regexp_replace(es.telefone_1, '^(\d{4})(\d{4})$', '\1-\2')
            WHEN length(es.telefone_1) = 9 THEN 
                regexp_replace(es.telefone_1, '^(\d{5})(\d{4})$', '\2-\3')
            ELSE es.telefone_1
        END
     ) AS telefone,
    em.ente_federativo_responsavel,
    CASE es.situacao_cadastral
    	WHEN 1 THEN 'NULA'
    	WHEN 2 THEN 'ATIVA'
    	WHEN 3 THEN 'SUSPENSA'
    	WHEN 4 THEN 'INAPTA'
    	WHEN 8 THEN 'BAIXADA'
    	ELSE null
    END AS situacao_cadastral,    
    es.data_situacao_cadastral,    
    mo.descricao,    
    es.situacao_especial,
    es.data_situacao_especial
FROM {{ get_s3_path(base_path='silver/cleaned', file_name='empresas') }} em 
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }} es ON es.cnpj_basico = em.cnpj_basico
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='municipios') }} mu ON mu.codigo = es.municipio
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='naturezas') }} n ON n.codigo = em.natureza_juridica
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='cnaes') }} cfp ON cfp.codigo = es.cnae_fiscal_principal
JOIN {{ get_s3_path(base_path='silver/cleaned', file_name='motivos') }} mo ON mo.codigo = es.motivo_situacao_cadastral