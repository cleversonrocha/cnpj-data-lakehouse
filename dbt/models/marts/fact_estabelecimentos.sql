{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_estabelecimentos')
    ]
) }}

SELECT
    e.sk_id AS sk_estabelecimento,
    em.sk_id AS sk_empresa,    
    COALESCE(CAST(STRFTIME('%Y%m%d', e.data_inicio_atividade) AS INTEGER),-1) AS sk_tempo_inicio_atividade,    
    COALESCE(CAST(STRFTIME('%Y%m%d', e.data_situacao_especial) AS INTEGER),-1) AS sk_tempo_situacoes_especiais,
    COALESCE(CAST(STRFTIME('%Y%m%d', e.data_situacao_cadastral) AS INTEGER),-1) AS sk_tempo_situacoes_cadastrais,    
    s.sk_id AS sk_situacoes,
    l.sk_id AS sk_localidades,
    SUBSTR(e.cnpj_basico, 1, 2) || '.' || SUBSTR(e.cnpj_basico, 3, 3) || '.' || SUBSTR(e.cnpj_basico, 6, 3) || '/' || e.cnpj_ordem || '-' || e.cnpj_dv AS cnpj_completo,    
    e.nome_fantasia,    
    e.tipo_logradouro || ' ' || e.logradouro AS logradouro,    
    e.numero,    
    e.complemento,
    e.bairro,
    CASE 
        WHEN e.cep IS NULL THEN e.cep
        ELSE SUBSTR(e.cep, 1, 5) || '-' || SUBSTR(e.cep, 6, 3)
    END AS cep,
    CASE 
        WHEN e.ddd_1 IS NULL OR e.telefone_1 IS NULL THEN NULL
        ELSE '(' || e.ddd_1 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE e.telefone_1
            END
    END AS telefone_1,
    CASE 
        WHEN e.ddd_2 IS NULL OR e.telefone_2 IS NULL THEN NULL
        ELSE '(' || e.ddd_2 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE e.telefone_2
            END
    END AS telefone_2,
    CASE 
        WHEN e.ddd_fax IS NULL OR e.fax IS NULL THEN NULL
        ELSE '(' || e.ddd_fax || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(e.fax, '\D', '', 'g')) = 9             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.fax, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(e.fax, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.fax, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE e.fax
            END
    END AS fax,  
    e.email,
    CAST(CASE WHEN e.identificador_matriz_filial = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_matriz,
    CAST(CASE WHEN e.identificador_matriz_filial = 2 THEN 1 ELSE 0 END AS TINYINT) AS is_filial,    
    CAST(CASE WHEN em.codigo_porte_empresa = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_me,
    CAST(CASE WHEN em.codigo_porte_empresa = 3 THEN 1 ELSE 0 END AS TINYINT) AS is_epp,
    CAST(CASE WHEN em.codigo_porte_empresa = 5 THEN 1 ELSE 0 END AS TINYINT) AS is_demais,
    CAST(CASE WHEN em.opcao_simples = 'S' THEN 1 ELSE 0 END AS TINYINT) AS is_simples,    
    CAST(CASE WHEN em.opcao_mei = 'S' THEN 1 ELSE 0 END AS TINYINT) AS is_mei,
    CAST(CASE WHEN e.situacao_cadastral = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_nula,
    CAST(CASE WHEN e.situacao_cadastral = 2 THEN 1 ELSE 0 END AS TINYINT) AS is_ativa,
    CAST(CASE WHEN e.situacao_cadastral = 3 THEN 1 ELSE 0 END AS TINYINT) AS is_suspensa,
    CAST(CASE WHEN e.situacao_cadastral = 4 THEN 1 ELSE 0 END AS TINYINT) AS is_inapta,
    CAST(CASE WHEN e.situacao_cadastral = 8 THEN 1 ELSE 0 END AS TINYINT) AS is_baixada,
    NOW() AS data_processamento
FROM {{ ref('int_estabelecimentos') }} e
JOIN {{ ref('dim_empresas') }} em ON em.cnpj_basico = e.cnpj_basico
JOIN {{ ref('dim_situacoes') }} s ON s.codigo_identificador_matriz_filial = e.identificador_matriz_filial
    AND s.codigo_situacao_cadastral = e.situacao_cadastral
    AND s.codigo_motivo_situacao_cadastral = e.motivo_situacao_cadastral
    AND s.codigo_situacao_especial = e.situacao_especial
JOIN {{ ref('dim_localidades') }} l ON l.codigo_municipio = e.municipio
    AND l.codigo_cidade_exterior = e.cidade_exterior
    AND l.codigo_uf = e.uf
    AND l.codigo_pais = e.pais