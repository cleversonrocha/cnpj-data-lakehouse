{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_estabelecimentos')
    ]
) }}

SELECT
    e.sk_id AS sk_estabelecimento,
    em.sk_id AS sk_empresa,
    tia.sk_id AS sk_tempo_inicio_atividade,
    COALESCE(tse.sk_id,-1) AS sk_tempo_situacoes_especiais,
    COALESCE(tsc.sk_id,-1) AS sk_tempo_situacoes_cadastrais,
    CAST(DENSE_RANK() OVER(ORDER BY e.municipio,e.cidade_exterior,e.uf,e.pais) AS INTEGER) AS sk_localidades,
    SUBSTR(e.cnpj_basico, 1, 2) || '.' || SUBSTR(e.cnpj_basico, 3, 3) || '.' || SUBSTR(e.cnpj_basico, 6, 3) || '/' || e.cnpj_ordem || '-' || e.cnpj_dv AS cnpj_completo,
    CASE 
        WHEN e.identificador_matriz_filial = 1 THEN 'MATRIZ'
        WHEN e.identificador_matriz_filial = 2 THEN 'FILIAL'
    END AS descricao_matriz_filial,
    e.nome_fantasia,    
    sc.descricao AS situacao_cadastral,
    scm.descricao AS motivo_situacao_cadastral,
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
    CAST(CASE WHEN e.identificador_matriz_filial = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_matrizes,
    CAST(CASE WHEN e.identificador_matriz_filial = 2 THEN 1 ELSE 0 END AS TINYINT) AS qtd_filiais,    
    CAST(CASE WHEN e.situacao_cadastral = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_nulas,
    CAST(CASE WHEN e.situacao_cadastral = 2 THEN 1 ELSE 0 END AS TINYINT) AS qtd_ativas,
    CAST(CASE WHEN e.situacao_cadastral = 3 THEN 1 ELSE 0 END AS TINYINT) AS qtd_suspensas,
    CAST(CASE WHEN e.situacao_cadastral = 4 THEN 1 ELSE 0 END AS TINYINT) AS qtd_inaptas,
    CAST(CASE WHEN e.situacao_cadastral = 8 THEN 1 ELSE 0 END AS TINYINT) AS qtd_baixadas,        
    NOW() AS data_processamento
FROM {{ ref('int_estabelecimentos') }} e
JOIN {{ ref('int_situacoes_cadastrais') }} sc ON sc.codigo = e.situacao_cadastral
LEFT JOIN {{ ref('dim_tempo_situacoes_cadastrais') }} tsc ON tsc.data_referencia = e.data_situacao_cadastral
JOIN {{ ref('int_situacoes_cadastrais_motivos') }} scm ON scm.codigo = e.motivo_situacao_cadastral
JOIN {{ ref('dim_tempo_inicio_atividades') }} tia ON tia.data_referencia = e.data_inicio_atividade
LEFT JOIN {{ ref('dim_tempo_situacoes_especiais') }} tse ON tse.data_referencia = e.data_situacao_especial
JOIN {{ ref('dim_empresas') }} em ON em.cnpj_basico = e.cnpj_basico