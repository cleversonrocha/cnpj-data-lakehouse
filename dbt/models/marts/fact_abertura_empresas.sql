-- {{ ref('dim_estabelecimentos') }} {{ ref('dim_empresas') }} {{ ref('dim_localizacao') }} {{ ref('dim_tempo') }}  ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_abertura_empresas')
    ]
) }}

SELECT        
    es.sk_id AS sk_estabelecimento,
    COALESCE(CAST(strftime(es.data_inicio_atividade, '%Y%m%d') AS INTEGER),19000101) AS sk_tempo_inicio_atividade,
    COALESCE(CAST(strftime(data_situacao_cadastral, '%Y%m%d') AS INTEGER),19000101) AS sk_tempo_data_situacao_cadastral,    
    l.sk_id AS sk_localizacao,  
    em.sk_id AS sk_empresa,
    CAST(1 AS SMALLINT) AS qtd_estabelecimentos,
    CAST(CASE WHEN em.is_mei = 'SIM' THEN 1 ELSE 0 END AS SMALLINT) AS qtd_mei,    
    CAST(CASE WHEN is_mei = 'NÃO (EX-MEI)' THEN 1 ELSE 0 END AS SMALLINT) AS qtd_ex_mei,
    CAST(CASE WHEN em.is_mei = 'NÃO' THEN 1 ELSE 0 END AS SMALLINT) AS qtd_nao_mei,
    CAST(CASE WHEN identificador_matriz_filial = 1 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_matrizes,
    CAST(CASE WHEN identificador_matriz_filial = 2 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_filiais,
    CAST(CASE WHEN situacao_cadastral = 2 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_ativas,
    CAST(CASE WHEN situacao_cadastral = 3 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_suspensas,
    CAST(CASE WHEN situacao_cadastral = 8 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_baixadas,
    CAST(CASE WHEN situacao_cadastral = 1 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_nulas,            
    CAST(CASE WHEN situacao_cadastral = 4 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_inaptas    
FROM {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }} es
JOIN {{ ref('dim_empresas') }} em ON em.sk_id = es.cnpj_basico
JOIN {{ ref('dim_localizacao') }} l ON l.uf = es.uf AND 
                                       l.desc_municipio = es.desc_municipio AND                                        
                                       l.pais = es.pais