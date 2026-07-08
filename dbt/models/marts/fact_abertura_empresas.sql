-- {{ ref('dim_tempo_inicio_atividade') }} {{ ref('dim_tempo_situacao_cadastral') }} {{ ref('dim_tempo_situacao_especial') }}  ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_abertura_empresas')
    ]
) }}

SELECT        
    es.sk_id,
    COALESCE(CAST(strftime(es.data_inicio_atividade, '%Y%m%d') AS INTEGER),0) AS sk_data_inicio_atividade,
    COALESCE(CAST(strftime(es.data_situacao_cadastral, '%Y%m%d') AS INTEGER),0) AS sk_data_situacao_cadastral,
    COALESCE(CAST(strftime(es.data_situacao_especial, '%Y%m%d') AS INTEGER),0) AS sk_data_situacao_especial,
    CAST(1 AS SMALLINT) AS qtd_estabelecimentos,
    CAST(CASE WHEN es.identificador_matriz_filial = 1 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_matrizes,
    CAST(CASE WHEN es.identificador_matriz_filial = 2 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_filiais,
    CAST(CASE WHEN es.situacao_cadastral = 2 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_ativas,
    CAST(CASE WHEN es.situacao_cadastral = 3 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_suspensas,
    CAST(CASE WHEN es.situacao_cadastral = 8 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_baixadas,
    CAST(CASE WHEN es.situacao_cadastral = 1 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_nulas,            
    CAST(CASE WHEN es.situacao_cadastral = 4 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_inaptas,
    CAST(CASE WHEN em.porte_empresa = 1 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_porte_micro,
    CAST(CASE WHEN em.porte_empresa = 3 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_porte_pequeno,
    CAST(CASE WHEN em.porte_empresa = 5 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_porte_demais,
    CAST(CASE WHEN em.porte_empresa = 0 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_nao_informado,
    CAST(CASE WHEN s.opcao_mei = 'S' AND s.data_exclusao_mei IS NULL THEN 1 ELSE 0 END AS SMALLINT) AS qtd_mei,
    cast(CASE WHEN s.opcao_mei = 'S' AND s.data_exclusao_mei IS NOT NULL THEN 1 ELSE 0 END AS SMALLINT) AS qtd_ex_mei,
    CAST(CASE WHEN s.opcao_mei = 'N' THEN 1 ELSE 0 END AS SMALLINT) AS qtd_nao_mei
FROM {{ ref('stg_estabelecimentos') }} es
JOIN {{ ref('stg_empresas') }} em ON em.cnpj_basico = es.cnpj_basico
LEFT JOIN {{ ref('stg_simples') }} s ON s.cnpj_basico = em.cnpj_basico