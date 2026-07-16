-- {{ ref('stg_empresas') }} {{ ref('dim_naturezas_juridicas') }} {{ ref('dim_portes_empresas') }} {{ ref('dim_entes_federativos') }} {{ ref('dim_is_mei') }} {{ ref('int_estabelecimentos_por_empresa') }} {{ ref('int_socios_por_empresa') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_empresas')
    ]
) }}

SELECT
    e.sk_id AS sk_id,
    nj.sk_id AS sk_natureza_juridica,
    pe.sk_id AS sk_portes_empresas,
    ef.sk_id AS sk_ente_federativo_responsavel,
    im.sk_id AS sk_mei,
    epe.qtd_estabelecimentos AS qtd_estabelecimentos,
    epe.qtd_matriz AS qtd_est_matriz,
    epe.qtd_filiais AS qtd_est_filiais,
    epe.qtd_nulas AS qtd_est_nulas,
    epe.qtd_ativas AS qtd_est_ativas,
    epe.qtd_suspensas AS qtd_est_suspensas,
    epe.qtd_inaptas AS qtd_est_inaptas,
    epe.qtd_baixadas AS qtd_est_baixadas,
    COALESCE(spe.qtd_socios,0) AS qtd_socios,
    COALESCE(spe.qtd_socios_pf,0) AS qtd_socios_pf,
    COALESCE(spe.qtd_socios_pj,0) AS qtd_socios_pj,
    COALESCE(spe.qtd_socios_estrangeiro,0) AS qtd_socios_estrangeiro,
    CAST(CASE WHEN pe.codigo = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_me,
    CAST(CASE WHEN pe.codigo = 3 THEN 1 ELSE 0 END AS TINYINT) AS qtd_pp,
    CAST(CASE WHEN pe.codigo = 5 THEN 1 ELSE 0 END AS TINYINT) AS qtd_demais,
    CAST(CASE WHEN pe.codigo = -1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_porte_nao_informado,        
    CAST(CASE WHEN im.codigo = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_mei,
    CAST(CASE WHEN im.codigo = 2 THEN 1 ELSE 0 END AS TINYINT) AS qtd_nao_mei        
FROM {{ ref('stg_empresas') }} e
JOIN {{ ref('dim_naturezas_juridicas') }} nj ON nj.codigo = e.natureza_juridica
JOIN {{ ref('dim_portes_empresas') }} pe ON pe.codigo = e.porte_empresa
JOIN {{ ref('dim_entes_federativos') }} ef ON ef.codigo = e.ente_federativo_responsavel
JOIN {{ ref('dim_is_mei') }} im ON im.codigo = e.is_mei
JOIN {{ ref('int_estabelecimentos_por_empresa') }} epe ON epe.sk_empresas = e.sk_id
LEFT JOIN {{ ref('int_socios_por_empresa') }} spe ON spe.sk_empresa_id = e.sk_id