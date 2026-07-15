-- {{ ref('dim_portes_empresas') }} {{ ref('dim_is_mei') }} {{ ref('int_estabelecimentos_por_empresa') }} {{ ref('int_socios_por_empresa') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_empresas')
    ]
) }}

SELECT
    e.sk_id,
    CAST(pe.sk_id AS TINYINT) AS sk_portes_empresas,
    CAST(m.sk_id AS TINYINT) AS sk_mei,
    CAST(epe.qtd_estabelecimentos AS SMALLINT) AS qtd_estabelecimentos,
    CAST(epe.qtd_matriz AS TINYINT) AS qtd_est_matriz,
    CAST(epe.qtd_filiais AS SMALLINT)  AS qtd_est_filiais,
    CAST(epe.qtd_nulas AS SMALLINT)  AS qtd_est_nulas,
    CAST(epe.qtd_ativas AS SMALLINT)  AS qtd_est_ativas,
    CAST(epe.qtd_suspensas AS SMALLINT)  AS qtd_est_suspensas,
    CAST(epe.qtd_inaptas AS SMALLINT)  AS qtd_est_inaptas,
    CAST(epe.qtd_baixadas AS SMALLINT)  AS qtd_est_baixadas,
    CAST(COALESCE(spe.qtd_socios,0) AS SMALLINT) AS qtd_socios,
    CAST(COALESCE(spe.qtd_socios_pf,0) AS SMALLINT) AS qtd_socios_pf,
    CAST(COALESCE(spe.qtd_socios_pj,0) AS SMALLINT) AS qtd_socios_pj,
    CAST(COALESCE(spe.qtd_socios_estrangeiro,0) AS SMALLINT) AS qtd_socios_estrangeiro,
    CAST(CASE WHEN pe.codigo = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_me,
    CAST(CASE WHEN pe.codigo = 3 THEN 1 ELSE 0 END AS TINYINT) AS qtd_pp,
    CAST(CASE WHEN pe.codigo = 5 THEN 1 ELSE 0 END AS TINYINT) AS qtd_demais,
    CAST(CASE WHEN pe.codigo = -1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_porte_nao_informado,        
    CAST(CASE WHEN m.codigo = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_mei,
    CAST(CASE WHEN m.codigo = 2 THEN 1 ELSE 0 END AS TINYINT) AS qtd_nao_mei        
FROM {{ ref('stg_empresas') }} e
JOIN {{ ref('dim_portes_empresas') }} pe ON pe.codigo = e.porte_empresa
JOIN {{ ref('dim_is_mei') }} m ON m.codigo = e.is_mei
JOIN {{ ref('int_estabelecimentos_por_empresa') }} epe ON epe.sk_empresas = e.sk_id
LEFT JOIN {{ ref('int_socios_por_empresa') }} spe ON spe.sk_empresa_id = e.sk_id