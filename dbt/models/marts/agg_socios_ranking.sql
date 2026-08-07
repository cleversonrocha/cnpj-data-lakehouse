{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/agg', file_name='agg_socios_ranking')
    ]
) }}

WITH totais AS (

    SELECT
        sk_empresa,
        SUM(is_pf) AS total_pf,
        SUM(is_pj) AS total_pj,
        SUM(is_estrangeiro) AS total_estrangeiro                
    FROM {{ ref('fact_socios') }}
    GROUP BY sk_empresa
)

SELECT
    sk_empresa,
    CAST(total_pf AS SMALLINT) AS total_pf,
    CAST(total_pj AS SMALLINT) AS total_pj,
    CAST(total_estrangeiro AS SMALLINT) AS total_estrangeiro,
    CAST(total_pf + total_pj + total_estrangeiro AS SMALLINT) AS total_socios,
    CAST(RANK() OVER (ORDER BY total_pf DESC) AS INTEGER) AS rank_pf,
    CAST(RANK() OVER (ORDER BY total_pj DESC) AS INTEGER) AS rank_pj,
    CAST(RANK() OVER (ORDER BY total_estrangeiro DESC) AS INTEGER) AS rank_estrangeiro,
    CAST(RANK() OVER (ORDER BY total_pf + total_pj + total_estrangeiro DESC) AS INTEGER) AS rank_total_socios
FROM totais