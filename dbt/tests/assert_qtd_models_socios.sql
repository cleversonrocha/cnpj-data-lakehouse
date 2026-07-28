-- tests/assert_qtd_models_socios.sql
WITH qtd_staging AS (
    SELECT        
        COUNT(cnpj_basico) as qtd
    FROM {{ ref('stg_socios') }}    
),
qtd_intermediate AS (
    SELECT        
        COUNT(cnpj_basico) as qtd
    FROM {{ ref('int_socios') }}    
),
qtd_marts AS (
    SELECT        
        COUNT(sk_socio) as qtd
    FROM {{ ref('fact_socios') }}    
)

SELECT    
    qtd_staging.qtd,
    qtd_intermediate.qtd,
    qtd_marts.qtd
FROM qtd_staging
CROSS JOIN qtd_intermediate
CROSS JOIN qtd_marts
WHERE qtd_staging.qtd != qtd_intermediate.qtd OR qtd_intermediate.qtd != qtd_marts.qtd