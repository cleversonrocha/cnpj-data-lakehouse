{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_localidades')
    ]
) }}

WITH localidades AS (
    SELECT
        DISTINCT
        CAST(DENSE_RANK() OVER(ORDER BY municipio,cidade_exterior,uf,pais) AS INTEGER) AS sk_id,
        municipio,
        cidade_exterior,
        uf,
        pais        
    FROM {{ ref('int_estabelecimentos') }}
)

SELECT
    l.sk_id,
    l.municipio,
    m.descricao AS descricao_municipio,
    l.cidade_exterior,
    ce.descricao AS descricao_cidade_exterior,
    l.uf,
    u.sigla AS descricao_uf,
    u.codigo AS regiao,
    u.regiao AS descricao_regiao,
    l.pais,
    p.descricao AS descricao_pais,
    NOW() AS data_processamento
FROM localidades l
JOIN {{ ref('int_municipios') }} m ON m.codigo = l.municipio
JOIN {{ ref('int_cidades_exterior') }} ce ON ce.codigo = l.cidade_exterior
JOIN {{ ref('int_ufs') }} u ON u.codigo = l.uf
JOIN {{ ref('int_paises') }} p ON p.codigo = l.pais