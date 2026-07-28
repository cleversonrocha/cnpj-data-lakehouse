{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_empresas')
    ]
) }}

SELECT 
    CAST(ROW_NUMBER() OVER(ORDER BY e.cnpj_basico) AS INTEGER) AS sk_id,
    e.cnpj_basico,
    e.capital_social,
    e.razao_social,
    e.natureza_juridica,
    n.descricao AS descricao_natureza_juridica,
    e.qualificacao_responsavel,
    q.descricao AS descricao_qualificacao_responsavel, 
    e.porte_empresa,
    pe.descricao AS descricao_porte_empresa,
    ef.codigo AS ente_federativo_responsavel,
    ef.descricao AS descricao_ente_federativo_responsavel,
    NOW() AS data_processamento
FROM {{ ref('int_empresas') }} e
JOIN {{ ref('int_naturezas') }} n ON n.codigo = e.natureza_juridica
JOIN {{ ref('int_qualificacoes') }} q ON q.codigo = e.qualificacao_responsavel
JOIN {{ ref('int_portes_empresas') }} pe ON pe.codigo = e.porte_empresa
JOIN {{ ref('int_entes_federativos') }} ef ON ef.codigo = e.ente_federativo_responsavel