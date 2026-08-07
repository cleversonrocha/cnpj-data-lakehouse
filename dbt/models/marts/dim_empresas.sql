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
    n.codigo AS codigo_natureza_juridica,    
    n.descricao AS natureza_juridica,    
    q.descricao AS qualificacao_responsavel,     
    pe.codigo AS codigo_porte_empresa,
    pe.descricao AS porte_empresa,    
    ef.descricao AS ente_federativo_responsavel,
    s.opcao_simples,
    s.opcao_mei,
    NOW() AS data_processamento
FROM {{ ref('int_empresas') }} e
JOIN {{ ref('int_naturezas') }} n ON n.codigo = e.natureza_juridica
JOIN {{ ref('int_qualificacoes') }} q ON q.codigo = e.qualificacao_responsavel
JOIN {{ ref('int_portes_empresas') }} pe ON pe.codigo = e.porte_empresa
JOIN {{ ref('int_entes_federativos') }} ef ON ef.codigo = e.ente_federativo_responsavel
LEFT JOIN {{ ref('int_simples') }} s ON s.cnpj_basico = e.cnpj_basico