{% macro get_s3_path(base_path, file_name) %}

    {% set ano_mes = get_ano_mes() %}
            
    -- Retorna a string do caminho formatada
    {{ return("'s3://" ~ base_path ~ "/" ~ ano_mes ~ "/" ~ file_name ~ ".parquet'") }}    

{% endmacro %}