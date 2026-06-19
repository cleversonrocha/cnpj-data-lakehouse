{% macro get_raw_path(base_path, file_name) %}

    {%- if execute -%}
        {% set ano_mes = modules.datetime.datetime.now().strftime('%Y_%m') %}
        
        -- Retorna a string do caminho formatada
        {{ return("'s3://" ~ base_path ~ "/" ~ ano_mes ~ "/" ~ file_name ~ ".parquet'") }}
    {%- endif -%}

{% endmacro %}