{% macro get_raw_path(base_path, file_name) %}

    {% if var('ano_mes', '') != '' %}    
        {% set ano_mes = var('ano_mes') %}
    {% else %}    
        {% set ano_mes = modules.datetime.datetime.now().strftime('%Y_%m') %}
    {% endif %}   
        
    -- Retorna a string do caminho formatada
    {{ return("'s3://" ~ base_path ~ "/" ~ ano_mes ~ "/" ~ file_name ~ ".parquet'") }}    

{% endmacro %}