{% macro get_ano_mes() %}
    {% if var('ano_mes', '') != '' %}
        {{ return(var('ano_mes')) }}
    {% else %}
        {{ return(modules.datetime.datetime.now().strftime('%Y_%m')) }}
    {% endif %}
{% endmacro %}