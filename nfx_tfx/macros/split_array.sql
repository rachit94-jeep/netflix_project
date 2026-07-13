{% macro split_array(column, delimiter='|') %}

SPLIT({{ column }}, '{{ delimiter }}')

{% endmacro %}