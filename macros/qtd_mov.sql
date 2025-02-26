{%- macro qtd_mov(suffx="kafi") -%}
IF(SUBSTR({{ suffx }}_tp_mov, 1, 1) = 'E', -{{ suffx }}_qt_mov, {{ suffx }}_qt_mov)
{%- endmacro -%}