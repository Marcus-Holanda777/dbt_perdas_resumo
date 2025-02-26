{%- macro strip_normalize(colname) -%}

{%- set target = 'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ' -%}
{%- set source = 'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ' -%}

UPPER(REGEXP_REPLACE(TRANSLATE(TRIM({{ colname }}), '{{ target }}', '{{ source }}'), ' +', ' '))

{%- endmacro -%}