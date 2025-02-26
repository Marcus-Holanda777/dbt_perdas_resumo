{% set tipos = get_tipos_filial() %}
{% set limit_ref = modules.datetime.datetime.strptime(var("limit_date"), "%Y-%m-%d") %}
{% set start_parser = modules.datetime.datetime.strptime(var("start", "1999-01-01"), "%Y-%m-%d") %}
{% set end_parser = modules.datetime.datetime.strptime(var("end", "1999-01-01"), "%Y-%m-%d") %}

with start_perdas as (
    select
        kp.kafi_cd_filial,
        kp.kafi_cd_produto,
        kp.kafi_vl_cmpcsicms,
        kp.kafi_dh_ocorrreal,
        UPPER(TRIM(kp.kafi_tp_mov)) as kafi_tp_mov,
        {{ qtd_mov() }} as kafi_qt_mov
    from {{ source('prevencao-perdas', 'kardex_perdas') }} as kp
    inner join
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_produto_mestre') }} as pm
        on kp.kafi_cd_produto = pm.prme_cd_produto
    where
        kp.kafi_tx_nr_docto not like '%INVT.INI.EF%'
        {{ servicos() }}
        {% if start_parser > limit_ref %}
        AND kp.kafi_fl_tipoperda is null
        {% endif %}
        and kp.kafi_dh_ocorrreal
        between timestamp '{{ start_parser.strftime("%Y-%m-%d 00:00:00.000") }}'
        and timestamp '{{ end_parser.strftime("%Y-%m-%d 23:59:59.999") }}'
),

add_tipo_perdas as (
    select
        kafi_cd_filial as filial,
        kafi_cd_produto as cod_prod,
        DATE_TRUNC('month', kafi_dh_ocorrreal) as periodo,
        {%- for coluna, eventos in tipos %}
            SUM(kafi_qt_mov) filter (
                where kafi_tp_mov in ({{ eventos }})
            ) as qtd_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            SUM(kafi_qt_mov * kafi_vl_cmpcsicms) filter (
                where kafi_tp_mov in ({{ eventos }})
            ) as vl_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %}
    from start_perdas
    group by 1, 2, 3
),

coalesce_tipos as (
    select
        filial,
        cod_prod,
        'filial' as origem,
        periodo,
        {%- for coluna, eventos in tipos %}
            COALESCE(qtd_{{ coluna }}, 0) as qtd_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            COALESCE(vl_{{ coluna }}, 0) as vl_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %}
    from add_tipo_perdas
),

final as (
    select
        filial,
        cod_prod,
        origem,
        {%- for coluna, eventos in tipos %}
            qtd_{{ coluna }}
            {%- if coluna ==  "ajustes" %}
                ,
                qtd_inventarios + qtd_{{ coluna }} as qtd_perda_desconhecida
            {%- elif coluna ==  "vencidos" %}
                ,
                qtd_avarias + qtd_{{ coluna }} as qtd_perda_conhecida
            {%- endif -%}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            vl_{{ coluna }}
            {%- if coluna ==  "ajustes" %}
                ,
                vl_inventarios + vl_{{ coluna }} as vl_perda_desconhecida
            {%- elif coluna ==  "vencidos" %}
                ,
                vl_avarias + vl_{{ coluna }} as vl_perda_conhecida
            {%- endif -%}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        periodo
    from coalesce_tipos
)

select * from final
