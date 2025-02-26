{% set tipos = get_tipos_cd() %}
{% set limit_ref = modules.datetime.datetime.strptime(var("limit_date"), "%Y-%m-%d") %}
{% set start_parser = modules.datetime.datetime.strptime(var("start", "1999-01-01"), "%Y-%m-%d") %}
{% set end_parser = modules.datetime.datetime.strptime(var("end", "1999-01-01"), "%Y-%m-%d") %}

with deposito_filial as (
    select
        depo_cd_deposito,
        CAST(SUBSTR(TRIM(depo_tn_cnpj), 13, 4) as int) as filial
    from {{ source("prevencao-perdas", "cosmos_v14b_dbo_deposito") }}
),

start_perdas as (
    select
        df.filial,
        kp.kade_cd_produto,
        kp.kade_vl_cmpcsicms,
        kp.kade_dh_ocorr,
        TRIM(kp.kade_tx_nr_docto) as kade_tx_nr_docto,
        UPPER(TRIM(kp.kade_tp_mov)) as kade_tp_mov,
        TRIM(kp.sub_tipo) as sub_tipo,
        {{ qtd_mov("kade") }} as kade_qt_mov
    from {{ source('prevencao-perdas', 'kardex_perdas_cd') }} as kp
    inner join
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_produto_mestre') }} as pm
        on kp.kade_cd_produto = pm.prme_cd_produto
    inner join
        deposito_filial as df
        on kp.kade_cd_deposito = df.depo_cd_deposito
    where
        kp.kade_tx_nr_docto not like '%EXTRA%'
        and kp.sub_tipo is not null
        {{ servicos() }}
        and kp.kade_dh_ocorr
        between timestamp '{{ start_parser.strftime("%Y-%m-%d 00:00:00.000") }}'
        and timestamp '{{ end_parser.strftime("%Y-%m-%d 23:59:59.999") }}'
),

add_tipo_perdas as (
    select
        filial,
        kade_cd_produto as cod_prod,
        DATE_TRUNC('month', kade_dh_ocorr) as periodo,
        {%- for coluna, eventos in tipos %}
            SUM(kade_qt_mov) filter (
                where sub_tipo {{ eventos }}
            ) as qtd_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            SUM(kade_qt_mov * kade_vl_cmpcsicms) filter (
                where sub_tipo {{ eventos }}
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
        'deposito' as origem,
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
