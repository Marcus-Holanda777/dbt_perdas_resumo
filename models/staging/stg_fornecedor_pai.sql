with view_forn_pai as (
    select
        pai.data_hora_cadastro,
        filho.codigo_fornecedor,
        pai.codigo_fornecedor_principal
    from
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_grupo_fornecedores_aporte_cab') }}
            as pai
    inner join
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_grupo_fornecedores_aporte_det') }}
            as filho
        on
            pai.id_grupo_fornecedores_aporte_cab
            = filho.id_grupo_fornecedores_aporte_cab
),

add_row_number as (
    select
        *,
        ROW_NUMBER()
            over (
                partition by codigo_fornecedor
                order by data_hora_cadastro desc
            )
            as id
    from view_forn_pai
),

drop_duplicates as (
    select
        codigo_fornecedor,
        codigo_fornecedor_principal
    from add_row_number
    where id = 1
),

final as (
    select
        codigo_fornecedor as cod_forn,
        codigo_fornecedor_principal as cod_forn_pai,
        {{ strip_normalize("forn_nm_fantasia") }} as forn_nm_pai,
        {{ cnpj_normalize("forn_tn_cnpj") }} as cnpj_forn_pai
    from drop_duplicates as pai
    inner join
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_fornecedor') }} as forn
        on pai.codigo_fornecedor_principal = forn.forn_cd_fornecedor
)

select * from final
