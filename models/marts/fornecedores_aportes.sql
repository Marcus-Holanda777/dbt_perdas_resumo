{{
    config(
        materialized='table',
        format='parquet',
        write_compression='ZSTD'
    )
}}

with view_fornecedores as (
    select
        forn_cd_fornecedor as cod_forn,
        {{ strip_normalize("forn_nm_fantasia") }} as forn_nm,
        {{ cnpj_normalize("forn_tn_cnpj") }} as cnpj_forn
    from {{ source('prevencao-perdas', 'cosmos_v14b_dbo_fornecedor') }}
),

join_pai as (
    select
        forn.*,
        coalesce(pai.cod_forn_pai, forn.cod_forn) as cod_forn_pai,
        coalesce(pai.forn_nm_pai, forn.forn_nm) as forn_nm_pai,
        coalesce(pai.cnpj_forn_pai, forn.cnpj_forn) as cnpj_forn_pai
    from view_fornecedores as forn
    left join
        {{ ref('stg_fornecedor_pai') }} as pai
        on forn.cod_forn = pai.cod_forn
),

join_comercial as (
    select
        pai.*,
        comercial.forn_comercial
    from join_pai as pai
    left join
        {{ ref('stg_fornecedor_comercial') }} as comercial
        on pai.cod_forn = comercial.cod_forn
),

join_sap as (
    select
        comercial.*,
        sap.fornecedor_principal_sap
    from join_comercial as comercial
    left join
        {{ ref('stg_fornecedor_sap') }} as sap
        on comercial.cod_forn = sap.forn_cd_fornecedor
),

final as (
    select
        cod_forn,
        forn_nm,
        cnpj_forn,
        cod_forn_pai,
        forn_nm_pai,
        cnpj_forn_pai,
        forn_comercial,
        fornecedor_principal_sap
    from join_sap
)

select * from final
