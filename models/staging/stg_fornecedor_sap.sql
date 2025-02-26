with view_forn_sap as (
    select
        forn_cd_fornecedor,
        fsma_codigo_sap_master,
        xxxx_dh_cad
    from {{ source('prevencao-perdas', 'cosmos_v14b_dbo_fornecedor') }}
    where fsma_codigo_sap_master is not null
),

add_duplicate as (
    select
        *,
        ROW_NUMBER()
            over (
                partition by fsma_codigo_sap_master
                order by xxxx_dh_cad desc
            )
            as id
    from view_forn_sap
),

drop_duplicate as (
    select *
    from add_duplicate
    where id = 1
),

final as (
    select
        fsma_codigo_sap_master as fornecedor_principal_sap,
        forn_cd_fornecedor
    from drop_duplicate
)

select * from final
