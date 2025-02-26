with view_forn_comercial as (
    select
        "codigo fornecedor principal deposito" as cod_forn,
        "fornecedor comercial" as forn_comercial
    from {{ source('planejamento_comercial', 'dim_produtos') }}
),

renamed as (
    select
        CAST(COALESCE(TRY_CAST(cod_forn as DOUBLE), 0) as INT) as cod_forn,
        {{ strip_normalize("forn_comercial") }} as forn_comercial
    from view_forn_comercial
),

filter_forn as (
    select * from renamed
    where cod_forn > 0
),

add_duplicates as (
    select
        *,
        ROW_NUMBER() over (partition by cod_forn) as id
    from filter_forn
),

final as (
    select
        cod_forn,
        forn_comercial
    from add_duplicates
    where id = 1
)

select * from final
