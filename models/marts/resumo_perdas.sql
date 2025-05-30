{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        format='parquet',
        write_compression='zstd',
        table_type='iceberg',
        partitioned_by=['month(periodo)'],
        table_properties={"optimize_rewrite_delete_file_threshold": "2"},
        pre_hook=[
            "{% if is_incremental(name = this.identifier ) %} DELETE FROM {{ this }} WHERE periodo between TIMESTAMP '{{ var('start') }} 00:00:00.000' and TIMESTAMP '{{ var('end') }} 23:59:59.999' {% endif %}"
        ],
        post_hook=[
            "OPTIMIZE {{ this.render_pure() }} REWRITE DATA USING BIN_PACK",
            "VACUUM {{ this.render_pure() }}",
        ],
        on_schema_change='sync_all_columns'
    )
}}

with final as (
    select * from {{ ref('stg_perdas_filial') }}
    union all
    select * from {{ ref('stg_perdas_deposito') }}
)

select * from final
