{#
    CCLFB (Part B benefit enhancement and demonstration codes) is a claim-line
    level file. The same claim line can be delivered in several monthly /
    run-out files, so we keep one row per claim line, preferring the most
    recent file. clm_line_num is zero padded in the raw file ('01'), so the
    dedupe partitions on the integer value to match claim_line_number in the
    physician and DME branches.
#}

with demo_codes as (

    select
          cur_clm_uniq_id
        , {{ try_to_cast_int('clm_line_num') }} as claim_line_number
        , clm_pbp_inclsn_amt
        , clm_pbp_rdctn_amt
        , clm_mdcr_ddctbl_amt
        , clm_sqstrtn_rdctn_amt
        , file_name
        , file_date
        , row_number() over (
            partition by cur_clm_uniq_id, {{ try_to_cast_int('clm_line_num') }}
            order by
                  file_date desc
                , file_name desc
          ) as row_num
    from {{ ref('stg_partb_demo_codes') }}
    where cur_clm_uniq_id is not null

)

select
      cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as claim_id
    , claim_line_number
    , {{ cast_numeric('clm_pbp_inclsn_amt') }} as clm_pbp_inclsn_amt
    , {{ cast_numeric('clm_pbp_rdctn_amt') }} as clm_pbp_rdctn_amt
    , {{ cast_numeric('clm_mdcr_ddctbl_amt') }} as clm_mdcr_ddctbl_amt
    , {{ cast_numeric('clm_sqstrtn_rdctn_amt') }} as clm_sqstrtn_rdctn_amt
    , file_name
    , file_date
from demo_codes
where row_num = 1
