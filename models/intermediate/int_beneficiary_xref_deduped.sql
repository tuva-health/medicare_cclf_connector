with staged_data as (

    select
          hicn_mbi_xref_ind
        , crnt_num
        , prvs_num
        , prvs_id_efctv_dt
        , prvs_id_obslt_dt
        , bene_rrb_num
        , file_name as file_name
        , file_row_number
        , ingest_datetime as ingest_date
    from {{ ref('stg_cclf9') }}

)

, parse_file_date as (

     select *
          , CAST(SUBSTRING(file_name, CHARINDEX('D', file_name) + 1, 6) as date )  as file_date 
     from staged_data

)

/* dedupe full rows that may appear in multiple files */
, add_row_num as (

    select *, row_number() over (
        partition by
              hicn_mbi_xref_ind
            , crnt_num
            , prvs_num
            , prvs_id_efctv_dt
            , prvs_id_obslt_dt
            , bene_rrb_num
        order by file_date desc
        ) as row_num
    from parse_file_date

)

select
      hicn_mbi_xref_ind
    , crnt_num
    , prvs_num
    , prvs_id_efctv_dt
    , prvs_id_obslt_dt
    , bene_rrb_num
    , file_name
    , file_date
    , ingest_date
from add_row_num
where row_num = 1