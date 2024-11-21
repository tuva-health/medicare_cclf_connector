with staged_data as (

    select
          hicn_mbi_xref_ind
        , crnt_num
        , prvs_num
        , prvs_id_efctv_dt
        , prvs_id_obslt_dt
        , bene_rrb_num
        , file_name
        , file_date
    from {{ ref('stg_beneficiary_xref') }}

)

/* window over previous MBI to get latest current MBI */
, add_row_num as (

    select *, row_number() over (
        partition by prvs_num
        order by file_date desc, prvs_id_efctv_dt desc
        ) as row_num
    from staged_data

)

/*
    check if the current MBI is listed as a previous MBI and
    get its latest current MBI
*/
, check_crnt_num as (
    select
          a.file_date
        , a.prvs_num
        , a.crnt_num
        , b.file_date as b_file_date
        , b.prvs_num as b_prvs_num
        , b.crnt_num as b_crnt_num
        , case
            when b.crnt_num is not null and b.file_date > a.file_date
            then b.crnt_num
            else a.crnt_num
            end as final_mbi
    from add_row_num as a
        left join add_row_num as b
            on a.crnt_num = b.prvs_num
            and b.prvs_num <> b.crnt_num
    where a.row_num = 1

)

select
      prvs_num
    , final_mbi as crnt_num
from check_crnt_num