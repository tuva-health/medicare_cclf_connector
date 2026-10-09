/*
  This model contains logic to convert member month grain to enrollment date
  spans, if needed.

  Outside the cms_alr_connector path, each enrollment row's MBI is replaced
  with the beneficiary's current MBI from CCLF9 (CCLF IP v43 5.1.1), before
  member months are rolled up, so months delivered under a previous MBI join
  the same person and span. The cms_alr_connector path applies the CCLF9
  mapping downstream, in int_eligibility_member_months_combined.
*/

with enrollment as (

    {% if var('cms_alr_connector', false) %}
    select *
    from {{ ref('stg_enrollment') }}
    {% else %}
    select
          coalesce(beneficiary_xref.crnt_num, stg_enrollment.current_bene_mbi_id) as current_bene_mbi_id
        , stg_enrollment.enrollment_start_date
        , stg_enrollment.enrollment_end_date
        , stg_enrollment.bene_member_month
    from {{ ref('stg_enrollment') }} as stg_enrollment
    left join {{ ref('int_beneficiary_xref_deduped') }} as beneficiary_xref
        on stg_enrollment.current_bene_mbi_id = beneficiary_xref.prvs_num
    {% endif %}

)

{% if var('cms_alr_connector', false) %}

select *
from enrollment

{% elif var('member_months_enrollment',False) == false -%}

select
      enrollment.current_bene_mbi_id as current_bene_mbi_id
    , enrollment_start_date
    , enrollment_end_date
from enrollment

{% else -%}

/* begin logic to create enrollment spans from member months */
, add_row_num as (

    select
          current_bene_mbi_id
        , cast(bene_member_month as date) as bene_member_month
        , row_number() over (
             partition by current_bene_mbi_id
             order by bene_member_month
           ) as row_num
    from enrollment

)

, add_lag_enrollment as (

    select
          current_bene_mbi_id
        , bene_member_month
        , row_num
        , lag(bene_member_month) over (
            partition by current_bene_mbi_id
            order by row_num
          ) as lag_enrollment
    from add_row_num

)

, calculate_lag_diff as (

    select
          current_bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , {{ datediff('lag_enrollment', 'bene_member_month', 'month') }} as lag_diff
    from add_lag_enrollment

)

, calculate_gaps as (

     select
          current_bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , lag_diff
        , case
            when lag_diff > 1 then 1
            else 0
          end as gap_flag
    from calculate_lag_diff

)

, calculate_groups as (

     select
          current_bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , lag_diff
        , gap_flag
        , sum(gap_flag) over (
            partition by current_bene_mbi_id
            order by row_num
            rows between unbounded preceding and current row
          ) as row_group
    from calculate_gaps

)

, rollup_groups as (

    select
          current_bene_mbi_id
        , row_group
        , min(bene_member_month) as enrollment_start_date
        , {{ last_day('max(bene_member_month)', 'month') }} as enrollment_end_date
    from calculate_groups
    group by
          current_bene_mbi_id
        , row_group

)

select
      rollup_groups.current_bene_mbi_id as current_bene_mbi_id
    , enrollment_start_date
    , enrollment_end_date
from rollup_groups

{%- endif %}
