with 
beneficiary_xref as (

    select
          crnt_num
        , prvs_num
    from {{ ref('int_beneficiary_xref_deduped') }}

)

, unpivoted_enrollment as (
    select
        *
    from {{ ref('int_unpivoted_enrollment') }} as d
)

, canonical_death as (
    select
        current_bene_mbi_id,
        max(bene_death_dt) as death_date
    from {{ ref('int_beneficiary_demographics_deduped') }} as d
    group by current_bene_mbi_id
)

, enrollment as (
    select
        bene_mbi_id,
        u.enrollment_month as enrollment_start_date,
        last_day(u.enrollment_month) as enrollment_end_date,
        d.death_date,
        1 as bene_member_month
    from unpivoted_enrollment u
    left join beneficiary_xref xref
        on u.bene_mbi_id = xref.prvs_num
   left outer join canonical_death d
        on d.current_bene_mbi_id = coalesce(xref.crnt_num, u.bene_mbi_id)
    where (d.death_date is null or u.enrollment_month <= d.death_date)
)


{% if var('member_months_enrollment',False) == false -%}

select distinct
      COALESCE(beneficiary_xref.crnt_num, enrollment.bene_mbi_id) as current_bene_mbi_id
    , 1 as row_group
    , enrollment_start_date
    , enrollment_end_date
from enrollment
    left outer join beneficiary_xref
        on enrollment.bene_mbi_id = beneficiary_xref.prvs_num

{% else -%}

/* begin logic to create enrollment spans from member months */
, add_row_num as (

    select
          bene_mbi_id
        , cast(enrollment_start_date as date) as enrollment_start_date
        , row_number() over (
             partition by bene_mbi_id
             order by enrollment_start_date
           ) as row_num
    from enrollment

)

, add_lag_enrollment as (

    select
          bene_mbi_id
        , enrollment_start_date
        , row_num
        , lag(enrollment_start_date) over (
            partition by bene_mbi_id
            order by row_num
          ) as lag_enrollment
    from add_row_num

)

, calculate_lag_diff as (

    select
          bene_mbi_id
        , enrollment_start_date
        , row_num
        , lag_enrollment
        , {{ datediff('lag_enrollment', 'enrollment_start_date', 'month') }} as lag_diff
    from add_lag_enrollment

)

, calculate_gaps as (

     select
          bene_mbi_id
        , enrollment_start_date
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
          bene_mbi_id
        , enrollment_start_date
        , row_num
        , lag_enrollment
        , lag_diff
        , gap_flag
        , sum(gap_flag) over (
            partition by bene_mbi_id
            order by row_num
            rows between unbounded preceding and current row
          ) as row_group
    from calculate_gaps

)

, rollup_groups as (

    select
          bene_mbi_id
        , row_group
        , min(enrollment_start_date) as enrollment_start_date
        , {{ last_day('max(enrollment_start_date)', 'month') }} as enrollment_end_date
    from calculate_groups
    group by
          bene_mbi_id
        , row_group

)

select
      coalesce(beneficiary_xref.crnt_num, rollup_groups.bene_mbi_id) as current_bene_mbi_id
    , row_group
    , enrollment_start_date
    , enrollment_end_date
from rollup_groups
    left join beneficiary_xref
        on rollup_groups.bene_mbi_id = beneficiary_xref.prvs_num

{%- endif %}
