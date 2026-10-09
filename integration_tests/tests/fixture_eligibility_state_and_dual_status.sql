{{ config(tags=['fixture']) }}

/*
    Eligibility state and dual status from CCLF8 (IP v43 Table 21).

    state        must be the USPS abbreviation from GEO_USPS_STATE_CD ('NY'),
                 not the numeric BENE_FIPS_STATE_CD ('36'). the_tuva_project
                 checks it against ansi_fips_state_abbreviation.
    dual status  CCLF8 reports non-duals as BENE_DUAL_STUS_CD 'NA', which
                 the_tuva_project does not accept, so it maps to null.
                 9TT0FK0XX21 is a non-dual; 9TT0FK0XX22 (S21b) keeps '02'.
*/

with eligibility as (

    select
          person_id
        , state
        , dual_status_code
    from {{ ref('eligibility') }}
    where person_id in ('9TT0FK0XX21', '9TT0FK0XX22')

)

select
      person_id
    , state
    , dual_status_code
from eligibility
where state is null
   or state <> 'NY'
   or (person_id = '9TT0FK0XX21' and dual_status_code is not null)
   or (person_id = '9TT0FK0XX22' and coalesce(dual_status_code, '') <> '02')

union all

/* both people must be present */
select
      expected.person_id
    , null as state
    , null as dual_status_code
from (
    select '9TT0FK0XX21' as person_id
    union all
    select '9TT0FK0XX22' as person_id
) as expected
left join eligibility
    on eligibility.person_id = expected.person_id
where eligibility.person_id is null
