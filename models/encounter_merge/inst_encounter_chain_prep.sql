
/**  Creating a list of patient's dates that include both from and thru to compare dates and determine days since last visit  **/
with date_union as(
  select
    h.bene_mbi_id
  	,h.cur_clm_uniq_id
  	,h.encounter_id
    ,clm_from_dt as claim_date
  	,clm_from_dt  
  	,clm_thru_dt
  	,1 as claim_from_flag
    ,0 as claim_thru_flag
    ,prncpl_dgns_cd
    ,admtg_dgns_cd
    ,fac_prvdr_npi_num
    ,bene_ptnt_stus_cd
  from {{ ref('inst_claims_unique')}} h
  left join {{ ref('inst_continuous_stay_crosswalk')}} c
  	on h.cur_clm_uniq_id = c.cur_clm_uniq_id
  where clm_type_cd <> '40'
  and c.cur_clm_uniq_id is null

union all
  
  select
	h.bene_mbi_id
  	,h.cur_clm_uniq_id
  	,h.encounter_id
    ,clm_thru_dt as claim_date
  	,clm_from_dt
  	,clm_thru_dt
  	,0 as claim_from_flag
  	,1 as claim_thru_flag
    ,prncpl_dgns_cd
    ,admtg_dgns_cd
    ,fac_prvdr_npi_num
    ,bene_ptnt_stus_cd
  from {{ ref('inst_claims_unique')}} h
  left join {{ ref('inst_continuous_stay_crosswalk')}} c
  	on h.cur_clm_uniq_id = c.cur_clm_uniq_id
  where clm_type_cd <> '40'
  and c.cur_clm_uniq_id is null
)
/** Numbering claims is enable sequential ordering  **/ 
,sort_date_union as(
  select 
  *
  ,row_number() over (partition by bene_mbi_id order by clm_thru_dt) AS row_sequence
  from date_union
 
)
/**  Using lag function to find the previous claims thru date, status, facility and claim  **/
,date_lag as(
  select 
    lag(claim_date) over (partition by bene_mbi_id order by row_sequence) as previous_thru_date
  	,lag(bene_ptnt_stus_cd) over (partition by bene_mbi_id order by row_sequence) as previous_bene_status
  	,lag(fac_prvdr_npi_num) over (partition by bene_mbi_id order by row_sequence) as previous_facility
    ,lag(cur_clm_uniq_id) over (partition by bene_mbi_id order by row_sequence) as previous_claim
    ,* 
  from sort_date_union
)
/** Performing date diff on previous thru date to current from date.  Filtering on from dates only to avoid date diff between same claim (i.e. LOS)  **/
 , date_last_visit as(
select 
  datediff(day, previous_thru_date, claim_date) as days_since_last_visit
  ,* 
from date_lag
where 1=1
and claim_from_flag = 1
)
select * from date_last_visit 
where 1=1
and days_since_last_visit in (0,1)
/** status of previous claim: 'still a patient'  **/
and previous_bene_status = '30'
/** eliminates transfers to diff facility  **/
and previous_facility = fac_prvdr_npi_num