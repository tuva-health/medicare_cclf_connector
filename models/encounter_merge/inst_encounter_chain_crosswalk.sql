{{ config(materialized='table') }}


/**  Need to consolidate both the claim ids from the previous claim but also the current claim.  
		This ensures that the first claim in the series in captured.
**/
with stage_crosswalk as(
  select 
      encounter_id
      ,previous_claim as cur_clm_uniq_id
      ,bene_mbi_id
  from {{ ref('inst_encounter_chain_prep')}}
  union
  select 
      encounter_id
      ,cur_clm_uniq_id as cur_clm_uniq_id
      ,bene_mbi_id
  from {{ ref('inst_encounter_chain_prep')}}
)

/**  Creating a crosswalk from encounter to claims  **/
select 
  max(encounter_id) as encounter_id
  ,bene_mbi_id
  ,cur_clm_uniq_id
from stage_crosswalk
group by
  cur_clm_uniq_id
  ,bene_mbi_id