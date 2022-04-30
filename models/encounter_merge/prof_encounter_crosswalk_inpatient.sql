select distinct
      p.cur_clm_uniq_id
      ,p.bene_mbi_id
      ,i.encounter_id
from {{ source('medicare_cclf','partb_physicians')}} p
inner join {{ ref('inst_claims_final')}} i
  on p.bene_mbi_id = i.bene_mbi_id
  and p.clm_thru_dt >= i.clm_from_dt
  and p.clm_thru_dt <= i.clm_thru_dt
  and (p.clm_dgns_1_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_2_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_3_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_4_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_5_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_6_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_7_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_8_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_9_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_10_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_11_cd = i.prncpl_dgns_cd
    	or p.clm_dgns_12_cd = i.prncpl_dgns_cd)
where 1=1
  and i.clm_type_cd = 60
  and p.clm_pos_cd in ('21'    -- Inpatient Hospital
                      ,'23'    -- Emergency Room - Hospital
                      ,'61'    -- Comprehensive Inpatient Rehab
                      ,'51'    -- Inpatient psychiatric facility
                      ,'52'    -- Psychiatric Facility-Partial Hospitalization
                      ,'25'    -- birthing center
                      )

