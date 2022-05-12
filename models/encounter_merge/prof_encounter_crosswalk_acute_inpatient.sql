    select distinct
        p.cur_clm_uniq_id
        ,p.bene_mbi_id
        ,i.encounter_id
    from {{ source('medicare_cclf','partb_physicians')}} p
    inner join {{ ref('inst_encounter_core')}} i
      on p.bene_mbi_id = i.patient_id
      and p.clm_thru_dt >= i.encounter_start_date
      and p.clm_thru_dt <= i.encounter_end_date
      and p.clm_line_num = '1'
    where 1=1
      and i.encounter_type = 'acute inpatient'
    and p.clm_pos_cd in ('21'    -- Inpatient Hospital
                        ,'23'    -- Emergency Room - Hospital
                        ,'61'    -- Comprehensive Inpatient Rehab
                        ,'51'    -- Inpatient psychiatric facility
                        ,'52'    -- Psychiatric Facility-Partial Hospitalization
                        ,'25'    -- birthing center
                        )
