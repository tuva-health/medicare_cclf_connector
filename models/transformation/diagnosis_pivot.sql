with diagnois_pivot as(
  select 
      *
  from 
      (select 
          cur_clm_uniq_id
          ,bene_mbi_id
          ,clm_dgns_cd
          ,clm_val_sqnc_num
          ,dgns_prcdr_icd_ind
       from {{ var('parta_diagnosis_code')}}
       )
  pivot(
      max(clm_dgns_cd) for clm_val_sqnc_num in ('1'
                                              , '2'
                                              , '3'
                                              , '4'
                                              , '5'
                                              , '6'
                                              , '7'
                                              , '8'
                                              , '9'
                                              , '10' 
                                              , '11' 
                                              , '12' 
                                              , '13' 
                                              , '14' 
                                              , '15' 
                                              , '16' 
                                              , '17' 
                                              , '18' 
                                              , '19' 
                                              , '20' 
                                              , '21' 
                                              , '22' 
                                              , '23' 
                                              , '24' 
                                              , '25' 
                                              )
    )piv (cur_clm_uniq_id, bene_mbi_id, dgns_prcdr_icd_ind, diagnosis_code_1, diagnosis_code_2, diagnosis_code_3, diagnosis_code_4, diagnosis_code_5, diagnosis_code_6
         , diagnosis_code_7, diagnosis_code_8, diagnosis_code_9, diagnosis_code_10, diagnosis_code_11, diagnosis_code_12, diagnosis_code_13, diagnosis_code_14, diagnosis_code_15
         , diagnosis_code_16, diagnosis_code_17, diagnosis_code_18, diagnosis_code_19, diagnosis_code_20, diagnosis_code_21, diagnosis_code_22, diagnosis_code_23, diagnosis_code_24
         , diagnosis_code_25)
)
, poa_pivot as(
  select 
  *
  from 
      (select 
          cur_clm_uniq_id
          ,bene_mbi_id
          ,clm_poa_ind
          ,clm_val_sqnc_num
       from {{ var('parta_diagnosis_code')}}
       )
  pivot(
      max(clm_poa_ind) for clm_val_sqnc_num in ('1'
                                              , '2'
                                              , '3'
                                              , '4'
                                              , '5'
                                              , '6'
                                              , '7'
                                              , '8'
                                              , '9'
                                              , '10' 
                                              , '11' 
                                              , '12' 
                                              , '13' 
                                              , '14' 
                                              , '15' 
                                              , '16' 
                                              , '17' 
                                              , '18' 
                                              , '19' 
                                              , '20' 
                                              , '21' 
                                              , '22' 
                                              , '23' 
                                              , '24' 
                                              , '25' 
                                              )
    )piv (cur_clm_uniq_id, bene_mbi_id, diagnosis_poa_1, diagnosis_poa_2, diagnosis_poa_3, diagnosis_poa_4, diagnosis_poa_5, diagnosis_poa_6
         , diagnosis_poa_7, diagnosis_poa_8, diagnosis_poa_9, diagnosis_poa_10, diagnosis_poa_11, diagnosis_poa_12, diagnosis_poa_13, diagnosis_poa_14, diagnosis_poa_15
         , diagnosis_poa_16, diagnosis_poa_17, diagnosis_poa_18, diagnosis_poa_19, diagnosis_poa_20, diagnosis_poa_21, diagnosis_poa_22, diagnosis_poa_23, diagnosis_poa_24
         , diagnosis_poa_25)
  )
  
select
  dx.cur_clm_uniq_id
  ,dx.bene_mbi_id
  ,dx.dgns_prcdr_icd_ind
  ,dx.diagnosis_code_1
  ,dx.diagnosis_code_2
  ,dx.diagnosis_code_3
  ,dx.diagnosis_code_4
  ,dx.diagnosis_code_5
  ,dx.diagnosis_code_6
  ,dx.diagnosis_code_7
  ,dx.diagnosis_code_8
  ,dx.diagnosis_code_9
  ,dx.diagnosis_code_10
  ,dx.diagnosis_code_11
  ,dx.diagnosis_code_12
  ,dx.diagnosis_code_13
  ,dx.diagnosis_code_14
  ,dx.diagnosis_code_15
  ,dx.diagnosis_code_16
  ,dx.diagnosis_code_17
  ,dx.diagnosis_code_18
  ,dx.diagnosis_code_19
  ,dx.diagnosis_code_20
  ,dx.diagnosis_code_21
  ,dx.diagnosis_code_22
  ,dx.diagnosis_code_23
  ,dx.diagnosis_code_24
  ,dx.diagnosis_code_25
  ,poa.diagnosis_poa_1
  ,poa.diagnosis_poa_2
  ,poa.diagnosis_poa_3
  ,poa.diagnosis_poa_4
  ,poa.diagnosis_poa_5
  ,poa.diagnosis_poa_6
  ,poa.diagnosis_poa_7
  ,poa.diagnosis_poa_8
  ,poa.diagnosis_poa_9
  ,poa.diagnosis_poa_10
  ,poa.diagnosis_poa_11
  ,poa.diagnosis_poa_12
  ,poa.diagnosis_poa_13
  ,poa.diagnosis_poa_14
  ,poa.diagnosis_poa_15
  ,poa.diagnosis_poa_16
  ,poa.diagnosis_poa_17
  ,poa.diagnosis_poa_18
  ,poa.diagnosis_poa_19
  ,poa.diagnosis_poa_20
  ,poa.diagnosis_poa_21
  ,poa.diagnosis_poa_22
  ,poa.diagnosis_poa_23
  ,poa.diagnosis_poa_24
  ,poa.diagnosis_poa_25
from diagnois_pivot dx
inner join poa_pivot poa
	on dx.cur_clm_uniq_id = poa.cur_clm_uniq_id