with procedure_pivot as(
  select 
      *
  from 
      (select 
          cur_clm_uniq_id
          ,bene_mbi_id
          ,clm_prcdr_cd
          ,clm_val_sqnc_num
          ,dgns_prcdr_icd_ind
       from {{ var('parta_procedure_code')}}
       )
  pivot(
      max(clm_prcdr_cd) for clm_val_sqnc_num in (1
                                                , 2
                                                , 3
                                                , 4
                                                , 5
                                                , 6
                                                , 7
                                                , 8
                                                , 9
                                                , 10
                                                , 11
                                                , 12
                                                , 13
                                                , 14
                                                , 15
                                                , 16
                                                , 17
                                                , 18
                                                , 19
                                                , 20
                                                , 21
                                                , 22
                                                , 23
                                                , 24
                                                , 25
                                                )
    )piv (cur_clm_uniq_id, bene_mbi_id, dgns_prcdr_icd_ind, procedure_code_1, procedure_code_2, procedure_code_3, procedure_code_4, procedure_code_5, procedure_code_6
         , procedure_code_7, procedure_code_8, procedure_code_9, procedure_code_10, procedure_code_11, procedure_code_12, procedure_code_13, procedure_code_14, procedure_code_15
         , procedure_code_16, procedure_code_17, procedure_code_18, procedure_code_19, procedure_code_20, procedure_code_21, procedure_code_22, procedure_code_23, procedure_code_24
         , procedure_code_25)
)
, date_pivot as(
  select 
  *
  from 
      (select 
          cur_clm_uniq_id
          ,bene_mbi_id
          ,clm_prcdr_prfrm_dt
          ,clm_val_sqnc_num
       from {{ var('parta_procedure_code')}}
       )
  pivot(
      max(clm_prcdr_prfrm_dt) for clm_val_sqnc_num in (1
                                                      , 2
                                                      , 3
                                                      , 4
                                                      , 5
                                                      , 6
                                                      , 7
                                                      , 8
                                                      , 9
                                                      , 10
                                                      , 11
                                                      , 12
                                                      , 13
                                                      , 14
                                                      , 15
                                                      , 16
                                                      , 17
                                                      , 18
                                                      , 19
                                                      , 20
                                                      , 21
                                                      , 22
                                                      , 23
                                                      , 24
                                                      , 25
                                                      )
    )piv (cur_clm_uniq_id, bene_mbi_id, procedure_date_1, procedure_date_2, procedure_date_3, procedure_date_4, procedure_date_5, procedure_date_6
         , procedure_date_7, procedure_date_8, procedure_date_9, procedure_date_10, procedure_date_11, procedure_date_12, procedure_date_13, procedure_date_14, procedure_date_15
         , procedure_date_16, procedure_date_17, procedure_date_18, procedure_date_19, procedure_date_20, procedure_date_21, procedure_date_22, procedure_date_23, procedure_date_24
         , procedure_date_25)
  )
  
select
  dx.cur_clm_uniq_id
  ,dx.bene_mbi_id
  ,dx.dgns_prcdr_icd_ind
  ,dx.procedure_code_1
  ,dx.procedure_code_2
  ,dx.procedure_code_3
  ,dx.procedure_code_4
  ,dx.procedure_code_5
  ,dx.procedure_code_6
  ,dx.procedure_code_7
  ,dx.procedure_code_8
  ,dx.procedure_code_9
  ,dx.procedure_code_10
  ,dx.procedure_code_11
  ,dx.procedure_code_12
  ,dx.procedure_code_13
  ,dx.procedure_code_14
  ,dx.procedure_code_15
  ,dx.procedure_code_16
  ,dx.procedure_code_17
  ,dx.procedure_code_18
  ,dx.procedure_code_19
  ,dx.procedure_code_20
  ,dx.procedure_code_21
  ,dx.procedure_code_22
  ,dx.procedure_code_23
  ,dx.procedure_code_24
  ,dx.procedure_code_25
  ,d.procedure_date_1
  ,d.procedure_date_2
  ,d.procedure_date_3
  ,d.procedure_date_4
  ,d.procedure_date_5
  ,d.procedure_date_6
  ,d.procedure_date_7
  ,d.procedure_date_8
  ,d.procedure_date_9
  ,d.procedure_date_10
  ,d.procedure_date_11
  ,d.procedure_date_12
  ,d.procedure_date_13
  ,d.procedure_date_14
  ,d.procedure_date_15
  ,d.procedure_date_16
  ,d.procedure_date_17
  ,d.procedure_date_18
  ,d.procedure_date_19
  ,d.procedure_date_20
  ,d.procedure_date_21
  ,d.procedure_date_22
  ,d.procedure_date_23
  ,d.procedure_date_24
  ,d.procedure_date_25
from procedure_pivot dx
inner join date_pivot d
	on dx.cur_clm_uniq_id = d.cur_clm_uniq_id
 