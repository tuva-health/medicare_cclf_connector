{{
    config( materialized='ephemeral' )
}}

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
      max(clm_prcdr_cd) for clm_val_sqnc_num in (1 as procedure_code_1
                                                , 2 as procedure_code_2
                                                , 3 as procedure_code_3
                                                , 4 as procedure_code_4
                                                , 5 as procedure_code_5
                                                , 6 as procedure_code_6
                                                , 7 as procedure_code_7
                                                , 8 as procedure_code_8
                                                , 9 as procedure_code_9
                                                , 10 as procedure_code_10
                                                , 11 as procedure_code_11
                                                , 12 as procedure_code_12
                                                , 13 as procedure_code_13
                                                , 14 as procedure_code_14
                                                , 15 as procedure_code_15
                                                , 16 as procedure_code_16
                                                , 17 as procedure_code_17
                                                , 18 as procedure_code_18
                                                , 19 as procedure_code_19
                                                , 20 as procedure_code_20
                                                , 21 as procedure_code_21
                                                , 22 as procedure_code_22
                                                , 23 as procedure_code_23
                                                , 24 as procedure_code_24
                                                , 25 as procedure_code_25
                                                )
    )piv
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
      max(clm_prcdr_prfrm_dt) for clm_val_sqnc_num in (1 as procedure_date_1
                                                      , 2 as procedure_date_2
                                                      , 3 as procedure_date_3
                                                      , 4 as procedure_date_4
                                                      , 5 as procedure_date_5
                                                      , 6 as procedure_date_6
                                                      , 7 as procedure_date_7
                                                      , 8 as procedure_date_8
                                                      , 9 as procedure_date_9
                                                      , 10 as procedure_date_10
                                                      , 11 as procedure_date_11
                                                      , 12 as procedure_date_12
                                                      , 13 as procedure_date_13
                                                      , 14 as procedure_date_14
                                                      , 15 as procedure_date_15
                                                      , 16 as procedure_date_16
                                                      , 17 as procedure_date_17
                                                      , 18 as procedure_date_18
                                                      , 19 as procedure_date_19
                                                      , 20 as procedure_date_20
                                                      , 21 as procedure_date_21
                                                      , 22 as procedure_date_22
                                                      , 23 as procedure_date_23
                                                      , 24 as procedure_date_24
                                                      , 25 as procedure_date_25
                                                      )
            )piv
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
 