select
  h.cur_clm_uniq_id
  ,h.bene_mbi_id
  ,h.clm_from_dt
  ,h.clm_thru_dt
  ,clm_bill_fac_type_cd
  ,clm_bill_clsfctn_cd
  ,prncpl_dgns_cd
  ,admtg_dgns_cd
  ,clm_mdcr_npmt_rsn_cd
  ,clm_pmt_amt
  ,bene_ptnt_stus_cd
  ,dgns_drg_cd
  ,clm_op_srvc_type_cd
  ,fac_prvdr_npi_num
  ,oprtg_prvdr_npi_num
  ,atndg_prvdr_npi_num
  ,othr_prvdr_npi_num
  ,clm_adjsmt_type_cd
  ,clm_admsn_type_cd
  ,clm_admsn_src_cd
  ,clm_bill_freq_cd
  ,clm_query_cd
  ,h.dgns_prcdr_icd_ind
  ,clm_mdcr_instnl_tot_chrg_amt
  ,clm_line_num
  ,clm_line_prod_rev_ctr_cd
  ,clm_line_instnl_rev_ctr_dt
  ,clm_line_hcpcs_cd
  ,clm_line_srvc_unit_qty
  ,clm_line_cvrd_pd_amt
  ,hcpcs_1_mdfr_cd
  ,hcpcs_2_mdfr_cd
  ,hcpcs_3_mdfr_cd
  ,hcpcs_4_mdfr_cd
  ,hcpcs_5_mdfr_cd
  ,clm_prod_type_cd
  ,dx.clm_val_sqnc_num as clm_dgns_val_sqnc_num
  ,clm_dgns_cd
  ,clm_poa_ind
  ,proc.clm_val_sqnc_num as clm_prcdr_val_sqnc_num
  ,clm_prcdr_cd
  ,clm_prcdr_prfrm_dt
from cclf.parta_claims_header h
inner join cclf.parta_claims_revenue_center_detail d
	on h.cur_clm_uniq_id = d.cur_clm_uniq_id
left join cclf.parta_diagnosis_code dx
	on h.cur_clm_uniq_id = dx.cur_clm_uniq_id
left join cclf.parta_procedure_code proc
	on h.cur_clm_uniq_id = proc.cur_clm_uniq_id