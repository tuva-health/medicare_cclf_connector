{% docs cclf_bene_mbi_id %}
Medicare Beneficiary Identifier assigned to a beneficiary.
{% enddocs %}

{% docs cclf_bene_hic_num %}
Legacy beneficiary HIC number field. This field is blank in CCLFs generated effective January 1, 2020 onward.
{% enddocs %}

{% docs cclf_bene_fips_state_cd %}
FIPS state code identifying the state where the beneficiary resides.
{% enddocs %}

{% docs cclf_bene_fips_cnty_cd %}
FIPS county code identifying the county where the beneficiary resides.
{% enddocs %}

{% docs cclf_bene_zip_cd %}
Beneficiary ZIP code from the Medicare enrollment record.
{% enddocs %}

{% docs cclf_bene_dob %}
Beneficiary date of birth.
{% enddocs %}

{% docs cclf_bene_sex_cd %}
Beneficiary sex code.
{% enddocs %}

{% docs cclf_bene_race_cd %}
Beneficiary race code.
{% enddocs %}

{% docs cclf_bene_age %}
Beneficiary age as of the current date.
{% enddocs %}

{% docs cclf_bene_mdcr_stus_cd %}
Beneficiary Medicare status code indicating the reason for Medicare entitlement.
{% enddocs %}

{% docs cclf_bene_dual_stus_cd %}
Beneficiary dual status code indicating eligibility for Medicare plus another program such as Medicaid.
{% enddocs %}

{% docs cclf_bene_death_dt %}
Beneficiary date of death.
{% enddocs %}

{% docs cclf_bene_rng_bgn_dt %}
Date the beneficiary enrolled in hospice.
{% enddocs %}

{% docs cclf_bene_rng_end_dt %}
Date the beneficiary ended hospice enrollment.
{% enddocs %}

{% docs cclf_bene_1st_name %}
Beneficiary first name.
{% enddocs %}

{% docs cclf_bene_midl_name %}
Beneficiary middle name.
{% enddocs %}

{% docs cclf_bene_last_name %}
Beneficiary last name.
{% enddocs %}

{% docs cclf_bene_orgnl_entlmt_rsn_cd %}
Beneficiary original entitlement reason code.
{% enddocs %}

{% docs cclf_bene_entlmt_buyin_ind %}
Indicator showing Medicare Part A and Part B entitlement and whether the state paid the Part B premium.
{% enddocs %}

{% docs cclf_bene_part_a_enrlmt_bgn_dt %}
Date the beneficiary became entitled to Medicare Part A benefits.
{% enddocs %}

{% docs cclf_bene_part_b_enrlmt_bgn_dt %}
Date the beneficiary became entitled to Medicare Part B benefits.
{% enddocs %}

{% docs cclf_bene_line_1_adr %}
First line of the beneficiary mailing address.
{% enddocs %}

{% docs cclf_bene_line_2_adr %}
Second line of the beneficiary mailing address.
{% enddocs %}

{% docs cclf_bene_line_3_adr %}
Third line of the beneficiary mailing address.
{% enddocs %}

{% docs cclf_bene_line_4_adr %}
Fourth line of the beneficiary mailing address.
{% enddocs %}

{% docs cclf_bene_line_5_adr %}
Fifth line of the beneficiary mailing address.
{% enddocs %}

{% docs cclf_bene_line_6_adr %}
Sixth line of the beneficiary mailing address.
{% enddocs %}

{% docs cclf_geo_zip_plc_name %}
Beneficiary city name.
{% enddocs %}

{% docs cclf_geo_usps_state_cd %}
USPS state code for the beneficiary address.
{% enddocs %}

{% docs cclf_geo_zip5_cd %}
Five-digit ZIP code for the beneficiary address geography.
{% enddocs %}

{% docs cclf_geo_zip4_cd %}
Four-digit ZIP code extension for the beneficiary address geography.
{% enddocs %}

{% docs cclf_file_name %}
Source file name for the loaded CCLF or enrollment extract.
{% enddocs %}

{% docs cclf_file_date %}
Date associated with the loaded file, typically parsed from the CCLF filename and used for deduplication.
{% enddocs %}

{% docs cclf_hicn_mbi_xref_ind %}
Indicator showing the type of cross-reference identifier in the beneficiary XREF file. For current files this is M for MBI.
{% enddocs %}

{% docs cclf_crnt_num %}
Current beneficiary identifier in the cross-reference file.
{% enddocs %}

{% docs cclf_prvs_num %}
Previous beneficiary identifier in the cross-reference file.
{% enddocs %}

{% docs cclf_prvs_id_efctv_dt %}
Date the previous beneficiary identifier became active.
{% enddocs %}

{% docs cclf_prvs_id_obslt_dt %}
Date the previous beneficiary identifier became obsolete.
{% enddocs %}

{% docs cclf_bene_rrb_num %}
Legacy beneficiary Railroad Board number. This field is blank in current CCLFs.
{% enddocs %}

{% docs cclf_current_bene_mbi_id %}
Current beneficiary MBI supplied by the supplemental enrollment source.
{% enddocs %}

{% docs cclf_enrollment_start_date %}
Enrollment span start date supplied by the supplemental enrollment source.
{% enddocs %}

{% docs cclf_enrollment_end_date %}
Enrollment span end date supplied by the supplemental enrollment source.
{% enddocs %}

{% docs cclf_bene_member_month %}
Member month date used when enrollment is provided as monthly records instead of spans.
{% enddocs %}

{% docs cclf_cur_clm_uniq_id %}
Unique identification number assigned to the claim.
{% enddocs %}

{% docs cclf_prvdr_oscar_num %}
Provider OSCAR number, also known as the Medicare provider number or CCN.
{% enddocs %}

{% docs cclf_clm_type_cd %}
Claim type code identifying the type of claim submitted through Medicare or Medicaid processing.
{% enddocs %}

{% docs cclf_clm_from_dt %}
First day on the billing statement that covers services rendered to the beneficiary.
{% enddocs %}

{% docs cclf_clm_thru_dt %}
Last day on the billing statement that covers services rendered to the beneficiary.
{% enddocs %}

{% docs cclf_clm_bill_fac_type_cd %}
First digit of the type of bill indicating the facility type that provided care.
{% enddocs %}

{% docs cclf_clm_bill_clsfctn_cd %}
Second digit of the type of bill indicating the service classification or location within the facility.
{% enddocs %}

{% docs cclf_prncpl_dgns_cd %}
Principal diagnosis code for the claim.
{% enddocs %}

{% docs cclf_admtg_dgns_cd %}
Admitting diagnosis code for the claim.
{% enddocs %}

{% docs cclf_clm_mdcr_npmt_rsn_cd %}
Medicare non-payment reason code for an institutional claim.
{% enddocs %}

{% docs cclf_clm_pmt_amt %}
Amount Medicare paid on the claim.
{% enddocs %}

{% docs cclf_clm_nch_prmry_pyr_cd %}
Primary payer code showing whether a payer other than Medicare had primary responsibility.
{% enddocs %}

{% docs cclf_prvdr_fac_fips_st_cd %}
FIPS state code for the facility that provided services.
{% enddocs %}

{% docs cclf_bene_ptnt_stus_cd %}
Patient discharge status code as of the claim through date.
{% enddocs %}

{% docs cclf_dgns_drg_cd %}
Diagnosis related group code for the claim.
{% enddocs %}

{% docs cclf_clm_op_srvc_type_cd %}
Outpatient service type code reported by the provider.
{% enddocs %}

{% docs cclf_fac_prvdr_npi_num %}
Facility provider NPI number associated with the claim.
{% enddocs %}

{% docs cclf_oprtg_prvdr_npi_num %}
Operating provider NPI number associated with the claim.
{% enddocs %}

{% docs cclf_atndg_prvdr_npi_num %}
Attending provider NPI number associated with the claim.
{% enddocs %}

{% docs cclf_othr_prvdr_npi_num %}
Other provider NPI number associated with the claim.
{% enddocs %}

{% docs cclf_clm_adjsmt_type_cd %}
Claim adjustment type code identifying whether the record is an original, cancellation, or adjustment claim.
{% enddocs %}

{% docs cclf_clm_efctv_dt %}
Date the claim was processed and added to the NCH.
{% enddocs %}

{% docs cclf_clm_idr_ld_dt %}
Date the claim was loaded into the IDR.
{% enddocs %}

{% docs cclf_bene_eqtbl_bic_hicn_num %}
Legacy beneficiary equitable BIC HICN number. This field is blank in current CCLFs.
{% enddocs %}

{% docs cclf_clm_admsn_type_cd %}
Admission type code indicating the type and priority of inpatient services.
{% enddocs %}

{% docs cclf_clm_admsn_src_cd %}
Admission source code indicating the referral source for the admission or visit.
{% enddocs %}

{% docs cclf_clm_bill_freq_cd %}
Third digit of the type of bill indicating the claim sequence in the episode of care.
{% enddocs %}

{% docs cclf_clm_query_cd %}
Claim query code indicating the type of payment record being processed, such as debit, credit, interim, or final.
{% enddocs %}

{% docs cclf_dgns_prcdr_icd_ind %}
ICD version indicator showing whether diagnosis or procedure codes derive from ICD-9, ICD-10, or an unknown source value.
{% enddocs %}

{% docs cclf_clm_mdcr_instnl_tot_chrg_amt %}
Total institutional claim charge amount.
{% enddocs %}

{% docs cclf_clm_mdcr_ip_pps_cptl_ime_amt %}
Capital indirect medical education amount for inpatient PPS claims.
{% enddocs %}

{% docs cclf_clm_oprtnl_ime_amt %}
Operational indirect medical education amount for the claim.
{% enddocs %}

{% docs cclf_clm_mdcr_ip_pps_dsprprtnt_amt %}
Capital disproportionate share amount for the claim.
{% enddocs %}

{% docs cclf_clm_hipps_uncompd_care_amt %}
Health Insurance Prospective Payment System uncompensated care amount.
{% enddocs %}

{% docs cclf_clm_oprtnl_dsprprtnt_amt %}
Operational disproportionate share amount for the claim.
{% enddocs %}

{% docs cclf_clm_blg_prvdr_oscar_num %}
Billing provider OSCAR number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_clm_blg_prvdr_npi_num %}
Billing provider NPI number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_clm_oprtg_prvdr_npi_num %}
Claim operating provider NPI number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_clm_atndg_prvdr_npi_num %}
Claim attending provider NPI number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_clm_othr_prvdr_npi_num %}
Claim other provider NPI number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_clm_cntl_num %}
Claim control number assigned by the Medicare claim processor or carrier.
{% enddocs %}

{% docs cclf_clm_org_cntl_num %}
Original claim control number that identifies the original claim in the payment system.
{% enddocs %}

{% docs cclf_clm_cntrctr_num %}
Contractor number assigned by CMS to identify the MAC that processed the claim.
{% enddocs %}

{% docs cclf_clm_line_num %}
Sequential number identifying a specific claim line within a claim.
{% enddocs %}

{% docs cclf_clm_line_from_dt %}
Date the service associated with the claim line began.
{% enddocs %}

{% docs cclf_clm_line_thru_dt %}
Date the service associated with the claim line ended.
{% enddocs %}

{% docs cclf_clm_line_prod_rev_ctr_cd %}
Product revenue center code assigned by the provider cost center.
{% enddocs %}

{% docs cclf_clm_line_instnl_rev_ctr_dt %}
Date associated with the institutional revenue center service.
{% enddocs %}

{% docs cclf_clm_line_hcpcs_cd %}
HCPCS code representing the procedure, supply, product, or service on the claim line.
{% enddocs %}

{% docs cclf_clm_line_srvc_unit_qty %}
Claim line service unit quantity.
{% enddocs %}

{% docs cclf_clm_line_cvrd_pd_amt %}
Amount Medicare paid for the covered service on the claim line.
{% enddocs %}

{% docs cclf_hcpcs_1_mdfr_cd %}
First HCPCS modifier code for the claim line.
{% enddocs %}

{% docs cclf_hcpcs_2_mdfr_cd %}
Second HCPCS modifier code for the claim line.
{% enddocs %}

{% docs cclf_hcpcs_3_mdfr_cd %}
Third HCPCS modifier code for the claim line.
{% enddocs %}

{% docs cclf_hcpcs_4_mdfr_cd %}
Fourth HCPCS modifier code for the claim line.
{% enddocs %}

{% docs cclf_hcpcs_5_mdfr_cd %}
Fifth HCPCS modifier code for the claim line.
{% enddocs %}

{% docs cclf_clm_rev_apc_hipps_cd %}
Revenue APC or HIPPS code for the claim line.
{% enddocs %}

{% docs cclf_clm_fac_prvdr_oscar_num %}
Facility provider OSCAR number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_clm_prod_type_cd %}
Claim product type code classifying the diagnosis category in the Part A diagnosis file.
{% enddocs %}

{% docs cclf_clm_val_sqnc_num %}
Arbitrary sequence number that uniquely identifies a diagnosis or procedure record within the claim.
{% enddocs %}

{% docs cclf_clm_dgns_cd %}
Diagnosis code identifying the beneficiary illness or disability.
{% enddocs %}

{% docs cclf_clm_poa_ind %}
Present-on-admission indicator for the diagnosis on the claim line.
{% enddocs %}

{% docs cclf_clm_prcdr_cd %}
Procedure code indicating the procedure performed during the claim period.
{% enddocs %}

{% docs cclf_clm_prcdr_prfrm_dt %}
Date the indicated procedure was performed.
{% enddocs %}

{% docs cclf_clm_fed_type_srvc_cd %}
Federal type of service code for the claim line.
{% enddocs %}

{% docs cclf_clm_pos_cd %}
Place of service code indicating where the service was provided.
{% enddocs %}

{% docs cclf_clm_prmry_pyr_cd %}
Primary payer code for the claim line in the DME file.
{% enddocs %}

{% docs cclf_payto_prvdr_npi_num %}
Pay-to provider NPI number identifying the billing provider for the claim line.
{% enddocs %}

{% docs cclf_ordrg_prvdr_npi_num %}
Ordering provider NPI number for the claim line.
{% enddocs %}

{% docs cclf_clm_carr_pmt_dnl_cd %}
Carrier payment denial code indicating who was paid or whether the claim was denied.
{% enddocs %}

{% docs cclf_clm_prcsg_ind_cd %}
Claim processing indicator code showing whether the service was allowed or why it was denied.
{% enddocs %}

{% docs cclf_clm_line_alowd_chrg_amt %}
Allowed charge amount for the claim line.
{% enddocs %}

{% docs cclf_clm_disp_cd %}
Claim disposition code describing the payment action taken on the claim.
{% enddocs %}

{% docs cclf_clm_rfrg_prvdr_npi_num %}
Referring or ordering provider NPI number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_rndrg_prvdr_type_cd %}
Rendering provider type code for the service on the claim line.
{% enddocs %}

{% docs cclf_rndrg_prvdr_fips_st_cd %}
FIPS state code for the rendering provider location.
{% enddocs %}

{% docs cclf_clm_prvdr_spclty_cd %}
Provider specialty code used in pricing the claim line service.
{% enddocs %}

{% docs cclf_clm_line_prmry_pyr_cd %}
Line-level primary payer code for the physician claim line.
{% enddocs %}

{% docs cclf_clm_line_dgns_cd %}
Principal diagnosis code reported on the physician claim line.
{% enddocs %}

{% docs cclf_clm_rndrg_prvdr_tax_num %}
Rendering provider tax number for the claim line.
{% enddocs %}

{% docs cclf_rndrg_prvdr_npi_num %}
Rendering provider NPI number for the claim line.
{% enddocs %}

{% docs cclf_clm_dgns_1_cd %}
First diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_2_cd %}
Second diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_3_cd %}
Third diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_4_cd %}
Fourth diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_5_cd %}
Fifth diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_6_cd %}
Sixth diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_7_cd %}
Seventh diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_8_cd %}
Eighth diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_9_cd %}
Ninth diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_10_cd %}
Tenth diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_11_cd %}
Eleventh diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_clm_dgns_12_cd %}
Twelfth diagnosis code reported on the physician claim.
{% enddocs %}

{% docs cclf_hcpcs_betos_cd %}
BETOS code representing the clinical category of the HCPCS service.
{% enddocs %}

{% docs cclf_clm_rndrg_prvdr_npi_num %}
Rendering provider NPI number sourced from the Medicare claims processing system.
{% enddocs %}

{% docs cclf_clm_line_ndc_cd %}
National Drug Code for the Part D claim line.
{% enddocs %}

{% docs cclf_prvdr_srvc_id_qlfyr_cd %}
Qualifier code showing the type of identifier used for the dispensing pharmacy or service provider.
{% enddocs %}

{% docs cclf_clm_srvc_prvdr_gnrc_id_num %}
Generic provider identifier associated with the service provider qualifier code.
{% enddocs %}

{% docs cclf_clm_dspnsng_stus_cd %}
Dispensing status code indicating whether the prescription was partially or completely filled.
{% enddocs %}

{% docs cclf_clm_daw_prod_slctn_cd %}
Dispense-as-written product selection code for the prescription claim.
{% enddocs %}

{% docs cclf_clm_line_days_suply_qty %}
Number of days the dispensed medication supply should cover.
{% enddocs %}

{% docs cclf_prvdr_prsbng_id_qlfyr_cd %}
Qualifier code showing the type of identifier used for the prescribing provider.
{% enddocs %}

{% docs cclf_clm_prsbng_prvdr_gnrc_id_num %}
Generic prescribing provider identifier associated with the prescribing qualifier code.
{% enddocs %}

{% docs cclf_clm_line_bene_pmt_amt %}
Amount paid by the beneficiary for the Part D claim line that is not reimbursed by a third party.
{% enddocs %}

{% docs cclf_clm_line_rx_srvc_rfrnc_num %}
Prescription service reference number identifying a prescription dispensed by a service provider on a service date.
{% enddocs %}

{% docs cclf_clm_line_rx_fill_num %}
Sequential fill number for the original prescription or refill.
{% enddocs %}

{% docs cclf_clm_phrmcy_srvc_type_cd %}
Pharmacy service type code for the Part D claim line.
{% enddocs %}

{% docs cclf_clm_actv_care_from_dt %}
Date the active care period began on the Part A claim.
{% enddocs %}

{% docs cclf_clm_ngaco_pbpmt_sw %}
Switch indicating the claim was processed under the Population-Based Payment (PBP) or All-Inclusive Population-Based Payment (AIPBP) benefit enhancement.
{% enddocs %}

{% docs cclf_clm_ngaco_pdschrg_hcbs_sw %}
Switch indicating the claim was processed under the Post-Discharge Home Visit benefit enhancement.
{% enddocs %}

{% docs cclf_clm_ngaco_snf_wvr_sw %}
Switch indicating the claim was processed under the SNF 3-Day Rule Waiver benefit enhancement.
{% enddocs %}

{% docs cclf_clm_ngaco_tlhlth_sw %}
Switch indicating the claim was processed under the Telehealth Expansion benefit enhancement.
{% enddocs %}

{% docs cclf_clm_ngaco_cptatn_sw %}
Switch indicating the claim was processed under a capitation benefit enhancement.
{% enddocs %}

{% docs cclf_clm_demo_1st_num %}
First Medicare demonstration special processing number associated with the claim.
{% enddocs %}

{% docs cclf_clm_demo_2nd_num %}
Second Medicare demonstration special processing number associated with the claim.
{% enddocs %}

{% docs cclf_clm_demo_3rd_num %}
Third Medicare demonstration special processing number associated with the claim.
{% enddocs %}

{% docs cclf_clm_demo_4th_num %}
Fourth Medicare demonstration special processing number associated with the claim.
{% enddocs %}

{% docs cclf_clm_demo_5th_num %}
Fifth Medicare demonstration special processing number associated with the claim.
{% enddocs %}

{% docs cclf_clm_pbp_inclsn_amt %}
PBP/AIPBP inclusion amount. The portion of the claim payment that is included in the Population-Based Payment calculation.
{% enddocs %}

{% docs cclf_clm_pbp_rdctn_amt %}
PBP/AIPBP reduction amount. The amount by which the Medicare fee-for-service payment on the claim (Part A) or claim line (Part B) was reduced because the ACO is paid prospectively under Population-Based Payment. Mapped to paid_reduced_by in the medical_claim model.
{% enddocs %}

{% docs cclf_clm_ngaco_cmg_wvr_sw %}
Switch indicating the claim was processed under the Care Management Home Visit waiver benefit enhancement.
{% enddocs %}

{% docs cclf_clm_instnl_per_diem_amt %}
Institutional per diem amount on the claim.
{% enddocs %}

{% docs cclf_clm_mdcr_ip_bene_ddctbl_amt %}
Medicare inpatient beneficiary deductible amount applied to the claim.
{% enddocs %}

{% docs cclf_clm_mdcr_coinsrnc_amt %}
Medicare coinsurance amount applied to the claim.
{% enddocs %}

{% docs cclf_clm_blood_lblty_amt %}
Beneficiary blood deductible liability amount on the claim.
{% enddocs %}

{% docs cclf_clm_instnl_prfnl_amt %}
Institutional professional component amount on the claim.
{% enddocs %}

{% docs cclf_clm_ncvrd_chrg_amt %}
Non-covered charge amount on the claim.
{% enddocs %}

{% docs cclf_clm_mdcr_ddctbl_amt %}
Medicare deductible amount applied to the claim (Part A) or claim line (Part B).
{% enddocs %}

{% docs cclf_clm_rlt_cond_cd %}
Claim related condition code. A Part A claim can appear on multiple rows of the CCLFA file, one per condition code.
{% enddocs %}

{% docs cclf_clm_oprtnl_outlr_amt %}
Operating outlier payment amount on the claim.
{% enddocs %}

{% docs cclf_clm_mdcr_new_tech_amt %}
Medicare new technology add-on payment amount on the claim.
{% enddocs %}

{% docs cclf_clm_islet_isoln_amt %}
Islet isolation add-on payment amount on the claim.
{% enddocs %}

{% docs cclf_clm_sqstrtn_rdctn_amt %}
Sequestration reduction amount applied to the claim payment.
{% enddocs %}

{% docs cclf_clm_1_rev_cntr_ansi_rsn_cd %}
ANSI claim adjustment reason code for the first revenue center on the claim.
{% enddocs %}

{% docs cclf_clm_1_rev_cntr_ansi_grp_cd %}
ANSI claim adjustment group code for the first revenue center on the claim.
{% enddocs %}

{% docs cclf_clm_mips_pmt_amt %}
Merit-based Incentive Payment System (MIPS) payment adjustment amount on the claim.
{% enddocs %}

{% docs cclf_clm_line_ngaco_pbpmt_sw %}
Switch indicating the claim line was processed under the Population-Based Payment (PBP) or All-Inclusive Population-Based Payment (AIPBP) benefit enhancement.
{% enddocs %}

{% docs cclf_clm_line_ngaco_pdschrg_hcbs_sw %}
Switch indicating the claim line was processed under the Post-Discharge Home Visit benefit enhancement.
{% enddocs %}

{% docs cclf_clm_line_ngaco_snf_wvr_sw %}
Switch indicating the claim line was processed under the SNF 3-Day Rule Waiver benefit enhancement.
{% enddocs %}

{% docs cclf_clm_line_ngaco_tlhlth_sw %}
Switch indicating the claim line was processed under the Telehealth Expansion benefit enhancement.
{% enddocs %}

{% docs cclf_clm_line_ngaco_cptatn_sw %}
Switch indicating the claim line was processed under a capitation benefit enhancement.
{% enddocs %}

{% docs cclf_clm_line_carr_hpsa_scrcty_cd %}
Carrier Health Professional Shortage Area (HPSA) or physician scarcity bonus code for the claim line.
{% enddocs %}
