select
      cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as cur_clm_uniq_id
    , cast(clm_line_num as {{ dbt.type_string() }}) as clm_line_num
    , cast(bene_mbi_id as {{ dbt.type_string() }}) as bene_mbi_id
    , cast(bene_hic_num as {{ dbt.type_string() }}) as bene_hic_num
    , cast(clm_type_cd as {{ dbt.type_string() }}) as clm_type_cd
    , cast(clm_from_dt as {{ dbt.type_string() }}) as clm_from_dt
    , cast(clm_thru_dt as {{ dbt.type_string() }}) as clm_thru_dt
    , cast(clm_fed_type_srvc_cd as {{ dbt.type_string() }}) as clm_fed_type_srvc_cd
    , cast(clm_pos_cd as {{ dbt.type_string() }}) as clm_pos_cd
    , cast(clm_line_from_dt as {{ dbt.type_string() }}) as clm_line_from_dt
    , cast(clm_line_thru_dt as {{ dbt.type_string() }}) as clm_line_thru_dt
    , cast(clm_line_hcpcs_cd as {{ dbt.type_string() }}) as clm_line_hcpcs_cd
    , cast(clm_line_cvrd_pd_amt as {{ dbt.type_string() }}) as clm_line_cvrd_pd_amt
    , cast(clm_prmry_pyr_cd as {{ dbt.type_string() }}) as clm_prmry_pyr_cd
    , cast(payto_prvdr_npi_num as {{ dbt.type_string() }}) as payto_prvdr_npi_num
    , cast(ordrg_prvdr_npi_num as {{ dbt.type_string() }}) as ordrg_prvdr_npi_num
    , cast(clm_carr_pmt_dnl_cd as {{ dbt.type_string() }}) as clm_carr_pmt_dnl_cd
    , cast(clm_prcsg_ind_cd as {{ dbt.type_string() }}) as clm_prcsg_ind_cd
    , cast(clm_adjsmt_type_cd as {{ dbt.type_string() }}) as clm_adjsmt_type_cd
    , cast(clm_efctv_dt as {{ dbt.type_string() }}) as clm_efctv_dt
    , cast(clm_idr_ld_dt as {{ dbt.type_string() }}) as clm_idr_ld_dt
    , cast(clm_cntl_num as {{ dbt.type_string() }}) as clm_cntl_num
    , cast(bene_eqtbl_bic_hicn_num as {{ dbt.type_string() }}) as bene_eqtbl_bic_hicn_num
    , cast(clm_line_alowd_chrg_amt as {{ dbt.type_string() }}) as clm_line_alowd_chrg_amt
    , cast(clm_disp_cd as {{ dbt.type_string() }}) as clm_disp_cd
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
from {{ source('medicare_cclf','partb_dme') }}