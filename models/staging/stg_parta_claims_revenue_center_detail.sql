select
      cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as cur_clm_uniq_id
    , cast(clm_line_num as {{ dbt.type_string() }}) as clm_line_num
    , cast(bene_mbi_id as {{ dbt.type_string() }}) as bene_mbi_id
    , cast(bene_hic_num as {{ dbt.type_string() }}) as bene_hic_num
    , cast(clm_type_cd as {{ dbt.type_string() }}) as clm_type_cd
    , cast(clm_line_from_dt as {{ dbt.type_string() }}) as clm_line_from_dt
    , cast(clm_line_thru_dt as {{ dbt.type_string() }}) as clm_line_thru_dt
    , cast(clm_line_prod_rev_ctr_cd as {{ dbt.type_string() }}) as clm_line_prod_rev_ctr_cd
    , cast(clm_line_instnl_rev_ctr_dt as {{ dbt.type_string() }}) as clm_line_instnl_rev_ctr_dt
    , cast(clm_line_hcpcs_cd as {{ dbt.type_string() }}) as clm_line_hcpcs_cd
    , cast(bene_eqtbl_bic_hicn_num as {{ dbt.type_string() }}) as bene_eqtbl_bic_hicn_num
    , cast(prvdr_oscar_num as {{ dbt.type_string() }}) as prvdr_oscar_num
    , cast(clm_from_dt as {{ dbt.type_string() }}) as clm_from_dt
    , cast(clm_thru_dt as {{ dbt.type_string() }}) as clm_thru_dt
    , cast(clm_line_srvc_unit_qty as {{ dbt.type_string() }}) as clm_line_srvc_unit_qty
    , cast(clm_line_cvrd_pd_amt as {{ dbt.type_string() }}) as clm_line_cvrd_pd_amt
    , cast(hcpcs_1_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_1_mdfr_cd
    , cast(hcpcs_2_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_2_mdfr_cd
    , cast(hcpcs_3_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_3_mdfr_cd
    , cast(hcpcs_4_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_4_mdfr_cd
    , cast(hcpcs_5_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_5_mdfr_cd
    , cast(clm_rev_apc_hipps_cd as {{ dbt.type_string() }}) as clm_rev_apc_hipps_cd
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
from {{ source('medicare_cclf','parta_claims_revenue_center_detail') }}