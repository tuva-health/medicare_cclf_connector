{{
    config(materialized='table')
}}

select cur_clm_uniq_id, row_number() over (partition by cur_clm_uniq_id order by clm_line_num) seq
,clm_line_hcpcs_cd, hcpcs_1_mdfr_cd,hcpcs_2_mdfr_cd,hcpcs_3_mdfr_cd,hcpcs_4_mdfr_cd,hcpcs_5_mdfr_cd 
from (
select  cur_clm_uniq_id, min(cast(clm_line_num as int)) clm_line_num
,clm_line_hcpcs_cd, hcpcs_1_mdfr_cd,hcpcs_2_mdfr_cd,hcpcs_3_mdfr_cd,hcpcs_4_mdfr_cd,hcpcs_5_mdfr_cd 
from 
{{var('partb_physicians')}} 
group by cur_clm_uniq_id
,clm_line_hcpcs_cd, hcpcs_1_mdfr_cd,hcpcs_2_mdfr_cd,hcpcs_3_mdfr_cd,hcpcs_4_mdfr_cd,hcpcs_5_mdfr_cd 
) x