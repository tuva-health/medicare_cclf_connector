select 
    cld.bene_mbi_id
    ,cld.cur_clm_uniq_id
    ,replace(replace(
		 listagg(
		',{
			"sequence" : '||cast(cld.clm_line_num as varchar)||',
			"revenue" : {
				"coding" : [
					{
						"system" : "https://www.nubc.org/CodeSystem/RevenueCodes",
						"code" : "'||cld.clm_line_prod_rev_ctr_cd||'"
					}
				]
			},
			' || ifnull('"productOrService" : {
				"coding" : [
					{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(cld.clm_line_hcpcs_cd,'') ||'"
					}'|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(cld.hcpcs_1_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(cld.hcpcs_2_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(cld.hcpcs_3_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(cld.hcpcs_4_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(cld.hcpcs_5_mdfr_cd,'') ||'"
					}','')
					||'
				]
			},','') || {# ifnull('
            "modifier":[
                
            ],','') || #} '
			"servicedPeriod" : {
				"start" : "'||ifnull(left(cast(cld.clm_line_from_dt as varchar),10),'')||'",
				"end" : "'||ifnull(left(cast(cld.clm_line_thru_dt as varchar),10),'')||'"
			}' || {#',
			"locationCodeableConcept" : {
				"coding" : [
					{
						"system" : "https://www.cms.gov/Medicare/Coding/place-of-service-codes/Place_of_Service_Code_Set",
						"code" : "21"
					}
				],
				"text" : "HOSPITAL - INPATIENT HOSPITAL"
			}' #}'
		}' ,'')  within group (order by cld.clm_line_num) 
		,chr(9),''),chr(10),'') 
		as itms

from {{var('parta_claims_revenue_center_detail')}} cld 
where cld.clm_line_num > 100
group by 
    cld.bene_mbi_id
    ,cld.cur_clm_uniq_id