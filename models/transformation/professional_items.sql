select 
    cur_clm_uniq_id
    ,bene_mbi_id
    ,listagg(
replace(replace(
'		{
			"sequence" : '||pc.clm_line_num||',
			"productOrService" : {
				"coding" : [
					{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(pc.clm_line_hcpcs_cd,'') ||'"
					}'|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(pc.hcpcs_1_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(pc.hcpcs_2_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(pc.hcpcs_3_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(pc.hcpcs_4_mdfr_cd,'') ||'"
					}','')|| ifnull('
					,{
						"system" : "http://www.ama-assn.org/go/cpt",
						"code" : "'||nullif(pc.hcpcs_5_mdfr_cd,'') ||'"
					}','')
					||'
				]
			},
			"servicedDate" : "'||pc.clm_line_from_dt||'",
			"locationCodeableConcept" : {
				"coding" : [
					{
						"system" : "https://www.cms.gov/Medicare/Coding/place-of-service-codes/Place_of_Service_Code_Set",
						"code" : "'||ifnull(pc.clm_pos_cd,'')||'"
					}
				]
			},
			"adjudication" : [
				{
					"category" : {
						"coding" : [
							{
								"system" : "http://terminology.hl7.org/CodeSystem/adjudication",
								"code" : "submitted",
								"display" : "Submitted Amount"
							}
						],
						"text" : "The total submitted amount for the claim or group or line item."
					},
					"amount" : {
						"value" : "'||ifnull(cast(cast(clm_line_alowd_chrg_amt as double) as varchar),'')||'",
						"currency" : "USD"
					}
				},
				{
					"category" : {
						"coding" : [
							{
								"system" : "http://terminology.hl7.org/CodeSystem/adjudication",
								"code" : "benefit",
								"display" : "Benefit Amount"
							}
						],
						"text" : "Amount payable under the coverage"
					},
					"amount" : {
						"value" : '|| ifnull(cast(cast(clm_line_cvrd_pd_amt as double) as varchar),'') ||',
						"currency" : "USD"
					}
				}
			]
		}'
		,chr(10),''),chr(9),'')
		,',')
    within group (order by cast(pc.clm_line_num as int) ) as itemlist
    


from {{var('partb_physicians')}}  pc
group by pc.cur_clm_uniq_id, pc.bene_mbi_id

