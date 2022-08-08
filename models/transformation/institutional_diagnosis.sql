with diags as (
    select
        dc.bene_mbi_id
        ,dc.cur_clm_uniq_id
        ,dc.clm_val_sqnc_num
        ,dc.clm_dgns_cd
        ,dc.clm_poa_ind
        ,case 
            when pch.cur_clm_uniq_id is not null then 'P'
            when ach.cur_clm_uniq_id is not null then 'A'
            else 'O' end as typ
    from {{var('parta_diagnosis_code')}} dc
    left join {{var('parta_claims_header')}} pch 
        on dc.bene_mbi_id = pch.bene_mbi_id and dc.cur_clm_uniq_id = pch.cur_clm_uniq_id and dc.clm_dgns_cd = pch.prncpl_dgns_cd
    left join {{var('parta_claims_header')}} ach 
        on dc.bene_mbi_id = ach.bene_mbi_id and dc.cur_clm_uniq_id = ach.cur_clm_uniq_id and dc.clm_dgns_cd = ach.admtg_dgns_cd

    union all 

    select
        pch.bene_mbi_id
        ,pch.cur_clm_uniq_id
        ,-1 as clm_val_sqnc_num
        ,pch.prncpl_dgns_cd
        ,'1'
        ,'P' as typ
    from {{var('parta_claims_header')}} pch 
    left join  {{var('parta_diagnosis_code')}} dc
        on dc.bene_mbi_id = pch.bene_mbi_id and dc.cur_clm_uniq_id = pch.cur_clm_uniq_id and dc.clm_dgns_cd = pch.prncpl_dgns_cd
    where dc.cur_clm_uniq_id is null   

    union all 

    select
        ach.bene_mbi_id
        ,ach.cur_clm_uniq_id
        ,0 as clm_val_sqnc_num
        ,ach.admtg_dgns_cd
        ,'1'
        ,'A' as typ
    from {{var('parta_claims_header')}} ach 
    left join {{var('parta_diagnosis_code')}} dc
        on dc.bene_mbi_id = ach.bene_mbi_id and dc.cur_clm_uniq_id = ach.cur_clm_uniq_id and dc.clm_dgns_cd = ach.admtg_dgns_cd
    where dc.cur_clm_uniq_id is null and ach.admtg_dgns_cd is not null
)

, rediags as (
select *, row_number() over (partition by bene_mbi_id,cur_clm_uniq_id order by clm_val_sqnc_num) as rw
from diags 
)

select 
    rediags.bene_mbi_id
    ,rediags.cur_clm_uniq_id
, replace(replace( '"diagnosis": ['||listagg(
' 		{
			"sequence" : '||cast(rediags.rw as varchar)||',
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||rediags.clm_dgns_cd||'"
					}
				]
			},
			"onAdmission" : {
				"coding" : [
					{
						"system" : "https://www.nubc.org/CodeSystem/PresentOnAdmissionIndicator",
						"code" : "'||ifnull(rediags.clm_poa_ind,'')||'"
					}
				]
			},
			"type" : ['|| case when rediags.typ = 'P' then '
				{
					"coding" : [
						{
							"system" : "http://terminology.hl7.org/CodeSystem/ex-diagnosistype",
							"code" : "principal",
							"display" : "Principal Diagnosis"
						}
					],
					"text" : "The single medical diagnosis that is most relevant to the patient''s chief complaint or need for treatment."
				}'
                 when rediags.typ = 'A' then '
				{
					"coding" : [
						{
							"system" : "http://terminology.hl7.org/CodeSystem/ex-diagnosistype",
							"code" : "admitting",
							"display" : "Admitting Diagnosis"
						}
					],
					"text" : "The diagnosis given as the reason why the patient was admitted to the hospital.."
				}' else '{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "other",
							"display" : "Other"
						}
					],
					"text" : "Required when other conditions coexist or develop subsequently during the treatment"
				}' end ||
                '
			]
		}',',') within group (order by rediags.rw) || '], ' ,chr(9),''),chr(10),'') as dgs
from rediags
group by 
    rediags.bene_mbi_id
    ,rediags.cur_clm_uniq_id