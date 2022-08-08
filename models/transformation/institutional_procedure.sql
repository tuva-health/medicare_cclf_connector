
select 
		pc.bene_mbi_id
		,pc.cur_clm_uniq_id
		,replace(replace(
			'"procedure": [' || listagg(
		 '{
			"sequence" : '||cast(pc.rw as varchar)||',
			"type" : [
				{
					"coding" : [
						' || case when pc.rw = 1 then '{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimProcedureType",
							"code" : "principal",
							"display" : "Principal"
						}
					],
					"text" : "The Principal Procedure is based on the relation of the procedure to the Principal Diagnosis"
				}' else '{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimProcedureType",
							"code" : "other",
							"display" : "Other"
						}
          ],
          "text" : "Other procedures performed during the inpatient institutional admission"
        }' end || '
			],
			"date" : "'||ifnull(left(cast(pc.clm_prcdr_prfrm_dt as varchar),10),'') ||'",
			"procedureCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://www.cms.gov/Medicare/Coding/ICD10",
						"code" : "'||ifnull(pc.clm_prcdr_cd,'')||'"
					}
				]
			}
		}'
		,',') || '],'
		,chr(9),''),chr(10),'') 
		as prcs
from (
select 
		pcs.bene_mbi_id
		,pcs.cur_clm_uniq_id
		,pcs.clm_prcdr_cd
		,pcs.clm_prcdr_prfrm_dt
		,row_number() over (partition by pcs.bene_mbi_id ,pcs.cur_clm_uniq_id order by clm_val_sqnc_num) as rw
from {{var('parta_procedure_code')}} pcs ) pc
group by 
		pc.bene_mbi_id
		,pc.cur_clm_uniq_id