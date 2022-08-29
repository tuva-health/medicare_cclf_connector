{{
    config( materialized='table' )
}}

with recursive cte as (
select a.bene_mbi_id,
       a.bene_member_month as strt,
       a.BENE_MEMBER_MONTH as fin,
       a.bene_mdcr_stus_cd,
       a.bene_dual_stus_cd,
       dateadd(month,1,a.bene_member_month) as nxt,
       1 as n
from {{var('beneficiary_demographics')}}  a
left join {{var('beneficiary_demographics')}}  isstart
  on a.bene_mbi_id = isstart.bene_mbi_id
  and a.bene_member_month = dateadd(month,1,isstart.bene_member_month)
  and ifnull(a.bene_mdcr_stus_cd,'') = ifnull(isstart.bene_mdcr_stus_cd,'')
  and ifnull(a.bene_dual_stus_cd,'') = ifnull(isstart.bene_dual_stus_cd,'')
where isstart.BENE_MBI_ID is null

union all

select cte.bene_mbi_id,
       cte.strt,
       cte.nxt as fin,
       cte.bene_mdcr_stus_cd,
       cte.bene_dual_stus_cd,
       dateadd(month,1,nxt.BENE_MEMBER_MONTH) as nxt,
        cte.n+1
from cte
left join {{var('beneficiary_demographics')}}  nxt
  on cte.bene_mbi_id = nxt.bene_mbi_id
  and cte.fin = dateadd(month,-1,nxt.bene_member_month)
  and ifnull(cte.bene_mdcr_stus_cd,'') = ifnull(nxt.bene_mdcr_stus_cd,'')
  and ifnull(cte.bene_dual_stus_cd,'') = ifnull(nxt.bene_dual_stus_cd,'')

where cte.nxt is not null


)
,rd as (select cte.BENE_MBI_ID,
               left(cast(cte.strt as varchar), 10)                  as cov_start,
               left(cast(dateadd(day, -1, cte.fin) as varchar), 10) as cov_end,
               cte.BENE_MDCR_STUS_CD,
               cte.BENE_DUAL_STUS_CD
        From cte
        where cte.nxt is null)




select rd.bene_mbi_id,
rd.cov_start,
replace(replace(
'{
	"resourceType" : "Coverage",
	"id" : "'||rd.bene_mbi_id||'_'||rd.cov_start||'",
	"meta" : {
		"lastUpdated" : "'||rd.cov_end||'T23:59:50.59.000Z",
		"source" : "Organization/Syntegra",
		"profile" : [
			"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-Coverage|1.2.0"
		]
	},
	"language" : "en-US",
	"text" : {
		"status" : "generated",
		"div" : ""
	},
  "extension" : [
    {
      "extension" : [{
        "url" : "https://resdac.org/cms-data/variables/beneficiary-medicare-status-code",
        "valueCode" : "'||ifnull(rd.bene_mdcr_stus_cd,'')||'"
      }],
      "text" : "'||
        case 
          when rd.bene_mdcr_stus_cd = '10' then 'Aged without end-stage renal disease (ESRD)'
          when rd.bene_mdcr_stus_cd = '11' then 'Aged with ESRD'
          when rd.bene_mdcr_stus_cd = '20' then 'Disabled without ESRD'
          when rd.bene_mdcr_stus_cd = '21' then 'Disabled with ESRD'
          when rd.bene_mdcr_stus_cd = '31' then 'ESRD only'
          else  'source value is missing or unknown' end ||
      '"
    },
    {
      "extension" : [{
        "url" : "https://resdac.org/cms-data/variables/medicare-medicaid-dual-eligibility-code-january",
        "valueCode" : "'||right('00'||ifnull(rd.bene_dual_stus_cd,''),2) ||'"
      }],
      "text" : "'||
        case 
          when rd.bene_dual_stus_cd = '0' then 'Eligible is not a Medicare beneficiary'
          when rd.bene_dual_stus_cd = '1' then 'Eligible is entitled to Medicare- Qualified Medicare Beneficiary (QMB) only'
          when rd.bene_dual_stus_cd = '2' then 'Eligible is entitled to Medicare-QMB and Medicaid coverage including prescription drugs'
          when rd.bene_dual_stus_cd = '3' then 'Eligible is entitled to Medicare- Specified Low-Income Medicare Beneficiary (SLMB) only'
          when rd.bene_dual_stus_cd = '4' then 'Eligible is entitled to Medicare-SLMB and Medicaid coverage including prescription drugs'
          when rd.bene_dual_stus_cd = '5' then 'Eligible is entitled to Medicare- Qualified Disabled Working Individual (QDWI)'
          when rd.bene_dual_stus_cd = '6' then 'Eligible is entitled to Medicare-Qualifying Individuals (QI)'
          when rd.bene_dual_stus_cd = '8' then 'Eligible is entitled to Medicare-Other Dual Eligibles (Non QMB, SLMB, QDWI or QI) including prescription drugs'
          when rd.bene_dual_stus_cd = '9' then 'Eligible is entitled to Medicare – but without Medicaid coverage (This code is to be used only with specific CMS approval)'
          when rd.bene_dual_stus_cd = '10' then 'Separate CHIP Eligible is entitled to Medicare'
          else  'source value is missing or unknown' end ||
      '"
    }
  ],
	"identifier" : [
		{
			"type" : {
				"coding" : [
					{
						"system" : "http://terminology.hl7.org/CodeSystem/v2-0203",
						"code" : "MB",
						"display" : "Member Number"
					}
				],
				"text" : "An identifier for the insured of an insurance policy (this insured always has a subscriber), usually assigned by the insurance carrier."
			},
			"system" : "https://www.medicare.gov/",
			"value" : "'||rd.bene_mbi_id||'",
			"assigner" : {
				"reference" : "Organization/Medicare",
				"display" : "Medicare"
			}
		}
	],
	"status" : "active",
	"policyHolder" : {
		"reference" : "Patient/'||rd.bene_mbi_id||'"
	},
	"subscriber" : {
		"reference" : "Patient/'||rd.bene_mbi_id||'"
	},
	"subscriberId" : "'||rd.bene_mbi_id||'",
	"beneficiary" : {
		"reference" : "Patient/'||rd.bene_mbi_id||'"
	},
	"dependent" : "01",
	"relationship" : {
		"coding" : [
			{
				"system" : "http://terminology.hl7.org/CodeSystem/subscriber-relationship",
				"code" : "self"
			}
		],
		"text" : "Self"
	},
	"period" : {
		"start" : "'||rd.cov_start||'",
		"end" : "'||rd.cov_end||'"
	},
	"payor" : [
		{
			"reference" : "Organization/Medicare",
			"display" : "Medicare"
		}
	]'{#,
	"classZZZZZZ???" : [
		{
			"type" : {
				"coding" : [
					{
						"system" : "http://terminology.hl7.org/CodeSystem/coverage-class",
						"code" : "group",
						"display" : "Group"
					}
				],
				"text" : "An employee group"
			},
			"value" : "MCHMO1",
			"name" : "MEDICARE HMO PLAN"
		},
		{
			"type" : {
				"coding" : [
					{
						"system" : "http://terminology.hl7.org/CodeSystem/coverage-class",
						"code" : "plan",
						"display" : "Plan"
					}
				],
				"text" : "A specific suite of benefits."
			},
			"value" : "GR5",
			"name" : "GR5-HMO DEDUCTIBLE"
		}
	],#}{#||'
	"network" : "ZZZGR5-HMO DEDUCTIBLE"
  '#}||'
}'
 ,chr(9),''),chr(10),'') 
fhir
from rd
