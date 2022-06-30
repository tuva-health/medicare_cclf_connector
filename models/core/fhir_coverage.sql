{{
    config( materialized='table' )
}}

with cte as (
select a.bene_mbi_id,a.bene_member_month, a.bene_mdcr_stus_cd,a.bene_dual_stus_cd,start.bene_mbi_id st,fin.bene_mbi_id fn
from {{var('beneficiary_demographics')}}  a
left join {{var('beneficiary_demographics')}} start 
  on a.bene_mbi_id = start.bene_mbi_id 
  and a.bene_member_month = dateadd(month,1,start.bene_member_month)
  and a.bene_mdcr_stus_cd = start.bene_mdcr_stus_cd
  and a.bene_dual_stus_cd = start.bene_dual_stus_cd
left join {{var('beneficiary_demographics')}} fin 
  on a.bene_mbi_id = fin.bene_mbi_id 
  and a.bene_member_month = dateadd(month,-1,fin.bene_member_month)
  and a.bene_mdcr_stus_cd = fin.bene_mdcr_stus_cd
  and a.bene_dual_stus_cd = fin.bene_dual_stus_cd
)
, rd as ( 
select cte.bene_mbi_id,
left(cast(cte.bene_member_month as varchar),10) as cov_start,
  left(cast(dateadd(day,-1,dateadd(month,1,(select top 1 bene_member_month 
  from cte cf
  where cf.bene_mbi_id = cte.bene_mbi_id
  and cf.bene_member_month >= cte.bene_member_month
  --and cf.bene_mdcr_stus_cd = cte.bene_mdcr_stus_cd
  --and cf.bene_dual_stus_cd = cte.bene_dual_stus_cd
  and cf.fn is null
  order by bene_member_month)))as varchar),10) as cov_end,
  cte.bene_mdcr_stus_cd,
  cte.bene_dual_stus_cd

from cte where cte.st is null
)



select rd.bene_mbi_id,
rd.cov_start,

'{
	"resourceType" : "Coverage",
	"id" : "'||rd.bene_mbi_id||'_'||rd.cov_start||'",
	"meta" : {
		"lastUpdated" : "'||rd.cov_end||'T23:59:50.99-04:00",
		"source" : "Organization/PayerOrganizationExample1",
		"profile" : [
			"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-Coverage|1.2.0"
		]
	},
	"language" : "en-US",
	"text" : {
		"status" : "generated",
		"div" : "<div xmlns=\\"http://www.w3.org/1999/xhtml\\" xml:lang=\\"en-US\\" lang=\\"en-US\\"><p><b>Generated Narrative</b></p><div style=\\"display: inline-block; background-color: #d9e0e7; padding: 6px; margin: 4px; border: 1px solid #8da1b4; border-radius: 5px; line-height: 60%\\"><p style=\\"margin-bottom: 0px\\">Resource \\"CoverageEx1\\" Updated \\"2020-10-30T13:48:01.846Z\\"	(Language \\"en-US\\") </p><p style=\\"margin-bottom: 0px\\">Information Source: Organization/PayerOrganizationExample1!</p><p style=\\"margin-bottom: 0px\\">Profile: <a href=\\"StructureDefinition-C4BB-Coverage.html\\">C4BB Coverage (version 1.2.0)</a></p></div><p><b>identifier</b>: An identifier for the insured of an insurance policy (this insured always has a subscriber), usually assigned by the insurance carrier.: 88800933501</p><p><b>status</b>: active</p><p><b>policyHolder</b>: <a href=\\"Patient-ExamplePatient1.html\\">Patient/ExamplePatient1</a> \\" TEST\\"</p><p><b>subscriber</b>: <a href=\\"Patient-ExamplePatient1.html\\">Patient/ExamplePatient1</a> \\" TEST\\"</p><p><b>subscriberId</b>: 888009335</p><p><b>beneficiary</b>: <a href=\\"Patient-ExamplePatient1.html\\">Patient/ExamplePatient1</a> \\" TEST\\"</p><p><b>dependent</b>: 01</p><p><b>relationship</b>: Self <span style=\\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\\"> (<a href=\\"http://terminology.hl7.org/2.1.0/CodeSystem-subscriber-relationship.html\\">SubscriberPolicyholder Relationship Codes</a>#self)</span></p><p><b>period</b>: 2020-01-01 --&gt; (ongoing)</p><p><b>payor</b>: <a href=\\"Organization-PayerOrganizationExample1.html\\">Organization/PayerOrganizationExample1: UPMC Health Plan</a> \\"UPMC Health Plan\\"</p><blockquote><p><b>class</b></p><p><b>type</b>: An employee group <span style=\\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\\"> (<a href=\\"http://terminology.hl7.org/2.1.0/CodeSystem-coverage-class.html\\">Coverage Class Codes</a>#group \\"Group\\")</span></p><p><b>value</b>: MCHMO1</p><p><b>name</b>: MEDICARE HMO PLAN</p></blockquote><blockquote><p><b>class</b></p><p><b>type</b>: A specific suite of benefits. <span style=\\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\\"> (<a href=\\"http://terminology.hl7.org/2.1.0/CodeSystem-coverage-class.html\\">Coverage Class Codes</a>#plan \\"Plan\\")</span></p><p><b>value</b>: GR5</p><p><b>name</b>: GR5-HMO DEDUCTIBLE</p></blockquote><p><b>network</b>: GR5-HMO DEDUCTIBLE</p></div>""
	},
  "extension" : [
    {
      "extension" : [{
        "url" : "https://resdac.org/cms-data/variables/beneficiary-medicare-status-code",
        "valueCode" : "'||p.bene_mdcr_stus_cd||'"
      }],
      "text" : "'||
        case 
          when p.bene_mdcr_stus_cd = '10' then 'Aged without end-stage renal disease (ESRD)'
          when p.bene_mdcr_stus_cd = '11' then 'Aged with ESRD'
          when p.bene_mdcr_stus_cd = '20' then 'Disabled without ESRD'
          when p.bene_mdcr_stus_cd = '21' then 'Disabled with ESRD'
          when p.bene_mdcr_stus_cd = '31' then 'ESRD only'
          else  'source value is missing or unknown' end 
      '"
    },
    {
      "extension" : [{
        "url" : "https://resdac.org/cms-data/variables/medicare-medicaid-dual-eligibility-code-january",
        "valueCode" : "'||right('00'||p.bene_dual_stus_cd,2) ||'"
      }],
      "text" : "'||
        case 
          when p.bene_dual_stus_cd = '0' then 'Eligible is not a Medicare beneficiary'
          when p.bene_dual_stus_cd = '1' then 'Eligible is entitled to Medicare- Qualified Medicare Beneficiary (QMB) only'
          when p.bene_dual_stus_cd = '2' then 'Eligible is entitled to Medicare-QMB and Medicaid coverage including prescription drugs'
          when p.bene_dual_stus_cd = '3' then 'Eligible is entitled to Medicare- Specified Low-Income Medicare Beneficiary (SLMB) only'
          when p.bene_dual_stus_cd = '4' then 'Eligible is entitled to Medicare-SLMB and Medicaid coverage including prescription drugs'
          when p.bene_dual_stus_cd = '5' then 'Eligible is entitled to Medicare- Qualified Disabled Working Individual (QDWI)'
          when p.bene_dual_stus_cd = '6' then 'Eligible is entitled to Medicare-Qualifying Individuals (QI)'
          when p.bene_dual_stus_cd = '8' then 'Eligible is entitled to Medicare-Other Dual Eligibles (Non QMB, SLMB, QDWI or QI) including prescription drugs'
          when p.bene_dual_stus_cd = '9' then 'Eligible is entitled to Medicare – but without Medicaid coverage (This code is to be used only with specific CMS approval)'
          when p.bene_dual_stus_cd = '10' then 'Separate CHIP Eligible is entitled to Medicare'
          else  'source value is missing or unknown' end 
      '"
    }
  ]
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
			"system" : "https://www.upmchealthplan.com/fhir/memberidentifier",
			"value" : "'||rd.bene_mbi_id||'",
			"assigner" : {
				"reference" : "Organization/ZZZMedicarePayerOrganizationExample1",
				"display" : "ZZZMedicareUPMC Health Plan"
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
		"starend" : "'||rd.cov_end||'"
	},
	"payor" : [
		{
			"reference" : "Organization/ZZZMedicare?PayerOrganizationExample1",
			"display" : "ZZZMedicare?UPMC Health Plan"
		}
	],'{#
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
from rd
