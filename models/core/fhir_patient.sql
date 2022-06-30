{{
    config( materialized='table' )
}}

select 
p.bene_mbi_id,
'{
	"resourceType" : "Patient",
	"id" : "'||p.bene_mbi_id||'",
	"meta" : {
		"lastUpdated" : "'||p.bene_member_month||'T00:00:00Z",
		"source" : "Organization/ZZZPayerOrganizationExample1",
		"profile" : [
			"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-Patient|1.2.0"
		]
	},
	"language" : "en-US",
	"text" : {
		"status" : "generated",
		"div" : "ZZZ<div xmlns=\\"http://www.w3.org/1999/xhtml\\" xml:lang=\\"en-US\\" lang=\\"en-US\\"><p><b>Generated Narrative</b></p><div style=\\"display: inline-block; background-color: #d9e0e7; padding: 6px; margin: 4px; border: 1px solid #8da1b4; border-radius: 5px; line-height: 60%\\"><p style=\\"margin-bottom: 0px\\">Resource \\"ExamplePatient1\\" Updated \\"2020-10-30T13:48:01.851Z\\"	(Language \\"en-US\\") </p><p style=\\"margin-bottom: 0px\\">Information Source: Organization/PayerOrganizationExample1!</p><p style=\\"margin-bottom: 0px\\">Profile: <a href=\\"StructureDefinition-C4BB-Patient.html\\">C4BB Patient (version 1.2.0)</a></p></div><p><b>identifier</b>: An identifier for the insured of an insurance policy (this insured always has a subscriber), usually assigned by the insurance carrier.: 88800933501</p><p><b>active</b>: true</p><p><b>name</b>: Member 01 Test </p><p><b>telecom</b>: ph: 5555555551, ph: 5555555552, ph: 5555555553, ph: 5555555554, ph: 5555555555(HOME), ph: 5555555556(WORK), <a href=\\"mailto:GXXX@XXXX.com\\">GXXX@XXXX.com</a>, fax: 5555555557</p><p><b>gender</b>: male</p><p><b>birthDate</b>: 1943-01-01</p><p><b>address</b>: </p><ul><li>123 Main Street PITTSBURGH PA 15239 </li><li>456 Murray Avenue PITTSBURGH PA 15217 </li></ul><p><b>maritalStatus</b>: unknown <span style=\\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\\"> (<a href=\\"http://terminology.hl7.org/2.1.0/CodeSystem-v3-NullFlavor.html\\">NullFlavor</a>#UNK)</span></p><h3>Communications</h3><table class=\\"grid\\"><tr><td>-</td><td><b>Language</b></td><td><b>Preferred</b></td></tr><tr><td>*</td><td>English <span style=\\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\\"> (<a href=\\"http://terminology.hl7.org/2.1.0/CodeSystem-v3-ietf3066.html\\">Tags for the Identification of Languages</a>#en)</span></td><td>true</td></tr></table><p><b>managingOrganization</b>: <a href=\\"Organization-PayerOrganizationExample1.html\\">Organization/PayerOrganizationExample1: UPMC Health Plan</a> \\"UPMC Health Plan\\"</p></div>"
	},
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
			"system" : "https://www.syntegra.io/memberidentifier",
			"value" : "'||p.bene_mbi_id||'",
			"assigner" : {
				"reference" : "OrganizationZZZMedicarePayerOrganizationExample1",
				"display" : "ZZZMedicare?UPMC Health Plan"
			}
		}
	],
	"active" : true,
	"name" : [
		{
			"family" : "'||isnull(p.bene_last_name,'')||'",
			"given" : [
				"'||isnull(p.bene_1st_name,'')||'"
				,"Test"
			]
		}
	],
	"telecom" : [
		{
			"system" : "phone",
			"value" : "(555)555-'||right('0000'||p.bene_mbi_id,4)||'",
			"rank" : 1
		},
		{
			"system" : "email",
			"value" : "'||isnull(p.bene_1st_name,'')||'.'||isnull(p.bene_last_name,'')||'@example.com",
			"rank" : 2
		}
	],
	"gender" : "'||case bene_sex_cd
          when '0' then 'unknown'
          when '1' then 'male'
          when '2' then 'female'
		  else '' 
     end  ||'",
	"birthDate" : "'||left(cast(bene_dob as varchar),10)||'",
	"address" : [
		{
			"type" : "physical",
			"line" : [
				"123 Fake Street"
			],
			"city" : "",
			"state" : "''",
			"postalCode" : ""
		}
	],
	"managingOrganization" : {
		"reference" : "Organization/ZZZMedicare?PayerOrganizationExample1",
		"display" : "ZZZMedicareUPMC Health Plan"
	}
}' as fhirPatient
from {{var('beneficiary_demographics')}}  p
inner join (
	select  
		bene_mbi_id as pid
		, max(bene_member_month) as mm 
	from {{var('beneficiary_demographics')}} 
	group by bene_mbi_id
) x
	on p.bene_mbi_id = x.pid and p.bene_member_month = x.mm
left join {{ref('fips_state_codes')}} st
	on st.fips_cd = p.bene_fips_state_cd