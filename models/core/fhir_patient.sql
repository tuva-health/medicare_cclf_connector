{{
    config( materialized='table' )
}}

select 
p.bene_mbi_id,
replace(replace(
'{
	"resourceType" : "Patient",
	"id" : "'||p.bene_mbi_id||'",
	"meta" : {
		"lastUpdated" : "'||p.bene_member_month||'T23:59:59.000Z",
		"source" : "Organization/Syntegra",
		"profile" : [
			"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-Patient|1.2.0"
		]
	},
	"language" : "en-US",
	"text" : {
		"status" : "generated",
		"div" : ""
	},
	'||ifnull('"extension": [
		{
			' || ifnull('"extension": [
				{
					"url": "ombCategory",
					"valueCoding": {
						"system": "urn:oid:2.16.840.1.113883.6.238",
						"code": "' || bene_race_cd || '",
						"display": "' || case bene_race_cd
          when '0' then 'unknown'
          when '1' then 'white'
          when '2' then 'black'
          when '3' then 'other'
          when '4' then 'asian'
          when '5' then 'hispanic'
          when '6' then 'north american native'
     end || '"
					}
				},','')|| '
				{
					"url": "text",
					"valueString": "' ||   case bene_race_cd
          when '0' then 'unknown'
          when '1' then 'white'
          when '2' then 'black'
          when '3' then 'other'
          when '4' then 'asian'
          when '5' then 'hispanic'
          when '6' then 'north american native'
     end ||  '"
				}
			],
			"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
		}],','')||'
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
			"value" : "'||p.bene_mbi_id||'",
			"assigner" : {
				"reference" : "Organizatio/Medicare",
				"display" : "Medicare"
			}
		}
	],
	"active" : true,
	"name" : [
		{
			"family" : "'||ifnull(p.bene_last_name,'')||'",
			"given" : [
				"'||ifnull(p.bene_1st_name,'')||'"
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
			"value" : "'||ifnull(p.bene_1st_name,'')||'.'||ifnull(p.bene_last_name,'')||'@example.com",
			"rank" : 2
		}
	],
	"gender" : "'||case bene_sex_cd
          when '0' then 'unknown'
          when '1' then 'male'
          when '2' then 'female'
		  else '' 
     end  ||'",
	"birthDate" : "'||left(cast(bene_dob as varchar),10)||'",' ||
	ifnull('"deceasedDateTime" : "'|| left(cast(bene_death_dt as varchar),10)||'",','')
	||'"address" : [
		{
			"type" : "physical",
			"line" : [
				"123 Fake Street"
			],
			"city" : "",
			"state" : "'||ifnull(st.state,'')||'",
			"postalCode" : ""
		}
	],
	"managingOrganization" : {
		"reference" : "Organization/Medicare",
		"display" : "Medicare"
	}
}' 
,chr(9),''),chr(10),'')
as  fhir
from {{var('beneficiary_demographics')}}  p
inner join (
	select  
		bene_mbi_id as pid
		, max(bene_member_month) as mm 
	from {{var('beneficiary_demographics')}} 
	group by bene_mbi_id
) x
	on p.bene_mbi_id = x.pid and p.bene_member_month = x.mm
left join {{ref('medicare_state_fips')}} st
	on st.fips_code = p.bene_fips_state_cd