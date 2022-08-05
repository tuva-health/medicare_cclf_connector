
{{
		config( materialized='table' )
}}

select
pc.cur_clm_uniq_id||'_'||pc.bene_mbi_id claim_id,
pc.bene_mbi_id patient_id,
replace(replace(
 '{
	"resourceType" : "ExplanationOfBenefit",
	"id" : "'|| pc.cur_clm_uniq_id||'_'|| cast(pc.bene_mbi_id as varchar) ||'",
	"meta" : {
		"source" : "Organization/Syntegra",
		"profile" : [
			"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-ExplanationOfBenefit-Professional-NonClinician|1.2.0"
		]
	},
	"text" : {
    "status" : "generated",
    "div" : ""
  },
	"identifier" : [
		{
			"type" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBIdentifierType",
						"code" : "uc",
						"display" : "Unique Claim ID"
					}
				],
				"text" : "Indicates that the claim identifier is that assigned by a payer for a claim received from a provider or subscriber"
			},
			"system" : "https://www.syntegra.io/EOBIdentifier",
			"value" : "'|| pc.cur_clm_uniq_id||'_'|| cast(pc.bene_mbi_id as varchar) ||'"
		}
	],
	"status" : "active",
	"type" : {
		"coding" : [
			{
				"system" : "http://terminology.hl7.org/CodeSystem/claim-type",
				"code" : "professional"
			}
		],
		"text" : "Professional"
	},
	"use" : "claim",
	"patient" : {
		"reference" : "Patient/'||pc.bene_mbi_id||'"
	},
	"billablePeriod" : {
		"start" : "'||pc.clm_from_dt||'",
		"end" : "'||pc.clm_thru_dt||'"
	},
	"created" : "'||pc.clm_from_dt||'T00:00:00Z",
	"insurer" : {
		"reference" : "Organization/Medicare",
		"display" : "Medicare"
	},
	"provider" : {
		"reference" : "Organization/'||ifnull(pc.payto_prvdr_npi_num,'')||'"
	},
	"payee" : {
		"type" : {
			"coding" : [
				{
					"system" : "http://terminology.hl7.org/CodeSystem/payeetype",
					"code" : "provider",
					"display" : "Provider"
				}
			],
			"text" : "Any benefit payable will be paid to the provider (Assignment of Benefit)."
		},
		"party" : {
			"reference" : "Organization/'||ifnull(pc.payto_prvdr_npi_num,'')||'"
		}
	},
	"outcome" : "complete",
	"careTeam" : [
		{
			"sequence" : 1,
			"provider" : {
				"reference" : "Organization/'||ifnull(pc.payto_prvdr_npi_num,'')||'"
			},
			"role" : {
				"coding" : [
					{
						"system" : "http://terminology.hl7.org/CodeSystem/claimcareteamrole",
						"code" : "primary",
						"display" : "Primary provider"
					}
				],
				"text" : "The primary care provider."
			}
		},
		{
			"sequence" : 2,
			"provider" : {
				"reference" : "Practitioner/'||ifnull(pc.ordrg_prvdr_npi_num,'')||'"
			},
			"role" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
						"code" : "performing",
						"display" : "Performing"
					}
				],
				"text" : "The performing physician"
			}
		}
	],
	"supportingInfo" : [
		{
			"sequence" : 1,
			"category" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType",
						"code" : "billingnetworkcontractingstatus",
						"display" : "Billing Network Contracting Status"
					}
				],
				"text" : "Indicates that the Billing Provider has a contract with the Payer as of the effective date of service or admission."
			},
			"code" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBPayerAdjudicationStatus",
						"code" : "contracted",
						"display" : "Contracted"
					}
				],
				"text" : "Indicates the provider was contracted for the service"
			}
		},
		{
			"sequence" : 2,
			"category" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType",
						"code" : "clmrecvddate",
						"display" : "Claim Received Date"
					}
				],
				"text" : "Date the claim was received by the payer."
			},
			"timingDate" : "'||ifnull(pc.clm_thru_dt,'')||'"
		}
	],
	"insurance" : [
		{
			"focal" : true,
			"coverage" : {
				"reference" : "Coverage/Medicare_'|| pc.bene_mbi_id ||'"
			}
		}
	],"item":['||pis.itemlist||']
	,"total":['||pts.total||']
	}'
	,chr(9),''),chr(10),'') 
	as  fhir
	from {{var('partb_dme')}} pc 
	inner join {{ref('dme_items')}} pis
		on pc.cur_clm_uniq_id = pis.cur_clm_uniq_id and pc.bene_mbi_id = pis.bene_mbi_id
	inner join {{ref('dme_totals')}} pts
		on pc.cur_clm_uniq_id = pts.cur_clm_uniq_id and pc.bene_mbi_id = pts.bene_mbi_id
	where pc.clm_line_num = '1'