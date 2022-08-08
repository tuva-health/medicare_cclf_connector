{{
    config( materialized='table' )
}}

select
pc.cur_clm_uniq_id claim_id,
pc.bene_mbi_id patient_id,
replace(replace(
 '{
	"resourceType" : "ExplanationOfBenefit",
	"id" : "'||pc.cur_clm_uniq_id||'",
	"meta" : {
		"source" : "Organization/Syntegra",
		"profile" : [
			"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-ExplanationOfBenefit-Professional-NonClinician|1.2.0"
		]
	},  "text" : {
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
			"system" : "https://www.medicare.gov/",
			"value" : "'||pc.cur_clm_uniq_id||'"
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
		"start" : "'||ifnull(pc.clm_from_dt,'')||'",
		"end" : "'||ifnull(pc.clm_thru_dt,'')||'"
	},
	"created" : "'||ifnull(pc.clm_from_dt,'')||'T00:00:00.000Z",
	"insurer" : {
		"reference" : "Organization/Medicare",
		"display" : "Medicare"
	},
	"provider" : {
		"reference" : "Organization/'||ifnull(pc.rndrg_prvdr_npi_num,'')||'"
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
			"reference" : "Organization/'||ifnull(pc.rndrg_prvdr_npi_num,'')||'"
		}
	},
	"outcome" : "complete",
	"careTeam" : [
		{
			"sequence" : 1,
			"provider" : {
				"reference" : "Organization/'||ifnull(pc.rndrg_prvdr_npi_num,'')||'"
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
		}'||/*,'
		{
			"sequence" : 2,
			"provider" : {
				"reference" : "Practitioner/Practitioner1"
			},
			"role" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
						"code" : "referring",
						"display" : "Referring"
					}
				],
				"text" : "The referring physician"
			}
		}*/'
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
	"diagnosis" : [
		{
			"sequence" : 1,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_1_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://terminology.hl7.org/CodeSystem/ex-diagnosistype",
							"code" : "principal",
							"display" : "Principal Diagnosis"
						}
					],
					"text" : "The single medical diagnosis that is most relevant to the patient''s chief complaint or need for treatment."
				}
			]
		}'
		||ifnull(',
		{
			"sequence" : 2,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_2_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 3,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_3_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 4,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_4_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 5,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_5_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 6,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_6_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 7,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_7_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 8,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_8_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 9,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_9_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 10,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_10_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 11,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_11_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')
		||ifnull(',
		{
			"sequence" : 12,
			"diagnosisCodeableConcept" : {
				"coding" : [
					{
						"system" : "http://hl7.org/fhir/sid/icd-10-cm",
						"code" : "'||nullif(pc.clm_dgns_12_cd,'')||'"
					}
				]
			},
			"type" : [
				{
					"coding" : [
						{
							"system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimDiagnosisType",
							"code" : "secondary",
							"display" : "secondary"
						}
					],
					"text" : "Required when necessary to report additional diagnoses on professional and non-clinician claims"
				}
			]
		}','')||
		'
	],
	"insurance" : [
		{
			"focal" : true,
			"coverage" : {
				"reference" : "Coverage/Medicare_'||pc.bene_mbi_id||'"
			}
		}
	]',chr(9),''),chr(10),'')  
	--as fhir1  ,
  ||

    ',"item":['||pis.itemlist||']' --as fhir2 ,
    ||
 ',"total":['||pts.total||']}' --as fhir3
as fhir
  from {{var('partb_physicians')}} pc 
  inner join {{ref('professional_items')}} pis
    on pc.cur_clm_uniq_id = pis.cur_clm_uniq_id and pc.bene_mbi_id = pis.bene_mbi_id
  inner join {{ref('professional_totals')}} pts
    on pc.cur_clm_uniq_id = pts.cur_clm_uniq_id and pc.bene_mbi_id = pts.bene_mbi_id
  where pc.clm_line_num = '1'