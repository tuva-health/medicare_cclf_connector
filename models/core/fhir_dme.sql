
{{
    config( materialized='table' )
}}

select
pc.cur_clm_uniq_id||'_'||pc.bene_mbi_id claim_id,
pc.bene_mbi_id patient_id,
 '{
  "resourceType" : "ExplanationOfBenefit",
  "id" : "'||pc.cur_clm_uniq_id||'",
  "meta" : {
    "source" : "Organization/ZZZPayerOrganizationExample1",
    "profile" : [
      "http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-ExplanationOfBenefit-Professional-NonClinician|1.2.0"
    ]
  },
  "text" :"ZZZ"
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
      "system" : "https://www.upmchealthplan.com/fhir/EOBIdentifier",
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
    "start" : "'||pc.clm_from_dt||'",
    "end" : "'||pc.clm_thru_dt||'"
  },
  "created" : "2020-08-24T00:00:00-04:00",
  "insurer" : {
    "reference" : "Organization/ZZZPayerOrganizationExample1",
    "display" : "ZZZUPMC Health Plan"
  },
  "provider" : {
    "reference" : "Organization/ZZZProviderOrganization1"
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
      "reference" : "Organization/ZZZProviderOrganization1'||isnull(pc.payto_prvdr_npi_num,'')||'"
    }
  },
  "outcome" : "complete",
  "careTeam" : [
    {
      "sequence" : 1,
      "provider" : {
        "reference" : "Organization/ZZZProviderOrganization1'||isnull(pc.payto_prvdr_npi_num,'')||'"
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
        "reference" : "Practitioner/ZZZOrderingPractitioner1'||isnull(pc.ordrg_prvdr_npi_num,'')||'"
      },
      "role" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
            "code" : "ZZZorderingreferring",
            "display" : "ZZZorderingReferring"
          }
        ],
        "text" : "The referring physician"
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
      "timingDate" : "'||isnull(pc.clm_thru_dt,'')||'"
    }
  ],
  "insurance" : [
    {
      "focal" : true,
      "coverage" : {
        "reference" : "Coverage/ZZZMedicare?CoverageEx1"
      }
    }
  ],"item":['||pis.itemlist||']
  ,"total":['||pts.total||']
  
  
  }'
  from {{var('partb_dme')}} pc 
  inner join {{ref('dme_items')}} pis
    on pc.cur_clm_uniq_id = pis.cur_clm_uniq_id and pc.bene_mbi_id = pis.bene_mbi_id
  inner join {{ref('dme_totals')}} pts
    on pc.cur_clm_uniq_id = pts.cur_clm_uniq_id and pc.bene_mbi_id = pts.bene_mbi_id
  where pc.clm_line_num = '1'