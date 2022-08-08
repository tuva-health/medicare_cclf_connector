{{
    config( materialized='table' )
}}


select 
ch.bene_mbi_id
,ch.cur_clm_uniq_id
,replace(replace(
  '{
  "resourceType" : "ExplanationOfBenefit",
  "id" : "'||ch.cur_clm_uniq_id||'",
  "meta" : {
    "lastUpdated" : "'||left(cast(ch.clm_thru_dt as varchar),10)||'T23:59:50.000Z",
    "source" : "Organization/Syntegra",
    "profile" : [
      ' || CASE when ch.clm_type_cd in ('60','50','20')  then
        '"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-ExplanationOfBenefit-Inpatient-Institutional"'
    else '"http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-ExplanationOfBenefit-Outpatient-Institutional"' end
||    ']
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
      "value" : "'||ch.cur_clm_uniq_id||'"
    }
  ],
  "status" : "active",
  "type" : {
    "coding" : [
      {
        "system" : "http://terminology.hl7.org/CodeSystem/claim-type",
        "code" : "institutional"
      }
    ],
    "text" : "Institutional"
  },
  "subType" : {
    "coding" : [
      {
        "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBInstitutionalClaimSubType",
        "code" : "'||
        case when ch.clm_type_cd in ('60','50','20') then 'inpatient'
        else 'outpatient' end
        ||'"
      }
    ],
    "text" : "'||
        case when ch.clm_type_cd in ('60','50','20') then 'Inpatient'
        else 'Outpatient' end
        ||'"
  },
  "use" : "claim",
  "patient" : {
    "reference" : "Patient/'||ch.bene_mbi_id||'"
  },
  "billablePeriod" : {
    "start" : "'||ifnull(left(cast(ch.clm_from_dt as varchar),10),'')||'",
    "end" : "'||ifnull(left(cast(clm_thru_dt as varchar),10),'')||'"
  },
  "created" : "'||ifnull(left(cast(ch.clm_from_dt as varchar),10),'')||'T00:00:00Z",
  "insurer" : {
    "reference" : "Organization/Medicare",
    "display" : "Medicare"
  },
  "provider" : {
    "reference" : "Organization/'||ifnull(ch.fac_prvdr_npi_num,'')||'"
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
      "reference" : "Organization/'||ifnull(ch.fac_prvdr_npi_num,'')||'"
    }
  },
  "outcome" : "complete",
  "careTeam" : [
    {
      "sequence" : 1,
      "provider" : {
        "reference" : "Practitioner/'||ifnull(ch.atndg_prvdr_npi_num,'')||'"
      },
      "role" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
            "code" : "attending",
            "display" : "Attending"
          }
        ],
        "text" : "The attending physician"
      }
    }'||ifnull(',
    {
      "sequence" : 2,
      "provider" : {
        "reference" : "Practitioner/'||nullif(ch.oprtg_prvdr_npi_num,'')||'"
      },
      "role" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
            "code" : "operating",
            "display" : "Operating"
          }
        ],
        "text" : "The operating physician"
      }
    }','')||'
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
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBPayerAdjudicationStatus",
            "code" : "innetwork"
          }
        ]
      }
    },
    {
      "sequence" : 3,
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType",
            "code" : "typeofbill",
            "display" : "Type of Bill"
          }
        ],
        "text" : "UB-04 Type of Bill (FL-04) provides specific information for payer purposes."
      },
      "code" : {
        "coding" : [
          {
            "system" : "https://www.nubc.org/CodeSystem/TypeOfBill",
            "code" : "'||ifnull(cast(ch.clm_bill_fac_type_cd || ch.clm_bill_clsfctn_cd || ch.clm_bill_freq_cd as varchar),'')||' "
          }
        ]
      }
    },
    {
      "sequence" : 4,
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType",
            "code" : "pointoforigin",
            "display" : "Point Of Origin"
          }
        ],
        "text" : "UB-04 Source of Admission (FL-15) identifies the place where the patient was identified as needing admission to a facility."
      },
      "code" : {
        "coding" : [
          {
            "system" : "https://www.nubc.org/CodeSystem/PointOfOrigin",
            "code" : "'||ifnull(ch.clm_admsn_src_cd,'')||' "
          }
        ]
      }
    },
    {
      "sequence" : 5,
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType",
            "code" : "admtype",
            "display" : "Admission Type"
          }
        ],
        "text" : "	UB-04 Priority of the admission (FL-14) indicates, for example, an admission type of elective indicates that the patient''s condition permitted time for medical services to be scheduled."
      },
      "code" : {
        "coding" : [
          {
            "system" : "https://www.nubc.org/CodeSystem/PriorityTypeOfAdmitOrVisit",
            "code" : "'||ifnull(ch.clm_admsn_type_cd,'')||' "
          }
        ]
      }
    },
    {
      "sequence" : 6,
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType",
            "code" : "drg",
            "display" : "DRG"
          }
        ],
        "text" : "DRG (Diagnosis Related Group), including the code system, the DRG version and the code value"
      },
      "code" : {
        "coding" : [
          {
            "system" : "https://www.nubc.org/CodeSystem/DRG",
            "code" : "'||ifnull(ch.dgns_drg_cd,'')||' "
          }
        ]
      }
    },
    {
      "sequence" : 7,
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType",
            "code" : "discharge-status",
            "display" : "	Discharge Status"
          }
        ],
        "text" : "UB-04 Discharge Status (FL-17) indicates the patient’s status as of the discharge date for a facility stay."
      },
      "code" : {
        "coding" : [
          {
            "system" : "https://www.nubc.org/CodeSystem/PatDischargeStatus",
            "code" : "'||ifnull(ch.bene_ptnt_stus_cd,'')||' "
          }
        ]
      }
    }
  ],' 
  ,chr(9),''),chr(10),'') 
  -- as fhir1,
    ||
  ifnull(id.dgs,'') -- as fhir2,
    ||
  ifnull(ipr.prcs,'') -- as fhir3,
    ||
  replace(replace('
  "insurance" : [
    {
      "focal" : true,
      "coverage" : {
        "reference" : "Coverage/Medicare_'|| ch.bene_mbi_id||'"
      }
    }
  ],'
  ,chr(9),''),chr(10),'')
    -- as fhir4,
    ||
  ifnull(ii.itms,'')
    --as fhir5,
    ||
  ifnull(ii2.itms,'')::varchar || case when ii.itms is null then '' else  '],' end
  -- as fhir6,
    ||
 replace(replace( {#'
  "adjudication" : [
    {
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBAdjudication",
            "code" : "noncovered",
            "display" : "Noncovered"
          }
        ],
        "text" : "The portion of the cost of this service that was deemed not eligible by the insurer because the service or member was not covered by the subscriber contract."
      },
      "amount" : {
        "value" : 0.0,
        "currency" : "USD"
      }
    }
  ],'||#}'
  "total" : [
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
        "value" : "'||ifnull(cast(ch.clm_mdcr_instnl_tot_chrg_amt as varchar),'')||'",
        "currency" : "USD"
      }
    }'||{#',
    {
      "category" : {
        "coding" : [
          {
            "system" : "http://terminology.hl7.org/CodeSystem/adjudication",
            "code" : "eligible",
            "display" : "Eligible Amount"
          }
        ],
        "text" : "Amount of the change which is considered for adjudication."
      },
      "amount" : {
        "value" : 1542.01,
        "currency" : "USD"
      }
    },
    {
      "category" : {
        "coding" : [
          {
            "system" : "http://terminology.hl7.org/CodeSystem/adjudication",
            "code" : "deductible",
            "display" : "Deductible"
          }
        ],
        "text" : "Amount deducted from the eligible amount prior to adjudication."
      },
      "amount" : {
        "value" : 0.0,
        "currency" : "USD"
      }
    },
    {
      "category" : {
        "coding" : [
          {
            "system" : "http://terminology.hl7.org/CodeSystem/adjudication",
            "code" : "copay",
            "display" : "CoPay"
          }
        ],
        "text" : "Patient Co-Payment"
      },
      "amount" : {
        "value" : 120.0,
        "currency" : "USD"
      }
    },
    {
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBAdjudication",
            "code" : "noncovered",
            "display" : "Noncovered"
          }
        ],
        "text" : "The portion of the cost of this service that was deemed not eligible by the insurer because the service or member was not covered by the subscriber contract."
      },
      "amount" : {
        "value" : 0.0,
        "currency" : "USD"
      }
    }'#}',
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
        "value" : "'||ifnull(cast(ch.clm_pmt_amt as varchar),'')||'",
        "currency" : "USD"
      }
    }'||{#',
    {
      "category" : {
        "coding" : [
          {
            "system" : "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBAdjudication",
            "code" : "memberliability",
            "display" : "Member liability"
          }
        ],
        "text" : "The amount of the member''s liability."
      },
      "amount" : {
        "value" : 0.0,
        "currency" : "USD"
      }
    }'#}'
  ]
}' 
  ,chr(9),''),chr(10),'')
  --  select top 100 *
as fhir
from {{ source(var('source_name'),'parta_claims_header') }} ch
left join {{ref('institutional_diagnosis')}} id 
  on ch.bene_mbi_id = id.bene_mbi_id
  and ch.cur_clm_uniq_id = id.cur_clm_uniq_id
left join {{ref('institutional_procedure')}} ipr
  on ch.bene_mbi_id = ipr.bene_mbi_id
  and ch.cur_clm_uniq_id = ipr.cur_clm_uniq_id
left join {{ref('institutional_items')}} ii
  on ch.bene_mbi_id = ii.bene_mbi_id
  and ch.cur_clm_uniq_id = ii.cur_clm_uniq_id
left join {{ref('institutional_items2')}} ii2
  on ch.bene_mbi_id = ii2.bene_mbi_id
  and ch.cur_clm_uniq_id = ii2.cur_clm_uniq_id