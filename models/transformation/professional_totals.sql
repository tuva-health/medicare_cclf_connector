select 
    cur_clm_uniq_id
    ,bene_mbi_id
    ,replace(replace(
'   {
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
            "value" : '||sum(clm_line_alowd_chrg_amt)||',
            "currency" : "USD"
        }
    },
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
        "value" : '||sum(clm_line_cvrd_pd_amt)||',
        "currency" : "USD"
        }
    }',chr(9),''),chr(10),'')
     total


from {{var('partb_physicians')}}  pc
group by pc.cur_clm_uniq_id, pc.bene_mbi_id

