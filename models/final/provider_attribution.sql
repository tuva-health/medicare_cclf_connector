
WITH aalr_assignment AS (
  SELECT 
    BENE_MBI_ID, 
    MASTER_ID AS TIN, 
    NPI_USED AS NPI
  FROM {{ ref("stg_aalr4_2025") }} a_2025
  QUALIFY ROW_NUMBER() OVER (PARTITION BY BENE_MBI_ID ORDER BY PCS_COUNT DESC) = 1
)

, provider_attribution as (

    select 
        p.bene_mbi_id as person_id,
        p.bene_mbi_id as patient_id,
        TO_CHAR(enrollment_month, 'YYYYMM') AS year_month,
        'medicare' as payer,
        'medicare' as plan,
        'medicare cclf' as data_source,
        p.NPI as payer_attributed_provider,
        p.TIN as payer_attributed_provider_practice,
        'ACO 1' as payer_attributed_provider_organization,
        'MSSP' as payer_attributed_provider_lob,
        p.NPI as custom_attributed_provider,
        p.TIN as custom_attributed_provider_practice,
        'ACO 1' as custom_attributed_provider_organization,
        'MSSP' as custom_attributed_provider_lob
    from aalr_assignment as p
    left join {{ ref('int_unpivoted_enrollment') }} as e
      on p.bene_mbi_id = e.bene_mbi_id
    where e.bene_mbi_id is not null
)

SELECT * FROM provider_attribution