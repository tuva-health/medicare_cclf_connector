with stage_enrollment as(
	select
  		bene_mbi_id
  		, bene_member_month
  		, row_number() over (partition by bene_mbi_id order by bene_member_month) as row_count
 	from {{ source('medicare_cclf','beneficiary_demographics')}}
	)
, consecutive_enrollment as(
	select
  		bene_mbi_id
  		,min(bene_member_month) as coverage_start_date
  		,max(bene_member_month)as coverage_end_date
  	from stage_enrollment
  	group by 
  		bene_mbi_id
  		,dateadd(month,-row_count,bene_member_month)
)

select
   cast(bene_mbi_id as varchar) as patient_id
  , cast(coverage_start_date as varchar) as coverage_start_date
  , cast(coverage_end_date as varchar) as coverage_end_date
  , cast(NULL as varchar) as payer
  , cast('medicare' as varchar) as payer_type
  , cast('cclf' as varchar) as data_source
from consecutive_enrollment