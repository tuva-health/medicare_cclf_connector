/**  Adjusting claim payments amount **/

with stage_adjustment as(
  select
    cur_clm_uniq_id
    ,clm_adjsmt_type_cd
    ,case clm_adjsmt_type_cd
    	when '0' then 'original'
    	when '1' then 'cancellation'
    	when '2' then 'adjustment'
    end as clm_adjsmt_type
    ,case
		/** Correcting payment amounts that are not correctly positive/negetive  **/
    	when clm_adjsmt_type_cd = '0' and cast(clm_pmt_amt as varchar) like '-%' then clm_pmt_amt * -1
    	when clm_adjsmt_type_cd = '1' and cast(clm_pmt_amt as varchar) not like '-%' then clm_pmt_amt * -1
    	when clm_adjsmt_type_cd = '2' and cast(clm_pmt_amt as varchar) like '-%' then clm_pmt_amt * -1
    else clm_pmt_amt
    end as clm_pmt_amt
  from {{ source('medicare_cclf','parta_claims_header')}}
)

select
	cur_clm_uniq_id
    ,sum(clm_pmt_amt) as total_payment_amount
from stage_adjustment
group by cur_clm_uniq_id