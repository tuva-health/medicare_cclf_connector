select * from {{ ref('institutional_claims')}}
union all
select * from {{ ref('physician_claims')}}
union all
select * from {{ ref('dme_claims')}}