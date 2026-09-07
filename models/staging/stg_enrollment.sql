-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with enrollment as (
  select pi.value as bene_mbi_id, * 
  from {{ source('fhir', 'patient')}} p
  inner join {{ source('fhir', 'patient_identifier')}} pi 
    on pi.patient_id = p.id
  where pi.system = 'http://hl7.org/fhir/sid/us-mbi'
)

{% if var('cms_alr_connector', var('demo_data_only', false)) %}

select *
from enrollment

{% else %}

{% set current_year = modules.datetime.date.today().year %}
{% for month in range(1, 13) %}

select
      bene_mbi_id as CURRENT_BENE_MBI_ID
    , date_from_parts({{ current_year }}, {{ month }}, 1) as ENROLLMENT_START_DATE
    , last_day(date_from_parts({{ current_year }}, {{ month }}, 1)) as ENROLLMENT_END_DATE
    , null as BENE_MEMBER_MONTH
    , null as FILE_NAME
    , null as FILE_DATE
from enrollment

  {% if not loop.last %}
  union all
  {% endif %}
{% endfor %}

{% endif %}
