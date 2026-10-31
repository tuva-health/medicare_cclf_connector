{#-
    Adds dual_status_code to each eligibility span, from CCLF8
    BENE_DUAL_STUS_CD (IP v43 Table 21). Both branches of eligibility.sql
    use it, so the rule is the same with or without cms_alr_connector. The
    rule is our choice; see the README's eligibility sections.

    1. Ignore deliveries whose raw code is blank (null, empty or spaces).
    2. Of the rest, take the latest delivery on or before the span's last
       month. If there is none, take the earliest later delivery. A span
       with no end date takes the latest delivery.
    3. Only then map 'NA' (no Medicaid) to null, because Tuva accepts only
       the numeric codes.

    Steps 2 and 3 must stay in this order. Choosing among codes that are
    already mapped would skip a later 'NA' and revive an earlier dual code.

    Arguments:
        spans         CTE or relation with current_bene_mbi_id, the
                      span_keys and end_date_column.
        demographics  CTE or relation with one CCLF8 row per
                      current_bene_mbi_id and coverage_month (the month of
                      the delivery's file date) and bene_dual_stus_cd.
        span_keys     columns that identify one span in `spans`.
        end_date_column  the span's last day.

    Returns a select of every column of `spans` plus dual_status_code and
    dual_status_delivery_rank (always 1). A beneficiary with no non-blank
    code in any delivery gets null.
-#}

{%- macro select_span_dual_status_code(spans, demographics, span_keys, end_date_column='enrollment_end_date') -%}

{%- set last_month -%}
    {{ date_from_parts(
        'year(coalesce(spans.' ~ end_date_column ~ ', cast(\'9999-12-31\' as date)))',
        'month(coalesce(spans.' ~ end_date_column ~ ', cast(\'9999-12-31\' as date)))',
        1
    ) }}
{%- endset -%}

select *
from (
    select
          spans.*
        , cast(nullif(nullif(trim(coded_deliveries.bene_dual_stus_cd), ''), 'NA') as {{ dbt.type_string() }}) as dual_status_code
        , row_number() over (
            partition by
                {%- for key in span_keys %}
                  {{ ',' if not loop.first }} spans.{{ key }}
                {%- endfor %}
            order by
                  case when coded_deliveries.coverage_month <= {{ last_month }} then 0 else 1 end
                , case when coded_deliveries.coverage_month <= {{ last_month }} then coded_deliveries.coverage_month end desc
                , coded_deliveries.coverage_month asc
          ) as dual_status_delivery_rank
    from {{ spans }} as spans
    left join (
        select
              current_bene_mbi_id
            , coverage_month
            , bene_dual_stus_cd
        from {{ demographics }}
        where nullif(trim(bene_dual_stus_cd), '') is not null
    ) as coded_deliveries
        on spans.current_bene_mbi_id = coded_deliveries.current_bene_mbi_id
) as ranked_deliveries
where dual_status_delivery_rank = 1

{%- endmacro -%}
