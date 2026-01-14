-- 2024 data
    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 1, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag1 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 2, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag2 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 3, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag3 is not null

    union all

       select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 4, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag4 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 5, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag5 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 6, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag6 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 7, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag7 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 8, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag8 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 9, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag9 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 10, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag10 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 11, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag11 is not null

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2024, 12, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2024") }} a_2024
    where enrollflag12 is not null

    union all

    -- 2025 data
    select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 1, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

    select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 2, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 3, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 4, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 5, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 6, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 7, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 8, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 9, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 10, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 11, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025

    union all

  select
        bene_mbi_id
        , DATE_FROM_PARTS(2025, 12, 1) as enrollment_month
        , enrollflag1 as enrollment_code
    from {{ ref("stg_aalr_2025") }} a_2025