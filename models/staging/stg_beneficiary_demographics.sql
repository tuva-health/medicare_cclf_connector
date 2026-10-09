with beneficiary_demographics as (
  SELECT 
    * 
  FROM
  {{ source('medicare_cclf','beneficiary_demographics') }}
)

select
      bene_mbi_id
    , bene_hic_num
    , bene_fips_state_cd
    , bene_fips_cnty_cd
    , bene_zip_cd
    , {{ try_to_cast_date('bene_dob') }} as bene_dob
    , bene_sex_cd
    , bene_race_cd
    , {{ try_to_cast_int('bene_age') }} as bene_age
    , bene_mdcr_stus_cd
    , bene_dual_stus_cd
    , {{ try_to_cast_date('bene_death_dt') }} as bene_death_dt
    , {{ try_to_cast_date('bene_rng_bgn_dt') }} as bene_rng_bgn_dt
    , {{ try_to_cast_date('bene_rng_end_dt') }} as bene_rng_end_dt
    , bene_1st_name
    , bene_midl_name
    , bene_last_name
    , bene_orgnl_entlmt_rsn_cd
    , bene_entlmt_buyin_ind
    , {{ try_to_cast_date('bene_part_a_enrlmt_bgn_dt') }} as bene_part_a_enrlmt_bgn_dt
    , {{ try_to_cast_date('bene_part_b_enrlmt_bgn_dt') }} as bene_part_b_enrlmt_bgn_dt
    , bene_line_1_adr
    , bene_line_2_adr
    , bene_line_3_adr
    , bene_line_4_adr
    , bene_line_5_adr
    , bene_line_6_adr
    , geo_zip_plc_name
    , geo_usps_state_cd
    , geo_zip5_cd
    , geo_zip4_cd
    , file_name
    , {{ try_to_cast_date('file_date') }} as file_date
from beneficiary_demographics
