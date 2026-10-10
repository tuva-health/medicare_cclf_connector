[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare CCLF Connector

## 🔗 Docs
Check out our [docs](https://thetuvaproject.com/) to learn about the project and how you can use it.
<br/><br/>

## 🧰 What does this repo do?

The Medicare CCLF Connector is a dbt project that maps raw Medicare CCLF claims data to the Tuva Input Layer, which is the first step in running the Tuva Project.  This connector expects your CCLF data to be organized into the tables outlined in this [CMS data dictionary](https://www.cms.gov/files/document/cclf-information-packet.pdf), which is the most recent format CMS uses to distribute CCLF files.
<br/><br/>  

## 🔁 Related claims and adjustments

CMS delivers every version of a claim (original, cancellation, adjustment). The connector
resolves them following the CCLF Information Packet (IP) v43, sections 3.3 and 5.1 to 5.3.
Where the IP leaves a choice open, the rule below is marked as our choice.

- Related claims are grouped by the CMS natural key (IP 5.1.2): billing OSCAR, from date,
  thru date and most recent MBI for Part A; claim control number and most recent MBI (plus
  line number) for Part B physician and DME; fill date, service provider, dispensing status,
  Rx reference number and fill number for Part D.
- Re-delivered copies of the same claim version (same claim ID, line, adjustment type and
  effective date) are collapsed to the most recently delivered file first.
- **Part A.** The IP defines the final action claims as what remains once each cancellation
  is matched with an original or adjustment claim and the matched pairs are removed (IP
  5.2.1). A related set can keep more than one final action claim, and a cancellation is
  never one, even when it shares its effective date with the adjustment that replaces the
  claim it cancels. Claim IDs carry no ordering meaning and never decide a winner. *Our
  choice*, where the IP leaves the pairing open: a cancellation pairs only with a claim of
  equal payment amount. An original of that amount is always eligible. An adjustment of
  that amount is eligible only if it strictly precedes the cancellation (an earlier
  effective date, or, when the effective dates are equal or missing, an earlier file
  date). Each cancellation takes the oldest eligible original, else the oldest eligible
  adjustment, oldest by effective date (missing dates first), then delivery date, then
  claim ID. Why: a cancellation copies the claim it cancels, and an original always comes
  before its cancellation, but an adjustment issued in the same action as a cancellation
  is its replacement, not its target. Without that test, a cancel and adjustment of equal
  amount whose original predates the files cancel each other and the final claim is lost
  (fixture S30). Known residual: in a client profile, about 0.2% of Part A related sets
  that contain a cancellation (15 of 7,194) can't be resolved by any exact-amount rule, for
  example when an original was re-priced, so its cancellation no longer matches it.
- **Part B physician and DME.** Within a related set that contains a cancellation or
  adjustment, one version wins, ranked by effective date (latest first, missing dates last),
  then *our choice* on a tie: adjustment, then original, then cancellation, as a
  cancellation is always paired with an earlier claim (IP 5.3.2 Table 6). Claim ID is the
  last key, for claims that tie on both. A winning cancellation is dropped, and the winner
  still carries the signed (debit/credit) sum of its set's lines.
- **Part D.** The file holds final action claims, so the most recent claim in a related set
  wins (IP 3.3). *Our choice* of what "most recent" means: latest delivery file, then latest
  effective date and IDR load date (missing dates last), then adjustment, cancellation,
  original, then claim ID. A winning cancellation is dropped.
- A related set made up only of original claims is a set of distinct final action claims
  (IP 5.2.1), so each original is kept as its own claim with its own amounts. The
  `adjustment_key` column on the Part B `int_*_claim_adr` models is the claim ID for those
  sets and a constant otherwise.
- **Part A paid amount.** A final action claim carries its own payment and charges. When the
  related set's full history is in the loaded files, the survivors' own payments add up to
  the set's signed debit/credit sum (IP 5.3), so totals are unchanged. The two differ only
  when a cancellation's partner is missing from the files: netting then gives only the
  change since the versions present. In fixture scenario S08 a cancellation ($6,400) and
  its adjustment ($6,650) arrive without the original. The adjustment keeps its own
  $6,650, where netting would give $250. A cancellation that matches no claim adds no
  claim.
- **Limitation: missing originals.** The connector can tell that a natural-key group has no
  original claim, or holds a cancellation that matches nothing, but not that the original is
  missing from the files altogether. A corrected thru date moves the adjustment to a new
  natural key while the original and its cancellation stay on the old one (IP 5.2.1), and an
  old MBI absent from the loaded CCLF9 files leaves earlier versions under a different key.
  In both cases the original is loaded, under another key. Claims are not flagged for this.

These rules are covered by dbt unit tests in `models/intermediate/_unit_tests.yml`.

## 🚫 Denied claims

The connector drops denied claims before they reach `medical_claim`, for every claim type:

- Part A institutional: claims with a Medicare non-payment reason (`CLM_MDCR_NPMT_RSN_CD`).
  Part A claims are accepted or denied in their entirety.
- Part B physician and DME: claims denied on `CLM_CARR_PMT_DNL_CD`, and individual line
  items denied on `CLM_PRCSG_IND_CD`. A missing `CLM_CARR_PMT_DNL_CD` is not a denial.

CCLF IP section 3.2 leaves this to the use case ("depending on your use of the data for
analysis, you may want to drop denied … claims"). The connector drops them because the Tuva
Project uses `medical_claim` for spend, utilization and measures, where a denied, unpaid
service would inflate counts. Denied claims are therefore not available downstream; analyses
of denials (prior authorization, medical necessity, coding) need the raw CCLF tables.

## 🕳️ Placeholders and missing dates

CMS fills fields that have no value with placeholders. The connector maps them to null
before they reach the Tuva Input Layer:

- **`1000-01-01` and `9999-12-31` in date fields → null.** The CCLF Information Packet
  (v43, section 3.6) says these dates fill fields that are not required or not available,
  and that they "should be treated as 'missing' or 'null' values". Every CCLF date column
  is converted in the staging models, so neither date reaches an output column. For
  example, Part D claims whose `CLM_EFCTV_DT` is `1000-01-01` get a null `paid_date`, and
  a CCLF9 `PRVS_ID_OBSLT_DT` of `9999-12-31` is null.
- **`~` in code and identifier fields → null (our choice).** The CCLF files use `~` in
  fields that do not apply to a claim: admission type and source on non-inpatient claims,
  operating and other NPI, unused diagnosis slots, present-on-admission indicators,
  `HCPCS_5_MDFR_CD`, Part D dispensing status, and others. The Information Packet does not
  define `~`; it only says that fields with no data are left blank (Appendix B). We treat
  `~` the same way:
  - Code and identifier columns of `medical_claim` and `pharmacy_claim` are null where
    the source value is `~`. `ccn` is null where `PRVDR_OSCAR_NUM` is `~` and on claims
    with no CCN (professional and DME), rather than the `000000` DuckDB used to produce.
  - A claim whose `CLM_ADMSN_TYPE_CD` is `~` has no admission, so it gets no
    `admission_date` or `discharge_date`.
  - The staging and intermediate models keep `~` as delivered. The related-claims logic
    groups and joins on raw source values (`PRVDR_OSCAR_NUM` joins the Part A files),
    and nulls would not match in those joins.

## 🪪 Eligibility (without `cms_alr_connector`)

The CCLF Information Packet does not define an eligibility model, and enrollment comes from
the `enrollment` source you supply, not from a CCLF file. These are the connector's own
rules for building `eligibility` from that source and the beneficiary demographics file
(CCLF8). The `cms_alr_connector` path builds eligibility differently and is not covered here.

- **One person per beneficiary across MBI changes.** Each enrollment row's MBI is replaced
  with the most recent MBI from the beneficiary XREF file (CCLF9) before member months are
  rolled into spans, so months delivered under a previous MBI belong to the same
  `person_id` and span. IP v43 5.1.1 requires this mapping for the claims natural key;
  extending it to the enrollment source is our choice, made so that claims and eligibility
  agree on `person_id`.
- **Demographics come from the nearest CCLF8 delivery.** CCLF8 arrives monthly, so a
  delivery rarely exists for every enrollment month. Each eligibility row takes its
  demographics (name, address, dual status and so on) from the CCLF8 delivered in the
  row's last month. If there is none, it uses the latest earlier delivery, and if there is
  no earlier one, the earliest later delivery. This rule is our choice.
- **A death date applies to the whole person.** CCLF8 reports `BENE_DEATH_DT` "if a
  decedent" (IP v43 2.4.1), typically from the first delivery
  after the death, which is often after the last enrollment month. The connector takes the
  death date from the latest delivery that reports one and sets `death_date` and
  `death_flag` on every eligibility row for that person. This rule is our choice. The
  connector does not end coverage at the death date; coverage follows the enrollment source.
- **State is the USPS abbreviation.** `state` comes from CCLF8 `GEO_USPS_STATE_CD`
  (IP v43 Table 21), the two-letter code the Tuva input layer expects, not the numeric
  `BENE_FIPS_STATE_CD`. This applies with or without `cms_alr_connector`.
- **Non-dual beneficiaries get a null dual status.** CCLF8 reports beneficiaries with no
  Medicaid as `BENE_DUAL_STUS_CD` `NA`. The Tuva input layer accepts only the numeric dual
  codes, so the connector maps `NA` to null and passes every other value through. The CMS-HCC
  mart treats a null dual status as non-dual, so risk scores are unchanged. This rule is our
  choice. It applies with or without `cms_alr_connector`.
- **Sex is the Tuva Core 1.0 column.** `BENE_SEX_CD` is published as `sex` (`male`,
  `female`, or `unknown`), the name and values the Tuva Core 1.0 eligibility contract
  requires. Columns the CCLF files do not supply (`medicaid_indicator`, `part_d_raf_type`,
  `low_income_subsidy_indicator`, `metal_level`, `csr_indicator`,
  `enrollment_duration_months`, `esrd_status`, `transplant_duration_months`) are null.
  `hospice_flag`, `institutional_snp_flag`, and `long_term_institutional_flag` are
  integers, with null meaning unknown.

## 🔌 Database Support

- DuckDB
- Snowflake

These are the warehouses the release gate (`CI -- All Warehouses`) builds on before every
release. Other warehouses may work but are not tested.
<br/><br/>  

## ✅ Quickstart Guide

### Step 1: Clone or Fork this Repository
Unlike [Tuva Core](https://github.com/tuva-health/tuva-core), this repo is a dbt project, not a dbt package. The dbt package name is still `the_tuva_project`. Clone or fork this repository to your local machine.
<br/><br/> 

### Step 2: Import Tuva Core
`packages.yml` pins Tuva Core 1.0.0 from the published GitHub tag `v1.0.0` (`tuva-health/tuva-core`). From the project root, run `dbt deps` to install that pin. The dbt package name remains `the_tuva_project`.
<br/><br/> 

### Step 3: Data Preparation

#### Source data:
The source table names the connector is expecting can be found in the 
`_sources.yml` config file. You can rename your source tables if needed or add an 
alias to the config.  

#### File Dates:
The field `file_date` is used throughout this connector to deduplicate data 
received across regular and run-out CCLFs. We recommend parsing this date from 
the filename (e.g., P.A****.ACO.ZC1Y**.Dyymmdd.Thhmmsst) and formatting it as 
"YYYY-MM-DD".

#### Enrollment Dates:
The CCLF specification does not have a field that can be mapped directly 
to `enrollment_start_date` and `enrollment_end_date`, and the Part A and Part B 
entitlement dates (BENE_PART_A_ENRLMT_BGN_DT, BENE_PART_B_ENRLMT_BGN_DT) are 
often incorrect or not useful for claims analytics.

We have included an additional source called `Enrollment` that can be
populated with enrollment dates relevant to your data. These enrollment
dates may come from an attribution file, beneficiary alignment report (BAR), or
any source you may have. You just need to create a source table with the 
following columns:

  1. `current_bene_mbi_id`
  2. `enrollment_start_date`
  3. `enrollment_end_date`
  4. `bene_member_month`
     * The connector includes logic to handle enrollment spans or member months.
     * If enrollment spans are available, leave this field null.
     * If enrollment spans are not available, populate this field with member 
       month dates in the format "YYYY-MM-DD" and set the variable 
       `member_months_enrollment` to true in the `dbt_project.yml` file.
<br/><br/> 

### Step 4: Configure Input Database and Schema
Next you need to tell dbt where your Medicare CCLF source data is located.  Do this using the variables `input_database` and `input_schema` in the `dbt_project.yml` file.  You also need to configure your `profile` in the `dbt_project.yml`.

The connector always reads these tables through `source('medicare_cclf', ...)` and ships no demo data. Releases before v0.3.0 had a `demo_data_only` var that switched to bundled, header-only seeds; that var and the seeds are gone, and setting it now has no effect. Development and CI runs use the `integration_tests` project, which loads fixture seeds where `source()` expects the raw tables (see [integration_tests/README.md](integration_tests/README.md)).
<br/><br/> 

### Step 5: Run
Finally, run the connector and the Tuva Project. For example, using dbt CLI you would `cd` to the project root folder in the command line and execute `dbt build`.  

Now you're ready to do claims data analytics!
<br/><br/>

## 🚀 Releasing

The `version:` in `dbt_project.yml` is the release version. Releases are tagged `v<version>`
(for example `v1.0.0`); the older tags `0.1.0`, `0.1.1` and `0.2.0` predate this process.
There is no changelog: release notes are generated from the merged PRs, grouped by their
release label (see `.github/release.yml`). Every PR carries exactly one of
`breaking-change`, `enhancement`, `bug`, `docs` or `ignore-for-release`; the
`release label` check enforces it.

To cut a release:

1. Open a PR from a branch in this repository that bumps `version:` in `dbt_project.yml`.
   A version ending in `-rc` makes a prerelease.
2. Run **Actions → CI -- All Warehouses → Run workflow** from `main` with the PR number.
   It builds the PR's test merge on every supported warehouse and posts the
   `CI / All Warehouses` status on the PR. Merge only once it passes for the PR's current
   head; rerun it after any new push or a change to `main`.
3. Merge. `create-release.yml` tags the merge commit `v<version>` and creates a **draft**
   GitHub Release with generated notes.
4. Review the draft's notes, then publish it (tick "Set as the latest release" unless it is a
   prerelease).

If the workflow fails after the merge, rerun it with **Actions → Create Release → Run
workflow** from `main`; it reuses an existing tag only when the tag points to the current
`main` commit. Projects install a release by tag:

```yaml
packages:
  - git: https://github.com/tuva-health/medicare_cclf_connector.git
    revision: v1.0.0
```
<br/><br/>

## 🙋🏻‍♀️ How do I contribute?
Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.
<br/><br/>

## 🤝 Join our community!
Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
