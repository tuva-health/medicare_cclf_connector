# Integration tests

`integration_tests` is the dbt project used for local development and CI of
`medicare_cclf_connector`. It installs the connector as a local package
(`packages.yml` → `local: ../`), loads fixture seeds where the connector's
`source()` expects raw CCLF tables, and builds the connector against them. All CI
and local runs use `--project-dir integration_tests`.

## Layout

- `dbt_project.yml`: the canonical, commented inventory of connector vars.
- `seeds/`: synthetic fixture seeds, one per CCLF source table (see
  [Fixtures](#fixtures)). They load into
  `var('input_database')`.`var('input_schema')`, so the connector reads them
  exactly as it reads a client's raw tables. Every column loads as a string.
- `tests/`: integration-only singular tests, one per fixture scenario or group
  of scenarios.
- `macros/`: CI helpers: schema naming, unit-test schema setup, and
  `drop_ci_schemas` for per-run cleanup.
- `profiles/`: one profile per supported warehouse. `profiles/local_duckdb` is
  the default for local runs and the DuckDB CI job; `profiles/snowflake` is the
  Snowflake CI job's.

## Fixtures

The seeds are small, fully synthetic CCLF tables. A generator kept outside this
repository builds them deterministically from the CCLF Information Packet v43
layouts. Every identifier is invented and visibly fake (MBIs `9TT0FK…`, claim IDs
`000000…`, ACO `A0000`, ZIP `00501`, names `SYN…`/`CCLFTEST`), and all dates fall
in an invented CY25/CY26 window. No real beneficiary, provider or claim data is
committed, and fixture files must not be hand-edited: change and rerun the
generator instead.

Each fixture row exists for a named scenario, and each scenario has a singular
test in `tests/` that asserts the outcome the CCLF Information Packet (v43,
sections 3 and 5) calls for. The tests do not assert the connector's current
behaviour. The scenarios cover:

- original claims of every claim type (S01);
- Part A related claims: cancel + adjustment chains, ties on
  `CLM_EFCTV_DT`, the IP 5.3.2 Table 4 example, original-only sets,
  cancellation-only sets, a corrected through date, and a 0,1,2,1,2 chain
  (S02-S11);
- re-delivery of the same claim version in two files (S12) and denied claims
  (S13);
- MBI history in CCLF9: a single change, a two-hop chain, a self-mapping row,
  a remapped previous MBI, two previous MBIs, and a pair that drops out of later
  files (S14-S19);
- eligibility: a death, an enrollment gap, and a beneficiary new in CY26
  (S20-S21b);
- Part B physician and DME related claims, including the IP 5.3.2 Table 6
  example (S22-S25), and Part D related claims (S26-S29);
- a Part A cancellation and adjustment with equal amounts, issued together,
  whose original predates the files (S30);
- the `~` placeholder and the `1000-01-01`/`9999-12-31` date sentinels.

Tests for known connector bugs carry the Linear issue key as a tag and fail
until the bug is fixed; select them with `--select tag:tuva-94` or
`--select tag:tuva-95`. Every fixture test carries the `fixture` tag.

## Local runs

From the repo root, `scripts/dbt-local` runs dbt with the uv-locked toolchain
(every adapter extra in `pyproject.toml`) against this project and the local
DuckDB profile
(`/tmp/medicare_cclf_connector_ci.duckdb`):

```sh
scripts/dbt-local deps
scripts/dbt-local seed --full-refresh --select package:integration_tests
scripts/dbt-local build --full-refresh \
  --select package:medicare_cclf_connector package:integration_tests \
  --exclude package:integration_tests,resource_type:seed --indirect-selection cautious
```

Set `DBT_PROFILES_DIR` (and `DBT_PROFILE`) to use another warehouse. Without
the wrapper: `uv run --extra duckdb dbt <cmd> --project-dir integration_tests
--profiles-dir integration_tests/profiles/local_duckdb`.

## CI

Every pull request to `main` runs `.github/workflows/ci.yml` and
`.github/workflows/release-label.yml`:

| Check | What it runs |
| --- | --- |
| `uv lock check` | `uv lock --check`: `uv.lock` is the single toolchain pin. |
| `dbt build / duckdb` | deps, parse, connector unit tests, fixture seeds, connector build. No secrets; runs on fork PRs too. |
| `dbt build / snowflake` | Same steps, then builds the connector and every installed package (the_tuva_project and its dependencies) downstream. Same-repo PRs only. |
| `CI / Snowflake` | Commit status on the PR head carrying the Snowflake build's result. Same-repo PRs get it from `ci.yml`; fork PRs only from [External PR CI](#fork-pull-requests). |
| `release label` | The PR has exactly one release label (see the Releasing section of the root README). |

The required checks are `uv lock check`, `dbt build / duckdb`, `CI / Snowflake`
and `release label`. `CI / Snowflake` stands in for `dbt build / snowflake`:
that job is skipped on fork PRs, and GitHub counts a skipped check as passing,
whereas a missing status blocks the merge until a maintainer has run the
Snowflake build.

Each job installs only its warehouse's adapter: `uv sync --locked --extra
<warehouse>`, one `pyproject.toml` extra per supported warehouse.

Each Snowflake run sets `tuva_schema_prefix` to
`ci_pr_<pr>_<head sha8>_r<run id>_a<attempt>`, loads fixtures into
`<prefix>_raw`, and writes every connector and Tuva schema as `<prefix>_*` or
`_<prefix>_*`; the profile's default schema is `<prefix>_default`. A final `always()` step runs `drop_ci_schemas` to drop them, so
concurrent PRs never share schemas. A new push cancels the PR's in-flight run.

`ci.yml` is also a reusable workflow (`workflow_call`) with inputs `warehouse`
(`duckdb`, `snowflake`, or `all`), `scope` (`full` or `connector`),
`checkout_ref`, and `schema_prefix`. With `publish_status`, plus the PR number
and its exact base, head and test-merge commits, it posts a commit status
(`CI / Snowflake`, or `CI / All Warehouses` for `all`) on the PR head: pending
when it starts, then the result. The status reports an error instead if the PR
or `main` moves while CI runs, and a run never overwrites a newer run's status.

### Fork pull requests

Fork PRs get the DuckDB check automatically; the Snowflake job is skipped
because it would expose repository secrets to fork code. After reviewing the
PR's code, a maintainer runs **Actions → External PR CI → Run workflow** from
`main` with the PR number. It pins the PR's current test-merge commit, runs
the Snowflake build through `ci.yml`, and posts `CI / Snowflake` on the PR
head. Rerun it after every new push to the fork PR.

### All warehouses

`CI -- All Warehouses` (`ci-all-warehouses.yml`) is the release gate. A
maintainer runs it from `main` with a same-repo PR's number before a release PR
merges; it builds the PR's test merge with `warehouse: all` and posts
`CI / All Warehouses` on the PR head. `all` is DuckDB and Snowflake, the
warehouses the root README lists as supported. It is not a required check,
since every PR would then need it.

To support another warehouse, add all of these in one PR: a `pyproject.toml`
extra for its adapter (then `uv lock`), a profile under `profiles/`, its secrets
and a case in `ci.yml` (the `all` warehouse list and the warehouse job's
settings check and `input_database`), and the README list, once a run passes.

## Snowflake authentication

Use a dedicated Snowflake service user with key-pair authentication. Repository
secrets live under **Settings → Secrets and variables → Actions**. Configure:

| Repository secret | Value |
| --- | --- |
| `DBT_SNOWFLAKE_CI_ACCOUNT` | Snowflake account identifier, without `https://` or `.snowflakecomputing.com` |
| `DBT_SNOWFLAKE_CI_USER` | Service user's login name |
| `DBT_SNOWFLAKE_CI_ROLE` | Role assigned to the service user |
| `DBT_SNOWFLAKE_CI_WAREHOUSE` | CI warehouse |
| `DBT_SNOWFLAKE_CI_DATABASE` | Dedicated, disposable CI database |
| `DBT_SNOWFLAKE_CI_SCHEMA` | Unused by `ci.yml`, which sets the profile's default schema to `<prefix>_default` per run |
| `DBT_SNOWFLAKE_CI_PRIVATE_KEY` | Entire PKCS#8 PEM private key, including header/footer and line breaks |
| `DBT_SNOWFLAKE_CI_PRIVATE_KEY_PASSPHRASE` | Passphrase for an encrypted key; omit for an unencrypted key |

The workflow no longer uses `DBT_SNOWFLAKE_CI_PASSWORD`. The role needs warehouse
usage, database usage, and permission to create schemas and build objects in the
CI database (each run creates and drops its own schemas). Use the existing CI
role where possible.

### Generate and register a key

Run locally, outside the repository. OpenSSL prompts for a passphrase:

```sh
umask 077
ci_key_dir=$(mktemp -d)
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out "$ci_key_dir/rsa_key.p8"
openssl pkey -in "$ci_key_dir/rsa_key.p8" -pubout -out "$ci_key_dir/rsa_key.pub"
sed '/-----/d' "$ci_key_dir/rsa_key.pub" | tr -d '\n'
```

A Snowflake administrator registers the printed public key on the CI user. Use
the Snowflake user object's name in this SQL, which can differ from its login
name. A named key avoids replacing a key used by another integration:

```sql
ALTER USER <CI_USER> ADD KEY PAIR medicare_cclf_ci
  PUBLIC_KEY = '<PUBLIC_KEY_WITHOUT_HEADERS_OR_LINE_BREAKS>';
```

Upload the private key directly into GitHub, then enter the same passphrase at
the second command's hidden prompt:

```sh
gh secret set DBT_SNOWFLAKE_CI_PRIVATE_KEY --repo tuva-health/medicare_cclf_connector < "$ci_key_dir/rsa_key.p8"
gh secret set DBT_SNOWFLAKE_CI_PRIVATE_KEY_PASSPHRASE --repo tuva-health/medicare_cclf_connector
```

Keep key material out of commits, issues, PR comments, and logs. For key rotation,
register a new named key before updating GitHub and remove the old key only after
a successful CI run. See [Snowflake key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth).

## Rerunning checks

Pushes to a PR branch start the checks automatically. After changing only
secrets, rerun the failed job from the PR's Checks tab; a rerun uses the
original commit and workflow, and gets fresh schemas from its new attempt
number.
