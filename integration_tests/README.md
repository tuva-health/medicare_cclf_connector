# Integration tests

`integration_tests` is the dbt project used for local development and CI of
`medicare_cclf_connector`. It installs the connector as a local package
(`packages.yml` → `local: ../`), loads fixture seeds where the connector's
`source()` expects raw CCLF tables, and builds the connector against them. All CI
and local runs use `--project-dir integration_tests`.

## Layout

- `dbt_project.yml`: the canonical, commented inventory of connector vars.
- `seeds/`: fixture seeds, one per CCLF source table. They load into
  `var('input_database')`.`var('input_schema')`, so the connector reads them
  exactly as it reads a client's raw tables. Every column loads as a string.
- `tests/`: integration-only singular tests.
- `macros/`: CI helpers: schema naming, unit-test schema setup, and
  `drop_ci_schemas` for per-run cleanup.
- `profiles/`: CI warehouse profiles. `profiles/local_duckdb` is the default
  for local runs and the DuckDB CI job.

## Local runs

From the repo root, `scripts/dbt-local` runs dbt with the uv-locked toolchain
against this project and the local DuckDB profile
(`/tmp/medicare_cclf_connector_ci.duckdb`):

```sh
scripts/dbt-local deps
scripts/dbt-local seed --full-refresh --select package:integration_tests
scripts/dbt-local build --full-refresh \
  --select package:medicare_cclf_connector package:integration_tests \
  --exclude package:integration_tests,resource_type:seed --indirect-selection cautious
```

Set `DBT_PROFILES_DIR` (and `DBT_PROFILE`) to use another warehouse. Without
the wrapper: `uv run dbt <cmd> --project-dir integration_tests --profiles-dir
integration_tests/profiles/local_duckdb`.

## CI

`.github/workflows/ci.yml` runs on every pull request to `main`:

| Check | What it runs |
| --- | --- |
| `uv lock check` | `uv lock --check`: `uv.lock` is the single toolchain pin. |
| `dbt build / duckdb` | deps, parse, connector unit tests, fixture seeds, connector build. No secrets; runs on fork PRs too. |
| `dbt build / snowflake` | Same steps, then builds the connector and every installed package (the_tuva_project and its dependencies) downstream. Same-repo PRs only. |

Each Snowflake run sets `tuva_schema_prefix` to
`ci_pr_<pr>_<head sha8>_r<run id>_a<attempt>`, loads fixtures into
`<prefix>_raw`, and writes every connector and Tuva schema as `<prefix>_*` or
`_<prefix>_*`; the profile's default schema is `<prefix>_default`. A final `always()` step runs `drop_ci_schemas` to drop them, so
concurrent PRs never share schemas. A new push cancels the PR's in-flight run.

`ci.yml` is also a reusable workflow (`workflow_call`) with inputs `warehouse`
(`duckdb`, `snowflake`, or `all`), `scope` (`full` or `connector`),
`checkout_ref`, and `schema_prefix`.

### Fork pull requests

Fork PRs get the DuckDB check automatically; the Snowflake job is skipped
because it would expose repository secrets to fork code. After reviewing the
PR's code, a maintainer runs **Actions → External PR CI → Run workflow** from
`main` with the PR number. It pins the PR's current test-merge commit and runs
the Snowflake build through `ci.yml`.

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
