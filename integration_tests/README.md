# Continuous integration

Pull requests run local DuckDB checks and a Snowflake full-refresh build. DuckDB
checks parse the project, run connector unit tests, load the bundled fixtures,
and build the connector. Snowflake runs `dbt build --full-refresh`, including
seeds, unit tests, data tests, and enabled Tuva package models. Other warehouse
workflows are manual. Snowflake runs are serialized because they share CI schemas.

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
| `DBT_SNOWFLAKE_CI_SCHEMA` | Default CI schema |
| `DBT_SNOWFLAKE_CI_PRIVATE_KEY` | Entire PKCS#8 PEM private key, including header/footer and line breaks |
| `DBT_SNOWFLAKE_CI_PRIVATE_KEY_PASSPHRASE` | Passphrase for an encrypted key; omit for an unencrypted key |

The workflow no longer uses `DBT_SNOWFLAKE_CI_PASSWORD`. The role needs warehouse
usage, database usage, and permission to create schemas and build objects in the
CI database. Use the existing CI role where possible. A single default schema
does not isolate this project: models and seeds also use configured schemas such
as `raw` and `input_layer`.

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

### Run checks on a PR

Pushes to the PR branch start both checks automatically. After changing only
secrets, rerun the failed Snowflake job from the PR's Checks tab. A rerun uses the
original commit and workflow; push workflow changes before rerunning checks.
Manual runs are also available under **Actions → Snowflake CICD Full Refresh →
Run workflow**; select the PR branch, not `main`.

Merge after both checks pass on the final revision and the repository's required
review is satisfied.
