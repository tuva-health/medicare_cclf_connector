# Agent instructions

- **Running dbt, tests or CI**: read [integration_tests/README.md](integration_tests/README.md).
  Every run uses `--project-dir integration_tests`; the toolchain is `uv` and `uv.lock`.
- **Opening a PR**: apply exactly one release label (`breaking-change`,
  `enhancement`, `bug`, `documentation`, `ignore-for-release`); the generated
  release notes are grouped by it.
- **Releasing or bumping `version:`**: read the Releasing section of [README.md](README.md#releasing).
