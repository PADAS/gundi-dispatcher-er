# Workflow Remediation Plan — gundi-dispatcher-er + gundi-workflows

_Last updated: 2026-06-01_

## Context

The most recent prod release run ([`25139549727`](https://github.com/PADAS/gundi-dispatcher-er/actions/runs/25139549727), branch `release-20260429`) showed `failure`. Investigation found:

- **Not a code failure.** Stage deploy, unit tests, zip, and stage secret update all passed.
- The `upload_prod / deploy` job sat in the `prod` GitHub Environment's **required-reviewers** gate for **720h (30 days = GitHub's max pending window)**, was never approved, and GitHub then auto-failed it: _"The deployment was rejected or didn't satisfy other protection rules."_
- Required reviewers on `prod`: `marianobrc`, `chrisdoehring`.

Separately, every job logs **Node.js 20 deprecation warnings**. GitHub forces Node 24 on **2026-06-02** and removes Node 20 on **2026-09-16**. These warnings did **not** cause the failure but are a near-term risk. All offending actions live in the shared `PADAS/gundi-workflows` repo, not in this repo.

## How deployment actually works (for reference)

This repo's CI **does not run `gcloud functions deploy`**. The release pipeline (`PADAS/gundi-workflows/.github/workflows/pipeline-dispatcher-zip-release.yml`):

1. `common` — compute `file_name` (`er-dispatcher-src-<ref>-<shortsha>`) + run `pytest` (Python 3.11).
2. `zip` — zip the whole source tree into `<file_name>.zip` (artifact `build-artifact`).
3. `upload_artifact_stage` — upload the zip to the **stage** GCS bucket.
4. `update_secret_stage` — patch GCP secret `er-dispatcher-defaults-stage`, key `deployment_settings.source_code_path`, to the new zip name.
5. `upload_prod` — upload the zip to the **prod** GCS bucket _(gated by `prod` env reviewers)_.
6. `update_secret_prod` — patch `er-dispatcher-defaults-prod` the same way.

The actual Cloud Function redeploy is performed downstream by whatever consumes `deployment_settings.source_code_path` (per-destination function, one function per outbound ER topic). `deploy_function.sh` is a manual/local helper only and is **stale** (`--runtime=python38`).

---

## Issues found

### A. Operational — prod release stuck / failed (root cause of the reported failure)
- Required-reviewer gate on `prod` was never actioned → 30-day timeout → job failure.
- Pipeline ordering means a stuck/failed `upload_prod` blocks `update_secret_prod`, so prod source path is never updated.

### B. Node 20 deprecation (gundi-workflows) — deadline 2026-06-02
Actions still on Node 20 across `gundi-workflows`:
- `actions/checkout@v4`, `actions/setup-python@v5`, `actions/upload-artifact@v4`, `actions/download-artifact@v4`
- `google-github-actions/auth@v2`, `setup-gcloud@v2`, `upload-cloud-storage@v2`, `get-gke-credentials@v2`
- `slackapi/slack-github-action@v1`

### C. Other concerns in gundi-workflows
1. **Shell-injection risk** — `_dispatcher_common_zip.yml` interpolates `${{ github.head_ref || github.ref_name }}` directly into a `run:` block, and `update_json_secret_key.yml` interpolates `${{ inputs.secret_value }}` / `inputs.secret_key` into `jq`. A crafted branch name can inject shell. Pass via `env:` instead.
2. **Inconsistent action majors** — `azure/setup-helm@v4` and `@v3` both used; `slack-github-action@v1` is two majors behind.
3. **Python version drift in this repo** — `.python-version` = 3.10, CI tests = 3.11, `deploy_function.sh` = python38. Pick one source of truth.
4. **Benign**: `process_gcloudignore` warning in `upload_to_gcs.yml` — the GCS upload runs against the downloaded artifact dir (no `.gcloudignore` present), so the warning is harmless. Optionally set `process_gcloudignore: false`.

---

## Plan

### Phase 1 — Unblock prod (immediate, this repo / GitHub UI)
- [ ] Decide whether `release-20260429` should still ship. If yes: re-run the failed run (or push a fresh `release-*` branch) and **approve the `prod` deployment promptly** in the run's "Review deployments" prompt.
- [ ] If that release is stale, ignore the failed run; ship a new release branch instead.
- [ ] Confirm with the team that updating the secret actually triggers a function redeploy (otherwise approval alone won't roll out new code).

### Phase 2 — Node 24 readiness (gundi-workflows, before 2026-06-02 if possible)
- [ ] **Fast mitigation:** set `env: FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true` at the workflow level (or runner) to confirm nothing breaks under Node 24.
- [ ] **Proper fix:** bump pinned actions to their Node 24 majors:
  - `actions/checkout@v5`, `actions/setup-python@v6`, `actions/upload-artifact@v5`, `actions/download-artifact@v5` (use latest available majors).
  - `google-github-actions/auth`, `setup-gcloud`, `upload-cloud-storage`, `get-gke-credentials` → latest Node 24-based releases.
  - `slackapi/slack-github-action@v2`.
- [ ] Validate via a PR pipeline run in a consumer repo (this one) before merging to `gundi-workflows@main` — **every dispatcher repo pins `@main`, so changes are global on merge.**

### Phase 3 — Hardening (gundi-workflows)
- [ ] Move all `${{ github.* }}` / `${{ inputs.* }}` values used in `run:` blocks into `env:` and reference `$VAR` to close the injection vector.
- [ ] Reconcile `azure/setup-helm` to a single major.
- [ ] Align Python version: update `.python-version` (and `deploy_function.sh` runtime, or delete the stale script) to match the 3.11 CI target.
- [ ] Optionally set `process_gcloudignore: false` in `upload_to_gcs.yml` to silence the benign warning.

### Phase 4 — Process improvements (optional)
- [ ] Consider whether `prod` truly needs a manual gate, or add a shorter wait timer + Slack notification so pending prod deploys are visible and don't silently expire.
- [ ] Pin `gundi-workflows` by tag/SHA from consumer repos instead of `@main` for reproducible releases.

## Notes / open questions
- Who owns `gundi-workflows`? Changes there affect all dispatcher repos — coordinate before merging.
- Does the secret update auto-trigger a function redeploy, or is there a separate apply step?
