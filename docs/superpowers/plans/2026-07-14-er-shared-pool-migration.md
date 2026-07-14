# ER Shared-Pool Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** New ER destinations attach to the shared dispatcher pool by default (dedicated dispatchers become an opt-in flag), and a portal management command migrates the existing fleet in batches with rollback, a 7-day cooling period, a drain check, and a shared-topic teardown guard.

**Architecture:** One new setting (`ER_SHARED_DISPATCHER_TOPIC`) is the single source of truth. `Integration._post_save` branches on it for new ER sites (shared topic in `additional`, no deployment). Three new verbs on the existing `manage.py dispatchers` command handle migrate/rollback/teardown, storing migration bookkeeping in `Integration.additional` and using the deployment row itself as the "not yet torn down" marker.

**Tech Stack:** Django 4.x + django-environ (cdip portal), google-cloud-pubsub (drain check), pytest + pytest-mock with the deployments app's existing conventions (`@override_settings(GCP_ENVIRONMENT_ENABLED=True)`, mocked deploy task, `transaction.on_commit` patched to immediate).

**Spec:** `gundi-dispatcher-er/docs/superpowers/specs/2026-07-14-er-shared-pool-migration-design.md`

## Global Constraints

- Repo: `/Users/chrisdo/padas/cdip`, branch `feature/er-shared-pool-migration` created from up-to-date `origin/main` (`git fetch origin && git checkout -b feature/er-shared-pool-migration origin/main`).
- Tests: `cd /Users/chrisdo/padas/cdip/cdip_admin && DB_HOST=localhost DB_PORT=5432 DB_NAME=cdip_portaldb DB_USER=cdip_dbuser DB_PASSWORD=cdip_dbpassword ../.venv/bin/pytest <target>`. Requires the `cdip-postgres` container (`docker compose up -d postgres` from the repo root) and, if any event-consumer tests get pulled in, redis on 30091 (`docker run -d --name cdip-redis-30091 -p 30091:6379 redis:7-alpine`; remove it afterwards).
- Setting name exactly `ER_SHARED_DISPATCHER_TOPIC`; empty string (the default) means **feature disabled** — the new-integration default AND all three command verbs must refuse/skip when it is unset. Prod value (set at rollout, not in code): `root-earthran-cUk0aiO-topic`.
- Opt-out flag exactly `additional["dedicated_dispatcher"]` (truthy = keep per-destination behavior).
- Bookkeeping keys exactly `additional["pre_migration_topic"]` and `additional["shared_pool_migrated_at"]` (ISO-8601 UTC string).
- Cooling period: `SHARED_POOL_COOLING_DAYS = 7` module constant; `--cooling-days` CLI override allowed everywhere but logged loudly.
- Teardown safety (verbatim from spec): assert `deployment.topic_name != ER_SHARED_DISPATCHER_TOPIC` before any deletion — deployment deletion deletes its recorded topic (`deployments/tasks.py:382`).
- `_post_save` writes to `additional` must use `Integration.objects.filter(pk=...).update(...)` — never `self.save()` (save-signal recursion).
- FK detach before teardown must use queryset `.update(integration=None)` (no save signals → no redeploy trigger).
- SMART / WPS Watch / TrapTagger behavior unchanged; v1 (`OutboundIntegrationConfiguration`) behavior unchanged.
- ER-site checks in Python use `integration.is_er_site` (normalized), not raw `type__value` string comparison.

---

### Task 1: Setting + new-ER shared-pool default

**Files:**
- Modify: `cdip_admin/cdip_admin/settings.py` (one setting)
- Modify: `cdip_admin/integrations/models/v2/models.py` (`Integration._post_save`, created branch, ~line 280)
- Test: `cdip_admin/deployments/tests/test_automatic_deployments.py` (append)

**Interfaces:**
- Consumes: existing `create_dispatcher_for_integration`, `settings.GCP_ENVIRONMENT_ENABLED`, the fixtures used by `test_automatic_dispatcher_deployments_v2` (`integration_type_er`, `organization`, `er_action_push_positions`, `mock_get_dispatcher_defaults_from_gcp_secrets`).
- Produces: `settings.ER_SHARED_DISPATCHER_TOPIC: str` (default `""`); new-ER-integration behavior that Tasks 2–4 rely on for "already on shared topic" states.

- [ ] **Step 1: Add the setting**

In `cdip_admin/cdip_admin/settings.py`, next to the other dispatcher-related settings:

```python
# Shared ER dispatcher pool. When set, new EarthRanger destinations are
# attached to this topic instead of getting a dedicated dispatcher, and the
# `dispatchers` command's shared-pool migration verbs operate against it.
# Empty string = feature disabled.
ER_SHARED_DISPATCHER_TOPIC = env.str("ER_SHARED_DISPATCHER_TOPIC", default="")
```

- [ ] **Step 2: Write the failing tests**

Append to `cdip_admin/deployments/tests/test_automatic_deployments.py`:

```python
@override_settings(GCP_ENVIRONMENT_ENABLED=True, ER_SHARED_DISPATCHER_TOPIC="root-er-test-topic")
def test_new_er_integration_defaults_to_shared_pool(
        mocker, organization, integration_type_er, er_action_push_positions,
        mock_get_dispatcher_defaults_from_gcp_secrets
):
    mocked_deployment_task = mocker.MagicMock()
    mocker.patch("deployments.models.deploy_serverless_dispatcher", mocked_deployment_task)
    mocker.patch(
        "deployments.utils.get_dispatcher_defaults_from_gcp_secrets",
        mock_get_dispatcher_defaults_from_gcp_secrets,
    )
    mocker.patch("deployments.models.transaction.on_commit", lambda fn: fn())

    integration = Integration.objects.create(
        type=integration_type_er,
        name="Shared Pool Reserve",
        owner=organization,
        base_url="https://sharedpool.pamdas.org",
    )

    integration.refresh_from_db()
    assert integration.additional.get("broker") == "gcp_pubsub"
    assert integration.additional.get("topic") == "root-er-test-topic"
    # No dedicated dispatcher was created or deployed
    assert not hasattr(integration, "dispatcher_by_integration") or \
        Integration.objects.filter(pk=integration.pk, dispatcher_by_integration__isnull=True).exists()
    mocked_deployment_task.delay.assert_not_called()


@override_settings(GCP_ENVIRONMENT_ENABLED=True, ER_SHARED_DISPATCHER_TOPIC="root-er-test-topic")
def test_dedicated_dispatcher_flag_preserves_old_behavior(
        mocker, organization, integration_type_er, er_action_push_positions,
        mock_get_dispatcher_defaults_from_gcp_secrets
):
    mocked_deployment_task = mocker.MagicMock()
    mocker.patch("deployments.models.deploy_serverless_dispatcher", mocked_deployment_task)
    mocker.patch(
        "deployments.utils.get_dispatcher_defaults_from_gcp_secrets",
        mock_get_dispatcher_defaults_from_gcp_secrets,
    )
    mocker.patch("deployments.models.transaction.on_commit", lambda fn: fn())

    integration = Integration.objects.create(
        type=integration_type_er,
        name="Dedicated Reserve",
        owner=organization,
        base_url="https://dedicated.pamdas.org",
        additional={"dedicated_dispatcher": True},
    )

    integration.refresh_from_db()
    assert integration.additional.get("topic") != "root-er-test-topic"
    assert Integration.objects.filter(pk=integration.pk, dispatcher_by_integration__isnull=False).exists()
    mocked_deployment_task.delay.assert_called_once()


@override_settings(GCP_ENVIRONMENT_ENABLED=True, ER_SHARED_DISPATCHER_TOPIC="root-er-test-topic")
def test_non_er_sites_unaffected_by_shared_pool_setting(
        mocker, organization, integration_type_smart, smart_action_push_events,
        mock_get_dispatcher_defaults_from_gcp_secrets
):
    mocked_deployment_task = mocker.MagicMock()
    mocker.patch("deployments.models.deploy_serverless_dispatcher", mocked_deployment_task)
    mocker.patch(
        "deployments.utils.get_dispatcher_defaults_from_gcp_secrets",
        mock_get_dispatcher_defaults_from_gcp_secrets,
    )
    mocker.patch("deployments.models.transaction.on_commit", lambda fn: fn())

    integration = Integration.objects.create(
        type=integration_type_smart,
        name="Smart Site",
        owner=organization,
        base_url="https://smart.example.org",
    )

    integration.refresh_from_db()
    assert integration.additional.get("topic") != "root-er-test-topic"
    assert Integration.objects.filter(pk=integration.pk, dispatcher_by_integration__isnull=False).exists()
    mocked_deployment_task.delay.assert_called_once()
```

Note: the pre-existing `test_automatic_dispatcher_deployments_v2` (same file) covers the setting-empty case — it runs without `ER_SHARED_DISPATCHER_TOPIC` (default `""`) and must keep passing unchanged, including its `integration_type_er` parametrization.

Adaptation note: if the exact patch target for the secrets helper differs (the existing tests patch it where the *caller* imports it), copy the working patch line from `test_automatic_dispatcher_deployments_v2` verbatim.

- [ ] **Step 3: Run to verify RED**

Run: `cd /Users/chrisdo/padas/cdip/cdip_admin && DB_HOST=localhost DB_PORT=5432 DB_NAME=cdip_portaldb DB_USER=cdip_dbuser DB_PASSWORD=cdip_dbpassword ../.venv/bin/pytest deployments/tests/test_automatic_deployments.py -v -k "shared_pool or dedicated or unaffected"`
Expected: `test_new_er_integration_defaults_to_shared_pool` FAILS (a deployment is still created; topic not set); the dedicated/non-ER tests PASS (current behavior).

- [ ] **Step 4: Implement the default**

In `cdip_admin/integrations/models/v2/models.py`, inside `Integration._post_save`, replace the created-branch dispatcher block:

```python
        if created:
            # Deploy serverless dispatchers for destinations
            if settings.GCP_ENVIRONMENT_ENABLED and any(
                    [self.is_er_site, self.is_smart_site, self.is_wpswatch_site, self.is_traptagger_site]
            ):
                create_dispatcher_for_integration(self)
```

with:

```python
        if created:
            # Deploy serverless dispatchers for destinations
            if settings.GCP_ENVIRONMENT_ENABLED and any(
                    [self.is_er_site, self.is_smart_site, self.is_wpswatch_site, self.is_traptagger_site]
            ):
                if (
                    self.is_er_site
                    and settings.ER_SHARED_DISPATCHER_TOPIC
                    and not (self.additional or {}).get("dedicated_dispatcher")
                ):
                    # New ER destinations attach to the shared dispatcher pool
                    # by default; a dedicated dispatcher is an explicit opt-in
                    # via additional["dedicated_dispatcher"]. Signal-safe write:
                    # a plain save() here would recurse through _post_save.
                    updated_additional = {
                        **(self.additional or {}),
                        "broker": "gcp_pubsub",
                        "topic": settings.ER_SHARED_DISPATCHER_TOPIC,
                    }
                    Integration.objects.filter(pk=self.pk).update(additional=updated_additional)
                    self.additional = updated_additional
                else:
                    create_dispatcher_for_integration(self)
```

- [ ] **Step 5: Run to verify GREEN, then the module**

Run: same `-k` command from Step 3, then the full module:
`... ../.venv/bin/pytest deployments/tests/test_automatic_deployments.py -v`
Expected: ALL PASS (including the pre-existing v1/v2 auto-deploy tests).

- [ ] **Step 6: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/cdip_admin/settings.py cdip_admin/integrations/models/v2/models.py cdip_admin/deployments/tests/test_automatic_deployments.py && git commit -m "Attach new ER destinations to the shared dispatcher pool by default

When ER_SHARED_DISPATCHER_TOPIC is set, new EarthRanger integrations get
the shared topic in additional (signal-safe update) and no dedicated
dispatcher; additional[dedicated_dispatcher]=true opts back in. Empty
setting preserves today's behavior everywhere."
```

---

### Task 2: `--migrate-to-shared`

**Files:**
- Modify: `cdip_admin/deployments/management/commands/dispatchers.py` (new args + handler methods)
- Test: `cdip_admin/deployments/tests/test_commands.py` (append)

**Interfaces:**
- Consumes: `settings.ER_SHARED_DISPATCHER_TOPIC` (Task 1); existing command plumbing (`add_arguments`, `handle`, `--max`, `--integration`).
- Produces: `Command.migrate_to_shared(integrations)` and the selection helper `Command._get_migratable_er_integrations(max_count, integration_id=None)`. Bookkeeping keys per the Global Constraints. Tasks 3–4 consume the keys.

- [ ] **Step 1: Write the failing tests**

Append to `cdip_admin/deployments/tests/test_commands.py` (it already imports `call_command`; add the model/settings imports below if missing):

```python
from django.test import override_settings
from integrations.models import Integration


SHARED = "root-er-test-topic"


def _make_er_destination_with_deployment(request, name, topic="destination-old-topic"):
    # Build an ER integration with a linked deployment, without triggering
    # real deploys (GCP_ENVIRONMENT_ENABLED is False by default in tests).
    integration_type_er = request.getfixturevalue("integration_type_er")
    organization = request.getfixturevalue("organization")
    from deployments.models import DispatcherDeployment
    integration = Integration.objects.create(
        type=integration_type_er,
        name=name,
        owner=organization,
        base_url=f"https://{name.lower().replace(' ', '')}.pamdas.org",
        additional={"broker": "gcp_pubsub", "topic": topic},
    )
    deployment = DispatcherDeployment.objects.create(
        name=f"dispatcher-{name.lower().replace(' ', '-')}",
        integration=integration,
        topic_name=topic,
        configuration={"env_vars": {"GCP_PROJECT_ID": "test-project"}},
    )
    return integration, deployment


@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_migrate_to_shared_flips_topic_and_stamps_bookkeeping(request, capsys):
    integration, deployment = _make_er_destination_with_deployment(request, "Reserve A")

    call_command("dispatchers", "--migrate-to-shared", "--max", "10")

    integration.refresh_from_db()
    assert integration.additional["topic"] == SHARED
    assert integration.additional["pre_migration_topic"] == "destination-old-topic"
    assert integration.additional["shared_pool_migrated_at"]  # ISO timestamp
    # Nothing deleted: the old deployment is the rollback lever
    deployment.refresh_from_db()
    assert deployment.integration_id == integration.id


@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_migrate_to_shared_skips_dedicated_and_already_migrated(request, capsys):
    dedicated, _ = _make_er_destination_with_deployment(request, "Dedicated B")
    dedicated.additional["dedicated_dispatcher"] = True
    dedicated.save()
    migrated, _ = _make_er_destination_with_deployment(request, "Done C", topic=SHARED)

    call_command("dispatchers", "--migrate-to-shared", "--max", "10")

    dedicated.refresh_from_db()
    assert dedicated.additional["topic"] == "destination-old-topic"
    migrated.refresh_from_db()
    assert "pre_migration_topic" not in migrated.additional  # untouched


@pytest.mark.django_db
def test_migrate_to_shared_refuses_when_setting_empty(request, capsys):
    _make_er_destination_with_deployment(request, "Reserve D")

    call_command("dispatchers", "--migrate-to-shared", "--max", "10")

    out = capsys.readouterr()
    assert "ER_SHARED_DISPATCHER_TOPIC" in (out.out + out.err)
    integration = Integration.objects.get(name="Reserve D")
    assert integration.additional["topic"] == "destination-old-topic"
```

- [ ] **Step 2: Run to verify RED**

Run: `... ../.venv/bin/pytest deployments/tests/test_commands.py -v -k migrate_to_shared`
Expected: FAIL — `CommandError: unrecognized arguments: --migrate-to-shared`.

- [ ] **Step 3: Implement**

In `cdip_admin/deployments/management/commands/dispatchers.py`:

Add to `add_arguments`:

```python
        parser.add_argument(
            "--migrate-to-shared",
            action="store_true",
            default=False,
            help="Move ER integrations onto the shared dispatcher pool topic (old dispatchers are kept for rollback)",
        )
        parser.add_argument(
            "--rollback-shared",
            action="store_true",
            default=False,
            help="Restore an integration's pre-migration topic (requires --integration; only while the old dispatcher still exists)",
        )
        parser.add_argument(
            "--teardown-migrated",
            action="store_true",
            default=False,
            help="Tear down old dispatchers of integrations migrated to the shared pool after the cooling period",
        )
        parser.add_argument(
            "--cooling-days",
            type=int,
            default=None,
            help="Override the shared-pool teardown cooling period (default 7 days). Use with care.",
        )
```

Add to `handle` (before the final success write, alongside the other elifs):

```python
        elif options["migrate_to_shared"]:
            if not self._require_shared_topic():
                return
            self.migrate_to_shared(
                self._get_migratable_er_integrations(
                    max_count=options["max"], integration_id=options.get("integration")
                )
            )
        elif options["rollback_shared"]:
            if not self._require_shared_topic():
                return
            self.rollback_shared(integration_id=options.get("integration"))
        elif options["teardown_migrated"]:
            if not self._require_shared_topic():
                return
            self.teardown_migrated(options=options)
```

Add methods (module constant near the top of the class file):

```python
SHARED_POOL_COOLING_DAYS = 7
```

```python
    def _require_shared_topic(self):
        if not settings.ER_SHARED_DISPATCHER_TOPIC:
            self.stderr.write(
                "ER_SHARED_DISPATCHER_TOPIC is not set - shared-pool verbs are disabled in this environment."
            )
            return False
        return True

    def _get_migratable_er_integrations(self, max_count, integration_id=None):
        shared = settings.ER_SHARED_DISPATCHER_TOPIC
        qs = Integration.objects.filter(dispatcher_by_integration__isnull=False)
        if integration_id:
            qs = qs.filter(id=integration_id)
        candidates = []
        for integration in qs.order_by("name"):
            if not integration.is_er_site:
                continue
            additional = integration.additional or {}
            if additional.get("dedicated_dispatcher"):
                continue
            if additional.get("topic") == shared:
                continue  # already migrated (or manually moved); teardown handles it
            candidates.append(integration)
            if len(candidates) >= max_count:
                break
        return candidates

    def migrate_to_shared(self, integrations):
        from django.utils import timezone
        shared = settings.ER_SHARED_DISPATCHER_TOPIC
        self.stdout.write(self.style.SUCCESS(
            f"Migrating {len(integrations)} ER integrations to shared topic {shared}..."
        ))
        for integration in integrations:
            try:
                additional = integration.additional or {}
                deployment = integration.dispatcher_by_integration
                pre_migration_topic = additional.get("topic") or deployment.topic_name
                updated = {
                    **additional,
                    "broker": "gcp_pubsub",
                    "topic": shared,
                    "pre_migration_topic": pre_migration_topic,
                    "shared_pool_migrated_at": timezone.now().isoformat(),
                }
                Integration.objects.filter(pk=integration.pk).update(additional=updated)
                self.stdout.write(
                    f"Migrated {integration.name} ({integration.id}) from {pre_migration_topic}. "
                    f"Old dispatcher {deployment.name} kept for rollback."
                )
            except Exception as e:
                self.stderr.write(f"Error migrating {integration.name} ({integration.id}): {e}")
                continue
```

(`Integration` and `settings` are already imported in this module.)

- [ ] **Step 4: Run to verify GREEN**

Run: `... ../.venv/bin/pytest deployments/tests/test_commands.py -v -k migrate_to_shared`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/deployments/management/commands/dispatchers.py cdip_admin/deployments/tests/test_commands.py && git commit -m "Add dispatchers --migrate-to-shared

Batched topic flip onto the shared ER pool with rollback bookkeeping
(pre_migration_topic + shared_pool_migrated_at in additional); old
dispatchers stay running as the rollback lever. Refuses when
ER_SHARED_DISPATCHER_TOPIC is unset; skips dedicated_dispatcher
integrations."
```

---

### Task 3: `--rollback-shared`

**Files:**
- Modify: `cdip_admin/deployments/management/commands/dispatchers.py` (one method; args landed in Task 2)
- Test: `cdip_admin/deployments/tests/test_commands.py` (append)

**Interfaces:**
- Consumes: bookkeeping keys from Task 2; the test helpers `_make_er_destination_with_deployment` and `SHARED` defined at the top of Task 2's test additions (same file).
- Produces: `Command.rollback_shared(integration_id)`.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_rollback_shared_restores_topic(request, capsys):
    integration, _ = _make_er_destination_with_deployment(request, "Reserve E")
    call_command("dispatchers", "--migrate-to-shared", "--integration", str(integration.id), "--max", "1")

    call_command("dispatchers", "--rollback-shared", "--integration", str(integration.id))

    integration.refresh_from_db()
    assert integration.additional["topic"] == "destination-old-topic"
    assert "pre_migration_topic" not in integration.additional
    assert "shared_pool_migrated_at" not in integration.additional


@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_rollback_shared_refuses_after_teardown(request, capsys):
    integration, deployment = _make_er_destination_with_deployment(request, "Reserve F")
    call_command("dispatchers", "--migrate-to-shared", "--integration", str(integration.id), "--max", "1")
    # Simulate teardown having removed the deployment
    from deployments.models import DispatcherDeployment
    DispatcherDeployment.objects.filter(pk=deployment.pk).delete()

    call_command("dispatchers", "--rollback-shared", "--integration", str(integration.id))

    out = capsys.readouterr()
    assert "no longer exists" in (out.out + out.err)
    integration.refresh_from_db()
    assert integration.additional["topic"] == SHARED  # unchanged
```

Note: raw queryset `.delete()` on the deployment is used to simulate teardown without triggering the GCP teardown task; if the model's `delete` is overridden to fire tasks, patch `deployments.models.delete_serverless_dispatcher` the way `test_deletion.py` does and copy its pattern.

- [ ] **Step 2: Run to verify RED**

Run: `... ../.venv/bin/pytest deployments/tests/test_commands.py -v -k rollback_shared`
Expected: FAIL — `AttributeError: 'Command' object has no attribute 'rollback_shared'`.

- [ ] **Step 3: Implement**

```python
    def rollback_shared(self, integration_id):
        if not integration_id:
            self.stderr.write("--rollback-shared requires --integration <id>")
            return
        integration = Integration.objects.filter(id=integration_id).first()
        if not integration:
            self.stderr.write(f"Integration {integration_id} not found")
            return
        additional = integration.additional or {}
        pre_migration_topic = additional.get("pre_migration_topic")
        if not pre_migration_topic:
            self.stderr.write(f"{integration.name} has no pre_migration_topic recorded - nothing to roll back")
            return
        if not Integration.objects.filter(pk=integration.pk, dispatcher_by_integration__isnull=False).exists():
            self.stderr.write(
                f"The old dispatcher for {integration.name} no longer exists (torn down). "
                "Rollback now requires redeploying a dedicated dispatcher (--deploy) before flipping the topic back."
            )
            return
        updated = {k: v for k, v in additional.items() if k not in ("pre_migration_topic", "shared_pool_migrated_at")}
        updated["topic"] = pre_migration_topic
        Integration.objects.filter(pk=integration.pk).update(additional=updated)
        self.stdout.write(self.style.SUCCESS(
            f"Rolled back {integration.name} to {pre_migration_topic}. The dormant dispatcher resumes consuming."
        ))
```

- [ ] **Step 4: Run to verify GREEN**

Run: `... ../.venv/bin/pytest deployments/tests/test_commands.py -v -k rollback_shared`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/deployments/management/commands/dispatchers.py cdip_admin/deployments/tests/test_commands.py && git commit -m "Add dispatchers --rollback-shared

Restores the pre-migration topic while the dormant dispatcher still
exists; refuses with guidance after teardown."
```

---

### Task 4: `--teardown-migrated` + drain-check helper

**Files:**
- Modify: `cdip_admin/deployments/utils.py` (drain-check helper)
- Modify: `cdip_admin/deployments/management/commands/dispatchers.py` (teardown method)
- Test: `cdip_admin/deployments/tests/test_commands.py` (append)

**Interfaces:**
- Consumes: bookkeeping keys and the `_make_er_destination_with_deployment`/`SHARED` test helpers (Task 2, same test file); existing `delete_serverless_dispatcher` task wiring on `DispatcherDeployment.delete()`; subscription naming `f"{deployment.name[:250]}-sub".replace("--", "-")` (must match `deployments/tasks.py`'s derivation exactly).
- Produces: `subscription_is_drained(subscription_name, configuration) -> bool` in `deployments/utils.py`; `Command.teardown_migrated(options)`.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_teardown_respects_cooling_period(request, mocker, capsys):
    integration, deployment = _make_er_destination_with_deployment(request, "Reserve G")
    call_command("dispatchers", "--migrate-to-shared", "--integration", str(integration.id), "--max", "1")
    mock_drained = mocker.patch(
        "deployments.management.commands.dispatchers.subscription_is_drained", return_value=True
    )
    mock_delete_task = mocker.patch("deployments.models.delete_serverless_dispatcher")

    # Migrated seconds ago: inside the cooling period -> nothing torn down
    call_command("dispatchers", "--teardown-migrated", "--max", "10")

    assert DispatcherDeployment.objects.filter(pk=deployment.pk).exists()
    mock_delete_task.delay.assert_not_called()

    # Backdate the stamp beyond the cooling period -> teardown proceeds
    integration.refresh_from_db()
    from datetime import timedelta
    from django.utils import timezone
    old = (timezone.now() - timedelta(days=8)).isoformat()
    integration.additional["shared_pool_migrated_at"] = old
    Integration.objects.filter(pk=integration.pk).update(additional=integration.additional)

    call_command("dispatchers", "--teardown-migrated", "--max", "10")

    integration.refresh_from_db()
    assert "shared_pool_migrated_at" not in integration.additional
    assert mock_drained.called
    assert not DispatcherDeployment.objects.filter(pk=deployment.pk, integration__isnull=False).exists()
    mock_delete_task.delay.assert_called_once()  # teardown task actually fired


@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_teardown_hard_skips_deployment_recording_shared_topic(request, mocker, capsys):
    integration, deployment = _make_er_destination_with_deployment(request, "Reserve H", topic=SHARED)
    # Corrupt state: deployment records the shared topic as its own
    DispatcherDeployment.objects.filter(pk=deployment.pk).update(topic_name=SHARED)
    from datetime import timedelta
    from django.utils import timezone
    Integration.objects.filter(pk=integration.pk).update(additional={
        **integration.additional,
        "shared_pool_migrated_at": (timezone.now() - timedelta(days=8)).isoformat(),
    })
    mocker.patch("deployments.management.commands.dispatchers.subscription_is_drained", return_value=True)
    mock_delete_task = mocker.patch("deployments.models.delete_serverless_dispatcher")

    call_command("dispatchers", "--teardown-migrated", "--max", "10")

    out = capsys.readouterr()
    assert "shared topic" in (out.out + out.err).lower()
    assert DispatcherDeployment.objects.filter(pk=deployment.pk).exists()
    mock_delete_task.delay.assert_not_called()


@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_teardown_skips_undrained_subscription(request, mocker, capsys):
    integration, deployment = _make_er_destination_with_deployment(request, "Reserve I")
    call_command("dispatchers", "--migrate-to-shared", "--integration", str(integration.id), "--max", "1")
    from datetime import timedelta
    from django.utils import timezone
    integration.refresh_from_db()
    Integration.objects.filter(pk=integration.pk).update(additional={
        **integration.additional,
        "shared_pool_migrated_at": (timezone.now() - timedelta(days=8)).isoformat(),
    })
    mocker.patch(
        "deployments.management.commands.dispatchers.subscription_is_drained", return_value=False
    )
    mock_delete_task = mocker.patch("deployments.models.delete_serverless_dispatcher")

    call_command("dispatchers", "--teardown-migrated", "--max", "10")

    assert DispatcherDeployment.objects.filter(pk=deployment.pk, integration__isnull=False).exists()
    mock_delete_task.delay.assert_not_called()


@pytest.mark.django_db
@override_settings(ER_SHARED_DISPATCHER_TOPIC=SHARED)
def test_teardown_stamps_manually_migrated_on_first_sight(request, mocker, capsys):
    # Manually moved to the shared topic previously: no stamp yet
    integration, deployment = _make_er_destination_with_deployment(request, "Reserve J", topic=SHARED)
    mocker.patch("deployments.management.commands.dispatchers.subscription_is_drained", return_value=True)
    mock_delete_task = mocker.patch("deployments.models.delete_serverless_dispatcher")

    call_command("dispatchers", "--teardown-migrated", "--max", "10")

    integration.refresh_from_db()
    assert integration.additional.get("shared_pool_migrated_at")  # stamped, not torn down
    assert DispatcherDeployment.objects.filter(pk=deployment.pk).exists()
    mock_delete_task.delay.assert_not_called()
```

Also add near the imports of the test file (if not already imported by earlier tasks' tests): `from deployments.models import DispatcherDeployment`.

Adaptation note: the patch target `deployments.models.delete_serverless_dispatcher` assumes the model's delete override imports the task there — confirm against `deployments/tests/test_deletion.py` and copy its working patch target if it differs.

- [ ] **Step 2: Run to verify RED**

Run: `... ../.venv/bin/pytest deployments/tests/test_commands.py -v -k teardown`
Expected: FAIL — `ImportError`/`AttributeError` for `subscription_is_drained` / `teardown_migrated`.

- [ ] **Step 3: Implement the drain-check helper**

In `cdip_admin/deployments/utils.py` (it already imports `pubsub_v1` via tasks conventions — add `from google.cloud import pubsub_v1` if absent):

```python
def subscription_is_drained(subscription_name, configuration):
    """Best-effort check that a push subscription has no deliverable backlog.

    Peeks with a non-blocking pull: an empty response after the function has
    been dormant for the cooling period is treated as drained. Any message
    seen is released immediately (ack deadline 0) and the subscription is
    treated as NOT drained.
    """
    env_vars = (configuration or {}).get("env_vars", {})
    project_id = env_vars.get("GCP_PROJECT_ID")
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 1},
        timeout=10,
    )
    if not response.received_messages:
        return True
    ack_ids = [m.ack_id for m in response.received_messages]
    subscriber.modify_ack_deadline(
        request={"subscription": subscription_path, "ack_ids": ack_ids, "ack_deadline_seconds": 0}
    )
    return False
```

- [ ] **Step 4: Implement the teardown verb**

In `dispatchers.py` (import `subscription_is_drained` from `deployments.utils` alongside the existing utils imports):

```python
    def teardown_migrated(self, options):
        from datetime import timedelta
        from django.utils import timezone

        shared = settings.ER_SHARED_DISPATCHER_TOPIC
        cooling_days = options.get("cooling_days")
        if cooling_days is None:
            cooling_days = SHARED_POOL_COOLING_DAYS
        else:
            self.stdout.write(self.style.WARNING(
                f"Cooling period OVERRIDDEN to {cooling_days} days (default {SHARED_POOL_COOLING_DAYS})."
            ))
        cutoff = timezone.now() - timedelta(days=cooling_days)

        qs = Integration.objects.filter(dispatcher_by_integration__isnull=False)
        if integration_id := options.get("integration"):
            qs = qs.filter(id=integration_id)
        processed = 0
        for integration in qs.order_by("name"):
            if processed >= options["max"]:
                break
            if not integration.is_er_site:
                continue
            additional = integration.additional or {}
            if additional.get("topic") != shared:
                continue  # not migrated
            deployment = integration.dispatcher_by_integration
            stamp = additional.get("shared_pool_migrated_at")
            if not stamp:
                # Manually migrated before this tooling existed: start its
                # cooling clock now instead of tearing down blind.
                Integration.objects.filter(pk=integration.pk).update(additional={
                    **additional, "shared_pool_migrated_at": timezone.now().isoformat(),
                })
                self.stdout.write(
                    f"{integration.name}: no migration stamp; stamped now, teardown after cooling period."
                )
                processed += 1
                continue
            from datetime import datetime
            migrated_at = datetime.fromisoformat(stamp)
            if migrated_at > cutoff:
                continue  # still cooling
            # SAFETY: deleting a deployment deletes its recorded topic
            # (deployments/tasks.py). Never delete one recording the shared topic.
            if deployment.topic_name and deployment.topic_name == shared:
                self.stderr.write(self.style.ERROR(
                    f"REFUSING teardown for {integration.name}: deployment {deployment.name} records the "
                    f"SHARED TOPIC as its own topic. Deleting it would destroy the shared pipeline. "
                    "Fix the deployment row manually."
                ))
                processed += 1
                continue
            subscription_name = f"{deployment.name[:250]}-sub".replace("--", "-")
            try:
                if not subscription_is_drained(subscription_name, deployment.configuration):
                    self.stdout.write(
                        f"{integration.name}: old subscription {subscription_name} not drained yet; skipping."
                    )
                    processed += 1
                    continue
            except Exception as e:
                self.stderr.write(f"{integration.name}: drain check failed ({e}); skipping.")
                processed += 1
                continue
            # Detach without save signals, then delete (fires the teardown task)
            DispatcherDeployment.objects.filter(pk=deployment.pk).update(integration=None)
            deployment.refresh_from_db()
            deployment.delete()
            cleaned = {k: v for k, v in additional.items() if k not in ("pre_migration_topic", "shared_pool_migrated_at")}
            Integration.objects.filter(pk=integration.pk).update(additional=cleaned)
            self.stdout.write(self.style.SUCCESS(
                f"Teardown triggered for {integration.name}'s old dispatcher {deployment.name} "
                f"(topic {deployment.topic_name})."
            ))
            processed += 1
```

(`DispatcherDeployment` is already imported at the top of `dispatchers.py`.)

- [ ] **Step 5: Run to verify GREEN, then the whole command test module**

Run: `... ../.venv/bin/pytest deployments/tests/test_commands.py -v`
Expected: ALL PASS (new verbs + all pre-existing command tests).

- [ ] **Step 6: Commit**

```bash
cd /Users/chrisdo/padas/cdip && git add cdip_admin/deployments/utils.py cdip_admin/deployments/management/commands/dispatchers.py cdip_admin/deployments/tests/test_commands.py && git commit -m "Add dispatchers --teardown-migrated with drain check and topic guard

Tears down dormant pre-migration dispatchers after the 7-day cooling
period (overridable, loudly). Refuses to delete any deployment that
records the shared topic as its own (deletion deletes the topic), and
skips subscriptions that still hold undelivered messages. Manually
migrated integrations get stamped on first sight instead of torn down
blind."
```

---

### Task 5: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the affected apps' suites**

Run: `cd /Users/chrisdo/padas/cdip/cdip_admin && DB_HOST=localhost DB_PORT=5432 DB_NAME=cdip_portaldb DB_USER=cdip_dbuser DB_PASSWORD=cdip_dbpassword ../.venv/bin/pytest deployments/ integrations/tests/test_calc_integration_status.py -v 2>&1 | tail -5`
Expected: ALL PASS.

- [ ] **Step 2: Report**

Summarize: branch, commits, and the rollout reminder from the spec — set `ER_SHARED_DISPATCHER_TOPIC` in stage first and run the full migrate → verify → (compressed `--cooling-days`) → teardown cycle there; prod value is `root-earthran-cUk0aiO-topic`; pre-flight items (Redis parity between fleets, portal-trace fallback bug, shared subscription ordering) are operational checks outside this code change.
