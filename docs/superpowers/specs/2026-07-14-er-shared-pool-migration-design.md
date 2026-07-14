# Design: Migrate all EarthRanger integrations to the shared dispatcher pool

**Date:** 2026-07-14
**Status:** Approved for planning
**Repos affected:** `cdip` (portal — code + management command); ops runbook steps in GCP

## Problem

ER destinations historically get one Cloud Function + PubSub topic each, deployed
by the portal (`Integration._post_save` → `create_dispatcher_for_integration`).
The new deployment scheme runs a small pool of shared dispatchers (higher
concurrency, push subscriptions, per-destination/per-family throttling) consuming
shared topics. Some integrations have already been migrated by hand (setting
`Integration.additional["topic"]` to the shared root topic), which leaves
orphaned `DispatcherDeployment` rows and live zombie functions, and there is no
systematic path for the rest of the fleet.

Confirmed mechanics this design builds on:

- Routing resolves the destination topic per message from
  `Integration.additional["topic"]`
  (`cdip-routing/app/core/pubsub.py:38`), defaulting to
  `destination-<id>-<env>`. Flipping the field moves traffic within routing's
  config-cache TTL.
- Routing already publishes event updates with `ordering_key = gundi_id`
  (`app/services/event_handlers.py:150`) — per-object ordering on a shared
  topic works if the subscription enables message ordering.
- `DispatcherDeployment` deletion tears down the function, **its recorded
  topic**, and its subscription (`deployments/tasks.py:382`) — deleting a
  deployment whose `topic_name` is the shared topic would destroy the shared
  pipeline. Any teardown path must assert against this.
- The `dispatchers` management command's `--update-source`/`--list-unused`
  machinery only sees FK-linked or fully-detached deployments respectively;
  manually-migrated integrations (FK intact, topic moved) are invisible to
  `--list-unused` and would be pointlessly redeployed by `--update-source`.

## Decisions (confirmed with the owner)

- **Topology:** ONE shared root topic + one shared ER dispatcher deployment,
  horizontally scaled. Fairness comes from the shipped throttling layer.
- **Standardized topic naming (decided 2026-07-14):**
  **`destination-earthranger-{env}`** (matching the Movebank shared-pool
  precedent, `destination-movebank-{env}`). The current production topic
  `root-earthran-cUk0aiO-topic` was auto-named via a placeholder integration
  and is **transitional** — see "Topic standardization & legacy cutover"
  below. Values provided via settings/helm, never hardcoded.
- **New ER destinations default to the shared pool.** A dedicated dispatcher is
  an explicit opt-in special case (`additional["dedicated_dispatcher"] = true`).
  **No current integrations are dedicated keepers** — the flag exists as a
  mechanism, with zero members at migration time.
- **Migration driven by a portal management command** (batched, auditable,
  resumable), not ad-hoc shell work or full automation.
- **Cooling period: 7 days** between migrating an integration and tearing down
  its old function; the dormant function is the rollback lever.
- **The in-flight `--update-source` fleet update is halted.** With no dedicated
  keepers, updating functions that will be deleted within weeks is wasted
  quota; the shared pool already runs the latest source.

## Component 1 — Setting

`ER_SHARED_DISPATCHER_TOPIC = env.str("ER_SHARED_DISPATCHER_TOPIC", "")` in
`cdip_admin` settings. Empty string means "feature disabled" — the
new-integration default and the migration command both refuse to act when
unset (prevents accidentally pointing integrations at a nonexistent topic in
environments that lack a shared pool). Prod value:
`root-earthran-cUk0aiO-topic`.

## Component 2 — New-integration default (portal)

In `Integration._post_save` (create path, `models.py:~285`), for
`self.is_er_site`:

- If `settings.ER_SHARED_DISPATCHER_TOPIC` is set and NOT
  `self.additional.get("dedicated_dispatcher")`: set
  `additional["topic"] = settings.ER_SHARED_DISPATCHER_TOPIC` and do **not**
  create a `DispatcherDeployment`.
- Else (flag set, or setting empty): today's behavior
  (`create_dispatcher_for_integration`).

SMART / WPS Watch / TrapTagger site behavior is unchanged.

Implementation note: `_post_save` runs after the row is written, so the
`additional["topic"]` assignment must persist via a signal-safe write (e.g.
`Integration.objects.filter(pk=self.pk).update(additional=...)`), never a
plain `self.save()`, to avoid save-signal recursion.

## Component 3 — Migration command (portal)

Three additions to `manage.py dispatchers`, following its existing style
(batched via `--max`, per-integration `--integration`, skip-and-report on
per-item failure):

**`--migrate-to-shared [--max N | --integration <id>]`**
Selects ER integrations with a linked `DispatcherDeployment` and
`additional.topic != ER_SHARED_DISPATCHER_TOPIC`, excluding
`dedicated_dispatcher` ones. Per integration:

1. Stamp `additional["pre_migration_topic"]` = current effective topic (the
   `additional.topic` value, or the `destination-<id>-<env>` default when
   unset) and `additional["shared_pool_migrated_at"]` = now (ISO, UTC).
2. Set `additional["topic"] = ER_SHARED_DISPATCHER_TOPIC`; save.

The old function keeps running: it drains messages already published to the
old topic and serves as the rollback target. Nothing is deleted here.

**`--rollback-shared --integration <id>`**
Restores `additional["topic"]` from `pre_migration_topic` and removes the two
bookkeeping keys. Refuses (with a clear message) if the old deployment row no
longer exists — after teardown, rollback means redeploying, which stays a
manual decision.

**`--teardown-migrated [--max N | --integration <id>]`**
Selects integrations whose `shared_pool_migrated_at` is ≥ 7 days old (constant
`SHARED_POOL_COOLING_DAYS = 7`) and which still have a linked deployment. Per
integration, in order:

1. **Safety assert:** `deployment.topic_name != ER_SHARED_DISPATCHER_TOPIC`
   (and not empty-vs-empty edge). Hard skip + loud error if violated —
   deleting such a row would tear down the shared topic.
2. **Drain check:** the old subscription
   (`{deployment.name[:250]}-sub`, dashes normalized — same derivation as the
   delete task) has zero undelivered messages (PubSub API:
   `num_undelivered_messages == 0`). Skip (retry next run) if not drained.
3. Detach the FK (`deployment.integration = None` via queryset `.update()` —
   no save signals) and `deployment.delete()` → existing task tears down
   function + old topic + old subscription and removes the row.
4. Remove `pre_migration_topic` / `shared_pool_migrated_at` from `additional`.

**Cleanup of the already-manually-migrated integrations:** they are exactly
"integrations whose topic is already the shared topic but with a linked
deployment". `--teardown-migrated` handles them too — they lack
`shared_pool_migrated_at`, so the command treats a missing stamp on an
already-shared-topic integration as "migrated at unknown time", stamps
`shared_pool_migrated_at = now` on first sight, and tears down after the
cooling period like everyone else. (Their functions have already been dormant,
but a uniform rule beats a special case; if faster cleanup is wanted, the
operator can pass `--integration` explicitly after verifying dormancy.)

## Topic standardization & legacy cutover (production)

The existing prod shared topic (`root-earthran-cUk0aiO-topic`) was created as
a side effect of a placeholder integration, whose `DispatcherDeployment`
records the shared topic as its own `topic_name` — a standing hazard: any
deletion of that placeholder's deployment deletes the shared topic. The
standardization retires both the ad-hoc name and the hazard:

1. **Create** `destination-earthranger-prod` and a push subscription pointing
   at the existing shared dispatcher service (same subscription config as the
   current shared subscription; `enable_message_ordering=True`). Stage gets
   `destination-earthranger-stage` from day one — no legacy step there.
2. **Point the setting at the standardized name** (helm values). From here,
   new ER destinations and `--migrate-to-shared` use the new topic.
3. **Re-point the hand-migrated integrations** (one-off shell, deliberately
   NOT `--migrate-to-shared`, which would stamp the legacy shared topic as
   `pre_migration_topic` and muddy rollback semantics):

   ```python
   OLD, NEW = "root-earthran-cUk0aiO-topic", "destination-earthranger-prod"
   from integrations.models import Integration
   for i in Integration.objects.filter(additional__topic=OLD):
       Integration.objects.filter(pk=i.pk).update(additional={**i.additional, "topic": NEW})
   ```

4. **Retire the legacy topic**: once the Monitoring drain check reads zero on
   the old subscription, the placeholder integration's deployment is
   reclaimable through the normal teardown path — with the setting now on the
   NEW name, the shared-topic guard no longer shields the old topic, and the
   teardown task deletes the old topic + subscription by design. Delete the
   placeholder integration last.

## Pre-flight requirements (before the first batch)

1. **Redis parity:** the shared deployment and all remaining per-destination
   functions must point at the same `REDIS_HOST`/`REDIS_DB`. Otherwise event
   updates for events delivered pre-migration miss the dispatched-observations
   cache — the `evu` retry loop already observed in production on 2026-07-13.
   Verify env of both fleets explicitly.
2. **Portal-trace fallback bug:** `get_dispatched_observation`'s portal
   fallback currently returns "list index out of range" for some traces
   (observed in prod). It is the cache's only backstop; investigate/fix as a
   separate work item before mass migration, or accept that pre-migration
   events' updates rely purely on Redis retention (1h TTL).
3. **Shared subscription** has `enable_message_ordering=True` (routing sets
   per-gundi_id ordering keys on event updates).
4. **Throttling posture:** the shared pool runs with `THROTTLING_ENABLED=true`
   (already done in prod) — it is the fairness mechanism that replaces
   per-destination isolation.

## Rollout sequence

1. Land the portal changes (setting, new-integration default, command).
2. Set `ER_SHARED_DISPATCHER_TOPIC` in stage; run the full cycle in stage:
   migrate a batch → verify → wait (compressed cooling acceptable in stage via
   a `--cooling-days` override, default 7) → teardown.
3. Set the setting in prod. From this moment new ER destinations attach to the
   shared pool — no new zombies are created.
4. Migrate production in batches (`--migrate-to-shared --max 10 → 25 → 50…`),
   verifying per batch: deliveries for migrated destinations visible in portal
   activity logs; old subscriptions draining to zero; shared-pool throttle
   deferrals and PubSub metrics steady.
5. After each batch's cooling period, `--teardown-migrated` reclaims the
   functions, topics, and quota.
6. End state check: zero ER `DispatcherDeployment` rows (except any future
   explicit dedicated cases), `--list-unused` clean, per-destination ER quota
   reclaimed.

## Failure handling

- Per-integration failures in any subcommand: report and continue (matching
  `update_dispatchers` behavior); the command is idempotent and re-runnable.
- Rollback before teardown: `--rollback-shared` flips the topic back;
  the dormant function resumes consuming immediately.
- Rollback after teardown: redeploy a dedicated dispatcher
  (`--deploy <integration-id>` still exists) and flip the topic back —
  slower, deliberate path.
- A migrated destination misbehaving inside the pool is contained by
  per-destination throttling caps and cooldowns; worst case, roll that one
  integration back.

## Testing

Portal unit tests (GCP and PubSub clients mocked, matching the deployments
app's existing test style):

- New ER integration with setting set → shared topic in `additional`, no
  deployment created; with `dedicated_dispatcher` flag → deployment created,
  no topic override; with setting empty → today's behavior.
- Non-ER site types unaffected.
- `--migrate-to-shared`: stamps + topic flip; skips dedicated; skips
  already-migrated; refuses when setting empty.
- `--rollback-shared`: restores topic; refuses after teardown.
- `--teardown-migrated`: cooling-period gate (including the stamp-on-first-
  sight rule for manually-migrated rows); the shared-topic safety assert;
  drain-check gate (undrained → skip, drained → teardown); FK detach uses
  `.update()` (no redeploy trigger).
- Stage end-to-end run is the integration test (rollout step 2).

## Out of scope

- Sharding the shared pool (single deployment is the decision; revisit only on
  scale evidence).
- Migrating SMART / WPS Watch / TrapTagger dispatchers (same pattern could
  apply later).
- The portal-trace fallback bug (pre-flight item 2) — separate investigation.
- Automated scheduling of migration batches (operator-driven by decision).

## Open items for the implementation plan

- Stage's shared topic name (env-specific setting value).
- Exact PubSub API call for the drain check from the portal's deployments
  code (monitoring API vs. pull-based peek), and required IAM.
- Whether `--cooling-days` override should be restricted to non-prod
  (recommended: allowed everywhere, logged loudly).
