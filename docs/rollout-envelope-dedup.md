# Rollout: envelope-scoped batch dedup

Ordering matters. Purging `dispatched_observation.*` before step 4 breaks the
legacy fallback and recreates the duplicate burst it exists to prevent.

1. **Scale prod Redis up.** Independent of this change and more urgent:
   `sintegrate-4bb07e9c` (project `cdip-prod1-78ca`, region `us-central1`) is a
   10 GB instance that hit 100%. Size for peak observation rate x 25h until this
   ships, then it can come back down.
2. **Deploy** with `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED=true` (the default).
   Confirm in traces that `dedup_source` is `batch_progress` for new envelopes
   and `legacy` for in-flight ones.
3. **Wait at least 25h** — longer than `MAX_EVENT_AGE_SECONDS` (86400), so no
   envelope predating the deploy can still be redelivered.
4. **Set `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED=false`** and redeploy. Confirm
   `dedup_source` is never `legacy`.
5. **Optionally purge leftovers.** `dispatched_observation.*` keys expire on
   their own within 25h of their last write; `scripts/redis_stale_key_cleaner.py`
   in the `cdip` repo (branch `backup/redis-prod-cleanup-prerebase`) can UNLINK
   them sooner.
6. **Scale Redis back down** once db3 is at its new steady state (~0.1-0.2 GB).
7. **Follow-up PR:** delete `_legacy_delivered_indices`,
   `BATCH_DEDUP_LEGACY_FALLBACK_ENABLED`, `utils.mark_observation_dispatched`,
   and `DISPATCHED_OBSERVATIONS_BATCH_CACHE_TTL`.

## Verifying in prod

Profile the keyspace read-only from a portal pod (Memorystore has no public
route and `CONFIG GET` is disabled):

    kubectl --context gundi-prod -n application exec -i <admin-portal-pod> \
      -c admin-portal -- python3 - < script.py

db3 is the ER dispatcher's keyspace. Expect `batch_progress.*` to dominate and
`dispatched_observation.*` to shrink to zero over 25h.
