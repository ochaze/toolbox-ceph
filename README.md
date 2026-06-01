# Ceph RGW Multisite Tools

**EXPERIMENTAL** — don't pass `--delete` without manual reviewing.

> **Ceph Version:** Verified against Ceph v21.0.0 (development/main, commit `906dba604de`). Also valid for Ceph v20.x (Squid) — the documented structures (`RGWBucketEntryPoint`, `RGWBucketInfo`, `rgw_bucket_dir_entry`) are compatible, including `cksum_type` and `sync_policy_info` fields which were introduced in v20.

Tools for maintaining healthy Ceph RGW multisite deployments.

## Table of Contents

**Scripts in this repository:**

1. **[`rgw-orphan-cleaner.py`](#rgw-orphan-cleanerpy)** — Cleans orphaned metadata/data in multisite RGW
2. **[`rgw-sync-gc.py`](#rgw-sync-gcpy)** — Cleans stale sync-status objects in the log pool
3. **[`rgw-sync-repair.py`](#rgw-sync-repairpy)** — Resets stuck per-bucket sync markers after replication changes

---

### rgw-orphan-cleaner.py

- [Ceph RGW Object Structure](#ceph-rgw-object-structure)
- [Problem](#problem)
- [Features](#features)
- [Requirements](#requirements)
- [Usage](#usage)
- [Output Format](#output-format)
- [How It Works](#how-it-works)
- [Examples](#examples)
- [Safety](#safety)
- [Known Limitations](#known-limitations)
- [Ceph's Built-in Alternatives](#cephs-built-in-alternatives)

### rgw-sync-gc.py

- [What It Cleans](#what-it-cleans)
- [Usage for Sync GC](#usage-for-sync-gc)
- [Output Format for Sync GC](#output-format-for-sync-gc)
- [Deployment Pattern for Sync GC](#deployment-pattern-for-sync-gc)
- [Why Sync GC Is Needed](#why-sync-gc-is-needed)

### rgw-sync-repair.py

- [What It Fixes](#what-it-fixes)
- [Usage for Sync Repair](#usage-for-sync-repair)
- [Output Format for Sync Repair](#output-format-for-sync-repair)
- [Why Sync Repair Is Needed](#why-sync-repair-is-needed)
- [Comparison with Sync GC](#comparison-with-sync-gc)

---

# rgw-orphan-cleaner.py

## RGW Orphan Cleaner

**EXPERIMENTAL** — don't pass `--delete` without manual reviewing.

A comprehensive script to detect and clean orphaned Ceph RGW metadata and data objects across multisite deployments.

## Ceph RGW Object Structure

```
┌──────────────────────────────┐
│   BUCKET (entrypoint)        │
│   bucket:name                │
└───────────┬──────────────────┘
            │
            ▼
┌──────────────────────────────┐
│   INSTANCE (metadata)        │
│   .bucket.meta.<tenant>:     │
│   <bucket>:<id>             │
└───────────┬──────────────────┘
            │
            ▼
┌──────────────────────────────┐
│   INDEX POOL                 │
│   .dir.<bucket_id>.shardX    │
└───────────┬──────────────────┘
            │
            ▼
┌──────────────────────────────┐
│   DATA POOL                  │
│   <bucket_id>_<object_key>  │
└──────────────────────────────┘
```

**Structure Details:**

1. RGWBucketEntryPoint (entrypoint) at src/rgw/rgw_common.h:1170 contains:
   - rgw_bucket bucket - the bucket identifier
   - rgw_owner owner - who owns the bucket
   - ceph::real_time creation_time - when it was created
   - bool linked - whether it's linked to a user
   - bool has_bucket_info - backward compatibility flag
   - RGWBucketInfo old_bucket_info - for old format compatibility

2. RGWBucketInfo (instance metadata) at src/rgw/rgw_common.h:1094 contains:
   - rgw_bucket bucket - bucket identifier
   - rgw_owner owner - owner
   - uint32_t flags - various flags (versioned, MFA, etc.)
   - std::string zonegroup - which zonegroup the bucket belongs to
   - ceph::real_time creation_time - creation timestamp
   - rgw_placement_rule placement_rule - where data is stored
   - bool has_instance_obj - whether instance object exists
   - RGWObjVersionTracker objv_tracker - version tracking
   - RGWQuotaInfo quota - bucket quota settings
   - rgw::BucketLayout layout - bucket index layout (sharding info)
   - bool requester_pays - AWS requester pays setting
   - bool has_website + RGWBucketWebsiteConf website_conf - static website config
   - bool swift_versioning + swift_ver_location - Swift versioning
   - std::map<std::string, uint32_t> mdsearch_config - metadata search config
   - rgw::cksum::Type cksum_type - checksum type
   - cls_rgw_reshard_status reshard_status + new_bucket_instance_id - resharding state
   - RGWObjectLock obj_lock - object lock (WORM) configuration
   - std::optional<rgw_sync_policy_info> sync_policy - multisite sync policy


3. INDEX POOL

Storage:
- Pool name: from zone_params.placement_pools[index_pool] or explicit placement
- Objects named: .dir.<bucket_id>.shardX (X = shard number)

Contents:
Each shard contains rgw_bucket_dir_entry records (src/cls/rgw/cls_rgw_types.h:373) per object:
- Key: Object name + instance (for versioning)
- Version: Epoch number
- Locator: RADOS locator string
- Flags: VERSIONED, CURRENT, DELETE_MARKER, COMMON_PREFIX
- Metadata (rgw_bucket_dir_entry_meta):
- Size, mtime, etag
- Owner / owner_display_name
- Content-type
- Storage class (STANDARD, etc.)
- Accounted size (post-compression/encryption)
- Appendable flag
- Pending map: Uncommitted operations

Purpose: Bucket listing, object existence checks, versioning state, listing with delimiters.

4. DATA POOL

Storage:
- Pool name: from placement_rule → storage_class.data_pool (e.g., default.rgw.data)
- Objects: RADOS objects containing actual user data

Object naming:
- Physical RADOS object name derived from rgw_obj → rgw_raw_obj
- Format involves bucket placement + object key hash
- Example pattern: <bucket_id>_<object_key_hash> or from manifest locations

Contents:
- Raw object data and metadata
- For multipart uploads: multiple RADOS objects (manifest tracks locations)
- Each rgw_raw_obj specifies exact pool + object ID (src/rgw/rgw_common.h)

Purpose: Stores actual object bytes, accessed directly via librados after index lookup resolves the location.

### Pool / Namespace Map

| Object Type              | Pool                       | Namespace |
|--------------------------|----------------------------|-----------|
| Entrypoints              | `<zone>.rgw.meta`          | `root`    |
| Instances (.bucket.meta) | `<zone>.rgw.meta`          | `root`    |
| Index shards (.dir.*)   | `<zone>.rgw.buckets.index` | (empty)   |
| Data objects             | `<zone>.rgw.buckets.data`  | (empty)   |

### Orphan Detection Logic

1. **Orphan Instance** — `.bucket.meta` exists in `meta:root`, but no corresponding entrypoint in metadata API.
2. **Stale Instance** — entrypoint exists but points to a different `bucket_id`. The old instance is safe to delete when:
   - `reshard_status == DONE` (resharding completed)
   - `reshard_status == NOT_RESHARDING` (old instance abandoned by delete/recreate, e.g. S3 replication changes)
   
   **Skipped (never deleted)** if `IN_PROGRESS` or `IN_LOGRECORD`.
3. **Orphan Index** — `.dir.` object in index pool, but no instance metadata for that `bucket_id`.
4. **Orphan Data** — data object in data pool, but no instance metadata for its `bucket_id`.

## Problem

In Ceph RGW multisite setups, orphaned metadata and data objects can accumulate when:
- Buckets are deleted but metadata cleanup fails
- Dynamic resharding creates stale instance objects
- Multisite sync doesn't propagate deletions consistently
- Garbage collection doesn't remove data objects

## Features

The script detects four types of orphans:

| Type | Description | Detection Method |
|------|-------------|------------------|
| **Orphan Instances** | Bucket instance metadata without entrypoint | Missing `bucket:name` for `bucket.instance:name:id` |
| **Stale Instances** | Old bucket instances after resharding | Entrypoint points to different bucket_id (always detected, deletion requires `--delete-stale`, see warning below) |
| **Orphan Index** | Index objects without bucket instance | Missing `.bucket.meta.` for `.dir.<>.*` |
| **Orphan Data** | Data objects without bucket metadata | Data pool objects with unknown bucket_id prefix |

> **⚠️ Warning about Stale Instances**: Deleting stale instances is **dangerous**. During an active resharding operation, the old bucket instance still exists while data is being copied. Deleting it mid-reshard will **corrupt the bucket and lose data**. The script checks the instance `reshard_status` field (like Ceph does), but stale instances are **excluded from automatic cleanup** unless you explicitly use `--delete-stale --yes-i-really-mean-it`.

## Requirements

- Python 3.7+
- `radosgw-admin` in PATH (with appropriate permissions)
- `rados` in PATH
- Read access to RGW meta and data pools
- Write access for `--delete` operations

## Usage

### Detection Only (Default)

```bash
# Basic detection
python3 rgw-orphan-cleaner.py

# Include data pool scan (slow on large clusters)
python3 rgw-orphan-cleaner.py --data-pool

# Output to file
python3 rgw-orphan-cleaner.py --output report.json
```

### Cleanup

```bash
# Interactive cleanup with confirmation
python3 rgw-orphan-cleaner.py --delete

# Non-interactive cleanup (like Ceph admin commands)
python3 rgw-orphan-cleaner.py --delete --yes-i-really-mean-it

# Full cleanup including data objects
python3 rgw-orphan-cleaner.py --delete --yes-i-really-mean-it --data-pool

# DANGEROUS: also delete stale instances
python3 rgw-orphan-cleaner.py --delete --yes-i-really-mean-it --delete-stale
```

### Safety Options

```bash
# Only remove instances for tenants with no active users
python3 rgw-orphan-cleaner.py --inactive-tenants-only

# Verify bucket stats (skip buckets that still respond)
python3 rgw-orphan-cleaner.py --verify-active

# Only consider orphans/objects from a specific time period
python3 rgw-orphan-cleaner.py --start-period-utc 2024-01-01T00:00:00Z --end-period-utc 2024-12-31T23:59:59Z

# Objects with mtime that could not be determined are skipped when a time period is set
python3 rgw-orphan-cleaner.py --end-period-utc 2026-05-22T00:00:00Z

# Combined safety check
python3 rgw-orphan-cleaner.py --verify-active --inactive-tenants-only --delete
```

## Output Format

The script outputs JSON with the following structure:

```json
{
  "zone": "gva2b",
  "meta_pool": "gva2b.rgw.meta",
  "index_pool": "gva2b.rgw.buckets.index",
  "data_pool": "gva2b.rgw.buckets.data",
  "timestamp": "2026-05-15T14:12:45+00:00",
  "summary": {
    "total_orphans": 1,
    "total_data_orphans": 144,
    "total_data_orphan_objects": 125000,
    "orphan_instances": 0,
    "stale_instances": 1,
    "skipped_instances": 0,
    "orphan_entrypoints": 0,
    "orphan_index": 11,
    "orphan_data_buckets": 144
  },
  "orphans": {
    "instances": [...],
    "stale_instances": [],
    "entrypoints": [...],
    "index": [...],
    "data": [...]
  },
  "skipped": [...]
}
```

## How It Works

### Metadata Detection

1. Lists all entrypoints via `radosgw-admin metadata list bucket`
2. Lists all instances via `radosgw-admin metadata list bucket.instance`
3. Lists RADOS objects in `meta:root` pool
4. Cross-references to find:
   - **Orphan instances**: Instance without entrypoint
   - **Stale instances**: Entrypoint points to different bucket_id (reshard)
   - **Orphan index**: Index without instance

### Data Detection

1. **Streams** all objects in data pool using `rados ls` with subprocess.Popen (avoids OOM on large clusters)
2. Extracts bucket_id from object name (`<bucket_id>_<key>`)
3. Compares against known bucket_ids from metadata
4. Flags orphaned data bucket IDs

### Performance

| Total Objects in Data Pool | Scan Time |
|---------------------------|-----------|
| < 1M | 1-10s |
| 1M - 100M | 30s - 5min |
| 100M - 1B | 5-30min |
| > 1B | 1h+ |

> **Note:** The script uses streaming (`subprocess.Popen`) to avoid loading all object names into memory. `rados ls` on 20B+ objects without streaming would OOM.

## Examples

### Example 1: Detect stale reshard instances

```bash
$ python3 rgw-orphan-cleaner.py
{
  "summary": {
    "total_orphans": 1,
    "stale_instances": 1,
    "orphan_index": 11
  },
  "orphans": {
    "stale_instances": [
      {
        "bucket_name": "ochaze",
        "bucket_id": "...37737.1",
        "active_bucket_id": "...50940.2"
      }
    ]
  }
}
```

### Example 2: Time period filtering

```bash
# Only consider orphans from a specific time period
python3 rgw-orphan-cleaner.py --start-period-utc 2024-01-01T00:00:00Z --end-period-utc 2024-12-31T23:59:59Z

# Combined safety check
python3 rgw-orphan-cleaner.py --verify-active --inactive-tenants-only --delete
```

### Example 3: Clean everything including data

```bash
$ python3 rgw-orphan-cleaner.py --delete --yes --data-pool
# Removing data objects for bucket_id ...57962.17...
#   Found 15000 objects to delete for ...57962.17
#   Progress: 1000/15000 (6.7%)
#   Progress: 2000/15000 (13.3%)
# ...
# Completed ...57962.17: 15000 removed, 0 failed (143 bucket IDs remaining)
```

## Safety

- **Dry run by default**: No objects are deleted unless `--delete` is specified
- **Confirmation prompt**: Requires user confirmation before deletion (use `--yes-i-really-mean-it` like Ceph admin commands)
- **Stale instance protection**: Stale instances are NEVER auto-deleted unless `--delete-stale` is used
- **Reshard status checking**: The script reads the instance `reshard_status` field like Ceph does. Safe to delete: `DONE` (reshard complete) and `NOT_RESHARDING` (old instances abandoned by delete/recreate). **Never deleted**: `IN_PROGRESS` or `IN_LOGRECORD` (active reshard).
- **Time period filtering**: Use `--start-period-utc` and `--end-period-utc` to filter orphans by object mtime. When a time filter is set and an object's mtime cannot be determined, it is conservatively **skipped** (not deleted).
- **Multiple passes required**: Orphans can have dependencies. Deleting an orphan instance may reveal orphaned index objects, and vice versa. Run the script 2-3 times to fully clean up all cascading orphans.

```bash
# Example: run multiple times until no orphans remain
$ python3 rgw-orphan-cleaner.py --delete --yes-i-really-mean-it
# Found 10 metadata orphan(s)
... cleanup happens ...

$ python3 rgw-orphan-cleaner.py --delete --yes-i-really-mean-it
# Found 3 metadata orphan(s)  (orphan instances revealed orphaned indexes)
... cleanup happens ...

$ python3 rgw-orphan-cleaner.py --delete --yes-i-really-mean-it
# No orphaned metadata or data found.
```

## Known Limitations

1. **Versioned buckets**: Script doesn't distinguish current vs. versioned object orphans.
2. **Stale instances**: The script is conservative with stale instance detection. Use `radosgw-admin reshard stale-instances list` for authoritative results.
3. **Large clusters**: While streaming prevents OOM, scanning data pools with billions of objects will take hours.

## Ceph's Built-in Alternatives

| Tool | Status | Use Case |
|------|--------|----------|
| `radosgw-admin orphans find` | ⚠️ Deprecated | Old official tool |
| `rgw-orphan-list` | ✅ Recommended | Official replacement for data orphans |
| This script | ✅ Custom | Metadata + data, tenant-aware, cross-zone |

### Orphans reappear after cleanup

In multisite setups, cleanup on one zone doesn't propagate automatically. Run the script on all zones separately.

---

# rgw-sync-gc.py

## RGW Sync Status Garbage Collector

A companion tool that cleans stale `bucket.sync-status.*` and `bucket.full-sync-status.*` objects from the RGW log pool. These accumulate when buckets are deleted and recreated in multisite deployments.

> **Note**: Unlike `rgw-orphan-cleaner.py` which handles metadata/data orphans, `rgw-sync-gc.py` only cleans **sync state** objects in the log pool.

## What It Cleans

| Object Type | OID Pattern | Condition for Deletion |
|-------------|-------------|-----------------------|
| Stale sync status | `bucket.sync-status.<zone>:<bucket>:<old_bucket_id>:<shard>` | `bucket_id` has no active entrypoint |
| Stale full sync status | `bucket.full-sync-status.<zone>:<bucket>:<old_bucket_id>` | `bucket_id` has no active entrypoint |
| Orphaned hints | `bucket.sync-source-hints.<bucket>` | Bucket no longer exists (with `--include-hints`) |

> **Important**: Only `bucket` **entrypoints** determine if a bucket is active. Orphaned `bucket.instance` metadata should be cleaned by `rgw-orphan-cleaner.py`, not this tool.

## Usage for Sync GC

### Detection Only (Dry Run)

```bash
# Basic scan
python3 rgw-sync-gc.py

# Only objects older than 7 days (safety buffer for ongoing operations)
python3 rgw-sync-gc.py --max-age-days 7

# Output to file
python3 rgw-sync-gc.py --output report.json
```

### Cleanup

```bash
# Interactive cleanup
python3 rgw-sync-gc.py --delete

# Automated cleanup with safety confirmation
python3 rgw-sync-gc.py --delete --yes-i-really-mean-it

# Cleanup with age threshold (recommended for production)
python3 rgw-sync-gc.py --delete --yes-i-really-mean-it --max-age-days 7
```

### Output Filtering

```bash
# Hide skipped (active) objects, show only stale
python3 rgw-sync-gc.py --no-skipped

# Show only summary line
python3 rgw-sync-gc.py --summary-only

# Combined: just the stale count
python3 rgw-sync-gc.py --no-skipped --summary-only
```

### Also Clean Hints

```bash
python3 rgw-sync-gc.py --delete --yes-i-really-mean-it --include-hints
```

## Output Format for Sync GC

```json
{
  "version": "1.0",
  "timestamp": "2026-05-27T11:10:00+00:00",
  "zone": "gva2b",
  "zone_id": "95829cfb-53a5-4a8f-8161-5de7f8fc32de",
  "log_pool": "gva2b.rgw.log",
  "config": {
    "max_age_days": 7,
    "include_hints": false
  },
  "summary": {
    "total_stale": 1305,
    "stale_sync_status": 761,
    "stale_full_sync_status": 544,
    "stale_hints": 0,
    "skipped": 50
  },
  "skipped_objects": [
    {
      "oid": "bucket.sync-status.32d...:ochaze:32d...75922.4:3",
      "type": "sync_status",
      "reason": "bucket still active"
    }
  ],
  "stale_objects": [
    {
      "oid": "bucket.sync-status.32d...:bucket1:32d...90476.2:7",
      "type": "sync_status",
      "reason": "bucket_id is inactive. Bucket no longer exists",
      "bucket_ids": ["32d...90476.2"],
      "bucket_names": ["f8e2d506.../bucket1"]
    }
  ]
}
```

**Design note:** Stale objects appear **last** in the JSON so they are easy to see when scrolling terminal output.

## Deployment Pattern for Sync GC

Run weekly on each secondary zone's mon node via cron:

```bash
# /etc/cron.d/rgw-sync-gc
0 2 * * 0 root cephadm shell -- python3 /root/rgw-sync-gc.py \
  --max-age-days 7 \
  --delete \
  --yes-i-really-mean-it \
  >> /var/log/rgw-sync-gc.log 2>&1
```

On containerized (cephadm) deployments, copy the script to a shared path or pass via stdin:

```bash
# Copy to host path that is bind-mounted into container
host# cp rgw-sync-gc.py /var/lib/ceph/<fsid>/home/
container# python3 /root/rgw-sync-gc.py ...

# Or pass via stdin
cat rgw-sync-gc.py | cephadm shell -- python3 - --summary-only
```

## Why Sync GC Is Needed

`rgw-orphan-cleaner.py` handles **metadata** orphans (bucket instances, entrypoints, index objects, data objects). `rgw-sync-gc.py` handles **sync state** orphans in the log pool. They are completely separate namespaces:

| Cleaner | Pool | Objects Cleaned |
|---------|------|------------------|
| `rgw-orphan-cleaner.py` | `*.rgw.meta`, `*.rgw.buckets.index`, `*.rgw.buckets.data` | `.bucket.meta.*`, `.dir.*`, data objects |
| `rgw-sync-gc.py` | `*.rgw.log` | `bucket.sync-status.*`, `bucket.full-sync-status.*`, hints |

Run **both** periodically for complete multisite hygiene.

---

# rgw-sync-repair.py

## RGW Sync Repair Tool

Diagnoses and fixes per-bucket sync marker issues in Ceph RGW multisite deployments where `bucket sync status` shows `"failed to read remote log"`.

> **Note**: This is a companion to `rgw-sync-gc.py`. GC cleans stale objects in the log pool; Repair fixes live sync markers that are pointing to invalid bilog positions.

## What It Fixes

When you delete and re-apply replication, or when buckets are recreated after deletion:

1. Old replication creates sync markers on the secondary zone
2. Deleting replication **does not** clean those markers
3. Re-applying replication creates a **new bucket instance** with a **new bilog**
4. The secondary zone's sync daemon tries to resume from its **old marker position**
5. Result: `failed to read remote log: (2) No such file or directory`

|`--check` detects|`--reset-sync` resolves|
|---|---|
|`failed_to_read_remote_log`|Runs `bucket sync init` + `bucket sync run` for the stuck source zone|
|`behind` on specific shards|Forces incremental sync to catch up|

## Usage for Sync Repair

### Check a Single Bucket

```bash
# Human-readable
python3 rgw-sync-repair.py --bucket tenant/bucket1 --check

# JSON output
python3 rgw-sync-repair.py --bucket tenant/bucket1 --check --json
```

### Reset Sync for a Stuck Bucket

```bash
# Check a bucket first
python3 rgw-sync-repair.py --bucket tenant/bucket1 --check

# Then reset sync if issues found
python3 rgw-sync-repair.py --bucket tenant/bucket1 --reset-sync

# Only target a specific source zone
python3 rgw-sync-repair.py --bucket tenant/bucket1 --reset-sync --source-zone gva2a
```

### Check All Buckets

```bash
# Check all buckets in the zone (read-only, slow on large deployments)
python3 rgw-sync-repair.py --check-all

# JSON for automation
python3 rgw-sync-repair.py --check-all --json
```

### Running Inside cephadm Container

```bash
# Inside container interactively
cp rgw-sync-repair.py /var/lib/ceph/<fsid>/home/
cephadm shell
# Inside container:
python3 /root/rgw-sync-repair.py --check --bucket tenant/bucket1
```

### Remote via SSH + mount

```bash
# Copy file and mount the cephadm home directory into container /root
scp rgw-sync-repair.py root@gva2b-object-cephmon-1:/var/lib/ceph/<fsid>/home/
ssh root@gva2b-object-cephmon-1 'cephadm shell --mount /var/lib/ceph/<fsid>/home/:/root -- python3 /root/rgw-sync-repair.py --check --bucket tenant/bucket1'

# Fix with same method
ssh root@gva2b-object-cephmon-1 'cephadm shell --mount /var/lib/ceph/<fsid>/home/:/root -- python3 /root/rgw-sync-repair.py   --reset-sync --bucket tenant/bucket1'
```

### Full Example for Production

```bash
# 1. Copy script to Ceph mon nodes
for node in gva2a-object-cephmon-1 gva2b-object-cephmon-1; do
  scp rgw-sync-repair.py root@$node:/var/lib/ceph/<fsid>/home/
done

# 2. Check a bucket across zones
ssh root@gva2b-object-cephmon-1 'cephadm shell --mount /var/lib/ceph/<fsid>/home/:/root -- python3 /root/rgw-sync-repair.py --check --bucket tenant/bucket1 --json'
```

> **Note**: Piping the script via stdin (`cat script.py | ssh ... cephadm shell -- python3 -`) does **not** work because `cephadm shell` stdio piping causes stdout to be lost. Always copy the script to `/var/lib/ceph/<fsid>/home/` and mount it, or run interactively inside the container.

## Output Format for Sync Repair

### Check Mode

```bash
$ python3 rgw-sync-repair.py --check --bucket f8e2d506252c4961ad0fa321abf1f1b5/bucket1

Bucket: f8e2d506252c4961ad0fa321abf1f1b5/bucket1
Status: failed
Total sources: 3
Healthy sources: 2
Issues: 1

Issues found:
  Zone: gva2a (32dac6d0-8eb2-48a1-bd1c-b218005172f7)
  Type: failed_to_read_remote_log
  Error: (2) No such file or directory
```

### Fix Mode

```bash
$ python3 rgw-sync-repair.py   --reset-sync --bucket f8e2d506252c4961ad0fa321abf1f1b5/bucket1

Bucket: f8e2d506252c4961ad0fa321abf1f1b5/bucket1
Status: fixed
Fixed: 1
Failed: 0

Fixed:
  Zone: gva2a
  Issue: failed_to_read_remote_log
  Init result: {'rc': 0}
  Run result: {'rc': 0}
```

### JSON Output

```json
{
  "timestamp": "2026-05-29T10:23:00+00:00",
  "bucket": "f8e2d506252c4961ad0fa321abf1f1b5/bucket1",
  "status": "failed",
  "total_sources": 3,
  "healthy_sources": 2,
  "issue_count": 1,
  "sources": [
    {
      "zone_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7",
      "zone_name": "gva2a",
      "status": "caught_up",
      "source_bucket": "f8e2d506252c4961ad0fa321abf1f1b5:bucket1",
      "bucket_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7.266814.1"
    },
    {
      "zone_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7",
      "zone_name": "gva2a",
      "status": "caught_up",
      "source_bucket": "f8e2d506252c4961ad0fa321abf1f1b5:bucket1",
      "bucket_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7.266814.1"
    },
    {
      "zone_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7",
      "zone_name": "gva2a",
      "status": "failed",
      "error_code": 2,
      "error_message": "No such file or directory",
      "source_bucket": null,
      "bucket_id": null
    }
  ],
  "issues": [
    {
      "zone_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7",
      "zone_name": "gva2a",
      "type": "failed_to_read_remote_log",
      "error_code": 2,
      "error_message": "No such file or directory"
    }
  ]
}
```

## Why Sync Repair Is Needed

After the sequence:
1. Upload objects to bucket (`rclone copy ...`)
2. Apply replication (`aws s3api put-bucket-replication ...`)
3. Delete and re-apply replication

Sync markers on the secondary zone keep pointing to the **old bilog**. The secondary's sync daemon is essentially reading from an expired bookmark. `rgw-sync-repair.py` resets that bookmark to the start of the new bilog.

> **Important**: `rgw-sync-gc.py` only removes **stale metadata objects** from the log pool (orphaned `bucket.sync-status.*` entries). It **does not** touch live sync markers. That's why a separate tool is needed.

## Comparison with Sync GC

| Tool | What It Does | When To Use | Pool Affected |
|------|-------------|-------------|---------------|
| `rgw-sync-gc.py` | Deletes stale `bucket.sync-status.*` and `bucket.full-sync-status.*` objects | After deleting/replacing replication configs; periodic cleanup | `*.rgw.log` |
| `rgw-sync-repair.py` | Resets live sync markers (`bucket sync init` + `bucket sync run`) | When `bucket sync status` shows `failed to read remote log` | Sync metadata (not RADOS objects) |

**Typical workflow:**
```bash
# 1. Clean stale metadata objects
python3 rgw-sync-gc.py --delete --yes-i-really-mean-it

# 2. Fix stuck sync markers on specific buckets
python3 rgw-sync-repair.py --check-all --json | jq '.results[] | select(.status=="failed") | .bucket'
# Then for each stuck bucket:
python3 rgw-sync-repair.py   --reset-sync --bucket tenant/stuck-bucket
```

**Exit Codes:**
- `0` — No issues found (check mode) or nothing to fix
- `1` — Issues found (check mode)
- `2` — Repairs were attempted (fix mode)
- `3` — Usage or runtime error
