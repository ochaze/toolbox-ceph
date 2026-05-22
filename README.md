# RGW Orphan Cleaner

EXPERIMENTAL, don´t pass `--delete` without manual reviewing.
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

### Pool / Namespace Map

| Object Type              | Pool                       | Namespace |
|--------------------------|----------------------------|-----------|
| Entrypoints              | `<zone>.rgw.meta`          | `root`    |
| Instances (.bucket.meta) | `<zone>.rgw.meta`          | `root`    |
| Index shards (.dir.*)   | `<zone>.rgw.buckets.index` | (empty)   |
| Data objects             | `<zone>.rgw.buckets.data`  | (empty)   |

### Orphan Detection Logic

1. **Orphan Instance** — `.bucket.meta` exists in `meta:root`, but no corresponding entrypoint in metadata API.
2. **Stale Instance** — entrypoint exists but points to a different `bucket_id` (reshard), confirmed with `reshard_status == DONE`. Skipped if `IN_PROGRESS` or `IN_LOGRECORD`.
3. **Orphan Index** — `.dir.` object in index pool, but no instance metadata for that `bucket_id`.
4. **Orphan Data** — data object in data pool, but no instance metadata for its `bucket_id`.

## Problem

In Ceph RGW multisite setups, orphaned metadata and data objects can accumulate when:
- Buckets are deleted but metadata cleanup fails
- Dynamic resharding creates stale instance objects
- Multisite sync doesn't propagate deletions consistently
- Garbage collection doesn't remove data objects
- ...

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
    }
  }
}
```

## How It Works
    "entrypoints": [...],
    "index": [...],
    "data": [
      {
        "type": "orphan_data",
        "bucket_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7.57962.17",
        "object_count": 15000,
        "pool": "gva2b.rgw.buckets.data"
      }
    ]
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
- **Reshard status checking**: The script reads the instance `reshard_status` field like Ceph does, and skips instances that are IN_PROGRESS or IN_LOGRECORD.
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
