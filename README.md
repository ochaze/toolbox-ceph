# RGW Orphan Cleaner

A comprehensive script to detect and clean orphaned Ceph RGW metadata and data objects across multisite deployments.

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
| **Stale Instances** | Old bucket instances after resharding | Entrypoint points to different bucket_id |
| **Orphan Index** | Index objects without bucket instance | Missing `.bucket.meta.` for `.dir.<>.*` |
| **Orphan Data** | Data objects without bucket metadata | Data pool objects with unknown bucket_id prefix |

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

# Non-interactive cleanup
python3 rgw-orphan-cleaner.py --delete --yes

# Full cleanup including data objects
python3 rgw-orphan-cleaner.py --delete --yes --data-pool
```

### Safety Options

```bash
# Only remove instances for tenants with no active users
python3 rgw-orphan-cleaner.py --inactive-tenants-only

# Verify bucket stats (skip buckets that still respond)
python3 rgw-orphan-cleaner.py --verify-active

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
    "stale_instances": [
      {
        "type": "stale_instance",
        "bucket_name": "ochaze",
        "bucket_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7.37737.1",
        "active_bucket_id": "32dac6d0-8eb2-48a1-bd1c-b218005172f7.50940.2",
        "oid": ".bucket.meta.ochaze:32...37737.1",
        "pool": "gva2b.rgw.meta",
        "namespace": "root",
        "reason": "entrypoint exists but points to different bucket_id"
      }
    ],
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

1. Lists all objects in data pool via `rados ls`
2. Extracts bucket_id from object name (`<bucket_id>_<key>`)
3. Compares against known bucket_ids from metadata
4. Flags orphaned data bucket IDs

### Performance

| Cluster Size | Metadata Scan | With Data Pool |
|-------------|---------------|----------------|
| Small (<1000 buckets) | < 1s | 1-10s |
| Medium (1000-100000) | 5-10s | 30-120s |
| Large (>100000) | 10-30s | 2+ min |


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

### Example 2: Find tenant-related false positives

```bash
# Shows 9400+ orphans but verifies them
$ python3 rgw-orphan-cleaner.py --verify-active

# Shows 0 orphans if all buckets are actually active
$ python3 rgw-orphan-cleaner.py --inactive-tenants-only
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
- **Confirmation prompt**: Requires user confirmation before deletion
- **Active bucket detection**: `--verify-active` checks if bucket still responds to stats
- **Tenant validation**: `--inactive-tenants-only` skips tenants with active users
- **Two-pass for consistency**: After deleting a stale instance, re-run to clean up index

## Known Limitations

3. **Versioned buckets**: Script doesn't distinguish current vs. versioned object orphans.

## Ceph's Built-in Alternatives

| Tool | Status | Use Case |
|------|--------|----------|
| `radosgw-admin orphans find` | ⚠️ Deprecated | Old official tool |
| `rgw-orphan-list` | ✅ Recommended | Official replacement for data orphans |
| This script | ✅ Custom | Metadata + data, tenant-aware, cross-zone |

### Orphans reappear after cleanup

In multisite setups, cleanup on one zone doesn't propagate automatically. Run the script on all zones separately.
