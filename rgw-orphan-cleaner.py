#!/usr/bin/env python3
"""
RGW Complete Orphan Cleaner

Detects and optionally cleans:
  - Orphan bucket instance metadata (instance without entrypoint, flags=0/2/34)
    → Race condition: entrypoint removed but BUCKET_DELETED never set.
      0  = active bucket
      2  = BUCKET_VERSIONED (active versioned bucket)
      34 = BUCKET_VERSIONED|BUCKET_OBJ_LOCK_ENABLED (versioned + lock)
      Requires manual cleanup.
  - Transient bucket instance metadata (instance without entrypoint, BUCKET_DELETED bit set in flags: 64/66/98)
    → BucketTrimInstanceCR will clean these up automatically. Optionally force-cleanup with --include-transient.
      64 = BUCKET_DELETED
      66 = BUCKET_DELETED|BUCKET_VERSIONED
      98 = BUCKET_DELETED|BUCKET_VERSIONED|BUCKET_OBJ_LOCK_ENABLED (deleted + versioned + lock)
  - Stale instances from resharding (entrypoint points elsewhere)
  - Orphan bucket index objects (index without known instance)
  - Orphan data objects (data without known bucket instance)

Usage:
    python3 rgw-orphan-cleaner.py                  # detection only
    python3 rgw-orphan-cleaner.py --delete         # cleanup after confirmation
    python3 rgw-orphan-cleaner.py --delete --yes-i-really-mean-it   # no prompt
    python3 rgw-orphan-cleaner.py --data-pool      # include data pool scan

Output: JSON report to stdout
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple


class RGWZone:
    """Auto-discovers RGW zone parameters via radosgw-admin."""

    def __init__(self):
        self.name: Optional[str] = None
        self.domain_root: Optional[str] = None
        self.index_pool: Optional[str] = None
        self.meta_pool: Optional[str] = None
        self.data_pool: Optional[str] = None
        self._discover()

    def _run(self, cmd: List[str]) -> Tuple[int, str, str]:
        """Run a shell command and return (rc, stdout, stderr)."""
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    def _discover(self):
        """Discover zone params from radosgw-admin zone get."""
        rc, out, err = self._run(["radosgw-admin", "zone", "get"])
        if rc != 0:
            raise RuntimeError(f"Failed to get zone info: {err.strip()}")

        zone_info = json.loads(out)
        self.name = zone_info.get("name", "unknown")

        # domain_root is usually "<zone>.rgw.meta:root"
        self.domain_root = zone_info.get("domain_root")
        if not self.domain_root:
            raise RuntimeError("Could not determine domain_root pool")

        # derive the base meta pool (e.g. "gva2b.rgw.meta")
        self.meta_pool = self.domain_root.split(":")[0]

        # index pool and data pool from placement pools
        # Look for 'default-placement' first, or the non-cache placement
        placement_pools = zone_info.get("placement_pools", {})
        if placement_pools:
            found_val = None
            if isinstance(placement_pools, dict):
                # Prefer 'default-placement', fall back to first key
                if "default-placement" in placement_pools:
                    found_val = placement_pools["default-placement"]
                else:
                    first_key = list(placement_pools.keys())[0]
                    found_val = placement_pools[first_key]
            elif isinstance(placement_pools, list):
                # Prefer element with key='default-placement', fall back to first
                for entry in placement_pools:
                    if isinstance(entry, dict) and entry.get("key") == "default-placement":
                        found_val = entry.get("val")
                        break
                if found_val is None and placement_pools:
                    first_entry = placement_pools[0]
                    if isinstance(first_entry, dict) and "val" in first_entry:
                        found_val = first_entry["val"]
                    elif isinstance(first_entry, dict):
                        found_val = first_entry

            if found_val and isinstance(found_val, dict):
                self.index_pool = found_val.get("index_pool")
                # Get data pool from storage classes
                storage_classes = found_val.get("storage_classes", {})
                if storage_classes:
                    first_sc = list(storage_classes.keys())[0]
                    self.data_pool = storage_classes[first_sc].get("data_pool")
                if not self.data_pool:
                    # Fallback: try standard naming
                    self.data_pool = f"{self.name}.rgw.buckets.data"
        if not self.index_pool:
            raise RuntimeError("Could not determine index pool")

        # Log pool for sync status entries
        self.log_pool = zone_info.get("log_pool")
        if not self.log_pool:
            # Fallback to standard naming
            self.log_pool = f"{self.name}.rgw.log"


class OrphanDetector:
    """Detects orphaned bucket metadata and data across RADOS pools."""

    def __init__(self, zone: RGWZone, verify_active: bool = False,
                 inactive_tenants_only: bool = False, scan_data_pool: bool = False,
                 start_period: Optional[datetime] = None,
                 end_period: Optional[datetime] = None):
        self.zone = zone
        self.verify_active = verify_active
        self.inactive_tenants_only = inactive_tenants_only
        self.scan_data_pool = scan_data_pool
        self.start_period = start_period
        self.end_period = end_period
        self.entrypoints: Dict[str, str] = {}       # bucket_name -> bucket_id
        self.instances: Dict[str, str] = {}         # bucket_id -> bucket_name
        self.index_objects: Dict[str, List[str]] = {}  # bucket_id -> [oid, ...]
        self.data_objects: Dict[str, int] = {}      # bucket_id -> count
        self.data_oid_sample: Dict[str, str] = {}   # bucket_id -> representative oid (for mtime)

        # Tracking from metadata API
        self.meta_entrypoints: Set[str] = set()
        self.meta_instances: Set[str] = set()

        # Tracking from RADOS listings
        self.rados_entrypoints: Set[str] = set()
        self.rados_instances: Dict[str, Dict[str, str]] = {}
        self.rados_index: Dict[str, List[str]] = {}

        # Active bucket IDs from metadata
        self.active_bucket_ids: Set[str] = set()

        # Cache for tenant verification
        self._active_tenants: Optional[Set[str]] = None
        self._bucket_stats_cache: Dict[str, bool] = {}

    def _run(self, cmd: List[str]) -> Tuple[int, str, str]:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    def _metadata_list(self, section: str) -> List[str]:
        """List keys via radosgw-admin metadata list <section>."""
        rc, out, err = self._run(["radosgw-admin", "metadata", "list", section])
        if rc != 0:
            print(json.dumps({"error": f"metadata list {section} failed: {err.strip()}"}))
            sys.exit(1)
        return json.loads(out) if out.strip() else []

    def _rados_ls_streaming(self, pool: str, namespace: str = ""):
        """Stream objects from a RADOS pool without loading all into memory.

        Uses subprocess.Popen to yield objects one at a time, avoiding OOM
        on pools with billions of objects.
        """
        cmd = ["rados", "-p", pool]
        if namespace:
            cmd += ["-N", namespace]
        cmd += ["ls"]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        try:
            for line in proc.stdout:
                line = line.strip()
                if line:
                    yield line
        finally:
            proc.stdout.close()
            proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(
                    f"rados ls failed for {pool}/{namespace}: return code {proc.returncode}"
                )

    def _rados_ls(self, pool: str, namespace: str = "") -> List[str]:
        """List objects in a RADOS pool/namespace.

        NOTE: For large pools (data pool), prefer _rados_ls_streaming()
        to avoid out-of-memory errors.
        """
        return list(self._rados_ls_streaming(pool, namespace))

    def _rados_stat(self, pool: str, namespace: str, oid: str) -> Optional[datetime]:
        """Return the mtime (as a timezone-aware UTC datetime) of a RADOS object, or None."""
        # Prefer JSON format for reliable parsing (Ceph >=14.x).
        cmd_json = ["rados", "-p", pool]
        if namespace:
            cmd_json += ["-N", namespace]
        cmd_json += ["stat", "--format=json", oid]
        rc, out, _ = self._run(cmd_json)
        if rc == 0 and out.strip():
            try:
                data = json.loads(out)
                mtime = data.get("mtime")
                if mtime:
                    # JSON mtime typically looks like "2024-01-15T10:30:00.000000Z"
                    # Handle ISO 8601 with timezone
                    mtime = mtime.replace("Z", "+00:00")
                    return datetime.fromisoformat(mtime)
            except (json.JSONDecodeError, ValueError):
                pass

        # Fallback: text format (e.g. "mtime: Mon Jan 15 10:30:00 2024")
        cmd = ["rados", "-p", pool]
        if namespace:
            cmd += ["-N", namespace]
        cmd += ["stat", oid]
        rc, out, _ = self._run(cmd)
        if rc != 0:
            return None
        for line in out.splitlines():
            if "mtime" in line:
                raw = line.split("mtime", 1)[-1].strip().lstrip(":")
                raw = raw.split(", size")[0].strip()
                for fmt in (
                    "%Y-%m-%dT%H:%M:%S.%f%z",
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%a %b %d %H:%M:%S %Y",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S",
                ):
                    try:
                        dt = datetime.strptime(raw, fmt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return dt.astimezone(timezone.utc)
                    except ValueError:
                        continue
        return None

    def _is_oid_in_time_period(self, pool: str, namespace: str, oid: str) -> Tuple[bool, Optional[str]]:
        """Check if a RADOS object's mtime falls within the configured time period.

        Returns (True, None) if the object is within the period.
        Returns (False, reason_str) if the object is outside the period or mtime could not be determined.
        """
        if self.start_period is None and self.end_period is None:
            return True, None
        mtime = self._rados_stat(pool, namespace, oid)
        return self._in_time_period(mtime)

    def _in_time_period(self, dt: Optional[datetime]) -> Tuple[bool, Optional[str]]:
        """Check if a datetime falls within the configured start/end period.

        Returns (True, None) if the datetime is within the period or no period is configured.
        Returns (False, reason_str) if the datetime is outside the period or unknown when a period is configured.
        """
        if self.start_period is None and self.end_period is None:
            return True, None
        if dt is None:
            return False, "mtime could not be determined, skipping for safety"
        if self.start_period and dt < self.start_period:
            return False, "outside specified time period"
        if self.end_period and dt > self.end_period:
            return False, "outside specified time period"
        return True, None

    def _parse_instance_oid(self, oid: str) -> Optional[Tuple[str, str, str]]:
        """Parse .bucket.meta OID into (tenant, bucket_name, bucket_id)."""
        if not oid.startswith(".bucket.meta."):
            return None
        rest = oid[len(".bucket.meta."):]
        last_colon = rest.rfind(":")
        if last_colon == -1:
            return None
        bucket_id = rest[last_colon + 1:]
        bucket_part = rest[:last_colon]
        first_colon = bucket_part.find(":")
        if first_colon != -1:
            tenant = bucket_part[:first_colon]
            bucket_name = bucket_part[first_colon + 1:]
        else:
            tenant = ""
            bucket_name = bucket_part
        return (tenant, bucket_name, bucket_id)

    def _rados_entrypoint_name(self, tenant: str, bucket_name: str) -> str:
        if tenant:
            return f"{tenant}/{bucket_name}"
        return bucket_name

    def _get_entrypoint_bucket_id(self, ep_name: str) -> Optional[str]:
        """Read entrypoint metadata to get current active bucket_id."""
        rc, out, err = self._run(
            ["radosgw-admin", "metadata", "get", f"bucket:{ep_name}"]
        )
        if rc != 0:
            return None
        try:
            data = json.loads(out)
            return data.get("data", {}).get("bucket", {}).get("bucket_id")
        except (json.JSONDecodeError, AttributeError):
            return None

    def _get_instance_reshard_status(self, bucket_id: str, ep_name: str = None) -> Optional[int]:
        """Read bucket instance metadata to check reshard status.

        Args:
            bucket_id: the bucket instance id
            ep_name: entrypoint name (tenant/bucket or bucket), used to build metadata key.
                     Tenant separator '/' is converted to ':' for the metadata key.

        Returns:
            0: NOT_RESHARDING
            1: IN_PROGRESS
            2: DONE
            3: IN_LOGRECORD
            None: error reading instance
        """
        # Build bucket.instance key: "tenant:bucket:bucket_id" or "bucket:bucket_id"
        if ep_name:
            # ep_name uses '/' to separate tenant, but metadata key uses ':'
            instance_name = ep_name.replace("/", ":", 1)
        else:
            instance_name = self.instances.get(bucket_id, bucket_id)

        instance_key = f"bucket.instance:{instance_name}:{bucket_id}"

        rc, out, err = self._run(
            ["radosgw-admin", "metadata", "get", instance_key]
        )
        if rc != 0:
            return None
        try:
            data = json.loads(out)
            return data.get("data", {}).get("bucket_info", {}).get("reshard_status")
        except (json.JSONDecodeError, AttributeError):
            return None

    def _get_instance_flags(self, bucket_id: str, ep_name: str = None) -> Optional[int]:
        """Read bucket instance metadata to check bucket_info flags.

        Args:
            bucket_id: the bucket instance id
            ep_name: entrypoint name (tenant/bucket or bucket), used to build metadata key.
                     Tenant separator '/' is converted to ':' for the metadata key.

        Returns:
            The flags integer value (e.g. 0, 34=BUCKET_VERSIONED|BUCKET_OBJ_LOCK_ENABLED,
            64=BUCKET_DELETED, 66=BUCKET_DELETED|BUCKET_VERSIONED,
            98=BUCKET_DELETED|BUCKET_VERSIONED|BUCKET_OBJ_LOCK_ENABLED), or None on error.
        """
        if ep_name:
            instance_name = ep_name.replace("/", ":", 1)
        else:
            instance_name = self.instances.get(bucket_id, bucket_id)

        instance_key = f"bucket.instance:{instance_name}:{bucket_id}"

        rc, out, err = self._run(
            ["radosgw-admin", "metadata", "get", instance_key]
        )
        if rc != 0:
            return None
        try:
            data = json.loads(out)
            return data.get("data", {}).get("bucket_info", {}).get("flags")
        except (json.JSONDecodeError, AttributeError):
            return None

    def _get_active_tenants(self) -> Set[str]:
        if self._active_tenants is not None:
            return self._active_tenants
        self._active_tenants = set()
        rc, out, err = self._run(["radosgw-admin", "user", "list"])
        if rc == 0:
            users = json.loads(out)
            for user in users:
                if "$" in user:
                    parts = user.split("$")
                    if len(parts) == 2:
                        self._active_tenants.add(parts[0])
        return self._active_tenants

    def _check_bucket_stats(self, tenant: str, bucket_name: str) -> bool:
        cache_key = f"{tenant}:{bucket_name}"
        if cache_key in self._bucket_stats_cache:
            return self._bucket_stats_cache[cache_key]
        bucket_arg = f"{tenant}/{bucket_name}" if tenant else bucket_name
        rc, _, _ = self._run(
            ["radosgw-admin", "bucket", "stats", "--bucket", bucket_arg]
        )
        is_active = (rc == 0)
        self._bucket_stats_cache[cache_key] = is_active
        return is_active

    def _is_safe_to_remove(self, info: Dict[str, str]) -> Tuple[bool, str]:
        tenant = info["tenant"]
        bucket_name = info["bucket"]
        if self.verify_active or self.inactive_tenants_only:
            is_active = self._check_bucket_stats(tenant, bucket_name)
            if is_active and self.verify_active:
                return False, "bucket stats succeeded - bucket may still be active"
            if self.inactive_tenants_only:
                active_tenants = self._get_active_tenants()
                if tenant and tenant in active_tenants:
                    return False, f"tenant '{tenant}' still has active users"
        return True, "safe to remove - no active tenant or bucket found"

    def _extract_bucket_id_from_data_oid(self, oid: str) -> Optional[str]:
        """Extract bucket_id from data pool object name.

        Format: <bucket_id>_<object_key>
        bucket_id format: <zone_id>.<number>.<number>
        """
        # Match pattern like: 32dac6d0-8eb2-48a1-bd1c-b218005172f7.57962.17_proc-...
        match = re.match(r"^([a-f0-9-]+\.\d+\.\d+)_.*", oid)
        if match:
            return match.group(1)
        return None

    def discover(self):
        """Phase 1: collect all metadata and RADOS objects."""

        # 1a. Metadata API: entrypoints
        for ep in self._metadata_list("bucket"):
            self.meta_entrypoints.add(ep)

        # 1b. Metadata API: instances
        # Track all instances and count per bucket_name
        instance_counts: Dict[str, int] = {}
        for inst in self._metadata_list("bucket.instance"):
            self.meta_instances.add(inst)
            colon_count = inst.count(":")
            if colon_count >= 2:
                parts = inst.rsplit(":", 1)
                bucket_id = parts[1]
                tenant_bucket = parts[0]
                bucket_name = tenant_bucket.replace(":", "/", 1)
            else:
                parts = inst.rsplit(":", 1)
                if len(parts) == 2:
                    bucket_name = parts[0]
                    bucket_id = parts[1]
                else:
                    continue
            self.instances[bucket_id] = bucket_name
            instance_counts[bucket_name] = instance_counts.get(bucket_name, 0) + 1

        # OPTIMIZATION: Build reverse mapping bucket_name -> bucket_id for single-instance buckets
        # This avoids the O(n²) inner loop
        single_instance_map: Dict[str, str] = {}
        for bid, bname in self.instances.items():
            # Only add if this bucket_name hasn't been seen yet
            # (If it has multiple instances, it won't be in this map)
            if bname not in instance_counts:
                continue
            if instance_counts[bname] == 1:
                single_instance_map[bname] = bid

        for ep_name in self.meta_entrypoints:
            if instance_counts.get(ep_name, 0) > 1:
                # Read actual entrypoint to get correct active bucket_id
                bucket_id = self._get_entrypoint_bucket_id(ep_name)
                if bucket_id:
                    self.entrypoints[ep_name] = bucket_id
                    self.active_bucket_ids.add(bucket_id)
            elif ep_name in single_instance_map:
                # Single instance bucket - use the instance we found (O(1) lookup!)
                bid = single_instance_map[ep_name]
                self.entrypoints[ep_name] = bid
                self.active_bucket_ids.add(bid)

        # Also add all known instances
        for bucket_id in self.instances:
            self.active_bucket_ids.add(bucket_id)

        # 1c. RADOS: domain_root objects
        rados_objs = self._rados_ls(self.zone.meta_pool, "root")
        for oid in rados_objs:
            parsed = self._parse_instance_oid(oid)
            if parsed:
                tenant, bucket_name, bucket_id = parsed
                full_name = self._rados_entrypoint_name(tenant, bucket_name)
                self.rados_instances[bucket_id] = {
                    "tenant": tenant,
                    "bucket": bucket_name,
                    "ep_name": full_name,
                    "oid": oid,
                }
            else:
                self.rados_entrypoints.add(oid)

        # Add RADOS instances to active IDs
        for bucket_id in self.rados_instances:
            self.active_bucket_ids.add(bucket_id)

        # 1e. RADOS: index pool
        index_objs = self._rados_ls(self.zone.index_pool)
        for oid in index_objs:
            if not oid.startswith(".dir."):
                continue
            rest = oid[len(".dir.") :]
            parts = rest.split(".")
            bucket_id = None
            for i in range(len(parts), 0, -1):
                candidate = ".".join(parts[:i])
                if candidate in self.instances or candidate in self.rados_instances:
                    bucket_id = candidate
                    break
            if not bucket_id:
                if len(parts) >= 2 and parts[-1].isdigit():
                    bucket_id = ".".join(parts[:-1])
                else:
                    bucket_id = rest
            if bucket_id not in self.index_objects:
                self.index_objects[bucket_id] = []
            self.index_objects[bucket_id].append(oid)

        # 1f. RADOS: data pool (if requested)
        if self.scan_data_pool and self.zone.data_pool:
            print(f"# Scanning data pool {self.zone.data_pool}...", file=sys.stderr)
            count = 0
            try:
                for oid in self._rados_ls_streaming(self.zone.data_pool):
                    count += 1
                    bucket_id = self._extract_bucket_id_from_data_oid(oid)
                    if bucket_id:
                        self.data_objects[bucket_id] = self.data_objects.get(bucket_id, 0) + 1
                        # Keep a sample OID per bucket_id for potential mtime lookup
                        if bucket_id not in self.data_oid_sample:
                            self.data_oid_sample[bucket_id] = oid
            except RuntimeError as e:
                print(json.dumps({"error": str(e)}))
                sys.exit(1)
            print(f"# Processed {count} data objects", file=sys.stderr)

    def detect(self) -> Dict:
        """Phase 2: cross-reference and detect all orphans."""

        orphan_instances = []
        transient_instances = []
        stale_instances = []
        skipped_instances = []
        orphan_entrypoints = []
        orphan_index = []
        orphan_data: Dict[str, Dict] = {}

        # Instance metadata in RADOS
        for bucket_id, info in self.rados_instances.items():
            ep_name = info["ep_name"]

            if ep_name in self.rados_entrypoints or ep_name in self.meta_entrypoints:
                active_id = self.entrypoints.get(ep_name)
                if active_id and bucket_id != active_id:
                    # entrypoint exists but points to different bucket_id - stale instance
                    reshard_status = self._get_instance_reshard_status(bucket_id, ep_name)
                    if reshard_status is None:
                        # Can't read instance - skip safely
                        skipped_instances.append(
                            {
                                "type": "skipped_stale_instance",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "active_bucket_id": active_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "reason": "could not read instance metadata to verify reshard status",
                            }
                        )
                        continue

                    # Ceph: cls_rgw_reshard_status
                    # 0=NOT_RESHARDING, 1=IN_PROGRESS, 2=DONE, 3=IN_LOGRECORD
                    if reshard_status == 1:  # IN_PROGRESS
                        skipped_instances.append(
                            {
                                "type": "skipped_stale_instance",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "active_bucket_id": active_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "reason": "reshard is IN_PROGRESS - deleting now would corrupt the bucket",
                            }
                        )
                        continue

                    if reshard_status == 3:  # IN_LOGRECORD
                        skipped_instances.append(
                            {
                                "type": "skipped_stale_instance",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "active_bucket_id": active_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "reason": "reshard is IN_LOGRECORD - background sync may still need this instance",
                            }
                        )
                        continue

                    if reshard_status == 0:  # NOT_RESHARDING
                        # Default state for buckets. Old instances may be abandoned
                        # after delete/recreate cycles (e.g. S3 replication changes).
                        # Safe to delete if entrypoint no longer references this bucket_id.
                        in_period, skip_reason = self._is_oid_in_time_period(self.zone.meta_pool, "root", info["oid"])
                        if not in_period:
                            skipped_instances.append({
                                "type": "skipped_stale_instance",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "active_bucket_id": active_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "reason": skip_reason,
                            })
                            continue
                        stale_instances.append({
                            "bucket_name": ep_name,
                            "bucket_id": bucket_id,
                            "active_bucket_id": active_id,
                            "oid": info["oid"],
                            "pool": self.zone.meta_pool,
                            "namespace": "root",
                            "tenant": info["tenant"],
                        })
                        continue

                    if reshard_status == 2:  # DONE
                        # Reshard completed - instance is safe to flag as stale
                        in_period, skip_reason = self._is_oid_in_time_period(self.zone.meta_pool, "root", info["oid"])
                        if not in_period:
                            skipped_instances.append({
                                "type": "skipped_stale_instance",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "active_bucket_id": active_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "reason": skip_reason or "outside specified time period",
                            })
                            continue
                        stale_instances.append(
                            {
                                "type": "stale_instance",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "active_bucket_id": active_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "reason": f"entrypoint exists but points to different bucket_id ({active_id}). Reshard status: DONE.",
                            }
                        )
            else:
                is_safe, reason = self._is_safe_to_remove(info)
                if not is_safe:
                    entry = {
                        "type": "skipped_instance",
                        "bucket_name": ep_name,
                        "bucket_id": bucket_id,
                        "oid": info["oid"],
                        "pool": self.zone.meta_pool,
                        "namespace": "root",
                        "tenant": info["tenant"],
                        "reason": f"Safety check failed: {reason}",
                    }
                    skipped_instances.append(entry)
                else:
                    in_period, skip_reason = self._is_oid_in_time_period(self.zone.meta_pool, "root", info["oid"])
                    if not in_period:
                        entry = {
                            "type": "skipped_instance",
                            "bucket_name": ep_name,
                            "bucket_id": bucket_id,
                            "oid": info["oid"],
                            "pool": self.zone.meta_pool,
                            "namespace": "root",
                            "tenant": info["tenant"],
                            "reason": skip_reason or "outside specified time period",
                        }
                        skipped_instances.append(entry)
                    else:
                        # Check if BUCKET_DELETED bit is set (bit 6 = 64)
                        # flags: 64=BUCKET_DELETED, 66=BUCKET_DELETED|BUCKET_VERSIONED,
                        #        98=BUCKET_DELETED|BUCKET_VERSIONED|BUCKET_OBJ_LOCK_ENABLED
                        flags = self._get_instance_flags(bucket_id, ep_name)
                        if flags is not None and flags & 64:
                            # BUCKET_DELETED bit set - BucketTrimInstanceCR will clean this up
                            transient_instances.append({
                                "type": "transient_instance_marked",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "flags": flags,
                                "reason": "BucketTrimInstanceCR will clean this up (BUCKET_DELETED bit set in flags: 64/66/98)",
                            })
                        elif flags == 0 or flags == 2 or flags == 34:
                            # Race condition: entrypoint removed but flags never set
                            # 0=active bucket, 2=BUCKET_VERSIONED (active versioned), 34=versioned+lock
                            orphan_instances.append({
                                "type": "orphan_instance_race",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "flags": flags,
                                "reason": "Race condition: entrypoint removed but flags shows active (0/2=active, 34=versioned+lock active). BUCKET_DELETED never set",
                            })
                        else:
                            # Unknown or unexpected flags value
                            orphan_instances.append({
                                "type": "orphan_instance",
                                "bucket_name": ep_name,
                                "bucket_id": bucket_id,
                                "oid": info["oid"],
                                "pool": self.zone.meta_pool,
                                "namespace": "root",
                                "tenant": info["tenant"],
                                "flags": flags,
                                "reason": f"Unexpected flags value ({flags}) on orphan instance (expected: 0/2=active, 34=versioned+lock, 64/66/98=deleted)",
                            })

        # Entrypoint in RADOS but no instance
        for ep in self.rados_entrypoints:
            if ep not in self.meta_entrypoints:
                bucket_id = self.entrypoints.get(ep)
                if not bucket_id or bucket_id not in self.rados_instances:
                    in_period, skip_reason = self._is_oid_in_time_period(self.zone.meta_pool, "root", ep)
                    if not in_period:
                        skipped_instances.append({
                            "type": "skipped_entrypoint",
                            "bucket_name": ep,
                            "oid": ep,
                            "pool": self.zone.meta_pool,
                            "namespace": "root",
                            "reason": skip_reason or "outside specified time period",
                        })
                        continue
                    orphan_entrypoints.append(
                        {
                            "type": "orphan_entrypoint",
                            "bucket_name": ep,
                            "oid": ep,
                            "pool": self.zone.meta_pool,
                            "namespace": "root",
                            "reason": "entrypoint object exists but no instance metadata found",
                        }
                    )

        # Index objects without known instance
        for bucket_id, oids in self.index_objects.items():
            if (
                bucket_id not in self.instances
                and bucket_id not in self.rados_instances
            ):
                for oid in oids:
                    in_period, skip_reason = self._is_oid_in_time_period(self.zone.index_pool, "", oid)
                    if not in_period:
                        skipped_instances.append({
                            "type": "skipped_index",
                            "bucket_id": bucket_id,
                            "oid": oid,
                            "pool": self.zone.index_pool,
                            "namespace": "",
                            "reason": skip_reason or "outside specified time period",
                        })
                        continue
                    orphan_index.append(
                        {
                            "type": "orphan_index",
                            "bucket_id": bucket_id,
                            "oid": oid,
                            "pool": self.zone.index_pool,
                            "namespace": "",
                            "reason": "index object exists but no bucket instance metadata found",
                        }
                    )

        # Data objects without known bucket instance
        if self.scan_data_pool:
            for bucket_id, count in self.data_objects.items():
                if bucket_id not in self.active_bucket_ids:
                    sample_oid = self.data_oid_sample.get(bucket_id)
                    if sample_oid:
                        in_period, skip_reason = self._is_oid_in_time_period(self.zone.data_pool, "", sample_oid)
                        if not in_period:
                            skipped_instances.append({
                                "type": "skipped_data",
                                "bucket_id": bucket_id,
                                "object_count": count,
                                "pool": self.zone.data_pool,
                                "namespace": "",
                                "reason": skip_reason or "outside specified time period",
                            })
                            continue
                    orphan_data[bucket_id] = {
                        "type": "orphan_data",
                        "bucket_id": bucket_id,
                        "object_count": count,
                        "pool": self.zone.data_pool,
                        "namespace": "",
                        "reason": "data objects exist but no bucket instance metadata found",
                    }

        return {
            "zone": self.zone.name,
            "meta_pool": self.zone.meta_pool,
            "index_pool": self.zone.index_pool,
            "data_pool": self.zone.data_pool,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "safety_checks": {
                "verify_active": self.verify_active,
                "inactive_tenants_only": self.inactive_tenants_only,
                "scan_data_pool": self.scan_data_pool,
                "detect_stale": True,
                "time_period": {
                    "start_period_utc": self.start_period.isoformat() if self.start_period else None,
                    "end_period_utc": self.end_period.isoformat() if self.end_period else None,
                },
            },
            "orphans": {
                "transient_instances": transient_instances,
                "instances": orphan_instances,
                "stale_instances": stale_instances,
                "entrypoints": orphan_entrypoints,
                "index": orphan_index,
                "data": list(orphan_data.values()),
            },
            "skipped": skipped_instances,
            "summary": {
                "total_orphans": len(orphan_instances)
                + len(stale_instances)
                + len(orphan_entrypoints)
                + len(orphan_index),
                "total_transient_instances": len(transient_instances),
                "total_data_orphans": len(orphan_data),
                "total_data_orphan_objects": sum(
                    d["object_count"] for d in orphan_data.values()
                ),
                "orphan_instances": len(orphan_instances),
                "transient_instances": len(transient_instances),
                "stale_instances": len(stale_instances),
                "skipped_instances": len(skipped_instances),
                "orphan_entrypoints": len(orphan_entrypoints),
                "orphan_index": len(orphan_index),
                "orphan_data_buckets": len(orphan_data),
                "total_entrypoints": len(self.rados_entrypoints),
                "total_instances": len(self.rados_instances),
                "total_index_objects": sum(len(v) for v in self.index_objects.values()),
                "total_data_objects": sum(self.data_objects.values())
                if self.scan_data_pool
                else 0,
            },
        }


class OrphanCleaner:
    """Handles safe removal of detected orphan objects."""

    def __init__(self, zone: RGWZone):
        self.zone = zone
        self.removed = []
        self.failed = []

    def _run(self, cmd: List[str]) -> Tuple[int, str, str]:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    def _rados_ls_streaming(self, pool: str, namespace: str = ""):
        proc = subprocess.Popen(
            ["rados", "-p", pool, "-N", namespace, "ls"]
            if namespace
            else ["rados", "-p", pool, "ls"],
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            for line in proc.stdout:
                line = line.strip()
                if line:
                    yield line
        finally:
            proc.stdout.close()
            proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(
                    f"rados ls failed for {pool}/{namespace or ''}: return code {proc.returncode}"
                )

    def remove(self, item: Dict, dry_run: bool = True) -> bool:
        oid = item["oid"]
        pool = item["pool"]
        ns = item.get("namespace", "")

        cmd = ["rados", "-p", pool]
        if ns:
            cmd += ["-N", ns]
        cmd += ["rm", oid]

        if dry_run:
            return True

        rc, out, err = self._run(cmd)
        if rc == 0:
            self.removed.append(item)
            return True
        else:
            item["error"] = err.strip()
            self.failed.append(item)
            return False

    def stream_remove_by_prefix(
        self, bucket_ids: Set[str], pool: str, dry_run: bool = True
    ) -> Dict[str, Tuple[int, int]]:
        """Remove all data objects matching any bucket_id prefix in a single pass.

        Streams the pool once, avoiding O(n²) repeated full scans.
        Returns {bucket_id: (removed_count, failed_count)}.
        """
        results: Dict[str, Tuple[int, int]] = {bid: (0, 0) for bid in bucket_ids}
        total_matched = 0

        print(f"# Streaming data pool {pool} for cleanup...", file=sys.stderr)

        try:
            for oid in self._rados_ls_streaming(pool):
                for bucket_id in bucket_ids:
                    if oid.startswith(bucket_id + "_"):
                        total_matched += 1
                        if dry_run:
                            removed, failed = results[bucket_id]
                            results[bucket_id] = (removed + 1, failed)
                        else:
                            rc, _, err = self._run(["rados", "-p", pool, "rm", oid])
                            removed, failed = results[bucket_id]
                            if rc == 0:
                                results[bucket_id] = (removed + 1, failed)
                            else:
                                results[bucket_id] = (removed, failed + 1)
                        # Report progress periodically
                        if total_matched % 10000 == 0:
                            total_removed = sum(r[0] for r in results.values())
                            total_failed = sum(r[1] for r in results.values())
                            print(
                                f"#   Progress: {total_matched} objects matched, "
                                f"removed: {total_removed}, failed: {total_failed}",
                                file=sys.stderr,
                            )
                        break
        except RuntimeError as e:
            print(json.dumps({"error": str(e)}))
            sys.exit(1)

        total_removed = sum(r[0] for r in results.values())
        total_failed = sum(r[1] for r in results.values())
        print(
            f"#   Done: {total_matched} objects matched, "
            f"removed: {total_removed}, failed: {total_failed}",
            file=sys.stderr,
        )

        return results


def _parse_sync_log_oid(oid: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract (bucket_name, bucket_id) from a sync log pool OID.

    Expected formats:
      bucket.sync-status.<zone_id>:<tenant>/<bucket>:<bucket_id>:<shard>
      bucket.full-sync-status.<zone_id>:<tenant>/<bucket>:<bucket_id>
      (tenant may be empty)
    """
    # Strip the prefix
    if oid.startswith("bucket.sync-status."):
        rest = oid[len("bucket.sync-status."):]
    elif oid.startswith("bucket.full-sync-status."):
        rest = oid[len("bucket.full-sync-status."):]
    else:
        return None, None

    # Bucket ID is reliably identifiable: UUID-like component with two numeric suffixes
    match = re.search(r'([a-f0-9-]+\.\d+\.\d+)', rest)
    if not match:
        return None, None
    bucket_id = match.group(1)

    # Everything before the bucket_id contains the bucket name
    prefix = rest[:match.start()]
    # Drop the zone_id portion (everything up to first ':')
    first_colon = prefix.find(':')
    if first_colon == -1:
        return None, None
    bucket_context = prefix[first_colon + 1:]
    # Trim leading/trailing ':' artifacts from empty tenant (::)
    bucket_context = bucket_context.lstrip(':').rstrip(':')
    # Extract bucket name after tenant separator
    if '/' in bucket_context:
        bucket_name = bucket_context.split('/', 1)[1]
    else:
        bucket_name = bucket_context
    return bucket_name, bucket_id


def check_sync_logs(
    zone: RGWZone,
    known_instances: Set[str],
    known_entrypoints: Set[str],
    delete: bool = False,
) -> Tuple[int, List[str], List[Dict]]:
    """Check for stale bucket.sync-status and sync-hint entries in the log pool.

    Returns (count, list_of_stale_oids, detected_items).
    If delete=True, actually removes the stale entries via rados rm.
    An entry is considered stale only when BOTH the instance AND the entrypoint
    are absent from known_instances / known_entrypoints — this avoids flagging
    sync logs for buckets that have been deleted and recreated with the same name.
    """
    detected = []
    stale_oids = []
    try:
        proc = subprocess.Popen(
            ["rados", "-p", zone.log_pool, "ls"],
            stdout=subprocess.PIPE,
            text=True,
        )
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue

            is_stale = False
            bucket_name = None
            bucket_id = None

            if line.startswith(("bucket.sync-status.", "bucket.full-sync-status.")):
                bucket_name, bucket_id = _parse_sync_log_oid(line)
                if not bucket_name and not bucket_id:
                    continue

                # Safety: only consider stale when both instance AND entrypoint are gone
                has_instance = bucket_id and bucket_id in known_instances
                has_entrypoint = bucket_name and bucket_name in known_entrypoints

                if not has_instance and not has_entrypoint:
                    is_stale = True

            elif line.startswith(("bucket.sync-source-hints.", "bucket.sync-target-hints.")):
                # Format: bucket.sync-source-hints.<tenant>/<bucket>
                # or bucket.sync-source-hints.<bucket> (no tenant)
                if line.startswith("bucket.sync-source-hints."):
                    rest = line[len("bucket.sync-source-hints."):]
                else:
                    rest = line[len("bucket.sync-target-hints."):]

                if "/" in rest:
                    tenant, bucket_name = rest.split("/", 1)
                else:
                    tenant = ""
                    bucket_name = rest

                ep_name = f"{tenant}/{bucket_name}" if tenant else bucket_name
                if ep_name not in known_entrypoints:
                    is_stale = True
            else:
                continue

            if not is_stale:
                continue

            stale_oids.append(line)
            item: Dict[str, str] = {"oid": line, "pool": zone.log_pool, "status": "detected"}
            if bucket_id:
                item["bucket_id"] = bucket_id
            if bucket_name:
                item["bucket_name"] = bucket_name

            if delete:
                proc = subprocess.run(
                    ["rados", "-p", zone.log_pool, "rm", line],
                    capture_output=True,
                    text=True,
                )
                rc = proc.returncode
                err = proc.stderr
                if rc == 0:
                    item["status"] = "removed"
                else:
                    item["status"] = "failed"
                    item["error"] = err.strip()

            detected.append(item)

        proc.stdout.close()
        proc.wait()
    except Exception as e:
        # Log pool access failure is non-fatal
        print(f"# WARNING: Could not scan log pool {zone.log_pool}: {e}", file=sys.stderr)

    return len(stale_oids), stale_oids, detected


def print_report(report: Dict):
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="RGW Complete Orphan Cleaner"
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        default=False,
        help="Enable deletion mode"
    )
    parser.add_argument(
        "--yes-i-really-mean-it",
        action="store_true",
        default=False,
        help="Skip interactive confirmation (like Ceph admin commands)"
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Output file path"
    )
    parser.add_argument(
        "--data-pool",
        action="store_true",
        default=False,
        help="Scan data pool for orphan objects"
    )
    parser.add_argument(
        "--verify-active",
        action="store_true",
        default=False,
        help="Verify bucket stats before flagging as orphan"
    )
    parser.add_argument(
        "--inactive-tenants-only",
        action="store_true",
        default=False,
        help="Only remove instances for tenants with no active users"
    )
    parser.add_argument(
        "--delete-stale",
        action="store_true",
        default=False,
        help="DANGEROUS: Allow deletion of stale bucket instances from resharding. Only delete instances with reshard_status=DONE that are outside any active reshard window. (use with --yes-i-really-mean-it)"
    )
    parser.add_argument(
        "--include-transient",
        action="store_true",
        default=False,
        help="Include transient instances (BUCKET_DELETED bit set in flags: 64/66/98) in cleanup. By default these are reported but skipped as BucketTrimInstanceCR will clean them up."
    )
    parser.add_argument(
        "--start-period-utc",
        type=str,
        default=None,
        help="Only include orphans/objects modified on or after this UTC time (ISO 8601, e.g. 2024-01-01T00:00:00Z)"
    )
    parser.add_argument(
        "--end-period-utc",
        type=str,
        default=None,
        help="Only include orphans/objects modified up to this UTC time (ISO 8601, e.g. 2024-12-31T23:59:59Z)"
    )
    parser.add_argument(
        "--include-sync-logs",
        action="store_true",
        default=False,
        help="When used with --delete, also remove stale bucket.sync-status entries from the zone log pool. Detection is always performed automatically."
    )
    args = parser.parse_args()

    # Parse time period arguments
    start_period: Optional[datetime] = None
    end_period: Optional[datetime] = None
    if args.start_period_utc:
        ts = args.start_period_utc.replace("Z", "+00:00")
        start_period = datetime.fromisoformat(ts)
        if start_period.tzinfo is None:
            start_period = start_period.replace(tzinfo=timezone.utc)
    if args.end_period_utc:
        ts = args.end_period_utc.replace("Z", "+00:00")
        end_period = datetime.fromisoformat(ts)
        if end_period.tzinfo is None:
            end_period = end_period.replace(tzinfo=timezone.utc)
    if start_period and end_period and start_period > end_period:
        print(json.dumps({"error": "--start-period-utc must be before --end-period-utc"}))
        sys.exit(1)

    try:
        zone = RGWZone()
    except RuntimeError as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)

    detector = OrphanDetector(
        zone,
        verify_active=args.verify_active,
        inactive_tenants_only=args.inactive_tenants_only,
        scan_data_pool=args.data_pool,
        start_period=start_period,
        end_period=end_period,
    )
    detector.discover()
    report = detector.detect()

    # --- Sync log check: always detect ---
    # Build sets of known (live) bucket names and instance IDs from metadata
    known_instances: Set[str] = set(detector.instances.keys())
    known_entrypoints: Set[str] = set(detector.meta_entrypoints) | set(detector.rados_entrypoints)

    sync_count, sync_oids, sync_detected = check_sync_logs(
        zone, known_instances=known_instances, known_entrypoints=known_entrypoints, delete=False
    )
    report["sync_logs"] = {
        "log_pool": zone.log_pool,
        "total_entries": sync_count,
        "sample_oids": sync_oids[:10],
        "deleted": False,
        "removed_count": 0,
        "failed_count": 0,
        "entries": sync_detected,
    }

    total = report["summary"]["total_orphans"]
    total_data = report["summary"]["total_data_orphans"]
    total_transient = report["summary"]["total_transient_instances"]

    # --- Deletion mode ---
    anything_to_clean = total > 0 or total_data > 0 or total_transient > 0 or sync_count > 0
    if args.delete and anything_to_clean:
        if not args.yes_i_really_mean_it:
            transient_msg = ""
            if total_transient > 0:
                transient_msg = f" + {total_transient} transient instance(s)"
            sync_msg = ""
            if sync_count > 0 and args.include_sync_logs:
                sync_msg = f" + {sync_count} sync log entries"
            print(
                f"# Found {total} metadata orphan(s){transient_msg} + {total_data} data orphan bucket(s){sync_msg}. Proceed? [y/N] ",
                end="",
                file=sys.stderr,
            )
            try:
                response = input().strip().lower()
            except (EOFError, KeyboardInterrupt):
                response = "n"
            if response not in ("y", "yes"):
                print("# Aborted.", file=sys.stderr)
                # Still emit JSON report before exit
                json_report = json.dumps(report, indent=2)
                if args.output == "-":
                    print(json_report)
                else:
                    with open(args.output, "w") as f:
                        f.write(json_report + "\n")
                sys.exit(1)

        cleaner = OrphanCleaner(zone)
        all_orphans = (
            report["orphans"]["instances"]
            + report["orphans"]["entrypoints"]
            + report["orphans"]["index"]
        )

        # Include transient instances only if --include-transient is explicitly requested
        if args.include_transient and report["orphans"].get("transient_instances"):
            transient_count = len(report["orphans"]["transient_instances"])
            print(
                f"# INFO: Including {transient_count} transient instance(s) for deletion. "
                "Normally these are cleaned up by BucketTrimInstanceCR.",
                file=sys.stderr,
            )
            all_orphans += report["orphans"]["transient_instances"]

        # Include stale instances only if --delete-stale is explicitly requested
        if args.delete_stale and report["orphans"].get("stale_instances"):
            stale_count = len(report["orphans"]["stale_instances"])
            print(
                f"# WARNING: Including {stale_count} stale instance(s) for deletion. "
                "This can corrupt active buckets if they are being resharded!",
                file=sys.stderr,
            )
            all_orphans += report["orphans"]["stale_instances"]

        if not all_orphans and total_data == 0:
            if total_transient > 0 and not args.include_transient:
                print(
                    f"# No true orphans to remove ({total_transient} transient instance(s) skipped, "
                    "use --include-transient to clean them up).",
                    file=sys.stderr,
                )
            else:
                print("# No orphans to remove.", file=sys.stderr)
        else:
            # Clean metadata orphans
            for item in all_orphans:
                ok = cleaner.remove(item, dry_run=False)
                status = "removed" if ok else "FAILED"
                print(f"# {status}: {item.get('type', item.get('bucket_id', 'unknown'))} {item['oid']}", file=sys.stderr)

            # Clean data orphans
            if args.data_pool and total_data > 0:
                data_orphans = report["orphans"]["data"]
                total_buckets = len(data_orphans)
                total_objects = report["summary"]["total_data_orphan_objects"]

                print(f"# Starting data cleanup: {total_buckets} bucket IDs, ~{total_objects} total objects", file=sys.stderr)

                # Collect all bucket IDs and do a single streaming pass
                bucket_ids = set(d["bucket_id"] for d in data_orphans)
                pool = data_orphans[0]["pool"] if data_orphans else zone.data_pool
                results = cleaner.stream_remove_by_prefix(bucket_ids, pool, dry_run=False)

                for data_entry in data_orphans:
                    bucket_id = data_entry["bucket_id"]
                    removed, failed = results.get(bucket_id, (0, 0))
                    data_entry["removed_count"] = removed
                    data_entry["failed_count"] = failed
                    print(f"# Completed {bucket_id}: {removed} removed, {failed} failed", file=sys.stderr)

        # Clean stale sync logs (after user confirmed deletion)
        if args.include_sync_logs and sync_count > 0:
            print(f"# Deleting {sync_count} stale sync log entries from {zone.log_pool}...", file=sys.stderr)
            _, _, sync_removed = check_sync_logs(
                zone, known_instances=known_instances, known_entrypoints=known_entrypoints, delete=True
            )
            report["sync_logs"]["deleted"] = True
            report["sync_logs"]["removed_count"] = len([r for r in sync_removed if r.get("status") == "removed"])
            report["sync_logs"]["failed_count"] = len([r for r in sync_removed if r.get("status") == "failed"])
            report["sync_logs"]["entries"] = sync_removed
            print(f"# Sync logs: {report['sync_logs']['removed_count']} removed, {report['sync_logs']['failed_count']} failed", file=sys.stderr)

        # Attach cleanup summary to the report
        report["cleanup"] = {
            "cleanup_completed": True,
            "metadata_removed": len(cleaner.removed),
            "metadata_failed": len(cleaner.failed),
            "details": {
                "removed": cleaner.removed,
                "failed": cleaner.failed
            }
        }

    # --- JSON summary is printed first ---
    json_report = json.dumps(report, indent=2)
    if args.output == "-":
        print(json_report)
    else:
        with open(args.output, "w") as f:
            f.write(json_report + "\n")

    # --- Text summary to stderr (at the bottom) ---
    if total == 0 and total_data == 0 and total_transient == 0:
        print("# No orphaned metadata or data found.", file=sys.stderr)
    else:
        print(
            f"# Found {total} metadata orphan(s). Use --delete to clean up.",
            file=sys.stderr,
        )
        if total_transient > 0:
            print(
                f"# Found {total_transient} transient instance(s) (BUCKET_DELETED bit set in flags: 64/66). "
                "These will be cleaned up by BucketTrimInstanceCR. Use --delete --include-transient to force cleanup.",
                file=sys.stderr,
            )
    if total_data > 0:
        total_data_objs = report["summary"]["total_data_orphan_objects"]
        print(
            f"# Found {total_data} data bucket ID(s) with ~{total_data_objs} orphan objects. Use --delete --data-pool to clean up.",
            file=sys.stderr,
        )

    # --- Sync log summary (always detected) ---
    if "sync_logs" in report:
        sync_info = report["sync_logs"]
        if sync_info["total_entries"] == 0:
            print("# No orphan sync log found.", file=sys.stderr)
        else:
            print(
                f"# Found {sync_info['total_entries']} stale sync log entries in {sync_info['log_pool']}. Use --delete --include-sync-logs to clean up.",
                file=sys.stderr,
            )
        if sync_info.get("deleted"):
            print(
                f"# Removed {sync_info.get('removed_count', 0)}, failed {sync_info.get('failed_count', 0)}.",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()
