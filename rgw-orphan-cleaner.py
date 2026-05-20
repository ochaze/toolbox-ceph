#!/usr/bin/env python3
"""
RGW Complete Orphan Cleaner

Detects and optionally cleans:
  - Orphan bucket instance metadata (instance without entrypoint)
  - Stale instances from resharding (entrypoint points elsewhere)
  - Orphan bucket index objects (index without known instance)
  - Orphan data objects (data without known bucket instance)

Usage:
    python3 rgw-orphan-cleaner.py                  # detection only
    python3 rgw-orphan-cleaner.py --delete         # cleanup after confirmation
    python3 rgw-orphan-cleaner.py --delete --yes   # no prompt
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


class OrphanDetector:
    """Detects orphaned bucket metadata and data across RADOS pools."""

    def __init__(self, zone: RGWZone, verify_active: bool = False,
                 inactive_tenants_only: bool = False, scan_data_pool: bool = False,
                 detect_stale: bool = False):
        self.zone = zone
        self.verify_active = verify_active
        self.inactive_tenants_only = inactive_tenants_only
        self.scan_data_pool = scan_data_pool
        self.detect_stale = detect_stale
        self.entrypoints: Dict[str, str] = {}       # bucket_name -> bucket_id
        self.instances: Dict[str, str] = {}         # bucket_id -> bucket_name
        self.index_objects: Dict[str, List[str]] = {}  # bucket_id -> [oid, ...]
        self.data_objects: Dict[str, int] = {}      # bucket_id -> count

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
        try:
            data = json.loads(out)
            reshard_status = data.get("data", {}).get("bucket_info", {}).get("reshard_status")
            if reshard_status is None:
                print(f"# DEBUG: no reshard_status in metadata for {instance_key}", file=sys.stderr)
            else:
                print(f"# DEBUG: reshard_status for {instance_key} = {reshard_status}", file=sys.stderr)
            return reshard_status
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"# DEBUG: JSON parse error for {instance_key}: {e}", file=sys.stderr)
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
            except RuntimeError as e:
                print(json.dumps({"error": str(e)}))
                sys.exit(1)
            print(f"# Processed {count} data objects", file=sys.stderr)

    def detect(self) -> Dict:
        """Phase 2: cross-reference and detect all orphans."""

        orphan_instances = []
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
                    # entrypoint exists but points to different bucket_id - potential stale instance
                    if not self.detect_stale:
                        # Deletion of stale instances is dangerous (mid-reshard corruption risk).
                        # Skip but warn user: use --detect-stale to delete.
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
                                "reason": "stale instance detection disabled (use --detect-stale). Manual deletion can corrupt active reshards.",
                            }
                        )
                        continue

                    # Check reshard status like Ceph does
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
                        # Different bucket_id but not marked as resharded - suspicious
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
                                "reason": f"entrypoint points to different bucket_id ({active_id}) but instance is NOT_RESHARDING - may be from manual bucket move or corruption",
                            }
                        )
                        continue

                    if reshard_status == 2:  # DONE
                        # Reshard completed - instance is safe to flag as stale
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
                entry = {
                    "type": "orphan_instance",
                    "bucket_name": ep_name,
                    "bucket_id": bucket_id,
                    "oid": info["oid"],
                    "pool": self.zone.meta_pool,
                    "namespace": "root",
                    "tenant": info["tenant"],
                    "reason": reason,
                }
                if is_safe:
                    orphan_instances.append(entry)
                else:
                    entry["type"] = "skipped_instance"
                    entry["reason"] = f"Safety check failed: {reason}"
                    skipped_instances.append(entry)

        # Entrypoint in RADOS but no instance
        for ep in self.rados_entrypoints:
            if ep not in self.meta_entrypoints:
                bucket_id = self.entrypoints.get(ep)
                if not bucket_id or bucket_id not in self.rados_instances:
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
                "detect_stale": self.detect_stale,
            },
            "summary": {
                "total_orphans": len(orphan_instances)
                + len(stale_instances)
                + len(orphan_entrypoints)
                + len(orphan_index),
                "total_data_orphans": len(orphan_data),
                "total_data_orphan_objects": sum(
                    d["object_count"] for d in orphan_data.values()
                ),
                "orphan_instances": len(orphan_instances),
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
            "orphans": {
                "instances": orphan_instances,
                "stale_instances": stale_instances,
                "entrypoints": orphan_entrypoints,
                "index": orphan_index,
                "data": list(orphan_data.values()),
            },
            "skipped": skipped_instances,
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
        "--detect-stale",
        action="store_true",
        default=False,
        help="Detect stale bucket instances from resharding.",
    )
    parser.add_argument(
        "--delete-stale",
        action="store_true",
        default=False,
        help="DANGEROUS: Allow deletion of stale instances (use with --yes-i-really-mean-it). Stale instance cleanup can corrupt active buckets during resharding."
    )
    args = parser.parse_args()

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
        detect_stale=args.detect_stale,
    )
    detector.discover()
    report = detector.detect()

    json_report = json.dumps(report, indent=2)
    if args.output == "-":
        print(json_report)
    else:
        with open(args.output, "w") as f:
            f.write(json_report + "\n")

    total = report["summary"]["total_orphans"]
    total_data = report["summary"]["total_data_orphans"]

    if total == 0 and total_data == 0:
        print("# No orphaned metadata or data found.", file=sys.stderr)
        sys.exit(0)

    if not args.delete:
        print(
            f"# Found {total} metadata orphan(s). Use --delete to clean up.",
            file=sys.stderr,
        )
        if total_data > 0:
            total_data_objs = report["summary"]["total_data_orphan_objects"]
            print(
                f"# Found {total_data} data bucket ID(s) with ~{total_data_objs} orphan objects. Use --delete --data-pool to clean up.",
                file=sys.stderr,
            )
        sys.exit(0)

    if not args.yes_i_really_mean_it:
        print(
            f"# Found {total} metadata orphan(s) + {total_data} data orphan bucket(s). Proceed? [y/N] ",
            end="",
            file=sys.stderr,
        )
        try:
            response = input().strip().lower()
        except (EOFError, KeyboardInterrupt):
            response = "n"
        if response not in ("y", "yes"):
            print("# Aborted.", file=sys.stderr)
            sys.exit(1)

    cleaner = OrphanCleaner(zone)
    all_orphans = (
        report["orphans"]["instances"]
        + report["orphans"]["entrypoints"]
        + report["orphans"]["index"]
    )

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
        print("# No orphans to remove.", file=sys.stderr)
        sys.exit(0)

    # Clean metadata orphans
    for item in all_orphans:
        ok = cleaner.remove(item, dry_run=False)
        status = "removed" if ok else "FAILED"
        print(f"# {status}: {item['type']} {item['oid']}", file=sys.stderr)

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

    summary = {
        "cleanup_completed": True,
        "metadata_removed": len(cleaner.removed),
        "metadata_failed": len(cleaner.failed),
        "details": {
            "removed": cleaner.removed,
            "failed": cleaner.failed
        }
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
