#!/usr/bin/env python3
"""
RGW Sync Status Garbage Collector

Cleans stale bucket.sync-status.* and bucket.full-sync-status.* objects
from the RGW log pool that accumulate when buckets are deleted/recreated.

Designed for S3aaS multisite deployments at thousands-of-customer scale.
Runs inside the cephadm container (rados, radosgw-admin available natively).

Usage:
    # Detection only (dry-run)
    python3 rgw-sync-gc.py
    
    # Automated cleanup with age threshold
    python3 rgw-sync-gc.py --delete --yes-i-really-mean-it --max-age-days 7
    
    # Save JSON report to file
    python3 rgw-sync-gc.py --output report.json
    
    # Also clean orphaned hint objects
    python3 rgw-sync-gc.py --delete --yes-i-really-mean-it --include-hints

If running outside the cephadm container, invoke via:
    cephadm shell -- python3 /path/to/rgw-sync-gc.py ...

Output: JSON report to stdout (or file)
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple


# Regex to match bucket_id patterns (UUID.NUMBER.NUMBER)
BUCKET_ID_RE = re.compile(
    r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.\d+\.\d+)"
)

# Regex to match tenant/bucket names in sync-status OIDs
# Format: tenant/bucket_name (tenant may be empty)
TENANT_BUCKET_RE = re.compile(
    r"(?:(?:[a-f0-9]+|[^/]+)/)?([^/:]+)$"
)


class RGWZone:
    """Auto-discovers RGW zone parameters."""

    def __init__(self):
        self.name: Optional[str] = None
        self.id: Optional[str] = None
        self.log_pool: Optional[str] = None
        self.domain_root: Optional[str] = None
        self._discover()

    def _run(self, cmd: List[str]) -> Tuple[int, str, str]:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    def _discover(self):
        rc, out, err = self._run(["radosgw-admin", "zone", "get"])
        if rc != 0:
            raise RuntimeError(f"Failed to get zone info: {err.strip()}")

        zone_info = json.loads(out)
        self.name = zone_info.get("name", "unknown")
        self.id = zone_info.get("id", "unknown")

        self.domain_root = zone_info.get("domain_root")
        if not self.domain_root:
            raise RuntimeError("Could not determine domain_root pool")

        # Log pool naming convention: <zone>.rgw.log
        self.log_pool = f"{self.name}.rgw.log"

        # Verify log pool exists
        rc, _, _ = self._run(["rados", "lspools"])
        if rc == 0:
            # We'll trust the naming convention, but could verify here
            pass


class SyncGC:
    """Scans and cleans stale sync-status objects from the RGW log pool."""

    def __init__(self, zone: RGWZone, max_age_days: Optional[int] = None,
                 include_hints: bool = False):
        self.zone = zone
        self.max_age_days = max_age_days
        self.include_hints = include_hints

        # Cache for metadata lookups
        self._active_bucket_ids: Optional[Set[str]] = None
        self._entrypoint_map: Optional[Dict[str, str]] = None  # bucket_name -> bucket_id
        self._all_bucket_ids: Optional[Set[str]] = None
        self._hint_orphans: List[Dict] = []

        # Stale objects found
        self.stale_sync_status: List[Dict] = []
        self.stale_full_sync: List[Dict] = []
        self.stale_hints: List[Dict] = []
        self.skipped: List[Dict] = []

    def _run(self, cmd: List[str]) -> Tuple[int, str, str]:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    def _rados_ls_streaming(self, pool: str, namespace: str = ""):
        """Stream objects from RADOS pool without loading all into memory."""
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
                    f"rados ls failed for {pool}: return code {proc.returncode}"
                )

    def _rados_stat_mtime(self, pool: str, oid: str) -> Optional[datetime]:
        """Get object mtime from RADOS."""
        cmd = ["rados", "-p", pool, "stat", "--format=json", oid]
        rc, out, _ = self._run(cmd)
        if rc == 0 and out.strip():
            try:
                data = json.loads(out)
                mtime = data.get("mtime")
                if mtime:
                    mtime = mtime.replace("Z", "+00:00")
                    return datetime.fromisoformat(mtime)
            except (json.JSONDecodeError, ValueError):
                pass
        return None

    def _load_metadata(self):
        """Load active bucket instance IDs and entrypoint mappings.
        
        Important: Only bucket IDs referenced by existing entrypoints are "active".
        Orphaned bucket instances (metadata exists but no entrypoint) are tracked
        separately and should be cleaned by rgw-orphan-cleaner.py, not this tool.
        """
        if self._all_bucket_ids is not None:
            return

        self._all_bucket_ids = set()      # bucket_ids with active entrypoints
        self._entrypoint_map = {}         # bucket_name -> bucket_id
        self._all_instances = set()       # ALL bucket.instance IDs (including orphaned)

        # Step 1: Get ALL bucket instances (for cross-referencing)
        rc, out, err = self._run(["radosgw-admin", "metadata", "list", "bucket.instance"])
        if rc == 0:
            instances = json.loads(out) if out.strip() else []
            for inst in instances:
                parts = inst.rsplit(":", 1)
                if len(parts) == 2:
                    self._all_instances.add(parts[1])

        # Step 2: Get entrypoints — these are the ONLY active buckets
        # An entrypoint maps bucket_name -> bucket_id
        rc, out, err = self._run(["radosgw-admin", "metadata", "list", "bucket"])
        if rc == 0:
            entrypoints = json.loads(out) if out.strip() else []
            for ep in entrypoints:
                bucket_id = self._get_entrypoint_bucket_id(ep)
                if bucket_id:
                    self._entrypoint_map[ep] = bucket_id
                    self._all_bucket_ids.add(bucket_id)

        # Step 3: Trace orphan instances via RADOS (for info, not for active list)
        try:
            meta_pool = self.zone.domain_root.split(":")[0]
            for oid in self._rados_ls_streaming(meta_pool, "root"):
                if oid.startswith(".bucket.meta."):
                    parsed = self._parse_instance_oid(oid)
                    if parsed:
                        _, _, bucket_id = parsed
                        self._all_instances.add(bucket_id)
        except RuntimeError:
            pass

    def _get_entrypoint_bucket_id(self, ep_name: str) -> Optional[str]:
        """Read entrypoint metadata to get current active bucket_id."""
        rc, out, _ = self._run(
            ["radosgw-admin", "metadata", "get", f"bucket:{ep_name}"]
        )
        if rc != 0:
            return None
        try:
            data = json.loads(out)
            return data.get("data", {}).get("bucket", {}).get("bucket_id")
        except (json.JSONDecodeError, AttributeError):
            return None

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

    def _parse_sync_status_oid(self, oid: str) -> Optional[Dict]:
        """
        Parse a bucket.sync-status OID.

        Returns dict with:
          - type: 'sync_status' or 'full_sync_status' or 'source_hints' or 'target_hints'
          - zone_id: source zone UUID
          - bucket_ids: list of bucket IDs referenced
          - bucket_names: list of tenant/bucket names referenced
          - shard: shard number (for sync_status)
        """
        result = {
            "type": None,
            "zone_id": None,
            "bucket_ids": [],
            "bucket_names": [],
            "shard": None,
            "raw": oid,
        }

        if oid.startswith("bucket.sync-status."):
            result["type"] = "sync_status"
            rest = oid[len("bucket.sync-status."):]
        elif oid.startswith("bucket.full-sync-status."):
            result["type"] = "full_sync_status"
            rest = oid[len("bucket.full-sync-status."):]
        elif oid.startswith("bucket.sync-source-hints."):
            result["type"] = "source_hints"
            rest = oid[len("bucket.sync-source-hints."):]
            result["bucket_names"].append(rest)
            return result
        elif oid.startswith("bucket.sync-target-hints."):
            result["type"] = "target_hints"
            rest = oid[len("bucket.sync-target-hints."):]
            result["bucket_names"].append(rest)
            return result
        else:
            return None

        # Extract zone_id (first UUID before first ':')
        first_colon = rest.find(":")
        if first_colon == -1:
            return None
        result["zone_id"] = rest[:first_colon]
        rest = rest[first_colon + 1:]

        # Find all bucket_ids in the remaining string
        for match in BUCKET_ID_RE.finditer(rest):
            result["bucket_ids"].append(match.group(1))

        # Find all tenant/bucket names
        # Split by ':' and look for parts containing '/'
        parts = rest.split(":")
        for part in parts:
            if "/" in part and not BUCKET_ID_RE.match(part):
                result["bucket_names"].append(part)

        # Extract shard number (last numeric part if present)
        for part in reversed(parts):
            if part.isdigit():
                result["shard"] = int(part)
                break

        return result

    def _is_bucket_id_active(self, bucket_id: str) -> bool:
        """Check if a bucket_id exists in metadata."""
        self._load_metadata()
        return bucket_id in self._all_bucket_ids

    def _is_bucket_name_active(self, bucket_name: str) -> bool:
        """Check if a bucket name has an active entrypoint."""
        self._load_metadata()
        return bucket_name in self._entrypoint_map

    def _get_active_bucket_id_for_name(self, bucket_name: str) -> Optional[str]:
        """Get the currently active bucket_id for a bucket name."""
        self._load_metadata()
        return self._entrypoint_map.get(bucket_name)

    def _check_age(self, oid: str) -> Tuple[bool, Optional[str]]:
        """Check if object is old enough to be considered stale."""
        if self.max_age_days is None:
            return True, None

        mtime = self._rados_stat_mtime(self.zone.log_pool, oid)
        if mtime is None:
            return False, "could not determine mtime"

        cutoff = datetime.now(timezone.utc) - timedelta(days=self.max_age_days)
        if mtime > cutoff:
            return False, f"too recent (mtime: {mtime.isoformat()})"

        return True, None

    def scan(self):
        """Stream the log pool and identify stale sync objects."""
        print(f"# Scanning {self.zone.log_pool} for stale sync objects...", file=sys.stderr)

        total_scanned = 0
        sync_status_count = 0
        full_sync_count = 0
        hint_count = 0

        try:
            for oid in self._rados_ls_streaming(self.zone.log_pool):
                total_scanned += 1

                parsed = self._parse_sync_status_oid(oid)
                if not parsed:
                    continue

                obj_type = parsed["type"]

                if obj_type == "sync_status":
                    sync_status_count += 1
                    self._evaluate_sync_status(oid, parsed)
                elif obj_type == "full_sync_status":
                    full_sync_count += 1
                    self._evaluate_full_sync_status(oid, parsed)
                elif obj_type in ("source_hints", "target_hints") and self.include_hints:
                    hint_count += 1
                    self._evaluate_hint(oid, parsed)

                if total_scanned % 10000 == 0:
                    print(
                        f"#   Scanned {total_scanned} objects... "
                        f"(sync_status: {sync_status_count}, full_sync: {full_sync_count})",
                        file=sys.stderr,
                    )

        except RuntimeError as e:
            raise RuntimeError(f"Failed to scan log pool: {e}")

        print(
            f"# Scan complete: {total_scanned} total objects, "
            f"{sync_status_count} sync-status, {full_sync_count} full-sync-status, "
            f"{hint_count} hints",
            file=sys.stderr,
        )

    def _evaluate_sync_status(self, oid: str, parsed: Dict):
        """Evaluate whether a sync-status object is stale."""
        bucket_ids = parsed["bucket_ids"]
        bucket_names = parsed["bucket_names"]

        if not bucket_ids:
            self.skipped.append({
                "oid": oid,
                "type": "sync_status",
                "reason": "could not parse bucket_id from OID",
            })
            return

        # Check age
        age_ok, age_reason = self._check_age(oid)
        if not age_ok:
            self.skipped.append({
                "oid": oid,
                "type": "sync_status",
                "reason": age_reason or "too recent",
            })
            return

        # Determine if stale:
        # A sync-status is stale if ALL referenced bucket_ids are inactive
        # AND the bucket_name has a new active instance (or no active instance at all)
        all_inactive = True
        active_ids = []
        inactive_ids = []

        for bid in bucket_ids:
            if self._is_bucket_id_active(bid):
                all_inactive = False
                active_ids.append(bid)
            else:
                inactive_ids.append(bid)

        if not all_inactive:
            # At least one bucket_id is still active → keep it
            self.skipped.append({
                "oid": oid,
                "type": "sync_status",
                "reason": f"references active bucket_id(s): {active_ids}",
                "active_ids": active_ids,
                "inactive_ids": inactive_ids,
            })
            return

        # All bucket_ids are inactive. Check if bucket_name has a new instance.
        has_newer_instance = False
        newer_instance_id = None
        for bname in bucket_names:
            active_id = self._get_active_bucket_id_for_name(bname)
            if active_id and active_id not in bucket_ids:
                has_newer_instance = True
                newer_instance_id = active_id
                break
            elif not active_id:
                # Bucket no longer exists at all
                has_newer_instance = True
                break

        if not has_newer_instance:
            # Bucket was deleted and never recreated → stale
            pass  # will be added to stale list below

        self.stale_sync_status.append({
            "oid": oid,
            "type": "sync_status",
            "bucket_ids": bucket_ids,
            "bucket_names": bucket_names,
            "inactive_ids": inactive_ids,
            "newer_instance_id": newer_instance_id,
            "shard": parsed.get("shard"),
            "reason": (
                f"all referenced bucket_ids are inactive. "
                f"Bucket has newer instance: {newer_instance_id}" if newer_instance_id
                else "all referenced bucket_ids are inactive. Bucket no longer exists"
            ),
        })

    def _evaluate_full_sync_status(self, oid: str, parsed: Dict):
        """Evaluate whether a full-sync-status object is stale."""
        bucket_ids = parsed["bucket_ids"]
        bucket_names = parsed["bucket_names"]

        if not bucket_ids:
            self.skipped.append({
                "oid": oid,
                "type": "full_sync_status",
                "reason": "could not parse bucket_id from OID",
            })
            return

        # Check age
        age_ok, age_reason = self._check_age(oid)
        if not age_ok:
            self.skipped.append({
                "oid": oid,
                "type": "full_sync_status",
                "reason": age_reason or "too recent",
            })
            return

        # Full sync status objects are stale if their bucket_id is not active
        for bid in bucket_ids:
            if self._is_bucket_id_active(bid):
                self.skipped.append({
                    "oid": oid,
                    "type": "full_sync_status",
                    "reason": f"references active bucket_id: {bid}",
                    "active_id": bid,
                })
                return

        # Check if bucket has newer instance
        has_newer = False
        newer_id = None
        for bname in bucket_names:
            active_id = self._get_active_bucket_id_for_name(bname)
            if active_id and active_id not in bucket_ids:
                has_newer = True
                newer_id = active_id
                break
            elif not active_id:
                has_newer = True
                break

        self.stale_full_sync.append({
            "oid": oid,
            "type": "full_sync_status",
            "bucket_ids": bucket_ids,
            "bucket_names": bucket_names,
            "newer_instance_id": newer_id,
            "reason": (
                f"bucket_id is inactive. "
                f"Newer instance: {newer_id}" if newer_id
                else "bucket_id is inactive. Bucket no longer exists"
            ),
        })

    def _evaluate_hint(self, oid: str, parsed: Dict):
        """Evaluate whether a sync hint object is stale."""
        bucket_names = parsed["bucket_names"]

        for bname in bucket_names:
            if self._is_bucket_name_active(bname):
                self.skipped.append({
                    "oid": oid,
                    "type": parsed["type"],
                    "reason": f"bucket '{bname}' is still active",
                })
                return

        self.stale_hints.append({
            "oid": oid,
            "type": parsed["type"],
            "bucket_names": bucket_names,
            "reason": "bucket no longer exists",
        })

    def get_report(self) -> Dict:
        """Generate JSON report with stale objects at the end for easy viewing."""
        
        # Build stale list in flat format for easy grepping
        stale_flat = []
        items = (
            [("sync_status", x) for x in self.stale_sync_status]
            + [("full_sync_status", x) for x in self.stale_full_sync]
            + [("hint", x) for x in self.stale_hints]
        )
        for _, item in items:
            stale_flat.append({
                "oid": item["oid"],
                "type": item["type"],
                "reason": item.get("reason", ""),
                "bucket_ids": item.get("bucket_ids", []),
                "bucket_names": item.get("bucket_names", []),
            })
        
        skipped_flat = []
        for item in self.skipped:
            skipped_flat.append({
                "oid": item["oid"],
                "type": item.get("type", "skipped"),
                "reason": item.get("reason", ""),
            })
        
        return {
            "version": "1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "zone": self.zone.name,
            "zone_id": self.zone.id,
            "log_pool": self.zone.log_pool,
            "config": {
                "max_age_days": self.max_age_days,
                "include_hints": self.include_hints,
            },
            "summary": {
                "total_stale": (
                    len(self.stale_sync_status)
                    + len(self.stale_full_sync)
                    + len(self.stale_hints)
                ),
                "stale_sync_status": len(self.stale_sync_status),
                "stale_full_sync_status": len(self.stale_full_sync),
                "stale_hints": len(self.stale_hints),
                "skipped": len(self.skipped),
            },
            # Stale objects listed LAST so they appear at the bottom of output
            # for easy viewing/action. Skipped (active) items come before.
            "skipped_objects": skipped_flat,
            "stale_objects": stale_flat,
        }


class SyncCleaner:
    """Removes stale sync objects from RADOS."""

    def __init__(self, zone: RGWZone):
        self.zone = zone
        self.removed = []
        self.failed = []

    def _run(self, cmd: List[str]) -> Tuple[int, str, str]:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    def remove(self, item: Dict, dry_run: bool = True) -> bool:
        oid = item["oid"]
        pool = self.zone.log_pool

        cmd = ["rados", "-p", pool, "rm", oid]

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


def print_report(report: Dict):
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="RGW Sync Status Garbage Collector"
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        default=False,
        help="Enable deletion mode",
    )
    parser.add_argument(
        "--yes-i-really-mean-it",
        action="store_true",
        default=False,
        help="Skip interactive confirmation",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Output file path (- for stdout)",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        help="Only consider objects older than N days",
    )
    parser.add_argument(
        "--include-hints",
        action="store_true",
        default=False,
        help="Also clean orphaned bucket.sync-source-hints and bucket.sync-target-hints",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Progress report interval (default: 1000)",
    )
    parser.add_argument(
        "--no-skipped",
        action="store_true",
        default=False,
        help="Do not include skipped (active) objects in output",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        default=False,
        help="Only output summary, omitting details",
    )
    args = parser.parse_args()

    try:
        zone = RGWZone()
    except RuntimeError as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)

    gc = SyncGC(
        zone,
        max_age_days=args.max_age_days,
        include_hints=args.include_hints,
    )

    try:
        gc.scan()
    except RuntimeError as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)

    report = gc.get_report()
    
    # Apply output filters
    if args.no_skipped:
        report.pop("skipped_objects", None)
    
    if args.summary_only:
        # Only keep summary and zone info
        filtered_report = {
            "version": report.get("version"),
            "timestamp": report.get("timestamp"),
            "zone": report.get("zone"),
            "summary": report.get("summary"),
            "total_stale": report["summary"]["total_stale"],
            "stale_count": report["summary"]["total_stale"],
        }
        report = filtered_report

    json_report = json.dumps(report, indent=2)

    if args.output == "-":
        print(json_report)
    else:
        with open(args.output, "w") as f:
            f.write(json_report + "\n")

    total_stale = report["summary"]["total_stale"]

    if total_stale == 0:
        print("# No stale sync objects found.", file=sys.stderr)
        sys.exit(0)

    if not args.delete:
        if not args.summary_only:
            print(
                f"# Found {total_stale} stale sync object(s). Use --delete to clean up.",
                file=sys.stderr,
            )
        else:
            print(f"# {total_stale} stale", file=sys.stderr)
        sys.exit(0)

    if not args.yes_i_really_mean_it:
        print(
            f"# Found {total_stale} stale sync object(s). Proceed with deletion? [y/N] ",
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

    cleaner = SyncCleaner(zone)
    all_stale = report.get("stale_objects", [])

    for item in all_stale:
        ok = cleaner.remove(item, dry_run=False)
        status = "removed" if ok else "FAILED"
        print(f"# {status}: {item['type']} {item['oid']}", file=sys.stderr)

    summary = {
        "cleanup_completed": True,
        "metadata_removed": len(cleaner.removed),
        "metadata_failed": len(cleaner.failed),
        "details": {
            "removed": cleaner.removed,
            "failed": cleaner.failed,
        },
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
