#!/usr/bin/env python3
"""
RGW Sync Repair Tool

Diagnoses and fixes per-bucket sync issues in Ceph RGW multisite deployments.

Designed to complement rgw-sync-gc.py (which cleans stale metadata objects).
This tool focuses on live sync marker issues where:
  - `bucket sync status` shows "failed to read remote log"
  - gva2b knows about the new bucket instance but can't resume sync

Usage:
    # Check a single bucket
    python3 rgw-sync-repair.py --bucket f8e2d506252c4961ad0fa321abf1f1b5/bucket1 --check

    # Reset sync markers for stuck source zones
    python3 rgw-sync-repair.py --bucket f8e2d506252c4961ad0fa321abf1f1b5/bucket1 --reset-sync

    # Check all buckets in the zone (check mode only)
    python3 rgw-sync-repair.py --check-all

    # Output JSON for automation
    python3 rgw-sync-repair.py --bucket f8e2d506252c4961ad0fa321abf1f1b5/bucket1 --check --json

Exit codes:
    0 = no issues found
    1 = issues found (check mode)
    2 = repair attempted
    3 = usage/runtime error
"""

import argparse
import json
import re
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


class SyncRepair:
    def __init__(self, bucket: str, source_zone: Optional[str] = None):
        # Normalize tenant separator: Ceph expects / not :
        self.bucket = bucket.replace(':', '/')
        self.source_zone = source_zone
        self.issues: List[Dict] = []
        self.fixed: List[Dict] = []
        self.failed: List[Dict] = []

    def _run(self, cmd: List[str]) -> Tuple[int, str, str]:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True)
            return proc.returncode, proc.stdout, proc.stderr
        except FileNotFoundError as e:
            return 127, "", f"Command not found: {cmd[0]}"
        except Exception as e:
            return 1, "", str(e)

    def _parse_sync_status(self, raw: str) -> List[Dict]:
        """Parse radosgw-admin bucket sync status text output."""
        sources = []
        current_source = None
        
        source_zone_pattern = re.compile(r'^\s+source zone\s+([a-f0-9-]+)\s+\(([^)]+)\)')
        source_bucket_pattern = re.compile(r'^\s+source bucket\s+([^[]+)\[([^]]+)\]')
        behind_shards_pattern = re.compile(r'^\s*behind shards:\s*\[([^\]]*)\]')
        caught_up_pattern = re.compile(r'^\s+bucket is caught up with source')
        failed_pattern = re.compile(r'^\s+failed to read remote log:\s*\((\d+)\)\s*(.+)')
        incremental_pattern = re.compile(r'^\s+incremental sync on\s+(\d+)\s+shards')

        for line in raw.splitlines():
            line = line.rstrip()
            
            # Start of a new source zone entry
            m = source_zone_pattern.match(line)
            if m:
                current_source = {
                    "zone_id": m.group(1),
                    "zone_name": m.group(2),
                    "source_bucket": None,
                    "bucket_id": None,
                    "status": "unknown",
                    "behind_shards": [],
                    "incremental_shards": None,
                    "error_code": None,
                    "error_message": None,
                }
                sources.append(current_source)
                continue
            
            # Source bucket info
            m = source_bucket_pattern.match(line)
            if m and current_source:
                current_source["source_bucket"] = m.group(1).strip()
                current_source["bucket_id"] = m.group(2).strip()
                continue
            
            # Status: caught up
            m = caught_up_pattern.match(line)
            if m and current_source:
                current_source["status"] = "caught_up"
                continue
            
            # Status: behind
            m = behind_shards_pattern.match(line)
            if m and current_source:
                shards_str = m.group(1).strip()
                if shards_str:
                    current_source["behind_shards"] = [
                        int(s) for s in shards_str.split(',') if s.strip()
                    ]
                current_source["status"] = "behind"
                continue
            
            # Status: failed to read remote log
            m = failed_pattern.match(line)
            if m and current_source:
                current_source["status"] = "failed"
                current_source["error_code"] = int(m.group(1))
                current_source["error_message"] = m.group(2).strip()
                continue
            
            # Incremental sync shards count
            m = incremental_pattern.match(line)
            if m and current_source:
                current_source["incremental_shards"] = int(m.group(1))
                continue

        return sources

    def check(self) -> Dict:
        """Check sync status and report issues."""
        cmd = ["radosgw-admin", "bucket", "sync", "status",
               f"--bucket={self.bucket}"]
        rc, out, err = self._run(cmd)
        
        if rc != 0:
            error_msg = err.strip() if err else out.strip() if out else f"radosgw-admin exited with code {rc}"
            return {
                "bucket": self.bucket,
                "error": error_msg,
                "status": "check_failed",
                "sources": [],
            }
        
        if not out.strip():
            # Empty output — bucket may not have any sync status recorded
            return {
                "bucket": self.bucket,
                "status": "healthy",
                "total_sources": 0,
                "healthy_sources": 0,
                "issue_count": 0,
                "sources": [],
                "issues": [],
                "note": "No sync status output returned. This bucket may not be configured for replication or sync has completed.",
            }
        
        sources = self._parse_sync_status(out)
        
        # Identify problematic sources
        self.issues = []
        for src in sources:
            if src["status"] == "failed":
                self.issues.append({
                    "zone_id": src["zone_id"],
                    "zone_name": src["zone_name"],
                    "type": "failed_to_read_remote_log",
                    "error_code": src["error_code"],
                    "error_message": src["error_message"],
                    "source_bucket": src["source_bucket"],
                    "bucket_id": src["bucket_id"],
                })
            elif src["status"] == "behind" and src["behind_shards"]:
                self.issues.append({
                    "zone_id": src["zone_id"],
                    "zone_name": src["zone_name"],
                    "type": "behind",
                    "behind_shards": src["behind_shards"],
                    "incremental_shards": src["incremental_shards"],
                    "source_bucket": src["source_bucket"],
                    "bucket_id": src["bucket_id"],
                })
        
        return {
            "bucket": self.bucket,
            "status": "failed" if self.issues else "healthy",
            "total_sources": len(sources),
            "healthy_sources": len(sources) - len(self.issues),
            "issue_count": len(self.issues),
            "sources": sources,
            "issues": self.issues,
        }

    def fix(self) -> Dict:
        """Fix sync issues by re-initializing sync for stuck sources."""
        if not self.issues:
            check_result = self.check()
            if not self.issues:
                return {
                    "bucket": self.bucket,
                    "status": "no_action_needed",
                    "message": "No sync issues detected",
                    "fixed_count": 0,
                    "failed_count": 0,
                    "fixed": [],
                    "failed": [],
                }
        
        for issue in self.issues:
            zone_id = issue["zone_id"]
            zone_name = issue["zone_name"]
            source_bucket = issue.get("source_bucket") or self.bucket
            bucket_id = issue.get("bucket_id")
            
            # Filter by source zone if specified
            if self.source_zone and zone_name != self.source_zone:
                continue
            
            step = {
                "zone_id": zone_id,
                "zone_name": zone_name,
                "issue_type": issue["type"],
                "init_result": None,
                "run_result": None,
            }
            
            # Step 1: bucket sync init
            cmd_init = [
                "radosgw-admin", "bucket", "sync", "init",
                f"--source-zone={zone_name}",
                f"--bucket={self.bucket}",
            ]
            
            rc_init, out_init, err_init = self._run(cmd_init)
            step["init_result"] = {
                "rc": rc_init,
            }
            if rc_init != 0:
                step["init_error"] = err_init.strip()
                self.failed.append(step)
                continue
            
            # Step 2: bucket sync run
            cmd_run = [
                "radosgw-admin", "bucket", "sync", "run",
                f"--source-zone={zone_name}",
                f"--bucket={self.bucket}",
            ]
            
            rc_run, out_run, err_run = self._run(cmd_run)
            step["run_result"] = {
                "rc": rc_run,
            }
            if rc_run != 0:
                step["run_error"] = err_run.strip()
                self.failed.append(step)
            else:
                self.fixed.append(step)
        
        return {
            "bucket": self.bucket,
            "status": "fixed" if self.fixed else "failed",
            "fixed_count": len(self.fixed),
            "failed_count": len(self.failed),
            "fixed": self.fixed,
            "failed": self.failed,
        }


def check_all_buckets(zones: List[str]) -> List[Dict]:
    """Check all buckets (requires listing all entrypoints). Expensive."""
    proc = subprocess.run(
        ["radosgw-admin", "metadata", "list", "bucket"],
        capture_output=True, text=True
    )
    if proc.returncode != 0:
        return [{"error": proc.stderr.strip()}]
    
    try:
        buckets = json.loads(proc.stdout) if proc.stdout.strip() else []
    except json.JSONDecodeError:
        return [{"error": "Failed to parse bucket list"}]
    
    results = []
    total = len(buckets)
    for i, bucket in enumerate(buckets):
        repair = SyncRepair(bucket=bucket)
        result = repair.check()
        results.append(result)
        # Progress to stderr so stdout stays clean for JSON/piping
        issues = sum(1 for r in results if r.get("status") == "failed")
        sys.stderr.write(
            f"\rChecked {i+1:>6}/{total} buckets... {issues} with issues found"
        )
        sys.stderr.flush()
    
    sys.stderr.write("\n")
    sys.stderr.flush()
    
    return results


def print_json(data: Dict):
    print(json.dumps(data, indent=2))


def print_human(data: Dict, check_mode: bool):
    if check_mode:
        print(f"Bucket: {data['bucket']}")
        print(f"Status: {data['status']}")
        
        if "error" in data:
            print(f"Error: {data['error']}")
            return
        
        print(f"Total sources: {data['total_sources']}")
        print(f"Healthy sources: {data['healthy_sources']}")
        print(f"Issues: {data['issue_count']}")
        
        if "note" in data:
            print(f"Note: {data['note']}")
        
        print()
        
        if data["issues"]:
            print("Issues found:")
            for issue in data["issues"]:
                print(f"  Zone: {issue['zone_name']} ({issue['zone_id']})")
                print(f"  Type: {issue['type']}")
                if issue["type"] == "failed_to_read_remote_log":
                    print(f"  Error: ({issue['error_code']}) {issue['error_message']}")
                elif issue["type"] == "behind":
                    print(f"  Behind shards: {issue['behind_shards']}")
                print()
    else:
        print(f"Bucket: {data['bucket']}")
        print(f"Status: {data['status']}")
        print(f"Fixed: {data['fixed_count']}")
        print(f"Failed: {data['failed_count']}")
        print()
        
        if data.get("fixed"):
            print("Fixed:")
            for fix in data["fixed"]:
                print(f"  Zone: {fix['zone_name']}")
                print(f"  Issue: {fix['issue_type']}")
                print(f"  Init result: {fix['init_result']}")
                print(f"  Run result: {fix['run_result']}")
                print()
        
        if data.get("failed"):
            print("Failed:")
            for failed in data["failed"]:
                print(f"  Zone: {failed['zone_name']}")
                print(f"  Issue: {failed['issue_type']}")
                if "init_error" in failed:
                    print(f"  Init error: {failed['init_error']}")
                if "run_error" in failed:
                    print(f"  Run error: {failed['run_error']}")
                print()


def main():
    parser = argparse.ArgumentParser(
        description="RGW Sync Repair Tool for Ceph multisite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              Check a bucket:
                %(prog)s --bucket tenant/bucket1 --check
            
              Reset sync for a bucket:
                %(prog)s --bucket tenant/bucket1 --reset-sync
            
              Check all buckets:
                %(prog)s --check-all --json
        """),
    )
    
    parser.add_argument(
        "--bucket",
        help="Bucket to check/reset-sync (format: [tenant/]bucket_name)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check sync status for the bucket",
    )
    parser.add_argument(
        "--reset-sync",
        action="store_true",
        dest="reset_sync",
        help="Reset sync markers for the bucket (runs bucket sync init + run)",
    )
    parser.add_argument(
        "--check-all",
        action="store_true",
        help="Check all buckets (slow, check-mode only)",
    )
    parser.add_argument(
        "--source-zone",
        help="Only target this source zone name",
        default=None,
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of human-readable text",
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.check_all:
        if args.reset_sync:
            parser.error("--check-all is check-mode only (use --bucket with --reset-sync)")
    else:
        if not args.bucket:
            parser.error("--bucket is required (or use --check-all)")
        if not args.check and not args.reset_sync:
            parser.error("Specify --check or --reset-sync")
    
    # Build output structure
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    if args.check_all:
        # Check all buckets
        results = check_all_buckets([])
        output["mode"] = "check_all"
        output["buckets_checked"] = len(results)
        output["buckets_with_issues"] = sum(
            1 for r in results if r.get("status") == "failed"
        )
        output["results"] = results
        
        if args.json:
            print_json(output)
        else:
            print(f"Buckets checked: {output['buckets_checked']}")
            print(f"With issues: {output['buckets_with_issues']}")
            for r in results:
                if r.get("status") == "failed":
                    print(f"\n❌ {r['bucket']}:")
                    for issue in r.get("issues", []):
                        print(f"   - {issue['zone_name']}: {issue['type']}")
                elif "error" in r:
                    print(f"\n⚠️  {r.get('bucket', '?')}: {r['error']}")
        
        sys.exit(1 if output["buckets_with_issues"] > 0 else 0)
    
    # Single bucket mode
    repair = SyncRepair(
        bucket=args.bucket,
        source_zone=args.source_zone,
    )
    
    if args.check:
        result = repair.check()
        output.update(result)
        
        if args.json:
            print_json(output)
        else:
            print_human(result, check_mode=True)
        
        sys.exit(1 if result.get("issue_count", 0) > 0 else 0)
    
    elif args.reset_sync:
        result = repair.fix()
        output.update(result)
        
        if args.json:
            print_json(output)
        else:
            print_human(result, check_mode=False)
        
        sys.exit(2 if result["fixed_count"] > 0 else 0)


if __name__ == "__main__":
    main()
