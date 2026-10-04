#!/usr/bin/env python3
"""
Compare two directories of dashboard JSON files (e.g. legacy vs DuckDB build).

    python scripts/compare_outputs.py OLD_DIR NEW_DIR

Lists of records are matched by their natural key (date, stop_id, trip_id, ...),
floats may differ by TOLERANCE (pandas rounds half-to-even, so a .x5 can land on
either side), and last_updated is ignored. Exits 1 if anything differs.
"""
import json
import sys
from pathlib import Path

TOLERANCE = 0.051
IGNORED = {"last_updated"}
RECORD_KEYS = [("incident_id",), ("id",), ("day_of_week", "hour"), ("date",), ("week_start",), ("month",),
               ("stop_id",), ("trip_id",), ("name",)]


def _key_fields(items: list) -> tuple | None:
    if not items or not all(isinstance(i, dict) for i in items):
        return None
    for fields in RECORD_KEYS:
        if all(all(f in i for f in fields) for i in items):
            keys = [tuple(i[f] for f in fields) for i in items]
            if len(set(keys)) == len(keys):
                return fields
    return None


def diff(old, new, path: str = "") -> list[str]:
    if isinstance(old, bool) or isinstance(new, bool):
        return [] if old == new else [f"{path}: {old!r} != {new!r}"]
    if isinstance(old, (int, float)) and isinstance(new, (int, float)):
        if isinstance(old, int) and isinstance(new, int):
            return [] if old == new else [f"{path}: {old} != {new}"]
        return [] if abs(old - new) <= TOLERANCE else [f"{path}: {old} != {new}"]
    if isinstance(old, dict) and isinstance(new, dict):
        out = []
        for k in sorted(set(old) | set(new), key=str):
            if k in IGNORED:
                continue
            if k not in new:
                out.append(f"{path}.{k}: missing in new")
            elif k not in old:
                out.append(f"{path}.{k}: only in new")
            else:
                out += diff(old[k], new[k], f"{path}.{k}")
        return out
    if isinstance(old, list) and isinstance(new, list):
        fields = _key_fields(old)
        if fields and fields == _key_fields(new):
            o = {tuple(i[f] for f in fields): i for i in old}
            n = {tuple(i[f] for f in fields): i for i in new}
            out = [f"{path}[{k}]: missing in new" for k in o.keys() - n.keys()]
            out += [f"{path}[{k}]: only in new" for k in n.keys() - o.keys()]
            for k in sorted(o.keys() & n.keys(), key=str):
                out += diff(o[k], n[k], f"{path}[{k}]")
            return out
        if len(old) != len(new):
            return [f"{path}: length {len(old)} != {len(new)}"]
        return [d for i, (a, b) in enumerate(zip(old, new)) for d in diff(a, b, f"{path}[{i}]")]
    return [] if old == new else [f"{path}: {old!r} != {new!r}"]


def compare_dirs(old_dir: Path, new_dir: Path) -> dict[str, list[str]]:
    results = {}
    for old_file in sorted(Path(old_dir).glob("*.json")):
        new_file = Path(new_dir) / old_file.name
        if not new_file.exists():
            results[old_file.name] = ["missing in new"]
            continue
        old = json.loads(old_file.read_text(encoding="utf-8"))
        new = json.loads(new_file.read_text(encoding="utf-8"))
        results[old_file.name] = diff(old, new)
    return results


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    results = compare_dirs(Path(sys.argv[1]), Path(sys.argv[2]))
    for name, problems in results.items():
        print(f"{'OK  ' if not problems else 'DIFF'} {name}" + (f" ({len(problems)} differences)" if problems else ""))
        for p in problems[:15]:
            print(f"     {p}")
    return 1 if any(results.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
