"""Rebuild facilities_town_boilerplate.json (2026-10-29).

    python tools/town_boilerplate.py <bodies.jsonl> [out.json]

<bodies.jsonl> is one JSON object per line, a read-only export of the stored
bodies of the systems in facilities_town.SPECS:

    select hospital_system, left(description, 3000) d from hospital_jobs
    where is_active and hospital_system in (<the SPECS systems>)

For each system it keeps the fingerprints of the sentences that recur across
its postings (facilities_town.boilerplate_keys: in at least
BOILERPLATE_MIN_ROWS postings and BOILERPLATE_MIN_SHARE of them): the "About
us" paragraph, the benefits paragraph, the EEO line. facilities_town reads a
posting's clinic and office words only in the sentences that are not
boilerplate. Run it when a system rewrites its boilerplate (its rows then
fall to "offsite" in the run log's "Town tables" line).
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import facilities_town  # noqa: E402


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    out_path = argv[2] if len(argv) > 2 else facilities_town.BOILERPLATE_FILE
    bodies = {}
    with open(argv[1], encoding="utf-8") as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                if r.get("hospital_system") in facilities_town.SPECS:
                    bodies.setdefault(r["hospital_system"], []).append(r.get("d") or "")
    out = {}
    for system, ds in sorted(bodies.items()):
        keys = facilities_town.boilerplate_keys(ds)
        if keys:
            out[system] = keys
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=0, sort_keys=True)
        f.write("\n")
    print(f"{len(out)} systems, {sum(len(v) for v in out.values())} boilerplate sentences -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
