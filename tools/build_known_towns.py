"""Build known_towns.json, the town list city_utils reads (2026-10-20).

    python tools/build_known_towns.py <cms.jsonl> <jobs.jsonl> <travel.jsonl> [out.json]

Each input is one JSON object per line, read-only exports of:

  cms.jsonl     select city, state, count(*) n from hospitals
                where city <> '' group by 1, 2
  jobs.jsonl    select city, state, count(distinct hospital_system) nsys,
                count(*) n from hospital_jobs where is_active and city <> ''
                and state ~ '^[A-Z]{2}$' group by 1, 2
  travel.jsonl  select city, state, count(distinct agency_name) nag,
                count(*) n from travel_jobs where is_active and city <> ''
                and state ~ '^[A-Z]{2}$' group by 1, 2

A town is listed when a CMS hospital sits in it, two or more health systems
post jobs in it, or two or more travel agencies list it, and its name passes
city_utils' place rules. "<Words> Center" / "Centre" names (the one class
the word rules cannot settle) are listed only from those sources or
CENTER_TOWNS below. The spelling is the mixed-case one the most rows use;
an all-caps CMS name is title-cased ("MCALLEN" -> "McAllen").
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import city_utils  # noqa: E402

# Real towns named "... Center" / "... Centre" that one employer alone posts
# in (checked by name 2026-10-20): they are towns, not facilities.
CENTER_TOWNS = {
    ("Mansfield Center", "CT"), ("Rockville Centre", "NY"), ("Sun City Center", "FL"),
    ("Sioux Center", "IA"), ("Center Valley", "PA"), ("Byron Center", "MI"),
    ("Brooklyn Center", "MN"), ("Sauk Centre", "MN"), ("Grundy Center", "IA"),
    ("Clay Center", "KS"), ("Clay Center", "NE"), ("Center Moriches", "NY"),
    ("Center Harbor", "NH"), ("Center Point", "TX"), ("Center Point", "AL"),
    ("Center Line", "MI"), ("Wallingford Center", "CT"), ("Centre Hall", "PA"),
    ("Pine Island Center", "FL"), ("Mendon Center", "NY"), ("Holiday City Center", "NJ"),
    ("Newton Center", "MA"), ("Newton Centre", "MA"), ("State Center", "IA"),
    ("Hale Center", "TX"), ("Lewis Center", "OH"),
}


_STATE_NAMES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA", "Colorado": "CO",
    "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}


def _title(c: str) -> str:
    if not (c.isupper() or c.islower()):
        return c
    out = []
    for w in c.lower().split(" "):
        if re.fullmatch(r"mc[a-z]{2,}", w):
            w = "Mc" + w[2].upper() + w[3:]
        elif w in ("of", "the", "on", "by", "and") and out:
            pass
        else:
            w = "-".join(p[:1].upper() + p[1:] for p in w.split("-"))
        out.append(w)
    return " ".join(out)


# A town only one travel agency lists is listed too, so a small town inside
# a site name is found ("Harpers Ferry Family Medicine", "MUSC Health
# Primary Care Blythewood"), but only a plain name: no comma, digit, "US-"
# feed prefix, building word, or "Center" / "Centre" / "Ctr" / "Plaza" end.
# A town only ONE employer files is not: that is where a site name filed as
# the town comes from ("Encircle" for "Encircle Health").
def _single_source_ok(city) -> bool:
    c = re.sub(r"\s+", " ", (city or "")).strip()
    return (bool(c) and not re.search(r"[,\d]|^US-|\b(?:center|centre|ctr|plaza)\.?$|^gss\b", c, re.I)
            and not city_utils._BUILDING_WORD_RE.search(c) and not city_utils._FACILITY_RE.search(c)
            and not re.fullmatch(r"[A-Z]{2,5}", c))


def _rows(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                yield json.loads(line)


def build(cms_path, jobs_path, travel_path):
    city_utils._KNOWN_TOWNS, city_utils._KNOWN_ANY = {}, set()
    cand = {}     # (key, st) -> {"src": set, "spell": {spelling: count}}

    def add(city, st, src, n):
        city = re.sub(r"\s+", " ", (city or "")).strip()
        st = (st or "").strip().upper()
        if st not in city_utils.STATE_CODES or not city:
            return
        k = city_utils.town_key(city)
        if not k:
            return
        c = cand.setdefault((k, st), {"src": set(), "spell": {}})
        c["src"].add(src)
        c["spell"][city] = c["spell"].get(city, 0) + int(n or 1)

    for r in _rows(cms_path):
        add(r.get("city"), r.get("state"), "cms", 1)
    for r in _rows(jobs_path):
        if int(r.get("nsys") or 0) >= 2:
            add(r.get("city"), r.get("state"), "jobs", r.get("n"))
    for r in _rows(travel_path):
        if int(r.get("nag") or 0) >= 2:
            add(r.get("city"), r.get("state"), "travel", r.get("n"))
        elif _single_source_ok(r.get("city")):
            add(r.get("city"), r.get("state"), "travel1", r.get("n"))
    for city, st in CENTER_TOWNS:
        add(city, st, "named", 1)

    state_names = {city_utils.town_key(n): code for n, code in _STATE_NAMES.items()}
    towns, dropped = {}, []
    for (k, st), c in sorted(cand.items()):
        if state_names.get(k) == st and st != "NY":
            dropped.append((k, st, "the state's own name"))
            continue
        spell = sorted(c["spell"].items(), key=lambda kv: (kv[0].isupper(), -kv[1], kv[0]))[0][0]
        spell = _title(spell)
        # the place rules, the Center rule aside (the sources settle it)
        if not city_utils.is_plausible_city(spell, known=False):
            dropped.append((spell, st, sorted(c["src"])))
            continue
        if city_utils._CENTER_END_RE.search(spell) and len(spell.split()) >= 2                 and "named" not in c["src"] and c["src"] <= {"jobs", "jobs1", "travel1"}:
            # a "Center" name only employers post: listed only when named above
            dropped.append((spell, st, sorted(c["src"])))
            continue
        towns.setdefault(st, {})[k] = spell
    return towns, dropped


def main():
    cms, jobs, travel = sys.argv[1:4]
    out = sys.argv[4] if len(sys.argv) > 4 else os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "known_towns.json")
    towns, dropped = build(cms, jobs, travel)
    n = sum(len(m) for m in towns.values())
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"built": "2026-10-20", "sources": "CMS hospitals; hospital_jobs and travel_jobs towns (plain names "
                   "from one source; Center names only from CMS, 2+ systems or agencies, or named)",
                   "towns": {st: dict(sorted(m.items())) for st, m in sorted(towns.items())}},
                  f, ensure_ascii=True, separators=(",", ":"), sort_keys=False)
        f.write("\n")
    print(f"{n} towns in {len(towns)} states -> {out}; {len(dropped)} candidates refused")
    for d in dropped:
        print("  refused:", d)


if __name__ == "__main__":
    main()
