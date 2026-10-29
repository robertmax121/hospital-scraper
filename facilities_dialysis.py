"""The clinic on dialysis-chain rows filed under the chain's name (2026-10-29).

The owner, 2026-10-29: "i thought you were gonna edit all the large
multihospital systems". Every large employer that posts for many sites under
one name is in scope, chains included. His two rules:
  1. cards and job pages show the SITE ("Apply to <site>"); the system name
     and logo stay on the job page and the system hub (hospital_system is
     never touched here: the upsert key, hub, filters, logo);
  2. named sites (hospitals, named clinics and centers) show their own name;
     generic offices, home-based and remote jobs show the system name with
     the town; a town is never guessed.

Where each chain's posting names its clinic (read 2026-10-29 off each feed
and the stored rows):

  DaVita (Phenom list, Workday apply link; 3,378 active rows, every one on
    the label). The stored link's first path segment is the clinic's Workday
    location, "<5-digit clinic number> - <clinic name>":
      ".../job/05107---Fort-Wayne-West/..."        DaVita Fort Wayne West Dialysis
      ".../job/12524---Renal-Center-of-Carrollton/" DaVita Renal Center of Carrollton
      ".../job/06243---Hunters-Creek-At-Home/..."  DaVita Hunters Creek at Home
    The same segment rides in the list's applyUrl every night and is kept on
    the stored row, so the nightly and the stored-row change list agree.
    Not named (decision 2, system label and the posting's own town):
      acute programs ("01076 - HSHS St Elizabeths Acute", "03404H1 - Yale
        Acutes Hospital 1"): DaVita staff working inside another employer's
        hospital, rotating between its units and often several hospitals;
      SNF programs ("SNF Dialysis At Laurels of Huber Heights"): inside a
        nursing home of another operator;
      offices, regions, float pools, remote, Integrated Kidney Care (IKC),
        VillageHealth, clinical research, labs of the business office, a
        de novo clinic not open yet ("Reidsville DeNovo"), a closed one
        ("CLSD Use 09415 ...").
  Fresenius Medical Care (Workday; 2,874 active rows on the label). The
    location descriptor (and the link's first segment) is a town ("Greenville,
    NC": 2,632 rows, no clinic named anywhere in the posting: system label,
    the town is the place) or a clinic code, "<ST><3 digits> <clinic> -
    Clinic" ("DE007 Christiana - Clinic", "AZ101 BMA Flagstaff JV - Clinic"):
    Fresenius Kidney Care <clinic>, the clinic brand Fresenius's postings use.
    "<code> <hospital> - Hospital" is an acute service inside another
    employer's hospital: system label, the hospital's town (HOSPITAL_SITES).
    Every clinic-code row was stored with no town (the generic parse reads
    none): the code's state is the posting's, and the town is the one the
    clinic's own name leads or ends with when that is a known town of that
    state ("FMCNA Grayslake" -> Grayslake, IL; "Juniper Fontana RAI" ->
    Fontana, CA); otherwise the town stays blank (never guessed). A DC code
    is Washington.
  US Renal Care (Jibe; 438 active rows on the label). location_name is
    "<code> - <clinic> <ST>" ("1207 - Gulfgate Kidney Center TX", "0021 -
    McAllen TX"): U.S. Renal Care <clinic> (Dialysis added when the name
    names no dialysis, kidney or renal), the brand U.S. Renal Care's own
    postings use. Offices ("TX00 - Dallas Corporate TX"), "Work @ Home",
    "remote" and joint-venture entities ("Faxton Inc", "Ulster Managed
    Inc") keep the label. location_name is in the live feed only (the
    stored row keeps city and state), so the stored rows are named by the
    first nightly after this lands.

Dialysis Clinic, Inc. (iCIMS) and Innovative Renal Care (Phenom) are left as
they are: DCI's postings name only a town; 17 of Innovative Renal Care's 178
postings carry a "Location: <clinic> - <street>" line (a follow-up).

Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase3/dialysis_work/changes.py).
"""
from __future__ import annotations

import re

from city_utils import known_town

DAVITA = "DaVita"
FRESENIUS = "Fresenius Medical Care"
USRC = "US Renal Care"
SYSTEMS = frozenset({DAVITA, FRESENIUS, USRC})

# the label a chain's rows carry (hospital_name equal to one of these, or
# blank, is "on the label")
LABELS = {
    DAVITA: {"davita", "davita inc", "davita kidney care", "davita dialysis"},
    FRESENIUS: {"fresenius medical care", "fresenius kidney care", "fresenius medical care north america",
                "fmcna", "fresenius"},
    USRC: {"us renal care", "u s renal care", "usrc"},
}

_STATE_CODES = frozenset("""AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE
NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY PR""".split())
_SMALL = {"of", "at", "the", "and", "on", "in"}
# whole-word spellings written out (a Workday location name is cut short or
# abbreviated: "Columbus Dwntwn Dialys", "Ocala Regional Kidney Cntr E")
_WORDS = {"cntr": "Center", "ctr": "Center", "cntrl": "Central", "dwntwn": "Downtown",
          "southwstrn": "Southwestern", "ctrl": "Central", "davita": "DaVita"}
# a last word cut short: "Dialys" / "Dialy" / "Dial" / "Dialysi" (and a lone
# "D" after a name: "UT Southwstrn Dallas D") is Dialysis; "Cent" / "Cente"
# after Dialysis / Renal / Kidney is Center
_DIALYSIS_CUT = {"d", "dial", "dialy", "dialys", "dialysi"}
_CENTER_CUT = {"cent", "cente"}
_TRAILING_ENTITY = {"llc", "inc", "trc", "jv", "rai"}


def _town_key(s) -> str:
    s = re.sub(r"[^a-z ]", "", (s or "").lower())
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bmount\b", "mt", s)
    s = re.sub(r"\bfort\b", "ft", s)
    return s.replace(" ", "")


def _fkey(s) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", (s or "").lower().replace("'", "").replace(".", "")).split())


def on_label(system: str, name) -> bool:
    """The row's hospital_name is the chain itself (or blank)."""
    k = _fkey(name)
    return not k or k in LABELS.get(system, set()) or k == _fkey(system)


def _segment(url) -> str:
    """The first path segment after /job/ of a Workday link ("" otherwise)."""
    m = re.search(r"\.(?:myworkdayjobs|myworkdaysite)\.com/.*?/job/([^/?#]+)/", url or "", re.I)
    return m.group(1) if m else ""


def _words_from_slug(slug: str) -> list[str]:
    """Words of a Workday slug: "---" is " - ", "--" a dropped character,
    "-" a space; " - " separators come back as "-" words."""
    parts = re.split(r"-{3,}", slug)
    out: list[str] = []
    for i, p in enumerate(parts):
        if i:
            out.append("-")
        out.extend(w for w in re.split(r"[-_\s]+", p) if w)
    return out


def _words_from_text(text: str) -> list[str]:
    out: list[str] = []
    for i, p in enumerate(re.split(r"\s+-\s+", (text or "").strip())):
        if i:
            out.append("-")
        out.extend(w for w in re.split(r"[\s_]+", p) if w)
    return out


def _case(w: str, first: bool, all_caps: bool) -> str:
    """One word's case: a name written all in capitals ("INWOOD DIALYSIS")
    is title-cased; otherwise a capitalised short word is an acronym kept as
    written ("East LA Plaza", "PMC", "NOLA"), a small word is lower case
    ("Renal Center of Hamilton", "Lithonia at Home")."""
    lw = w.lower()
    if lw in _WORDS:
        return _WORDS[lw]
    if all_caps:
        return lw if (lw in _SMALL and not first) else (w.capitalize() if w.isalpha() else w)
    if w.isupper():
        return w
    if lw in _SMALL and not first:
        return lw
    if w.islower() and w.isalpha():
        return w.capitalize()
    return w


def _tidy(words: list[str], state: str = "") -> list[str]:
    """Spellings written out, a trailing state code ("Port Orange - FL",
    "Jerseyville Dialysis IL"; `state` in any case: "Norwalk East Ca") and
    legal-entity words dropped, " - " separators dropped, cut last words
    completed."""
    w = [x for x in words]
    # trailing entity words and state codes, in any order ("Dialysis Center LLC",
    # "Bloomington Dialysis Unit TRC", "Balcones Dialysis - TX")
    changed = True
    while changed and len(w) > 1:
        changed = False
        last = w[-1]
        if last == "-":
            w.pop(); changed = True; continue
        if last.lower() in _TRAILING_ENTITY:
            w.pop(); changed = True; continue
        up = last.upper()
        if up in _STATE_CODES and (w[-2] == "-" or (up == state.upper() if state else last.isupper())):
            w.pop(); changed = True; continue
    w = [x for x in w if x != "-"]
    if not w:
        return w
    lw = w[-1].lower()
    if lw in _DIALYSIS_CUT and len(w) > 1 and (lw != "d" or len(w) > 2):
        w[-1] = "Dialysis"
    elif lw in _CENTER_CUT and len(w) > 1 and w[-2].lower() in ("dialysis", "renal", "kidney"):
        w[-1] = "Center"
    letters = [x for x in w if x.isalpha() and len(x) > 1]
    all_caps = bool(letters) and all(x.isupper() for x in letters) and any(len(x) > 3 for x in letters)
    return [_case(x, i == 0, all_caps) for i, x in enumerate(w)]


# ── DaVita ──────────────────────────────────────────────────────────────────
_DV_SEG_RX = re.compile(r"^(\d{5})(H\d+)?-{3}(.+)$")
# words that make a DaVita location something other than a clinic (decision 2:
# the system label with the posting's own town)
_DV_OFFICE = {"office", "offices", "hq", "remote", "remotes", "virtual", "region", "division", "divisional",
              "cbo", "pool", "float", "ikc", "ckcc", "sga", "denovo", "ventures", "dcr", "team",
              "clsd", "corporate", "billing", "admin", "administrative"}
_DV_ACUTE = {"acute", "acutes"}
_DV_HOME = {"home", "ht", "pd", "hhd"}
_DV_CLINIC_WORDS = {"dialysis", "kidney", "renal", "nephrology"}


def davita_site(url) -> tuple[str, str | None]:
    """(kind, name) for a DaVita row's Workday link. kind: "clinic" (a named
    clinic or home-dialysis center; name is its public name), "acute",
    "snf", "office" (name None: the system label), "" (no DaVita location in
    the link)."""
    m = _DV_SEG_RX.match(_segment(url))
    if not m:
        return "", None
    if m.group(2):
        return "acute", None             # "03404H1 - Yale Acutes Hospital 1"
    words = _words_from_slug(m.group(3))
    low = [x.lower() for x in words]
    if not low:
        return "", None
    if low[0] in ("snf", "clsd"):
        return ("snf" if low[0] == "snf" else "office"), None
    if any(x in _DV_ACUTE for x in low):
        return "acute", None
    if any(x in _DV_OFFICE for x in low) or "village health" in " ".join(low):
        return "office", None            # VillageHealth (care management), not "Jersey Village Dialysis"
    if "group" in low and ("med" in low or "medical" in low):
        return "office", None            # "Aurora Med Group Sheboygan": a partner's clinic
    if "laboratory" in low or "lab" in low:
        if low[:1] == ["davita"]:
            return "clinic", " ".join(_tidy(words))      # "DaVita Laboratory Services", a named site
        return "office", None
    tidy = _tidy(words)
    if not tidy:
        return "", None
    tl = [x.lower() for x in tidy]
    name = " ".join(tidy)
    if tl[0] != "davita":
        name = "DaVita " + name
    named_kind = (set(tl) & (_DV_CLINIC_WORDS | _DV_HOME)) or "of" in tl or (
        len(tidy[-1]) >= 2 and tidy[-1].isupper() and tidy[-1].isalpha() and tl[-1] not in ("ii", "iii", "iv"))
    if not named_kind:
        name += " Dialysis"              # "05107 - Fort Wayne West" -> DaVita Fort Wayne West Dialysis
    return "clinic", name


# ── Fresenius ───────────────────────────────────────────────────────────────
_FME_CODE_RX = re.compile(r"^([A-Za-z]{2})(\d{3})[\s-]+(.+?)(?:\s+-\s+|-{3})([A-Za-z ]+)$")
# an acute service inside another employer's hospital: the hospital's town
HOSPITAL_SITES = {
    "PA336": ("Philadelphia", "PA", "cms 390174 (Thomas Jefferson University Hospital)"),
}
_FME_PREFIX_RX = re.compile(r"^(?:Fmc\s+Dial\s+Svcs\s+Of|FMCNA|FMC|BMA)\s+", re.I)
# what may follow a town at the head of a clinic's name and still leave the
# town the clinic's ("Little Rock West", "Gilbert II", "Roseville Home",
# "Kingman Dialysis"); anything else makes it a street, a district or a
# region ("Webster Avenue", "Marina Bay", "Jefferson Co Dialysis")
_LEAD_OK = {"north", "south", "east", "west", "ne", "nw", "se", "sw", "n", "s", "e", "w", "central", "downtown",
            "home", "ii", "iii", "iv", "dialysis", "capd", "haemo", "hemo", "airport", "kidney", "center", "renal"}


def fresenius_code(text) -> tuple[str, str, str, str] | None:
    """(state, code, clinic words, kind) for a clinic-code descriptor or
    slug ("DE007 Christiana - Clinic", "DE007-Christiana---Clinic"), None
    for a town descriptor ("Greenville, NC")."""
    t = (text or "").strip()
    m = _FME_CODE_RX.match(t)
    if not m:
        return None
    st = m.group(1).upper()
    if st not in _STATE_CODES:
        return None
    return st, st + m.group(2), m.group(3), m.group(4).strip().lower()


def _clinic_words(raw: str, slug: bool) -> list[str]:
    words = _words_from_slug(raw) if slug else _words_from_text(raw)
    text = _FME_PREFIX_RX.sub("", " ".join(x for x in words if x != "-"))
    return [x for x in text.split() if x]


def _town_from_name(words: list[str], state: str) -> str:
    """The known town of `state` a clinic's name is named for ("" if none):
    the name is the town, the town and a qualifier ("Little Rock West",
    "Gilbert II", "Kingman Dialysis"), or a district and the town ("West
    Tucson", "Juniper Fontana", "Old Town Scottsdale"); up to four words,
    the longest first."""
    n = len(words)
    for size in range(min(4, n), 0, -1):
        if all(x.lower() in _LEAD_OK for x in words[size:]):
            t = known_town(" ".join(words[:size]), state)
            if t:
                return t
    for size in range(min(4, n - 1), 0, -1):
        t = known_town(" ".join(words[n - size:]), state)
        # "Western Hartford" may be West Hartford, a town of its own
        if t and words[n - size - 1].lower() not in ("of", "at", "western", "eastern", "northern", "southern"):
            return t
    return ""


def fresenius_site(loc_or_url) -> tuple[str, str | None, str, str, str] | None:
    """(kind, name, city, state, source) for a Fresenius descriptor or link,
    None for a town descriptor. kind "clinic" (name = Fresenius Kidney Care
    <clinic>), "hospital" / "office" (name None: the system label). city is
    "" when neither the code nor the clinic's name gives one."""
    text = loc_or_url or ""
    seg = _segment(text)
    got = fresenius_code(seg if seg else text)
    if not got:
        return None
    st, code, raw, kind = got
    words = _clinic_words(raw, slug=bool(seg))
    if kind == "hospital":
        hit = HOSPITAL_SITES.get(code)
        if hit:
            return "hospital", None, hit[0], hit[1], hit[2]
        return "hospital", None, "", st, f"code {code}"
    if kind != "clinic":
        return "office", None, "", st, f"code {code}"
    tidy = _tidy(words, st)
    if not tidy:
        return "office", None, "", st, f"code {code}"
    tl = [x.lower() for x in tidy]
    if "vascular" in tl and tl.index("vascular") == 0:
        # a vascular-access center names itself only with its place ("Orange
        # County Vascular Access"); "FMCNA Vascular Care LA" keeps the label
        return "office", None, "", st, f"code {code}"
    if st == "DC":
        city, src = "Washington", f"code {code} (DC)"
    else:
        city = _town_from_name(tidy, st)
        src = f"code {code}" + (" (the clinic's name names the town)" if city else "")
    if "vascular" in tl:
        name = " ".join(tidy)            # "Orange County Vascular Access": a vascular center, own name
    else:
        name = "Fresenius Kidney Care " + " ".join(tidy)
    return "clinic", name, city, st, src


_FME_TOWN_SEG_RX = re.compile(r"^(.+?)-([A-Za-z]{2})(?:-USA)?$")


def fresenius_town(url) -> tuple[str, str] | None:
    """(town, ST) from a town segment ("Spokane-WA-USA", "N-Myrtle-Beach-SC"),
    the board's own town, when it is a known town of that state."""
    m = _FME_TOWN_SEG_RX.match(_segment(url))
    if not m:
        return None
    st = m.group(2).upper()
    if st not in _STATE_CODES:
        return None
    raw = m.group(1).replace("-", " ")
    raw = re.sub(r"^N\s", "North ", raw)
    t = known_town(raw, st) or known_town(m.group(1).replace("-", " "), st)
    return (t, st) if t else None


# ── US Renal Care ───────────────────────────────────────────────────────────
_USRC_RX = re.compile(r"^\s*([A-Za-z0-9]{2,6})\s+-\s+(.+?)\s*$")
_USRC_OFFICE = {"corporate", "office", "remote", "home", "work", "inc", "managed", "llc", "lab", "laboratory",
                "acute", "hospital", "billing", "region", "regional", "zz"}


def usrc_facility(location_name) -> str | None:
    """hospital_name for a US Renal Care location_name, None to keep the
    label ("TX00 - Dallas Corporate TX", "9999 - Work @ Home ZZ",
    "1228 - Faxton Inc NY", "remote")."""
    m = _USRC_RX.match(location_name or "")
    if not m:
        return None
    words = [w for w in re.split(r"\s+", m.group(2).replace("@", " ")) if w]
    low = [w.lower() for w in words]
    if any(w in _USRC_OFFICE for w in low):
        return None
    tidy = _tidy(words)
    if not tidy:
        return None
    tl = {x.lower() for x in tidy}
    name = "U.S. Renal Care " + " ".join(tidy)
    if not tl & {"dialysis", "kidney", "renal"}:
        name += " Dialysis"
    return name


# ── one row ─────────────────────────────────────────────────────────────────
_ADDR_RX = re.compile(r"(?m)^[^\n]*?,\s*([A-Za-z .'-]+?),\s*([A-Za-z ]+?),\s*\d{5}(?:-?\d{4})?,\s*United States of America\s*$")
_STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "district of columbia": "DC", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "puerto rico": "PR",
}


def davita_address_town(description) -> tuple[str, str] | None:
    """(town, ST) of the street-address line a DaVita posting opens with
    ("4916 Illinois RdSuite 118, Fort Wayne, Indiana, 46804, United States of
    America"), the clinic's own address; None when there is none."""
    m = _ADDR_RX.search((description or "")[:600])
    if not m:
        return None
    st = _STATE_NAMES.get(m.group(2).strip().lower())
    return (m.group(1).strip(), st) if st else None


def resolve_row(system, *, hospital_name="", url="", city="", state="", description=""):
    """(name, city, state, how) for one row of a chain on its label, or None
    when nothing changes. name None keeps the label (the town may still be
    filled). how: "clinic", "acute", "snf", "office", "hospital", "town".
    Shared by the scraper (finalize) and the stored-row change list."""
    if system not in (DAVITA, FRESENIUS) or not on_label(system, hospital_name):
        return None
    city = (city or "").strip()
    state = (state or "").strip()
    if system == DAVITA:
        kind, name = davita_site(url)
        if not kind:
            return None
        if not city:
            # the posting's own street address; DaVita's head office default
            # (SYSTEM_LOCATION_DEFAULTS) never fills a named clinic
            got = davita_address_town(description)
            if got:
                city, state = got
        return name, city, state, kind
    got = fresenius_site(url)
    if got is None:
        if not city:
            t = fresenius_town(url)
            if t and (not state or state == t[1]):
                return None, t[0], t[1], "town"
        return None
    kind, name, c2, s2, _src = got
    if city and (re.search(r"\d", city) or not known_town(city, s2)):
        # the generic parse read the descriptor, not a town ("CA033 Camarillo -
        # Clinic" kept whole on stored rows, "Clinic" on tonight's list)
        city = ""
    if not city:
        # the clinic code's state is the posting's (a parse of "Vascular Care
        # LA" read Louisiana); the town is the clinic's own when its name
        # gives one, else blank
        return name, c2, s2, kind
    if state and state != s2:
        return None, city, state, kind + " other state"
    if c2 and _town_key(c2) != _town_key(city):
        # the posting sits in another town than the one the clinic is named
        # for ("CA019 Oxnard" at Long Beach): no site is named
        return None, city, state or s2, kind + " other town"
    return name, city, state or s2, kind


# review B 2026-10-29 (facility release 3): one row nobody foresaw must not
# end the pass for every row after it. The row that raises keeps the name and
# town it came with and counts as "error" in the pass's stats.
_ROW_FIELDS = ("hospital_name", "city", "state", "location")


def _row_guard(fn, job, *args):
    keep = [(a, getattr(job, a)) for a in _ROW_FIELDS if hasattr(job, a)]
    try:
        return fn(job, *args)
    except Exception:
        for a, v in keep:
            try:
                setattr(job, a, v)
            except Exception:
                pass
        return "error"


def apply_jobs(jobs) -> dict:
    """finalize_jobs pass: DaVita and Fresenius rows on their label take the
    clinic's name (and a town only where the row has none). Returns
    {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        system = getattr(job, "hospital_system", "")
        if system not in (DAVITA, FRESENIUS):
            continue
        how = _row_guard(_apply_one, job, system)
        d = stats.setdefault(system, {})
        d[how] = d.get(how, 0) + 1
    return stats


def _apply_one(job, system) -> str:
    """apply_jobs on one DaVita / Fresenius row; returns how."""
    got = resolve_row(system, hospital_name=job.hospital_name, url=job.url, city=job.city,
                      state=job.state, description=job.description)
    how = "none"
    if got:
        name, city, state, how = got
        if name:
            job.hospital_name = name
        if (city, state) != ((job.city or "").strip(), (job.state or "").strip()) and (city or state):
            job.city, job.state = city, state
            job.location = f"{city}, {state}" if city else state
    return how
