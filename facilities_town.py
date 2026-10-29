"""The hospital on a system-label row, from the town the posting names (2026-10-29).

The owner, 2026-10-29: every hospital inside a multi-hospital system must be
named and placed correctly on the site and in the dashboard, and the applies
retro-assigned to the right hospital ("any multi system hospital is subject to
this"). Decisions: (1) cards and job pages show the HOSPITAL, the system name
and logo stay on the job page and the system hub; (2) named sites (hospitals,
named clinics and centers) show their own name; generic offices, home-based
and remote jobs show the system name with the town; never guess a town.

This module is the TOWN family of that work: systems whose postings name no
facility at all (no Workday site, no Phenom businessUnit, no body "Facility:"
line), but whose hospitals each sit in a town no other hospital of the same
system shares. Ex: Legacy Health's iCIMS feed says "Legacy Health" on every
row; a posting in Gresham, OR can only be Legacy Mount Hood Medical Center,
while a posting in Portland (Emanuel, Good Samaritan, Randall Children's,
Unity Center) cannot be placed and stays on the system label.

One TownSpec per system:
  hospitals   EVERY hospital of the system, with the town of its own street
              address (the CMS hospital list in the database, "cms <id>", or
              the system's own published list, "site"). A town with exactly one
              of them maps to it; a town with two or more maps to none (the
              town alone does not tell which), and is reported as shared.
  keep        towns the table never maps, with the reason: the system's name
              is the campus's own name there (Mayo Clinic, Rochester), the
              system hires mostly for offices or clinics there, or the second
              hospital in that town is not certain.
  sites       named sites inside a hospital of the table (a children's
              hospital in the main hospital's building): named only when the
              posting itself names them.
  default     the town the scraper stamps when a feed gives none
              (SYSTEM_LOCATION_DEFAULTS / WD_TENANT_DEFAULT). A row at that town
              is placed only when the posting itself names the town (a URL
              path segment such as a Workday site, or a location line of its
              text) or names one hospital (title or location line, which then
              gives the hospital's own town); a stamped town is no evidence.
  entity      optional: a regular expression for an employer line the system
              prints at the top of every posting ("Penn State Health - Holy
              Spirit Medical Center Location: US:PA: Camp Hill"), and what each
              value means (a hospital of the table, or "" for a medical group
              or an office, which keeps the label).

What is never renamed (the row keeps the system label and its own town):
  - a remote, home-based, virtual or hybrid posting (REMOTE_RX)
  - a clinic, practice, medical group, urgent care, home health, hospice,
    senior living or ambulatory site, or a system-office role (revenue cycle,
    IT, payroll, finance, recruiting, marketing, research ...) (OFFSITE_RX),
    unless the posting's title or location line names the town's hospital
  - a posting whose title or location line names a different hospital of the
    same system (the posting knows better than the town)
  - a row whose hospital_name is already a facility (only the system label is
    replaced: hospital_system is never touched)

A town with two or more hospitals of the system still places a row whose
title or location line names one of them ("RN - ICU - Legacy Emanuel",
"Location: Henry Ford St. John Hospital"): the posting says the hospital.

City and state: the row keeps its own town as it writes it ("Saint Paul"
stays "Saint Paul"; "PORTLAND" becomes "Portland"). The town changes only
when the row sat at the stamped default town (or had none) and the posting
itself names the town or the hospital (a Workday site "Mt-Pleasant-Iowa", a
"Location: US:PA: Camp Hill" line, "Staff RN - Cherokee"), or when the feed
put a site code in the town field ("RMC" is Trenton).

At run time (apply_jobs, called from scraper.finalize_jobs after every
adapter hook and facility_names) a row that carries no body tonight (a known
body the scraper sends blank) keeps its stored decision (STORED, read once
per run), so a row never flips between nights.

Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase2/town/changes.py), so the database holds what
the scraper writes on the next run.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata

# ── Text normalisation ──────────────────────────────────────────────────────
_MOJIBAKE = (("\u00e2\u0080\u0099", "'"), ("\u00e2\u0080\u0098", "'"), ("\u00e2\u0080\u009c", '"'),
             ("\u00e2\u0080\u009d", '"'), ("\u00e2\u0080\u0093", "-"), ("\u00e2\u0080\u0094", "-"),
             ("\u00c2\u00a0", " "))


def clean_text(s) -> str:
    s = str(s or "")
    for bad, good in _MOJIBAKE:
        s = s.replace(bad, good)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("\u2013", "-").replace("\u2014", "-")
    return " ".join(s.split())


def name_key(s) -> str:
    """Lookup key for a name: lower case, "saint" -> "st", apostrophes and
    "the" dropped, every other run of non-alphanumerics one space."""
    s = clean_text(s).lower().replace("'", "").replace("&", " and ")
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\bthe\b", " ", s)
    return " ".join(s.split())


def town_key(city) -> str:
    """Lookup key for a town: "Saint Paul", "St. Paul" and "ST PAUL" are one
    town; "Mount"/"Mt", "Fort"/"Ft" too; spaces do not count ("LaGrange" is
    "La Grange"). A trailing ", ST" or ZIP is dropped ("Gresham, OR")."""
    s = clean_text(city)
    if "," in s:
        s = s.split(",")[0]
    s = s.lower()
    s = re.sub(r"\bsaint\b|\bste?\b\.?", "st", s)
    s = re.sub(r"\bmount\b|\bmt\b\.?", "mt", s)
    s = re.sub(r"\bfort\b|\bft\b\.?", "ft", s)
    return re.sub(r"[^a-z0-9]", "", s)


# ── What keeps the label ────────────────────────────────────────────────────
# A remote or home-based posting is never a hospital's, whatever it names.
REMOTE_RX = re.compile(
    r"\b(?:remote(?!\s+(?:telemetry|cardiac|monitor))|work\s+from\s+home|work-from-home|wfh|"
    r"telecommut\w*|teleworke?\w*|home[- ]based|virtual(?!\s+(?:sitter|nurs\w*|care\s+team))|hybrid)\b", re.I)
# The title words of a clinic, a practice, an outpatient or senior-care site,
# or a role a system office holds. "Advanced Practice" is a hospital role;
# "Clinical" is not "Clinic".
OFFSITE_RX = re.compile(
    r"\b(?:clinics?|medical\s+group|physicians?\s+(?:group|practice|office|network|partners)|"
    r"(?<!advanced\s)practice(?!\s+(?:council|lead|specialist))|primary\s+care|family\s+(?:medicine|practice|health)|"
    r"internal\s+medicine\s+(?:office|clinic)|pediatric\s+(?:office|practice)|"
    r"urgent\s+care|walk[- ]in|express\s*care|quick\s*care|convenient\s+care|immediate\s+care|"
    r"retail|community\s+pharmacy|(?-i:LTC)|"
    r"home\s+(?:health|care|infusion|medical)|homecare|in[- ]home|hospice|"
    r"senior\s+living|assisted\s+living|independent\s+living|memory\s+care|nursing\s+home|long[- ]term\s+care|"
    r"living\s+center|care\s+center|good\s+samaritan\s+society|pace\s+program|"
    r"outpatient\s+(?:center|centre|clinic)|ambulatory|surgery\s+cent(?:er|re)|surgical\s+cent(?:er|re)|ASC|"
    r"imaging\s+cent(?:er|re)|patient\s+service\s+cent(?:er|re)|PSC|dialysis|occupational\s+health|"
    r"medical\s+office|physician\s+office|school[- ]based|"
    r"corporate|system\s+office|shared\s+services?|enterprise|system[- ]?wide|"
    r"revenue\s+cycle|central\s+business\s+office|CBO|patient\s+financial\s+services|billing|coding|coders?|"
    r"payroll|accounts\s+(?:payable|receivable)|accountant|accounting|finance|financial\s+analyst|analyst|"
    r"software|developer|programmer|information\s+(?:technology|systems|services)|IT|IS&T|informatics|"
    r"data\s+(?:scientist|engineer|architect)|cyber\w*|network\s+engineer|"
    r"marketing|communications\s+(?:specialist|manager|director|coordinator)|foundation|philanthrop\w*|"
    r"development\s+officer|fundrais\w*|gift\s+officer|recruiter|talent\s+acquisition|human\s+resources|"
    r"leadership\s+development|organizational\s+development|"
    r"(?-i:HR)\s+(?:generalist|specialist|coordinator|manager|director|assistant|partner|representative|analyst)|"
    r"legal|counsel|attorney|paralegal|compliance|privacy|internal\s+audit|auditor|"
    r"research|(?<!lab\s)(?<!laboratory\s)(?<!lab\.\s)scientist|postdoc\w*|post-doctoral|faculty|professor|"
    r"lecturer|instructor|"
    r"call\s+cent(?:er|re)|contact\s+cent(?:er|re)|access\s+cent(?:er|re)|scheduling\s+cent(?:er|re)|"
    r"health\s+plan|insurance|population\s+health)\b", re.I)
# Lines of a posting that say where it is: "Location: ...", "Department: ...".
_WHERE_KEYS = (r"(?:work\s+)?location|department|dept|facility(?:\s+name)?|site|entity|company|"
               r"practice|organization|business\s+unit|work\s+site|address|city")
_WHERE_RX = re.compile(r"\b(?:" + _WHERE_KEYS + r")\s*:\s*([^\n|]{2,160})", re.I)
# The first characters of a body hold the header lines that matter.
HEAD_CHARS = 1500


def where_lines(description) -> list[str]:
    """The values of the posting's location-type lines in its first
    HEAD_CHARS characters (inline "Key: value" runs are cut at the next key)."""
    head = str(description or "")[:HEAD_CHARS]
    out = []
    for m in _WHERE_RX.finditer(head):
        v = re.split(r"\s+[A-Z][A-Za-z ]{1,24}:\s", m.group(1))[0]
        out.append(clean_text(v)[:120])
    return out


def is_remote(title, description) -> bool:
    if REMOTE_RX.search(clean_text(title)):
        return True
    head = clean_text(str(description or "")[:HEAD_CHARS])
    return bool(re.search(
        r"\b(?:this\s+(?:is\s+a\s+|position\s+is\s+|role\s+is\s+)(?:fully\s+)?remote|fully\s+remote|"
        r"remote\s+(?:position|role|opportunity|work)|100%\s+remote|work\s+from\s+home|work-from-home|"
        r"telecommut\w+|home[- ]based\s+(?:position|role))\b", head, re.I)) or any(
        REMOTE_RX.search(v) for v in where_lines(description))


# Title words of a role a clinic or a physician office holds.
OFFICE_ROLE_RX = re.compile(
    r"\b(?:medical\s+assistant|certified\s+medical\s+assistant|(?-i:MA|CMA|RMA)|front\s+(?:desk|office)|receptionist|"
    r"patient\s+services?\s+rep\w*|practice\s+(?:manager|coordinator|administrator|supervisor)|"
    r"office\s+(?:coordinator|manager|assistant|supervisor))\b", re.I)
# A physician or advanced practice provider is hired by the medical group
# and practises in its clinics as well as the hospital: the posting is the
# hospital's only when its title says an inpatient service.
PROVIDER_RX = re.compile(
    r"\b(?:physicians?|surgeon|psychiatrist|psychologist|neuropsychologist|cardiologist|oncologist|"
    r"neurologist|urologist|dermatologist|endocrinologist|gastroenterologist|nephrologist|pulmonologist|"
    r"rheumatologist|pediatrician|obstetrician|gynecologist|ob/?gyn|otolaryngologist|ophthalmologist|"
    r"(?-i:MD|DO|NP|PA|PA-C|APP|APRN|FNP)|nurse\s+practitioner|physician\s+assistant|"
    r"advanced\s+practice\s+(?:provider|clinician|registered\s+nurse))\b", re.I)
INPATIENT_RX = re.compile(
    r"\b(?:hospitalist|nocturnist|intensivist|laborist|emergency|(?-i:ED|ER|ICU|NICU|PICU|CVICU|MICU|SICU)|"
    r"anesthesi\w*|crna|critical\s+care|inpatient|neonatolog\w*|trauma|labor\s+(?:and|&)\s+delivery|"
    r"pathologist|radiologist|neuro\s*hospitalist|surgical\s+first\s+assist\w*|operating\s+room|"
    r"acute\s+care|interventional|cardiothoracic|cardiac\s+surgery|neonatal|pediatric\s+intensive)\b", re.I)


def is_group_provider(title) -> bool:
    t = clean_text(title)
    return bool(PROVIDER_RX.search(t)) and not INPATIENT_RX.search(t)


# The first lines of a body that name the posting's own department or site
# ("Summary Job Description: BHMG Internal Medicine and Pediatrics ..."):
# the words of a clinic, a group practice or a non-hospital site.
INTRO_RX = re.compile(
    r"\b(?:clinics?|medical\s+group|physicians?\s+(?:group|practice|office)s?|urgent\s+care|walk[- ]in|"
    r"occupational\s+(?:medicine|health)|occ\s+med|primary\s+care|family\s+(?:medicine|practice)|"
    r"internal\s+medicine\s+and\s+pediatrics|outpatient\s+(?:center|centre|office)|ambulatory|"
    r"home\s+(?:health|care|infusion)|hospice|senior\s+living|assisted\s+living|nursing\s+home|"
    r"long[- ]term\s+care|medical\s+office|physician\s+office|surgery\s+cent(?:er|re))\b", re.I)
INTRO_CHARS = 300
_INTRO_LEAD_RX = re.compile(
    r"^(?:\s*(?:summary|overview|description|job\s+description|job\s+summary|position\s+summary|"
    r"position\s+description|about\s+the\s+(?:job|role|position))\s*:?\s*)+", re.I)
# What a posting says about its own setting. The system's inventory ("two
# hospitals and 40 clinics", "urgent care centers") names no setting, so the
# plural and the inventory words do not count.
SETTING_RX = re.compile(
    r"\b(?:(?:within|in|at|for|to|join)\s+(?:an?|our|the|this|one\s+of\s+our)\s+(?:[\w-]+\s+){0,3}?"
    r"(?:clinic|physician\s+office|medical\s+office|physician\s+practice|medical\s+practice)(?![\w-])"
    r"|clinic\s+setting|office\s+setting|physician\s+practice(?!s)|medical\s+practice(?!s)"
    r"|outpatient\s+clinic(?![\w-])|urgent\s+care(?!\s+(?:centers|clinics|locations|facilities|sites))"
    r"|occupational\s+medicine|walk-in\s+clinic|clinic\s+hours|clinics?\s+(?:is|are)\s+open)", re.I)
# A medical group named by its initials ("BHMG"); IMG is a physician's
# training, not a group.
GROUP_RX = re.compile(r"\b(?!IMG\b)[A-Z]{1,4}MG\b|\bMedical\s+Group\b")


def _strip_names(spec, text) -> str:
    """The text without the system's own names, so "Mayo Clinic" or
    "Hudson Hospital & Clinic" is no clinic word."""
    if spec is None:
        return text
    for rx in spec.name_rxs:
        text = rx.sub(" ", text)
    return text


# A sentence about the system itself ("Capital Health ... includes two
# hospitals, an outpatient center, satellite ED ..."; "With more than 800
# physicians ..., Carilion's unique clinic structure ...") lists what the
# system has, not where this job is: it is read past.
INVENTORY_RX = re.compile(
    r"\b(?:includ(?:es|ing)|comprised?|consists?|affiliation|network\s+of|family\s+of|system\s+of|"
    r"(?:is|are)\s+(?:a|an|the|one\s+of\s+the)\s+[\w\s,'-]{0,80}?(?:system|network|organization|provider|"
    r"resource|leader|employer|hospital|institution|center)s?\b|serv(?:es|ing)\s+(?:more|over|approximately|"
    r"the|patients|communities|residents|\d)|offers?\s+(?:a\s+)?(?:full|comprehensive|wide|broad)|"
    r"more\s+than\s+\d|over\s+\d|approximately\s+\d|"
    r"(?:\d[\d,]*|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|dozens\s+of|hundreds\s+of)\s+"
    r"(?:[\w-]+\s+){0,3}?(?:hospitals|clinics|locations|physicians|providers|employees|team\s+members|sites|"
    r"facilities|beds|counties|states)|nationally\s+recognized|as\s+well\s+as|"
    r"such\s+as|our\s+(?:hospitals|clinics|locations|facilities))\b", re.I)
_HOSP_AND_CLINICS_RX = re.compile(r"\bhospitals?\s+(?:and|&)\s+clinics?\b", re.I)
_SENTENCE_RX = re.compile(r"(?<=[.!?])\s+|\s*\n\s*|\s+[\u2022*]\s+")


def sentences(description) -> list[str]:
    """The sentences of a body's first HEAD_CHARS characters, cleaned."""
    head = str(description or "")[:HEAD_CHARS]
    return [clean_text(x) for x in _SENTENCE_RX.split(head) if x and x.strip()]


def sentence_key(sentence) -> str:
    """A short fingerprint of a sentence: its name_key's first 120
    characters, hashed. "" for a sentence too short to be boilerplate."""
    k = name_key(sentence)[:120]
    if len(k) < 25:
        return ""
    return hashlib.sha1(k.encode("utf-8")).hexdigest()[:10]


# Each system's boilerplate: the sentences that recur across its postings
# (the "About us" paragraph, the benefits paragraph, the EEO line), read off
# the stored bodies (boilerplate_keys) and kept in
# facilities_town_boilerplate.json beside this module. A posting's own setting
# is in the sentences that are not boilerplate. A system that rewrites its
# boilerplate makes no row wrong: an unknown sentence with a clinic word only
# keeps the label, until the file is rebuilt (tools/town_boilerplate.py).
BOILERPLATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "facilities_town_boilerplate.json")
BOILERPLATE_MIN_ROWS = 3        # a sentence in at least this many postings ...
BOILERPLATE_MIN_SHARE = 0.08    # ... and in at least this share of the system's postings
try:
    with open(BOILERPLATE_FILE, encoding="utf-8") as _f:
        BOILERPLATE: dict = {k: frozenset(v) for k, v in json.load(_f).items()}
except (OSError, ValueError):
    BOILERPLATE = {}


def boilerplate_keys(descriptions) -> list[str]:
    """The fingerprints of the sentences that recur in a system's bodies
    (BOILERPLATE_MIN_ROWS / BOILERPLATE_MIN_SHARE), sorted."""
    descriptions = [d for d in descriptions if len(str(d or "").strip()) >= 200]
    df: dict = {}
    for d in descriptions:
        for k in {sentence_key(x) for x in sentences(d)}:
            if k:
                df[k] = df.get(k, 0) + 1
    need = max(BOILERPLATE_MIN_ROWS, BOILERPLATE_MIN_SHARE * len(descriptions))
    return sorted(k for k, n in df.items() if n >= need)


def setting_text(description, spec=None) -> str:
    """The first HEAD_CHARS characters of a body, cleaned, without the
    system's boilerplate (BOILERPLATE), without the sentences about the
    system itself (INVENTORY_RX) and without the system's own names."""
    boiler = BOILERPLATE.get(spec.system, frozenset()) if spec is not None else frozenset()
    keep = [x for x in sentences(description)
            if not INVENTORY_RX.search(x) and (not boiler or sentence_key(x) not in boiler)]
    return _HOSP_AND_CLINICS_RX.sub(" ", _strip_names(spec, " ".join(keep)))


def is_offsite(title, description, spec=None) -> bool:
    t = _strip_names(spec, clean_text(title))
    if OFFSITE_RX.search(t) or OFFICE_ROLE_RX.search(t) or is_group_provider(t):
        return True
    if any(OFFSITE_RX.search(_strip_names(spec, v)) for v in where_lines(description)):
        return True
    head = setting_text(description, spec)
    intro = _INTRO_LEAD_RX.sub("", head)[:INTRO_CHARS]
    return bool(INTRO_RX.search(intro) or SETTING_RX.search(head) or GROUP_RX.search(head))


# ── Per-system tables ───────────────────────────────────────────────────────
# Words that alone name no one hospital ("University Hospital" is not named by
# "university"; "Children's Hospital" not by "children's").
_GENERIC_CORE = frozenset((
    "university", "childrens", "children", "womens", "women", "general", "specialty", "surgical", "surgery",
    "orthopedic", "orthopaedic", "heart", "cancer", "behavioral", "rehabilitation", "rehab", "psychiatric",
    "east", "west", "north", "south", "northeast", "northwest", "southeast", "southwest", "central", "main",
    "downtown", "valley", "county", "city", "st", "saint", "new", "the", "institute", "pavilion", "care",
    "clinic", "clinics", "global", "family", "baptist", "methodist", "presbyterian", "lutheran", "mercy",
    "good", "samaritan", "inter", "intercommunity", "area", "district", "unit", "tower"))


class TownSpec:
    def __init__(self, system, hospitals, *, emit=(), labels=(), default=None, keep=None, sites=(),
                 entity=None, town_line=None):
        self.system = system                      # hospital_system as stored
        self.emit = tuple(emit)                   # scraper emit names (HOSPITAL_SYSTEM_ALIASES keys)
        self.labels = frozenset(name_key(x) for x in (system, *emit, *labels))
        self.default = default                    # (town, state) the scraper stamps, or None
        self.keep = {}                            # (town key, ST) -> why the town is never mapped
        for where, why in (keep or {}).items():
            town, st = [p.strip() for p in where.rsplit(",", 1)]
            self.keep[(town_key(town), st)] = why
        self.hospitals = []                       # (name, city, state, source)
        self.raw_hospitals = []                   # (name, city, state, source, also-towns)
        self.towns: dict = {}                     # (town key, ST) -> (name, city, state, source)
        self.shared: dict = {}                    # (town key, ST) -> [names] (2+ hospitals)
        self.codes = set()                        # (town key, ST) of site codes in the town field
        by_town: dict = {}
        for h in hospitals:
            name, city, state, source = h[:4]
            also = tuple(h[4]) if len(h) > 4 else ()
            for t in also:
                if t.startswith("="):
                    self.codes.add((town_key(t[1:]), state))
            also = tuple(t.lstrip("=") for t in also)
            self.hospitals.append((name, city, state, source))
            self.raw_hospitals.append((name, city, state, source, also))
            for t in (city, *also):
                by_town.setdefault((town_key(t), state), []).append((name, city, state, source))
        for k, hs in by_town.items():
            names = sorted({h[0] for h in hs})
            if len(names) == 1:
                if k not in self.keep:
                    self.towns[k] = hs[0]
            else:
                self.shared[k] = names
        self.names = {name_key(h[0]): h for h in self.hospitals}
        # name keys a posting may use for each hospital (its name, and its
        # name without the system's own words), for "names another hospital"
        self.keys = {}
        sys_words = set(name_key(system).split()) | {"hospital", "medical", "center", "health", "regional", "system", "systems",
                                                     "memorial", "community", "campus", "of", "and", "at", "in"}
        # ... and the keys that name it more than its town does ("Genesys",
        # "Macomb"; not "Warren" for Henry Ford Warren Hospital, nor
        # "Lexington" in "Kentucky Cardiology - Lexington, KY")
        self.named_keys = {}
        for h in self.hospitals:
            k = name_key(h[0])
            self.keys[h[0]] = {k}
            self.named_keys[h[0]] = {k}
            core = " ".join(w for w in k.split() if w not in sys_words)
            if len(core) >= 5 and set(core.split()) - _GENERIC_CORE:
                self.keys[h[0]].add(core)
                if core != name_key(h[1]):
                    self.named_keys[h[0]].add(core)
        self.keep_keys = set()
        for h in self.hospitals:
            if (town_key(h[1]), h[2]) in self.keep:
                self.keep_keys |= self.named_keys[h[0]]
        self.sites = []                           # (name, city, state, (keys...))
        for name, city, state, keys in sites:
            self.sites.append((name, city, state, tuple(name_key(k) for k in keys)))
        self.entity = None
        if entity:
            rx, values = entity
            self.entity = (re.compile(rx, re.I), {name_key(k): v for k, v in values.items()})
            for v in values.values():
                if v and v not in {h[0] for h in self.hospitals} | {s[0] for s in self.sites}:
                    raise ValueError(f"{system}: entity value names unknown hospital {v!r}")
        self.town_line = re.compile(town_line, re.I) if town_line else None
        self.table_names = frozenset({h[0] for h in self.hospitals} | {x[0] for x in self.sites})
        # the system's own names, longest first, to take out of a text
        # before its clinic words are read (_strip_names)
        names = {system, *emit, *labels} | {h[0] for h in self.hospitals} | {x[0] for x in self.sites}
        self.name_rxs = tuple(
            re.compile(r"(?<![\w])" + r"\s+".join(map(re.escape, n.split())) + r"(?![\w])", re.I)
            for n in sorted(names, key=len, reverse=True) if n.strip())
        for h in self.hospitals:
            if name_key(h[0]) in self.labels:
                raise ValueError(f"{system}: {h[0]!r} is the system label")

    def is_label(self, hospital_name) -> bool:
        k = name_key(hospital_name)
        return not k or k in self.labels or bool(re.fullmatch(r"n a|na|unknown|none|tbd|null", k))

    def town(self, city, state):
        return self.towns.get((town_key(city), (state or "").strip().upper()))

    def at_default(self, city, state) -> bool:
        """The row's town is the stamped default: its spelling exactly (a feed's
        "Saint Paul" is not the stamp "St. Paul"), in the default's state."""
        if not self.default:
            return False
        return (clean_text(city).lower() == self.default[0].lower()
                and (state or "").strip().upper() == self.default[1])


def _h(name, city, state, source, *also):
    return (name, city, state, source, also)


SPECS: dict[str, TownSpec] = {}


def _spec(system, hospitals, **kw):
    s = TownSpec(system, hospitals, **kw)
    SPECS[system] = s
    return s


# ── The decision ────────────────────────────────────────────────────────────
def _words_rx(text):
    words = re.findall(r"[a-z0-9]+", str(text).lower())
    if not words:
        return None
    return re.compile(r"(?<![a-z0-9])" + r"[\s.+_-]*".join(map(re.escape, words)) + r"(?![a-z0-9])", re.I)


_STATE_NAMES = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas", "CA": "california", "CO": "colorado",
    "CT": "connecticut", "DE": "delaware", "DC": "district of columbia", "FL": "florida", "GA": "georgia",
    "HI": "hawaii", "ID": "idaho", "IL": "illinois", "IN": "indiana", "IA": "iowa", "KS": "kansas",
    "KY": "kentucky", "LA": "louisiana", "ME": "maine", "MD": "maryland", "MA": "massachusetts",
    "MI": "michigan", "MN": "minnesota", "MS": "mississippi", "MO": "missouri", "MT": "montana",
    "NE": "nebraska", "NV": "nevada", "NH": "new hampshire", "NJ": "new jersey", "NM": "new mexico",
    "NY": "new york", "NC": "north carolina", "ND": "north dakota", "OH": "ohio", "OK": "oklahoma",
    "OR": "oregon", "PA": "pennsylvania", "RI": "rhode island", "SC": "south carolina", "SD": "south dakota",
    "TN": "tennessee", "TX": "texas", "UT": "utah", "VT": "vermont", "VA": "virginia", "WA": "washington",
    "WV": "west virginia", "WI": "wisconsin", "WY": "wyoming"}


def _slug(text) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", str(text).lower()))


_SLUG_WORDS = {"saint": "st", "ste": "st", "mount": "mt", "fort": "ft"}


def _town_slug(text) -> str:
    """A slug with "Saint"/"St", "Mount"/"Mt", "Fort"/"Ft" made one."""
    return "-".join(_SLUG_WORDS.get(w, w) for w in re.findall(r"[a-z0-9]+", str(text).lower()))


def _url_segments(url) -> list[str]:
    u = re.sub(r"^[a-z]+://[^/]+", "", str(url or ""), flags=re.I).split("?")[0].split("#")[0]
    u = re.sub(r"%20|\+|_", "-", u)
    return [s.lower() for s in u.split("/") if s]


def _url_names_town(url, town, state) -> bool:
    """A whole URL path segment is the town ("/job/Pascagoula-MS/...",
    "/job/West-Burlington-Iowa/...", "/job/whittier/..."), or a Workday
    site segment starts with it ("/job/Gulfport---Main-Campus/")."""
    t = _town_slug(town)
    if not t:
        return False
    st = state.lower()
    sname = _slug(_STATE_NAMES.get(state, ""))
    tail = re.compile(r"^(?:-(?:" + re.escape(st) + (r"|" + re.escape(sname) if sname else "") + r"))?"
                      r"(?:---.*|-(?:hospital|main|campus|medical|center|regional)(?:-.*)?)?$")
    for seg in _url_segments(url):
        seg = _town_slug(seg.replace("---", " zzsep ")).replace("-zzsep-", "---")
        seg = re.sub(r"^(?:us-)?" + re.escape(st) + r"-(?=[a-z])", "", seg) if seg.startswith(("us-", st + "-")) \
            else seg
        if seg.startswith(t) and tail.match(seg[len(t):]):
            return True
    return False


def _all_towns(spec):
    """(town key, ST) -> a spelling, for every town the table knows: its
    hospitals' towns (mapped, shared or kept) and the default."""
    out = {}
    for h in spec.hospitals:
        out.setdefault((town_key(h[1]), h[2]), h[1])
    for name, city, state, src, also in spec.raw_hospitals:
        for t in also:
            out.setdefault((town_key(t), state), t)
    if spec.default:
        out.setdefault((town_key(spec.default[0]), spec.default[1]), spec.default[0])
    return out


def posting_towns(spec, url, description) -> set:
    """The table's towns (keys) the posting itself names: a URL path segment,
    its location lines, or the system's own town line. (Not the adapter's
    location text: a stored row's location is the scraper's own "City, ST",
    and the nightly run must decide as the stored change list does.)"""
    texts = where_lines(description)
    if spec.town_line:
        m = spec.town_line.search(clean_text(str(description or "")[:HEAD_CHARS]))
        if m:
            texts.append(m.group(1))
    found = set()
    for (k, st), town in _all_towns(spec).items():
        rx = _words_rx(town)
        if rx and (any(rx.search(t) for t in texts if t) or _url_names_town(url, town, st)):
            found.add((k, st))
    return found


def _names(spec, look, name, title) -> bool:
    """The title or a location line names this hospital: by its full name,
    or by the part of its name that says more than its town ("Genesys",
    "Cherokee") when the title is no clinic's or office's ("Physical
    Therapist - Home Health Care - Macomb County" does not name Henry Ford
    Macomb Hospital)."""
    full = name_key(name)
    if _has_key(look, {full}):
        return True
    cores = spec.named_keys[name] - {full}
    if not cores or not _has_key(look, cores):
        return False
    t = _strip_names(spec, clean_text(title))
    return not (OFFSITE_RX.search(t) or OFFICE_ROLE_RX.search(t))


def _named_hospitals(spec, look, title="") -> list:
    """The table's hospitals the title or a location line names (_names)."""
    return [h[0] for h in spec.hospitals if _names(spec, look, h[0], title)]


def _has_key(text_key, keys) -> bool:
    t = " " + text_key + " "
    return any(" " + k + " " in t for k in keys if k)


def decide(system, hospital_name, city, state, title="", description="", url=""):
    """(hospital_name, city, state, how) for one row of a TOWN system, or
    (None, None, None, how) when the row keeps what it has.
    Renamed: "town" (the town's one hospital), "named" (the town's hospital,
    which the posting's title or location line also names), "site" (a named
    site inside it), "entity" (the system's employer line).
    Kept: "not-table", "named-already", "remote", "entity-office",
    "other-hospital", "offsite", "shared", "keep", "default" (the stamped
    default town and the posting names none), "no-town"."""
    spec = SPECS.get(system) or EMIT.get(system)
    if spec is None:
        return None, None, None, "not-table"
    if not spec.is_label(hospital_name):
        return None, None, None, "named-already"
    st = (state or "").strip().upper()
    stamped = not clean_text(city) or spec.at_default(city, st)
    if is_remote(title, description):
        return None, None, None, "remote"

    # The system's own employer line names the hospital (or an office).
    if spec.entity:
        rx, values = spec.entity
        m = rx.search(clean_text(str(description or "")[:HEAD_CHARS]))
        v = values.get(name_key(m.group(1))) if m else None
        if v == "":
            return None, None, None, "entity-office"
        if v:
            h = spec.names.get(name_key(v)) or next(
                (s[:3] for s in spec.sites if s[0] == v), None)
            if not stamped:
                return v, _town_spelling(spec, city, st), st, "entity"
            if spec.town_line:
                tm = spec.town_line.search(clean_text(str(description or "")[:HEAD_CHARS]))
                if tm and clean_text(tm.group(1)):
                    town = clean_text(tm.group(1))
                    return v, _town_spelling(spec, town, h[2]), h[2], "entity"
            return v, h[1], h[2], "entity"

    look = name_key(" | ".join([clean_text(title)] + where_lines(description)))
    # The town: the row's own, unless it is blank or the stamped default; then
    # only a town the posting itself names counts, or the one hospital its
    # title or location line names (with that hospital's own town).
    if stamped:
        named = posting_towns(spec, url, description)
        if len(named) != 1:
            only = _named_hospitals(spec, look, title)
            if len(only) == 1 and not _has_key(look, spec.keep_keys):
                h = spec.names[name_key(only[0])]
                return h[0], h[1], h[2], "named"
            return None, None, None, ("default" if clean_text(city) else "no-town")
        key = next(iter(named))
    else:
        key = (town_key(city), st)
    if key in spec.keep:
        return None, None, None, "keep"
    if key in spec.shared:
        # Two hospitals in the town: the one the posting names, if it names one.
        only = [n for n in _named_hospitals(spec, look, title) if n in spec.shared[key]]
        if len(only) == 1:
            h = spec.names[name_key(only[0])]
            return h[0], _town_spelling(spec, city, st) if not stamped else h[1], h[2], "named"
        return None, None, None, "shared"
    hit = spec.towns.get(key)
    if hit is None:
        return None, None, None, "no-town"
    name, hcity, hstate = hit[:3]
    if not stamped:
        hcity, hstate = _town_spelling(spec, city, st), st

    # The posting names a different hospital of the system: it knows better.
    for other, keys in spec.keys.items():
        if other != name and _has_key(look, keys) and not _has_key(look, spec.keys[name]):
            return None, None, None, "other-hospital"
    # A named site inside the town's hospital.
    for sname, scity, sstate, skeys in spec.sites:
        if (town_key(scity), sstate) == key and _has_key(look, skeys):
            return sname, hcity, hstate, "site"
    if _names(spec, look, name, title):
        return name, hcity, hstate, "named"
    if is_offsite(title, description, spec):
        return None, None, None, "offsite"
    return name, hcity, hstate, "town"


def _town_spelling(spec, city, st):
    """The row's own town as the row writes it ("Saint Paul" stays "Saint
    Paul"), in mixed case when the feed shouts it ("PORTLAND" is the table's
    "Portland"); a site code in the town field ("RMC", spec.codes) is the
    hospital's own town."""
    k = (town_key(city), st)
    if k in spec.codes:
        hit = spec.towns.get(k)
        if hit:
            return hit[1]
    c = clean_text(city)
    if c.isupper() and len(c) > 3:
        for (tk, tst), town in _all_towns(spec).items():
            if (tk, tst) == k:
                return town
        return c.title()
    return c


# ── Run time: the scraper's Jobs ────────────────────────────────────────────
# Under this many characters the row carries no body tonight: the stored body
# is kept (scraper._keep_stored_body sends a blank) or it is a list teaser.
# On such a night the stored decision stands (STORED): a body read on an
# earlier night named the clinic, the office or the hospital, and the row
# must not flip between nights.
NO_BODY_CHARS = 1000
STORED_PAGE = 1000
STORED: dict = {}          # (system, job_id) -> (hospital_name, city, state)
_STORED_LOADED = [False]


def set_stored(rows) -> int:
    """{(hospital_system, job_id): (hospital_name, city, state)} for the
    stored active rows of the table's systems. Returns the map's size."""
    out = {}
    for r in rows or []:
        spec = SPECS.get(r.get("hospital_system"))
        jid = r.get("job_id")
        if spec is None or jid in (None, ""):
            continue
        out[(spec.system, str(jid))] = (clean_text(r.get("hospital_name")), clean_text(r.get("city")),
                                        clean_text(r.get("state")).upper())
    STORED.clear()
    STORED.update(out)
    _STORED_LOADED[0] = True
    return len(out)


def load_stored() -> int:
    """Read-only PostgREST passes (the same SUPABASE_URL / key the upsert
    uses) over the active rows of the table's systems, one system at a time,
    keyset-paged on id (the facility_names.load_known_facilities pattern).
    Without credentials the map stays empty and every row is decided from
    what it carries tonight."""
    import json
    import os
    import time
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    try:
        import sb_keys
        sb_url = os.environ.get("SUPABASE_URL", "")
        sb_key = sb_keys.supabase_key()
    except Exception:
        sb_url, sb_key = "", ""
    if not sb_url or not sb_key:
        return set_stored([])
    rows = []
    for system in SPECS:
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        last = 0
        try:
            while True:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,"
                     f"city,state&is_active=is.true&hospital_system={flt}&id=gt.{last}&order=id.asc"
                     f"&limit={STORED_PAGE}")
                rq = _urlreq.Request(u, headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
                chunk = None
                for attempt in range(3):
                    try:
                        with _urlreq.urlopen(rq, timeout=60) as resp:
                            chunk = json.loads(resp.read().decode())
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(3 * (attempt + 1))
                if not chunk:
                    break
                rows.extend(chunk)
                last = chunk[-1]["id"]
                if len(chunk) < STORED_PAGE:
                    break
        except Exception:
            continue          # that system's rows are decided from what they carry tonight
    return set_stored(rows)


def apply_job(job, canon=None) -> str:
    """decide() on one scraper Job, in place (hospital_name, city, state).
    Returns how. hospital_system is never touched."""
    system = canon(job.hospital_system) if canon else job.hospital_system
    spec = SPECS.get(system) or EMIT.get(system)
    if spec is None:
        return "not-table"
    if not spec.is_label(job.hospital_name):
        return "named-already"
    if len(str(job.description or "").strip()) < NO_BODY_CHARS:
        kept = STORED.get((spec.system, str(job.job_id)))
        if kept is not None:
            # No body tonight: the stored decision stands. A stored facility
            # name stays whoever wrote it (this table, another family's hook
            # on a night it read the body, or a stored-row change list), so
            # a body-less night never puts the system's name back.
            name, city, state = kept
            if name and not spec.is_label(name):
                job.hospital_name = name
                if (not clean_text(job.city) or spec.at_default(job.city, job.state)) and city:
                    job.city, job.state = city, state or job.state
                return "kept"
            return "kept-label"
    name, city, state, how = decide(system, job.hospital_name, job.city, job.state, job.title,
                                    job.description, job.url)
    if name:
        job.hospital_name = name
        job.city = city
        job.state = state
    return how


def apply_jobs(jobs, canon=None) -> dict:
    """apply_job over a run's rows (finalize_jobs, after the adapters'
    facility hooks and facility_names, before normalize_job). Loads the
    stored names once. Returns {system: {how: rows}} for the label rows of
    the table's systems."""
    if not _STORED_LOADED[0]:
        try:
            load_stored()
        except Exception:
            set_stored([])
    stats: dict = {}
    for job in jobs:
        how = apply_job(job, canon)
        if how in ("not-table", "named-already"):
            continue
        system = canon(job.hospital_system) if canon else job.hospital_system
        d = stats.setdefault((SPECS.get(system) or EMIT.get(system)).system, {})
        d[how] = d.get(how, 0) + 1
    return stats


# ── The tables ──────────────────────────────────────────────────────────────
# Each list holds every hospital of the system (a town with two is left), with
# the town of its own street address. "cms <id>" = the database's CMS hospital
# list (public.hospitals); "site" = the system's own published list of
# hospitals. Names are each hospital's public name, written once.

_spec("Children's Minnesota", [
    _h("Children's Minnesota Minneapolis Hospital", "Minneapolis", "MN", "cms 243302"),
    _h("Children's Minnesota St. Paul Hospital", "St. Paul", "MN", "site (cms 243302 is both campuses)"),
], labels=("Children's Hospitals and Clinics of Minnesota",))

_spec("Mayo Clinic", [
    _h("Mayo Clinic Hospital, Rochester", "Rochester", "MN", "cms 240010"),
    _h("Mayo Clinic Hospital, Jacksonville", "Jacksonville", "FL", "cms 100151"),
    _h("Mayo Clinic Hospital, Phoenix", "Phoenix", "AZ", "cms 030103"),
    _h("Mayo Clinic Health System in Albert Lea", "Albert Lea", "MN", "cms 240043"),
    _h("Mayo Clinic Health System in Austin", "Austin", "MN", "cms 240043 (Austin campus)"),
    _h("Mayo Clinic Health System in Cannon Falls", "Cannon Falls", "MN", "cms 241346"),
    _h("Mayo Clinic Health System in Fairmont", "Fairmont", "MN", "cms 240166"),
    _h("Mayo Clinic Health System in Lake City", "Lake City", "MN", "cms 241338"),
    _h("Mayo Clinic Health System in Mankato", "Mankato", "MN", "cms 240093"),
    _h("Mayo Clinic Health System in New Prague", "New Prague", "MN", "cms 241361"),
    _h("Mayo Clinic Health System in Red Wing", "Red Wing", "MN", "cms 240018"),
    _h("Mayo Clinic Health System in St. James", "St. James", "MN", "cms 241333"),
    _h("Mayo Clinic Health System in Waseca", "Waseca", "MN", "cms 241345"),
    _h("Mayo Clinic Health System - Northland in Barron", "Barron", "WI", "cms 521315"),
    _h("Mayo Clinic Health System - Chippewa Valley in Bloomer", "Bloomer", "WI", "cms 521314"),
    _h("Mayo Clinic Health System in Eau Claire", "Eau Claire", "WI", "cms 520070"),
    _h("Mayo Clinic Health System in La Crosse", "La Crosse", "WI", "cms 520004"),
    _h("Mayo Clinic Health System - Red Cedar in Menomonie", "Menomonie", "WI", "cms 521340"),
    _h("Mayo Clinic Health System - Oakridge in Osseo", "Osseo", "WI", "cms 521302"),
    _h("Mayo Clinic Health System in Sparta", "Sparta", "WI", "cms 521305"),
], default=("Rochester", "MN"), keep={
    "Rochester, MN": "flagship campus: the system's name is the campus's own name (Mayo Clinic, Rochester)",
    "Jacksonville, FL": "flagship campus: the system's name is the campus's own name (Mayo Clinic in Florida)",
})

_spec("Children's Health", [
    _h("Children's Medical Center Dallas", "Dallas", "TX", "cms 453302"),
    _h("Children's Medical Center Plano", "Plano", "TX", "cms 453316"),
])

_spec("Baptist Health (KY/IN)", [
    _h("Baptist Health Louisville", "Louisville", "KY", "cms 180130"),
    _h("Baptist Health Lexington", "Lexington", "KY", "cms 180103"),
    _h("Baptist Health Floyd", "New Albany", "IN", "cms 150044"),
    _h("Baptist Health Hardin", "Elizabethtown", "KY", "cms 180012"),
    _h("Baptist Health Paducah", "Paducah", "KY", "cms 180104"),
    _h("Baptist Health La Grange", "La Grange", "KY", "cms 180138"),
    _h("Baptist Health Corbin", "Corbin", "KY", "cms 180080"),
    _h("Baptist Health Richmond", "Richmond", "KY", "cms 180049"),
    _h("Baptist Health Deaconess Madisonville", "Madisonville", "KY", "cms 180093"),
], emit=("Baptist Health",), default=("Louisville", "KY"))

_spec("Northwestern Medicine", [
    _h("Northwestern Memorial Hospital", "Chicago", "IL", "cms 140281"),
    _h("Northwestern Medicine Central DuPage Hospital", "Winfield", "IL", "cms 140242"),
    _h("Northwestern Medicine Lake Forest Hospital", "Lake Forest", "IL", "cms 140130"),
    _h("Northwestern Medicine Palos Hospital", "Palos Heights", "IL", "cms 140062"),
    _h("Northwestern Medicine McHenry Hospital", "McHenry", "IL", "cms 140116"),
    _h("Northwestern Medicine Delnor Hospital", "Geneva", "IL", "cms 140211"),
    _h("Northwestern Medicine Kishwaukee Hospital", "DeKalb", "IL", "cms 140286"),
    _h("Northwestern Medicine Huntley Hospital", "Huntley", "IL", "site (cms 140116 Huntley campus)"),
    _h("Northwestern Medicine Woodstock Hospital", "Woodstock", "IL", "site (cms 140116 Woodstock campus)"),
    _h("Northwestern Medicine Valley West Hospital", "Sandwich", "IL", "cms 141340"),
    _h("Northwestern Medicine Marianjoy Rehabilitation Hospital", "Wheaton", "IL", "site"),
], default=("Chicago", "IL"), keep={
    "Chicago, IL": "Northwestern Memorial (Prentice, Stone Institute) plus the system office and the medical group",
})

_spec("HealthPartners", [
    _h("Regions Hospital", "St. Paul", "MN", "cms 240106"),
    _h("Park Nicollet Methodist Hospital", "St. Louis Park", "MN", "cms 240053"),
    _h("Lakeview Hospital", "Stillwater", "MN", "cms 240066"),
    _h("Hudson Hospital & Clinic", "Hudson", "WI", "cms 521335"),
    _h("Amery Hospital & Clinic", "Amery", "WI", "cms 521308"),
    _h("Westfields Hospital & Clinic", "New Richmond", "WI", "cms 521345"),
    _h("Olivia Hospital & Clinic", "Olivia", "MN", "cms 241306"),
    _h("Hutchinson Health", "Hutchinson", "MN", "cms 240187"),
], default=("St. Paul", "MN"))

_spec("Franciscan Health", [
    _h("Franciscan Health Indianapolis", "Indianapolis", "IN", "cms 150162"),
    _h("Franciscan Health Mooresville", "Mooresville", "IN", "cms 150057"),
    _h("Franciscan Health Indianapolis Orthopedic Hospital", "Carmel", "IN", "cms 150193"),
    _h("Franciscan Health Lafayette East", "Lafayette", "IN", "cms 150109"),
    _h("Franciscan Health Crawfordsville", "Crawfordsville", "IN", "cms 150022"),
    _h("Franciscan Health Michigan City", "Michigan City", "IN", "cms 150015"),
    _h("Franciscan Health Munster", "Munster", "IN", "cms 150165"),
    _h("Franciscan Health Crown Point", "Crown Point", "IN", "cms 150126"),
    _h("Franciscan Health Dyer", "Dyer", "IN", "cms 150090"),
    _h("Franciscan Health Rensselaer", "Rensselaer", "IN", "cms 151324"),
    _h("Franciscan Health Olympia Fields", "Olympia Fields", "IL", "cms 140172"),
])

_spec("UCLA Health", [
    _h("Ronald Reagan UCLA Medical Center", "Los Angeles", "CA", "cms 050262"),
    _h("Resnick Neuropsychiatric Hospital at UCLA", "Los Angeles", "CA", "cms 054009"),
    _h("UCLA Santa Monica Medical Center", "Santa Monica", "CA", "cms 050112"),
    _h("UCLA West Valley Medical Center", "West Hills", "CA", "cms 050481"),
])

_spec("UChicago Medicine", [
    _h("University of Chicago Medical Center", "Chicago", "IL", "cms 140088"),
    _h("UChicago Medicine Comer Children's Hospital", "Chicago", "IL", "cms 140088 (Comer)"),
    _h("UChicago Medicine Ingalls Memorial", "Harvey", "IL", "cms 140191"),
    _h("UChicago Medicine Crown Point", "Crown Point", "IN", "cms 150195"),
], default=("Chicago", "IL"), keep={
    "Hinsdale, IL": "UChicago Medicine AdventHealth hospital: AdventHealth's employees, not this board's",
    "La Grange, IL": "UChicago Medicine AdventHealth hospital: AdventHealth's employees, not this board's",
    "Bolingbrook, IL": "UChicago Medicine AdventHealth hospital: AdventHealth's employees, not this board's",
    "Glendale Heights, IL": "UChicago Medicine AdventHealth hospital: AdventHealth's employees, not this board's",
})

_spec("Capital Health", [
    _h("Capital Health Regional Medical Center", "Trenton", "NJ", "cms 310092", "=RMC"),
    _h("Capital Health Medical Center - Hopewell", "Pennington", "NJ", "cms 310044", "Hopewell"),
], default=("Pennington", "NJ"))

_spec("Luminis Health", [
    _h("Luminis Health Anne Arundel Medical Center", "Annapolis", "MD", "cms 210023"),
    _h("Luminis Health J. Kent McNew Family Medical Center", "Annapolis", "MD", "cms 214020"),
    _h("Luminis Health Doctors Community Medical Center", "Lanham", "MD", "cms 210051"),
], default=("Annapolis", "MD"))

_spec("Great River Health", [
    _h("Great River Medical Center", "West Burlington", "IA", "cms 160057"),
    _h("Henry County Health Center", "Mount Pleasant", "IA", "cms 161356"),
    _h("Fort Madison Community Hospital", "Fort Madison", "IA", "site"),
], default=("West Burlington", "IA"))

_spec("Memorial Health System", [
    _h("Marietta Memorial Hospital", "Marietta", "OH", "cms 360147"),
    _h("Selby General Hospital", "Marietta", "OH", "cms 361319"),
    _h("Sistersville General Hospital", "Sistersville", "WV", "cms 511304"),
])

_spec("Sanford Health", [
    _h("Sanford USD Medical Center", "Sioux Falls", "SD", "cms 430027"),
    _h("Sanford Medical Center Fargo", "Fargo", "ND", "cms 350011"),
    _h("Sanford Medical Center Bismarck", "Bismarck", "ND", "cms 350015"),
    _h("Sanford Bemidji Medical Center", "Bemidji", "MN", "cms 240100"),
    _h("Sanford Medical Center Aberdeen", "Aberdeen", "SD", "cms 430097"),
    _h("Sanford Worthington Medical Center", "Worthington", "MN", "cms 240022"),
    _h("Sanford Thief River Falls Medical Center", "Thief River Falls", "MN", "cms 241381"),
    _h("Sanford Behavioral Health Center", "Thief River Falls", "MN", "cms 244018"),
    _h("Sanford Luverne Medical Center", "Luverne", "MN", "cms 241371"),
    _h("Sanford Canby Medical Center", "Canby", "MN", "cms 241347"),
    _h("Sanford Jackson Medical Center", "Jackson", "MN", "cms 241315"),
    _h("Sanford Tracy Medical Center", "Tracy", "MN", "cms 241303"),
    _h("Sanford Westbrook Medical Center", "Westbrook", "MN", "cms 241302"),
    _h("Sanford Wheaton Medical Center", "Wheaton", "MN", "cms 241304"),
    _h("Sanford Bagley Medical Center", "Bagley", "MN", "cms 241328"),
    _h("Sanford Sheldon Medical Center", "Sheldon", "IA", "cms 161381"),
    _h("Sanford Hillsboro Medical Center", "Hillsboro", "ND", "cms 351329"),
    _h("Sanford Mayville Medical Center", "Mayville", "ND", "cms 351309"),
    _h("Sanford Canton-Inwood Medical Center", "Canton", "SD", "cms 431333"),
    _h("Sanford Chamberlain Medical Center", "Chamberlain", "SD", "cms 431329"),
    _h("Sanford Clear Lake Medical Center", "Clear Lake", "SD", "cms 431307"),
    _h("Sanford Vermillion Medical Center", "Vermillion", "SD", "cms 431336"),
    _h("Sanford Webster Medical Center", "Webster", "SD", "cms 431311"),
    _h("Marshfield Medical Center", "Marshfield", "WI", "cms 520037"),
    _h("Marshfield Medical Center - Weston", "Weston", "WI", "cms 520202"),
    _h("Marshfield Medical Center - Minocqua", "Minocqua", "WI", "cms 520212"),
    _h("Marshfield Medical Center - Eau Claire", "Eau Claire", "WI", "cms 520210"),
    _h("Marshfield Medical Center - Rice Lake", "Rice Lake", "WI", "cms 520011"),
    _h("Marshfield Medical Center - Beaver Dam", "Beaver Dam", "WI", "cms 520076"),
    _h("Marshfield Medical Center - Ladysmith", "Ladysmith", "WI", "cms 521328"),
    _h("Marshfield Medical Center - Neillsville", "Neillsville", "WI", "cms 521323"),
    _h("Marshfield Medical Center - Park Falls", "Park Falls", "WI", "cms 521325"),
    _h("Marshfield Medical Center - River Region", "Stevens Point", "WI", "cms 520215"),
    _h("Marshfield Medical Center - Dickinson", "Iron Mountain", "MI", "cms 230055"),
], default=("Sioux Falls", "SD"))

_spec("Northside Hospital", [
    _h("Northside Hospital Atlanta", "Atlanta", "GA", "cms 110161"),
    _h("Northside Hospital Forsyth", "Cumming", "GA", "cms 110005"),
    _h("Northside Hospital Cherokee", "Canton", "GA", "cms 110008"),
    _h("Northside Hospital Gwinnett", "Lawrenceville", "GA", "cms 110087"),
    _h("Northside Hospital Duluth", "Duluth", "GA", "cms 110252"),
], default=("Atlanta", "GA"))

_spec("Albany Med", [
    _h("Albany Medical Center Hospital", "Albany", "NY", "cms 330013"),
    _h("Glens Falls Hospital", "Glens Falls", "NY", "cms 330191"),
    _h("Columbia Memorial Health", "Hudson", "NY", "cms 330094"),
    _h("Saratoga Hospital", "Saratoga Springs", "NY", "cms 330222"),
])

_spec("MU Health Care", [
    _h("University Hospital", "Columbia", "MO", "cms 260141"),
    _h("MU Health Care Children's Hospital", "Columbia", "MO", "site"),
    _h("Ellis Fischel Cancer Center", "Columbia", "MO", "site"),
    _h("Missouri Orthopaedic Institute", "Columbia", "MO", "site"),
    _h("Missouri Psychiatric Center", "Columbia", "MO", "site"),
    _h("Capital Region Medical Center", "Jefferson City", "MO", "site"),
], labels=("University of Missouri Health Care",))

_spec("St. Joseph's Health", [
    _h("St. Joseph's University Medical Center", "Paterson", "NJ", "cms 310019"),
    _h("St. Joseph's Wayne Medical Center", "Wayne", "NJ", "site (cms 310019 Wayne campus)"),
], sites=[
    ("St. Joseph's Children's Hospital", "Paterson", "NJ", ("St. Joseph's Children's Hospital", "Childrens Hospital")),
])

_spec("Billings Clinic", [
    _h("Billings Clinic Hospital", "Billings", "MT", "cms 270004"),
    _h("Billings Clinic Bozeman", "Bozeman", "MT", "site"),
    _h("Stillwater Billings Clinic", "Columbus", "MT", "cms 271330"),
    _h("Billings Clinic Broadwater", "Townsend", "MT", "cms 271333"),
    _h("Beartooth Billings Clinic", "Red Lodge", "MT", "cms 271326"),
], keep={
    "Billings, MT": "flagship: the system's name is the campus's own name (Billings Clinic)",
})

_spec("TMC Healthcare", [
    _h("Tucson Medical Center", "Tucson", "AZ", "cms 030006"),
    _h("Northern Cochise Community Hospital", "Willcox", "AZ", "cms 031302"),
], default=("Tucson", "AZ"))

_spec("Halifax Health", [
    _h("Halifax Health Medical Center", "Daytona Beach", "FL", "cms 100017"),
    _h("Halifax Health - UF Health Medical Center of Deltona", "Deltona", "FL", "cms 100330"),
], default=("Daytona Beach", "FL"))

_spec("Methodist Hospitals", [
    _h("Methodist Hospitals Northlake Campus", "Gary", "IN", "cms 150002 (Northlake)"),
    _h("Methodist Hospitals Southlake Campus", "Merrillville", "IN", "cms 150002 (Southlake)"),
])

_spec("Anderson Hospital", [
    _h("Community Hospital of Staunton", "Staunton", "IL", "cms 141306"),
])

_spec("NorthBay Health", [
    _h("NorthBay Medical Center", "Fairfield", "CA", "cms 050367"),
    _h("NorthBay VacaValley Hospital", "Vacaville", "CA", "site (cms 050367 Vacaville campus)"),
])

_spec("South Central Regional Medical Center", [
    _h("Magee General Hospital", "Magee", "MS", "cms 250124"),
])

_spec("Ochsner Health", [
    _h("Ochsner Medical Center", "New Orleans", "LA", "cms 190036"),
    _h("Ochsner Baptist", "New Orleans", "LA", "site"),
    _h("Ochsner Medical Center - Baton Rouge", "Baton Rouge", "LA", "cms 190202"),
    _h("Ochsner Medical Center - Kenner", "Kenner", "LA", "cms 190274"),
    _h("Ochsner Medical Center - West Bank Campus", "Gretna", "LA", "site"),
    _h("Ochsner Medical Center - Northshore", "Slidell", "LA", "site"),
    _h("Slidell Memorial Hospital", "Slidell", "LA", "site"),
    _h("Ochsner Lafayette General Medical Center", "Lafayette", "LA", "cms 190002"),
    _h("Ochsner University Hospital & Clinics", "Lafayette", "LA", "cms 190006"),
    _h("Ochsner LSU Health Shreveport", "Shreveport", "LA", "cms 190098"),
    _h("Ochsner LSU Health Shreveport - St. Mary Medical Center", "Shreveport", "LA", "cms 190317"),
    _h("Ochsner LSU Health Monroe", "Monroe", "LA", "cms 190011"),
    _h("Ochsner St. Mary", "Morgan City", "LA", "cms 190014"),
    _h("Ochsner St. Anne Hospital", "Raceland", "LA", "cms 191324"),
    _h("Ochsner American Legion Hospital", "Jennings", "LA", "cms 190053"),
    _h("Ochsner Abrom Kaplan Memorial Hospital", "Kaplan", "LA", "cms 191322"),
    _h("Ochsner Acadia General Hospital", "Crowley", "LA", "cms 190044"),
    _h("Ochsner St. Martin Hospital", "Breaux Bridge", "LA", "cms 191302"),
    _h("Ochsner Medical Center - Hancock", "Bay St. Louis", "MS", "cms 250162"),
    _h("Ochsner Laird Hospital", "Union", "MS", "cms 251322"),
    _h("Ochsner Scott Regional", "Morton", "MS", "cms 251323"),
    _h("Ochsner Watkins Hospital", "Quitman", "MS", "cms 251316"),
    _h("Ochsner Stennis Hospital", "De Kalb", "MS", "cms 251335"),
    _h("Ochsner Choctaw General", "Butler", "AL", "cms 011304"),
], default=("New Orleans", "LA"), keep={
    "Meridian, MS": "Ochsner Rush Health runs two hospitals in Meridian (Rush Foundation Hospital, the specialty hospital)",
    "Houma, LA": "Chabert Medical Center and the Terrebonne General partnership both sit in Houma",
    "Covington, LA": "the St. Tammany Health System partnership: whose employee is not certain",
    "Chalmette, LA": "St. Bernard Parish Hospital: a parish hospital Ochsner manages",
    "Luling, LA": "St. Charles Parish Hospital: a parish hospital Ochsner manages",
})

_spec("Henry Ford Health", [
    _h("Henry Ford Hospital", "Detroit", "MI", "cms 230053"),
    _h("Henry Ford St. John Hospital", "Detroit", "MI", "cms 230165"),
    _h("Henry Ford Jackson Hospital", "Jackson", "MI", "cms 230092"),
    _h("Henry Ford Macomb Hospital", "Clinton Township", "MI", "cms 230047"),
    _h("Henry Ford Genesys Hospital", "Grand Blanc", "MI", "cms 230197"),
    _h("Henry Ford West Bloomfield Hospital", "West Bloomfield", "MI", "cms 230302", "W Bloomfield"),
    _h("Henry Ford Wyandotte Hospital", "Wyandotte", "MI", "cms 230146"),
    _h("Henry Ford Providence Southfield Hospital", "Southfield", "MI", "cms 230019"),
    _h("Henry Ford Providence Novi Hospital", "Novi", "MI", "cms 230019 (Novi campus)"),
    _h("Henry Ford Rochester Hospital", "Rochester", "MI", "cms 230254"),
    _h("Henry Ford Warren Hospital", "Warren", "MI", "cms 230195"),
    _h("Henry Ford River District Hospital", "East China", "MI", "cms 230241"),
    _h("Henry Ford Madison Heights Hospital", "Madison Heights", "MI", "site"),
], default=("Detroit", "MI"))

_spec("Jefferson Health", [
    _h("Thomas Jefferson University Hospital", "Philadelphia", "PA", "cms 390174"),
    _h("Jefferson Einstein Philadelphia Hospital", "Philadelphia", "PA", "cms 390142"),
    _h("Jefferson Torresdale Hospital", "Philadelphia", "PA", "cms 390115 (Torresdale)", "=NE Torresdale Main Hosp"),
    _h("Jefferson Abington Hospital", "Abington", "PA", "cms 390231"),
], default=("Philadelphia", "PA"))

_spec("UTMB Health", [
    _h("UTMB Health John Sealy Hospital", "Galveston", "TX", "cms 450018"),
    _h("UTMB Health Jennie Sealy Hospital", "Galveston", "TX", "site"),
    _h("UTMB Health Clear Lake Campus", "Webster", "TX", "site"),
    _h("UTMB Health League City Campus", "League City", "TX", "site"),
    _h("UTMB Health Angleton Danbury Campus", "Angleton", "TX", "site"),
])

_spec("Penn State Health", [
    _h("Penn State Health Milton S. Hershey Medical Center", "Hershey", "PA", "cms 390256"),
    _h("Penn State Health Holy Spirit Medical Center", "Camp Hill", "PA", "cms 390004"),
    _h("Penn State Health St. Joseph Medical Center", "Reading", "PA", "cms 390096"),
    _h("Penn State Health Lancaster Medical Center", "Lancaster", "PA", "cms 390339"),
    _h("Penn State Health Hampden Medical Center", "Enola", "PA", "cms 390336"),
], default=("Hershey", "PA"), sites=[
    ("Penn State Health Children's Hospital", "Hershey", "PA", ("Penn State Health Children's Hospital",
                                                                "Penn State Children's Hospital")),
], entity=(r"\bPenn\s+State\s+Health\s*-\s*(.+?)\s+Location\s*:", {
    "Hershey Medical Center": "Penn State Health Milton S. Hershey Medical Center",
    "Milton S. Hershey Medical Center": "Penn State Health Milton S. Hershey Medical Center",
    "Holy Spirit Medical Center": "Penn State Health Holy Spirit Medical Center",
    "Holy Spirit Hospital": "Penn State Health Holy Spirit Medical Center",
    "St. Joseph Medical Center": "Penn State Health St. Joseph Medical Center",
    "St. Joseph": "Penn State Health St. Joseph Medical Center",
    "Lancaster Medical Center": "Penn State Health Lancaster Medical Center",
    "Hampden Medical Center": "Penn State Health Hampden Medical Center",
    "Children's Hospital": "Penn State Health Children's Hospital",
    "Medical Group": "", "Community Medical Group": "", "Shared Services": "", "System Services": "",
}), town_line=r"Location\s*:\s*US\s*:\s*PA\s*:\s*([A-Za-z .'-]+?)(?:\s+and\s|\s+Work\s+Type|\s*$)")

_spec("St. Luke's University Health Network", [
    _h("St. Luke's University Hospital - Bethlehem Campus", "Bethlehem", "PA", "cms 390049", "Fountain Hill"),
    _h("St. Luke's Hospital - Allentown Campus", "Allentown", "PA", "site (cms 390049 Allentown campus)"),
    _h("St. Luke's Hospital - Sacred Heart Campus", "Allentown", "PA", "site"),
    _h("St. Luke's Hospital - Anderson Campus", "Easton", "PA", "cms 390326"),
    _h("St. Luke's Hospital - Easton Campus", "Easton", "PA", "cms 390162"),
    _h("St. Luke's Hospital - Grand View Campus", "Sellersville", "PA", "cms 390057"),
    _h("St. Luke's Hospital - Upper Bucks Campus", "Quakertown", "PA", "cms 390035"),
    _h("St. Luke's Hospital - Carbon Campus", "Lehighton", "PA", "cms 390335"),
    _h("St. Luke's Hospital - Monroe Campus", "Stroudsburg", "PA", "cms 390330"),
    _h("St. Luke's Miners Campus", "Coaldale", "PA", "cms 390183"),
    _h("St. Luke's Warren Campus", "Phillipsburg", "NJ", "cms 310060"),
], default=("Bethlehem", "PA"))

_spec("UW Health", [
    _h("UW Health University Hospital", "Madison", "WI", "cms 520098"),
    _h("UW Health East Madison Hospital", "Madison", "WI", "site"),
    _h("UW Health SwedishAmerican Hospital", "Rockford", "IL", "cms 140228"),
], default=("Madison", "WI"), keep={
    "Rockford, IL": "the scan's review counts two UW Health hospitals in Rockford; not certain",
})

_spec("Vandalia Health", [
    _h("CAMC General Hospital", "Charleston", "WV", "cms 510022"),
    _h("CAMC Memorial Hospital", "Charleston", "WV", "cms 510022 (Memorial campus)"),
    _h("CAMC Women and Children's Hospital", "Charleston", "WV", "cms 510022 (Women and Children's)"),
    _h("CAMC Teays Valley Hospital", "Hurricane", "WV", "site"),
])

_spec("Boston Medical Center Health System", [
    _h("Boston Medical Center", "Boston", "MA", "cms 220031"),
    _h("Boston Medical Center - Brighton", "Brighton", "MA", "cms 220036"),
    _h("Boston Medical Center - Brockton", "Brockton", "MA", "cms 220111"),
], default=("Boston", "MA"))

_spec("Munson Healthcare", [
    _h("Munson Medical Center", "Traverse City", "MI", "cms 230097"),
    _h("Munson Healthcare Otsego Memorial Hospital", "Gaylord", "MI", "cms 230133"),
    _h("Munson Healthcare Cadillac Hospital", "Cadillac", "MI", "cms 230081"),
    _h("Munson Healthcare Grayling Hospital", "Grayling", "MI", "cms 230058"),
    _h("Munson Healthcare Charlevoix Hospital", "Charlevoix", "MI", "cms 231322"),
    _h("Munson Healthcare Manistee Hospital", "Manistee", "MI", "cms 230303"),
    _h("Munson Healthcare Paul Oliver Memorial Hospital", "Frankfort", "MI", "cms 231300"),
    _h("Kalkaska Memorial Health Center", "Kalkaska", "MI", "cms 231301"),
])

_spec("LifeBridge Health", [
    _h("Sinai Hospital of Baltimore", "Baltimore", "MD", "cms 210012"),
    _h("Levindale Hebrew Geriatric Center and Hospital", "Baltimore", "MD", "cms 210064"),
    _h("Grace Medical Center", "Baltimore", "MD", "site"),
    _h("Northwest Hospital", "Randallstown", "MD", "cms 210040"),
    _h("Carroll Hospital", "Westminster", "MD", "cms 210033"),
], default=("Baltimore", "MD"))

_spec("Baptist Health (AR)", [
    _h("Baptist Health Medical Center - Little Rock", "Little Rock", "AR", "cms 040114"),
    _h("Baptist Health Medical Center - North Little Rock", "North Little Rock", "AR", "cms 040036"),
    _h("Baptist Health Medical Center - Conway", "Conway", "AR", "cms 040154"),
    _h("Baptist Health Medical Center - Arkadelphia", "Arkadelphia", "AR", "cms 041321"),
    _h("Baptist Health Medical Center - Heber Springs", "Heber Springs", "AR", "cms 041312"),
    _h("Baptist Health - Fort Smith", "Fort Smith", "AR", "cms 040055"),
    _h("Baptist Health - Van Buren", "Van Buren", "AR", "cms 040018"),
    _h("Baptist Health Medical Center - Hot Spring County", "Malvern", "AR", "cms 040076"),
    _h("Baptist Health Medical Center - Drew County", "Monticello", "AR", "cms 040051"),
    _h("Baptist Health Medical Center - Stuttgart", "Stuttgart", "AR", "cms 040072"),
], keep={
    "Little Rock, AR": "the Little Rock campus also holds Baptist Health's rehabilitation and extended care hospitals",
    "North Little Rock, AR": "the rehabilitation institute's campus is not certain",
})

_spec("Penn Highlands Healthcare", [
    _h("Penn Highlands DuBois", "DuBois", "PA", "cms 390086"),
    _h("Penn Highlands Mon Valley", "Monongahela", "PA", "cms 390147"),
    _h("Penn Highlands Brookville", "Brookville", "PA", "cms 391312"),
    _h("Penn Highlands Elk", "St. Marys", "PA", "cms 391315"),
    _h("Penn Highlands Connellsville", "Connellsville", "PA", "cms 390184"),
    _h("Penn Highlands Huntingdon", "Huntingdon", "PA", "cms 390056"),
    _h("Penn Highlands Tyrone", "Tyrone", "PA", "cms 391307"),
])

_spec("Legacy Health", [
    _h("Legacy Emanuel Medical Center", "Portland", "OR", "cms 380007"),
    _h("Legacy Good Samaritan Medical Center", "Portland", "OR", "cms 380017"),
    _h("Randall Children's Hospital", "Portland", "OR", "site"),
    _h("Legacy Mount Hood Medical Center", "Gresham", "OR", "cms 380025"),
    _h("Legacy Meridian Park Medical Center", "Tualatin", "OR", "cms 380089"),
    _h("Legacy Salmon Creek Medical Center", "Vancouver", "WA", "cms 500150"),
    _h("Legacy Silverton Medical Center", "Silverton", "OR", "cms 380029"),
], default=("Portland", "OR"))

_spec("UCI Health", [
    _h("UCI Medical Center", "Orange", "CA", "cms 050348"),
    _h("UCI Health - Fountain Valley", "Fountain Valley", "CA", "cms 050570"),
    _h("UCI Health - Lakewood", "Lakewood", "CA", "cms 050581"),
    _h("UCI Health - Los Alamitos", "Los Alamitos", "CA", "cms 050551"),
    _h("UCI Health - Placentia Linda", "Placentia", "CA", "cms 050589"),
], keep={
    "Irvine, CA": "the Irvine campus is clinics and the university; its hospital's opening is not certain",
})

_spec("Northeast Georgia Health", [
    _h("Northeast Georgia Medical Center Gainesville", "Gainesville", "GA", "cms 110029"),
    _h("Northeast Georgia Medical Center Braselton", "Braselton", "GA", "site (cms 110029 Braselton campus)"),
    _h("Northeast Georgia Medical Center Barrow", "Winder", "GA", "cms 110045"),
    _h("Northeast Georgia Medical Center Habersham", "Demorest", "GA", "cms 110041"),
    _h("Northeast Georgia Medical Center Lumpkin", "Dahlonega", "GA", "cms 110237"),
])

_spec("Loma Linda University Health", [
    _h("Loma Linda University Medical Center", "Loma Linda", "CA", "cms 050327"),
    _h("Loma Linda University Children's Hospital", "Loma Linda", "CA", "cms 050778"),
    _h("Loma Linda University Medical Center - Murrieta", "Murrieta", "CA", "cms 050770"),
    _h("Loma Linda University Behavioral Medicine Center", "Redlands", "CA", "site"),
], default=("Loma Linda", "CA"))

_spec("Carilion Clinic", [
    _h("Carilion Roanoke Memorial Hospital", "Roanoke", "VA", "cms 490024"),
    _h("Carilion New River Valley Medical Center", "Christiansburg", "VA", "cms 490042"),
    _h("Carilion Rockbridge Community Hospital", "Lexington", "VA", "cms 491304"),
    _h("Carilion Franklin Memorial Hospital", "Rocky Mount", "VA", "cms 490089"),
    _h("Carilion Giles Community Hospital", "Pearisburg", "VA", "cms 491302"),
    _h("Carilion Tazewell Community Hospital", "Tazewell", "VA", "cms 490117"),
], default=("Roanoke", "VA"), sites=[
    ("Carilion Children's", "Roanoke", "VA", ("Carilion Children's", "Carilion Childrens Hospital")),
])

_spec("VCU Health", [
    _h("VCU Medical Center", "Richmond", "VA", "cms 490032"),
    _h("VCU Health Community Memorial Hospital", "South Hill", "VA", "cms 490098"),
    _h("VCU Health Tappahannock Hospital", "Tappahannock", "VA", "cms 490084"),
], default=("Richmond", "VA"), sites=[
    ("Children's Hospital of Richmond at VCU", "Richmond", "VA", ("Children's Hospital of Richmond", "CHoR")),
])

_spec("Concord Hospital", [
    _h("Concord Hospital - Laconia", "Laconia", "NH", "cms 300005"),
    _h("Concord Hospital - Franklin", "Franklin", "NH", "cms 301306"),
], default=("Concord", "NH"))

_spec("Singing River Health System", [
    _h("Singing River Pascagoula Hospital", "Pascagoula", "MS", "cms 250040"),
    _h("Singing River Ocean Springs Hospital", "Ocean Springs", "MS", "cms 250040 (Ocean Springs campus)"),
    _h("Singing River Gulfport Hospital", "Gulfport", "MS", "cms 250123"),
], default=("Pascagoula", "MS"))

_spec("Willis Knighton Health System", [
    _h("Willis-Knighton Medical Center", "Shreveport", "LA", "cms 190111"),
    _h("Willis-Knighton South", "Shreveport", "LA", "site"),
    _h("Willis-Knighton Pierremont Health Center", "Shreveport", "LA", "site"),
    _h("Willis-Knighton Bossier Health Center", "Bossier City", "LA", "site"),
], labels=("Willis Knighton Health",), default=("Shreveport", "LA"))

_spec("St. Elizabeth Healthcare", [
    _h("St. Elizabeth Edgewood", "Edgewood", "KY", "cms 180035"),
    _h("St. Elizabeth Florence", "Florence", "KY", "cms 180045"),
    _h("St. Elizabeth Fort Thomas", "Fort Thomas", "KY", "cms 180001"),
    _h("St. Elizabeth Dearborn", "Lawrenceburg", "IN", "cms 150086"),
    _h("St. Elizabeth Grant", "Williamstown", "KY", "cms 181311"),
], default=("Edgewood", "KY"))

_spec("Independence Health System", [
    _h("Westmoreland Hospital", "Greensburg", "PA", "cms 390145"),
    _h("Latrobe Hospital", "Latrobe", "PA", "cms 390219"),
    _h("Frick Hospital", "Mount Pleasant", "PA", "cms 390217"),
    _h("Butler Memorial Hospital", "Butler", "PA", "cms 390168"),
    _h("Clarion Hospital", "Clarion", "PA", "cms 390093"),
])

_spec("Vitruvian Health", [
    _h("Hamilton Medical Center", "Dalton", "GA", "cms 110001"),
    _h("Bradley Medical Center", "Cleveland", "TN", "site"),
])

_spec("Brown University Health", [
    _h("Rhode Island Hospital", "Providence", "RI", "cms 410007"),
    _h("The Miriam Hospital", "Providence", "RI", "cms 410012"),
    _h("Emma Pendleton Bradley Hospital", "East Providence", "RI", "cms 414003"),
    _h("Newport Hospital", "Newport", "RI", "cms 410006"),
], default=("Providence", "RI"))

_spec("Valley Health (VA)", [
    _h("Winchester Medical Center", "Winchester", "VA", "cms 490005"),
    _h("Shenandoah Memorial Hospital", "Woodstock", "VA", "cms 491305"),
    _h("Warren Memorial Hospital", "Front Royal", "VA", "cms 490033"),
    _h("War Memorial Hospital", "Berkeley Springs", "WV", "cms 511309"),
    _h("Page Memorial Hospital", "Luray", "VA", "cms 491307"),
    _h("Hampshire Memorial Hospital", "Romney", "WV", "cms 511311"),
], default=("Winchester", "VA"))

_spec("ChristianaCare", [
    _h("Christiana Hospital", "Newark", "DE", "cms 080001"),
    _h("Wilmington Hospital", "Wilmington", "DE", "site (cms 080001 Wilmington campus)"),
    _h("ChristianaCare Union Hospital", "Elkton", "MD", "cms 210032"),
], default=("Newark", "DE"))

_spec("Bellin Health", [
    _h("Bellin Hospital", "Green Bay", "WI", "cms 520049"),
    _h("Bellin Psychiatric Center", "Green Bay", "WI", "cms 524038"),
    _h("Bellin Health Oconto Hospital", "Oconto", "WI", "cms 521356"),
])

_spec("St. Bernards Healthcare", [
    _h("St. Bernards Medical Center", "Jonesboro", "AR", "cms 040020"),
    _h("St. Bernards CrossRidge Medical Center", "Wynne", "AR", "cms 041307"),
    _h("St. Bernards Five Rivers Medical Center", "Pocahontas", "AR", "cms 040779"),
], keep={
    "Jonesboro, AR": "St. Bernards Behavioral Health is a second hospital in Jonesboro",
})

_spec("Bryan Health", [
    _h("Bryan Medical Center", "Lincoln", "NE", "cms 280003"),
    _h("Kearney Regional Medical Center", "Kearney", "NE", "cms 280134"),
    _h("Grand Island Regional Medical Center", "Grand Island", "NE", "cms 280139"),
    _h("Merrick Medical Center", "Central City", "NE", "cms 281328"),
    _h("Crete Area Medical Center", "Crete", "NE", "cms 281354"),
], default=("Lincoln", "NE"))

_spec("Palomar Health", [
    _h("Palomar Medical Center Escondido", "Escondido", "CA", "cms 050115"),
    _h("Palomar Medical Center Poway", "Poway", "CA", "cms 050636"),
], default=("Escondido", "CA"))

_spec("Union Hospital", [
    _h("Union Hospital Clinton", "Clinton", "IN", "cms 151326"),
])

_spec("Larkin Community Hospital", [
    _h("Larkin Palm Springs Campus", "Hialeah", "FL", "cms 100050"),
    _h("Larkin Behavioral Health Services", "Hollywood", "FL", "cms 104079"),
])

_spec("Unity Health", [
    _h("Unity Health - White County Medical Center", "Searcy", "AR", "cms 040014"),
    _h("Unity Health - Specialty Care Campus", "Searcy", "AR", "site"),
    _h("Unity Health - Newport", "Newport", "AR", "cms 041332"),
    _h("Unity Health - Jacksonville", "Jacksonville", "AR", "cms 040164"),
])

_spec("Benefis Health System", [
    _h("Benefis Hospital", "Great Falls", "MT", "cms 270012"),
    _h("Benefis Teton Medical Center", "Choteau", "MT", "cms 271307"),
], default=("Great Falls", "MT"))

_spec("PIH Health", [
    _h("PIH Health Whittier Hospital", "Whittier", "CA", "cms 050169"),
    _h("PIH Health Downey Hospital", "Downey", "CA", "cms 050393"),
    _h("PIH Health Good Samaritan Hospital", "Los Angeles", "CA", "cms 050471"),
], default=("Whittier", "CA"))

_spec("Finger Lakes Health", [
    _h("Geneva General Hospital", "Geneva", "NY", "cms 330058"),
    _h("Soldiers & Sailors Memorial Hospital", "Penn Yan", "NY", "cms 331314"),
], default=("Geneva", "NY"))

_spec("Hudson Regional Health", [
    _h("Secaucus University Hospital", "Secaucus", "NJ", "cms 310118"),
    _h("Hoboken University Hospital", "Hoboken", "NJ", "cms 310040"),
    _h("Bayonne University Hospital", "Bayonne", "NJ", "cms 310025"),
])

_spec("Southeast Georgia Health System", [
    _h("Southeast Georgia Health System Brunswick Campus", "Brunswick", "GA", "cms 110025"),
    _h("Southeast Georgia Health System Camden Campus", "St. Marys", "GA", "cms 110146"),
])

_spec("Blanchard Valley Health System", [
    _h("Blanchard Valley Hospital", "Findlay", "OH", "cms 360095"),
    _h("Bluffton Hospital", "Bluffton", "OH", "cms 361322"),
])

_spec("Tidelands Health", [
    _h("Tidelands Waccamaw Community Hospital", "Murrells Inlet", "SC", "cms 420098"),
    _h("Tidelands Georgetown Memorial Hospital", "Georgetown", "SC", "cms 420020"),
], default=("Murrells Inlet", "SC"))

_spec("Skagit Regional Health", [
    _h("Skagit Valley Hospital", "Mount Vernon", "WA", "cms 500003"),
    _h("Cascade Valley Hospital", "Arlington", "WA", "cms 500060"),
])

_spec("Hoag", [
    _h("Hoag Hospital Newport Beach", "Newport Beach", "CA", "cms 050224"),
    _h("Hoag Hospital Irvine", "Irvine", "CA", "site"),
    _h("Hoag Orthopedic Institute", "Irvine", "CA", "cms 050769"),
])

_spec("North Country Healthcare", [
    _h("Androscoggin Valley Hospital", "Berlin", "NH", "cms 301310"),
    _h("Weeks Medical Center", "Lancaster", "NH", "cms 301303"),
    _h("Upper Connecticut Valley Hospital", "Colebrook", "NH", "cms 301300"),
])

_spec("Ephraim Mcdowell Health", [
    _h("Ephraim McDowell Regional Medical Center", "Danville", "KY", "cms 180048"),
    _h("Ephraim McDowell James B. Haggin Hospital", "Harrodsburg", "KY", "cms 181302"),
    _h("Ephraim McDowell Fort Logan Hospital", "Stanford", "KY", "cms 181315"),
], default=("Danville", "KY"))

_spec("Main Line Health", [
    _h("Lankenau Medical Center", "Wynnewood", "PA", "cms 390195"),
    _h("Bryn Mawr Hospital", "Bryn Mawr", "PA", "cms 390139"),
    _h("Riddle Hospital", "Media", "PA", "cms 390222"),
    _h("Paoli Hospital", "Paoli", "PA", "cms 390153"),
    _h("Bryn Mawr Rehabilitation Hospital", "Malvern", "PA", "site"),
], default=("Wynnewood", "PA"))

_spec("Salem Health", [
    _h("Salem Hospital", "Salem", "OR", "cms 380051"),
    _h("Salem Health West Valley Hospital", "Dallas", "OR", "cms 381308"),
])

_spec("Memorial Health", [
    _h("Springfield Memorial Hospital", "Springfield", "IL", "cms 140148"),
    _h("Decatur Memorial Hospital", "Decatur", "IL", "cms 140135"),
    _h("Jacksonville Memorial Hospital", "Jacksonville", "IL", "cms 141352"),
    _h("Abraham Lincoln Memorial Hospital", "Lincoln", "IL", "cms 141322"),
    _h("Taylorville Memorial Hospital", "Taylorville", "IL", "cms 141339"),
], default=("Springfield", "IL"))

_spec("Ridgeview", [
    _h("Ridgeview Medical Center", "Waconia", "MN", "cms 240056"),
    _h("Ridgeview Sibley Medical Center", "Arlington", "MN", "cms 241311"),
    _h("Ridgeview Le Sueur Medical Center", "Le Sueur", "MN", "cms 241375"),
])

_spec("KPC Health", [
    _h("Anaheim Global Medical Center", "Anaheim", "CA", "cms 050744"),
    _h("Chapman Global Medical Center", "Orange", "CA", "cms 050745"),
    _h("Hemet Global Medical Center", "Hemet", "CA", "cms 050390"),
    _h("Menifee Global Medical Center", "Menifee", "CA", "cms 050684", "Sun City"),
    _h("South Coast Global Medical Center", "Santa Ana", "CA", "site"),
    _h("Orange County Global Medical Center", "Santa Ana", "CA", "cms 050746"),
    _h("Victor Valley Global Medical Center", "Victorville", "CA", "cms 050517", "=Victor Valley"),
])

_spec("Baptist Health (Alabama)", [
    _h("Baptist Medical Center South", "Montgomery", "AL", "cms 010023"),
    _h("Baptist Medical Center East", "Montgomery", "AL", "cms 010149"),
    _h("Prattville Baptist Hospital", "Prattville", "AL", "cms 010108"),
], default=("Montgomery", "AL"))

_spec("Cayuga Health", [
    _h("Cayuga Medical Center", "Ithaca", "NY", "cms 330307"),
    _h("Schuyler Hospital", "Montour Falls", "NY", "cms 331313"),
])

_spec("Astria Health", [
    _h("Astria Sunnyside Hospital", "Sunnyside", "WA", "cms 501330"),
    _h("Astria Toppenish Hospital", "Toppenish", "WA", "cms 500037"),
])

_spec("Regional West Health Services", [
    _h("Regional West Medical Center", "Scottsbluff", "NE", "cms 280061"),
])

_spec("Union General Health System", [
    _h("Union General Hospital", "Blairsville", "GA", "cms 110051"),
    _h("Chatuge Regional Hospital", "Hiawassee", "GA", "cms 111324"),
])

_spec("Emanate Health", [
    _h("Emanate Health Inter-Community Hospital", "Covina", "CA", "cms 050382"),
    _h("Emanate Health Queen of the Valley Hospital", "West Covina", "CA", "site"),
    _h("Emanate Health Foothill Presbyterian Hospital", "Glendora", "CA", "cms 050597"),
], default=("Covina", "CA"))

_spec("Rady Children's Health", [
    _h("Children's Hospital of Orange County", "Orange", "CA", "cms 053304"),
    _h("Children's Hospital at Mission", "Mission Viejo", "CA", "site"),
    _h("Rady Children's Hospital - San Diego", "San Diego", "CA", "cms 053303"),
], default=("Orange", "CA"))

_spec("Blessing Health System", [
    _h("Illini Community Hospital", "Pittsfield", "IL", "cms 141315"),
], labels=("Blessing Hospital",), default=("Quincy", "IL"), keep={
    "Quincy, IL": "Blessing Hospital's own town: the row already names it",
})

EMIT: dict[str, TownSpec] = {e: s for s in SPECS.values() for e in s.emit}
