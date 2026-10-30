"""Facility release 5, review A (2026-10-30): the names, towns and
single-site verdicts of phase 5 checked against the stored postings.

The owner's rules stand: (1) cards and job pages show the HOSPITAL or named
site ("Apply to <site>"); the system name and logo stay on the job page and
the system hub (hospital_system is never touched here); (2) named sites
(hospitals, named clinics, centers, agencies' named branches, named
communities) show their own name; generic offices, home-based, remote,
multi-site pools and regional roles show the system name with the town; a
town or a site is never guessed (the posting itself must name the site; a
town with exactly one hospital of the system takes it only under the phase 2
town-table rule of facilities_town).

What the review found, and what this module does about it:

  SINGLE-SITE VERDICTS THAT WERE WRONG (the label is one hospital's own name,
  and these rows sit at another hospital of the same system):
    Union Hospital       Union Health also runs Union Hospital Regional in
                         Terre Haute (its boilerplate: "Union Health consists
                         of Union Hospital, Union Hospital Regional, Union
                         Hospital Clinton"; "The Emergency Care Department at
                         Union Hospital Regional"). Its departments carry the
                         code "UHR" in the title ("RN-UHR EMERGENCY ROOM",
                         "TECH-UHR CAT SCAN"): those rows are Union Hospital
                         Regional's, not the main hospital's.
    Cabell Huntington Hospital  the Marshall Health Network's label (Cabell
                         Huntington Hospital and St. Mary's Medical Center in
                         Huntington, Rivers Health in Point Pleasant, CMS
                         510012). facilities_text names a row whose opening
                         line names its hospital; a Point Pleasant row with no
                         such line sat on "Cabell Huntington Hospital". Point
                         Pleasant holds one hospital of the network: the town
                         table (facilities_town's rule) gives it to Rivers
                         Health; Huntington, with two, is kept.
    Children's National Hospital  the Workday site
                         "Rehabilitation-and-Specialized-Care-Bunker-Hill-Road"
                         (1731 Bunker Hill Road NE) is the Children's National
                         Rehabilitation and Specialized Care Center (RSC), the
                         system's post-acute pediatric centre with inpatient
                         units (the postings' own words), not the main campus.
    Billings Clinic      a courtesy posting for an affiliate names the
                         affiliate in its title ("Radiologic Technologist -
                         Daniels Memorial Healthcare, Scobey, MT", "CEO - Vista
                         Plains Health (Harlowton, Montana)"): that named site,
                         in its own town.

  NAMES THE PHASE 5 PASSES GAVE TO ROLES NO ONE SITE HOLDS (decision 2: they
  keep the system name with their town). For a row still on the label the
  town tables are told to leave it (job._town_skip); a row a phase 5 pass
  named tonight gets the label back (its town stays: it came from the
  posting).
    - a system office's title: sourcing, supply chain analytics, a command
      center, couriers, provider access, workforce planning, informaticists,
      application specialists, business development, regulatory affairs,
      desktop support, centralized scheduling or monitoring, incentive programs, deposit
      preparation, prior authorization, durable medical equipment, a
      performance improvement consultant or office, travel-at-home therapy;
    - a title that names two of the system's sites or a cross-site team
      ("Pediatric Resource Team (Cross Bay)", "PICU (BCH-Mission Bay/Oakland)",
      "Float Oakland & Walnut Creek", "Liberty/Centerville Locations",
      "Wilson & BGH", "Registered Respiratory Therapist | Brevard County");
    - a body that says so (rows of phase 5's own town tables only; on a
      night with no body the town table keeps the stored decision, which the
      SQL leaves on the label): "provides system-wide leadership", "is a system-wide support
      service", "float between multiple campus locations", "RN - Float/
      Resource - Madras and Prineville", "Able to Float to other Northern
      Campuses", "centralized scheduling";
    - UIHC: a title naming its Jefferson Street clinics, which sit across
      the street from the Downtown hospital.

Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase5/release5_work/reviewA), so the database
holds what the scraper writes on the next run.
"""
from __future__ import annotations

import logging
import re

import facilities_town as FT

logger = logging.getLogger("scraper.facility_fac5_review")

clean_text = FT.clean_text
_h = FT._h

UNION = "Union Hospital"
CABELL = "Cabell Huntington Hospital"
CNH = "Children's National Hospital"
BILLINGS = "Billings Clinic"
UCSF = "UCSF Health"
CINCY = "Cincinnati Children's"
UHS = "United Health Services"
HEALTH_FIRST = "Health First"
UIHC = "University of Iowa Hospitals and Clinics"

# The systems phase 5 named (the three change lists), and the four this
# review adds: only their rows are looked at.
PHASE5_SYSTEMS = frozenset({
    "UMass Memorial Health", "DCH Health System", "Appalachian Regional Healthcare", "Logan Health",
    "United Health Services", "WVU Medicine", "Telecare Corporation", "Shriners Children's", "Sharp HealthCare",
    "University of Iowa Hospitals and Clinics", "St. Charles Health", "Riverside Health System", "Hendrick Health",
    "St. Luke's University Health Network", "Health First", "Premier Health", "UCSF Health", "Temple Health",
    "Cape Cod Healthcare", "Bellin Health", "Corewell Health", "MaineHealth", "UT Southwestern Medical Center",
    "Cincinnati Children's", "Mary Washington Healthcare", "Norton Sound Regional Hospital", "UW Medicine",
    "NYU Langone Health", "Mass General Brigham", "Stormont Vail Health", "Children's Hospital of Philadelphia",
    "Novant Health", "Cleveland Clinic", "Beth Israel Lahey Health", "Banner Health", "UTHealth Houston",
    "Children's Wisconsin", "Akron Children's", "Stanford Health Care",
    UNION, CABELL, CNH, BILLINGS,
})


# The systems whose town table phase 5 (or this review) registered in
# facilities_town: the review tells those tables to leave a role no one site
# holds. Tables from earlier phases (Billings Clinic, Union Hospital, Temple,
# Bellin, St. Luke's) keep their own rule.
P5_TOWN_SYSTEMS = frozenset({
    "Appalachian Regional Healthcare", "DCH Health System", "Shriners Children's", "St. Charles Health",
    "Children's Wisconsin", "Premier Health", "Hendrick Health", "Sharp HealthCare", "Mary Washington Healthcare",
    "UW Medicine", "Children's Hospital of Philadelphia", "Stormont Vail Health", "Cape Cod Healthcare",
    "United Health Services", "Logan Health", "Health First", "UMass Memorial Health", "Novant Health", CABELL,
})


# ══════════════════════════════════════════════════════════════════════════
#  1. Cabell Huntington Hospital: Point Pleasant's one hospital
# ══════════════════════════════════════════════════════════════════════════
if CABELL in FT.SPECS:
    raise ValueError(f"facilities_town already has a town table for {CABELL}")
CABELL_TOWNS = FT._spec(CABELL, [
    _h("St. Mary's Medical Center", "Huntington", "WV", "cms 510007"),
    _h("Rivers Health", "Point Pleasant", "WV", "cms 510012"),
], default=("Huntington", "WV"), keep={
    "Huntington, WV": "two of the network's hospitals (Cabell Huntington Hospital, the label's own name, and "
                      "St. Mary's Medical Center): facilities_text reads the posting's opening line"})


# ══════════════════════════════════════════════════════════════════════════
#  2. Named sites the posting itself names
# ══════════════════════════════════════════════════════════════════════════
# Union Hospital: the department code "UHR" in the title ("RN-UHR EMERGENCY
# ROOM", "TECH-MRI-REGISTERED-MRI UHR").
_UHR_RX = re.compile(r"(?<![A-Za-z0-9])UHR(?![A-Za-z0-9])")
UNION_REGIONAL = ("Union Hospital Regional", "Terre Haute", "IN",
                  "posting (department code UHR; 'Union Health consists of Union Hospital, Union Hospital Regional, "
                  "Union Hospital Clinton')")

# Children's National: the Workday site segment of the posting's own link.
CNH_SITES = {
    "rehabilitationandspecializedcarebunkerhillroad": (
        "Children's National Rehabilitation and Specialized Care Center", "Washington", "DC",
        "posting (Workday site; 1731 Bunker Hill Road NE; 'Children's National Rehabilitation and Specialized "
        "Care Center (RSC)')"),
}

# Billings Clinic's affiliates, as a courtesy posting's title names them.
BILLINGS_AFFILIATES = (
    (re.compile(r"\bDaniels\s+Memorial\s+Health\s*care\b", re.I), ("Daniels Memorial Healthcare", "Scobey", "MT")),
    (re.compile(r"\bRoundup\s+Memorial\s+Hospital\b", re.I), ("Roundup Memorial Hospital", "Roundup", "MT")),
    (re.compile(r"\bNorth\s+Big\s+Horn\s+Hospital\b", re.I), ("North Big Horn Hospital", "Lovell", "WY")),
    (re.compile(r"\bThree\s+Rivers\s+Health\b", re.I), ("Three Rivers Health", "Basin", "WY")),
    (re.compile(r"\bVista\s+Plains\s+Health\b", re.I), ("Vista Plains Health", "Harlowton", "MT")),
    (re.compile(r"\bColstrip\s+Medical\s+Center\b", re.I), ("Colstrip Medical Center", "Colstrip", "MT")),
    (re.compile(r"\bRiverPeak\s+Health\b", re.I), ("RiverPeak Health", "Riverton", "WY")),
)


def _link_site(url) -> str:
    u = str(url or "")
    if "/job/" not in u:
        return ""
    return re.sub(r"[^a-z0-9]", "", u.split("/job/", 1)[1].split("/", 1)[0].lower())


def _same_town(city, state, site) -> bool:
    c, s = clean_text(city), clean_text(state).upper()
    if not c:
        return s in ("", site[2])
    return FT.town_key(c) == FT.town_key(site[1]) and s in ("", site[2])


def site_for(system, title, description, city, state, url=""):
    """(name, city, state, source) of the named site the posting names, or
    None. Title and link only (every night carries both)."""
    t = clean_text(title)
    if FT.REMOTE_RX.search(t) or FT.MULTI_SITE_RX.search(t):
        return None
    if system == UNION:
        if _UHR_RX.search(t) and _same_town(city, state, UNION_REGIONAL):
            return UNION_REGIONAL
        return None
    if system == CNH:
        hit = CNH_SITES.get(_link_site(url))
        return hit if hit and _same_town(city, state, hit) else None
    if system == BILLINGS:
        hits = {site for rx, site in BILLINGS_AFFILIATES if rx.search(t)}
        if len(hits) != 1:
            return None
        site = hits.pop()
        return site + ("title",) if _same_town(city, state, site) else None
    return None


# ══════════════════════════════════════════════════════════════════════════
#  3. Roles no one site holds
# ══════════════════════════════════════════════════════════════════════════
OFFICE_TITLE_RX = re.compile(
    r"\b(?:sourcing|supply\s+chain\s+analytic\w*|command\s+center|couriers?|provider\s+access|"
    r"workforce\s+planning|informaticist|applications?\s+specialist|business\s+development|regulatory\s+affairs|"
    r"desktop\s+support|centraliz\w+\s+(?:scheduling|scheduler|monitoring)|incentive\s+program|deposit\s+prep\w*|"
    r"prior\s+authori[sz]ation|durable\s+medical\s+equipment|applied\s+health\s+services|"
    r"performance\s+improvement\s+(?:consultant|office)|travel\s+at\s+home)\b", re.I)
# A title naming two of the system's sites, or a team that crosses them.
MULTI_TITLE_BY_SYSTEM = {
    UCSF: re.compile(
        r"\bcross[\s-]*bay\b|"
        r"\b(?:mission\s*bay|oakland|san\s*francisco|walnut\s*creek|parnassus|mount\s*zion|mt\.?\s*zion)\s*(?:/|&|\band\b)\s*"
        r"(?:mission\s*bay|oakland|san\s*francisco|walnut\s*creek|parnassus|mount\s*zion|mt\.?\s*zion)\b|"
        r"\bfloat\b[^|]*\b(?:oakland|walnut\s*creek)\b|\bcommunity\s+hospitals\b|\badult\s+services\b|"
        r"\bchildren[’']?s\s+health\b", re.I),
    CINCY: re.compile(
        r"\b(?:liberty|burnet|college\s+hill|centerville|green\s+township|anderson|mason|kenwood)\s*/\s*"
        r"(?:liberty|burnet|college\s+hill|centerville|green\s+township|anderson|mason|kenwood)\b", re.I),
    UHS: re.compile(r"\bWilson\s*(?:&|/|\band\b)\s*(?:BGH|Binghamton)\b|\b(?:BGH|Binghamton\s+General)\s*(?:&|/|\band\b)\s*"
                    r"Wilson\b", re.I),
    HEALTH_FIRST: re.compile(r"\|\s*[A-Z][a-z]+\s+County\b"),
    UIHC: re.compile(r"\bJefferson\s+Street\b", re.I),
}
BODY_SCOPE_RX = re.compile(
    r"\bprovides?\s+system[\s-]*wide\s+leadership\b|\bis\s+a\s+system[\s-]*wide\s+support\s+service\b|"
    r"\bfloat\s+between\s+multiple\s+campus\b|\bwilling\s+to\s+float\s+between\s+locations\b|"
    r"\bfloat\s+to\s+other\s+(?:\w+\s+)?campuses\b|\bfloat\s*/\s*resource\s*[–—-]+\s*[A-Za-z]+\s+and\s+[A-Za-z]+\b|"
    r"\bcentralized\s+scheduling\b|\bserve\s+multiple\s+hospitals\b", re.I)


def scope_by_title(system, title) -> str:
    """Why the title says no one site holds the job, or ""."""
    t = clean_text(title)
    if OFFICE_TITLE_RX.search(t):
        return "office-title"
    rx = MULTI_TITLE_BY_SYSTEM.get(system)
    if rx is not None and rx.search(t):
        return "multi-title"
    return ""


def scope_by_body(description) -> str:
    return "scope-body" if BODY_SCOPE_RX.search(clean_text(description)) else ""


# ══════════════════════════════════════════════════════════════════════════
#  4. Applying it (finalize_jobs, after the phase 5 passes, before the
#     town tables)
# ══════════════════════════════════════════════════════════════════════════
def snapshot(jobs) -> dict:
    """{id(job): hospital_name} of the phase 5 systems' rows before the
    phase 5 passes run."""
    return {id(j): j.hospital_name for j in jobs if (j.hospital_system or "").strip() in PHASE5_SYSTEMS}


# The names phase 5 gives before finalize_jobs' phase 5 passes (and so before
# the snapshot): UCSF's three hospital buildings from the Oracle work location
# (facilities_fac5_a.ORACLE_EXTRA through facilities_oracle_other) and
# Banner's Alzheimer's Institute from the Workday site (facilities_p5_b). A row
# carrying one of them is treated as a phase 5 name.
P5_EARLY_NAMES = {
    UCSF: frozenset({"UCSF Helen Diller Medical Center at Parnassus Heights", "UCSF Medical Center at Mission Bay",
                     "UCSF Medical Center at Mount Zion"}),
    "Banner Health": frozenset({"Banner Alzheimer's Institute", "Banner Alzheimer's Institute Tucson"}),
}


def _is_label(system, name) -> bool:
    spec = FT.SPECS.get(system) or FT.EMIT.get(system)
    if spec is not None:
        return spec.is_label(name)
    k = FT.name_key(name)
    return not k or k == FT.name_key(system)


def apply_facility(job, before=None) -> str:
    s = (job.hospital_system or "").strip()
    if s not in PHASE5_SYSTEMS:
        return ""
    name = job.hospital_name
    was = before.get(id(job), name) if before is not None else name
    label_now, label_was = _is_label(s, name), _is_label(s, was)
    early = name in P5_EARLY_NAMES.get(s, ())
    if not label_was and not early:
        return ""                                  # named before phase 5 (an adapter's or an earlier family's)
    why = scope_by_title(s, job.title)
    if not label_now:
        # a phase 5 pass named it tonight: a role no one site holds gets the label back
        if why:
            job.hospital_name = s
            return why
        return ""
    # still on the label: a named site the posting names ...
    hit = site_for(s, job.title, job.description, job.city, job.state, job.url)
    if hit:
        job.hospital_name = hit[0]
        if hit[1] and not _same_town(job.city, job.state, hit):
            job.city, job.state, job.location = hit[1], hit[2], f"{hit[1]}, {hit[2]}"
        return "site"
    # ... else the town tables leave a role no one site holds (the phase 5
    # tables only: an earlier phase's table keeps its own rule)
    if s in P5_TOWN_SYSTEMS:
        why = why or scope_by_body(job.description)
        if why:
            job._town_skip = True
            return why
    return ""


ROW_ERRORS: dict = {}


def apply_facilities(jobs, before=None) -> dict:
    """apply_facility over a run's rows. A row it raises on keeps the name
    and town it had and is counted ("error"). Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        prev = (job.hospital_name, job.city, job.state, job.location)
        try:
            how = apply_facility(job, before)
        except Exception as e:
            job.hospital_name, job.city, job.state, job.location = prev
            n = ROW_ERRORS.get(job.hospital_system, 0) + 1
            ROW_ERRORS[job.hospital_system] = n
            if n <= 3:
                logger.warning(f"Phase 5 review: one {job.hospital_system} row kept its name ({type(e).__name__}: {e})")
            how = "error"
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats
