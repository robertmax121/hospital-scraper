"""Hospitals inside multi-hospital systems: the Oracle HCM work locations and
the feed codes of Jackson Health, NYU Langone, CHS, Renown, Hawaii Pacific
and Deaconess (2026-10-29, the oracle-other family).

The owner, 2026-10-29: every hospital inside a multi-hospital system must be
named and placed correctly on the site and in the dashboard, and applies
retro-assigned to the right hospital ("any multi system hospital is subject
to this"). Decisions: (1) cards and job pages show the HOSPITAL, the system
name and logo stay on the job page and the system hub; (2) named sites
(hospitals, named clinics and centers) show their own name, generic offices,
home-based and remote jobs show the system name with the town; never guess a
town.

The scan (facility_scale_2026-10-29/scan/SCAN.json) found these systems'
rows filed under the system name, or under a raw feed code ("Jackson Main",
"South", "Corporate"). Where each posting names its site differs by feed:

  Oracle HCM   the requisition's work location (workLocation LocationName,
               in the list payload every night): Baptist Memorial "BMH -
               Desoto Hospital", Adventist Health "AHHF - Hospital",
               WellSpan "1001 S George St-York Hospital-EMP", Atlantic
               Health "100 Madison Ave" (the hospital's own street address)
  feed codes   the adapter's own facility value, a code or a cut name:
               Jackson Health "Jackson Main", "North", "Corporate"; NYU
               Langone "NYU Langone Hospital--Long Island"; CHS "CHS
               Corporate"; Renown "Renown Regional Med Cntr"; Hawaii
               Pacific "Straub Medical Center, Honolulu, HI"; Deaconess "IL
               Crossroads Hospital"

Siblings: the posting text (an "Entity:" or "Location:" line) is read by
facilities_text, the text family, for every system whose body names the site
(the Infor / HealthcareSource / Taleo ones: UNC, Bayhealth, Lee, Kaleida,
BayCare ...); the town-only systems (Penn State Health, Vandalia, Mayo ...)
are facilities_town's. Nothing here reads a body. Where a text or town
sibling also lists an Oracle tenant here (UChicago, UC Health, HealthPartners,
Loma Linda, UW Health, NorthBay, St. Joseph's, Hoag), this table reads the
posting's own work location at list time, with the same clean names, so a
row named here is no longer on the label when those run. IU Health and
TriHealth bodies name a hospital too, but a body line can name a hospital in
another town than the posting (a Riley clinic in Fort Wayne); the work
location cannot, so only it is read.

Each system has one table here: the hospitals (one clean public name, the
town of its own street address, and where that town comes from: "cms <id>"
the CMS hospital list in the database, "address <street>" the facility's
published address), and the feed spellings that name each one. A spelling
the table does not list names nothing: the row stays on the system label (or
keeps its feed value), as before. Nothing is fuzzy-matched; a site the table
does not know yet waits for its spelling to be added.

How the tables reach the rows (one path, the one the 10-15 systems use):
  - each system becomes a facility_names.Spec (facility_names.register);
    finalize_jobs' facility_names.apply_facilities then resolves the
    adapter's value through it, keeps the posting's own town, and fills the
    hospital's town only where the posting has none or sits at the system's
    head office default (facility_names._place). hospital_system never
    changes (the upsert key, the hub, the filters, the logo).
  - Oracle: ORACLE_SYSTEMS join scraper.ORACLE_LOCATION_FACILITY, so
    scrape_oracle reads the work location through oracle_location() (a few
    tenants' spellings are cleaned first, Ex: Loma Linda's "Campus/Building/
    Floor"), and oracle_site_town() moves the row to the hospital's town when
    the work location's own address is in it. The work location is in the
    list payload every night, so nothing stored has to be kept.
  - feed codes: rename_jobs() runs in finalize_jobs just before
    apply_facilities and turns a code into the hospital's clean name (then
    placed like any other) or, for an office or a unit that is not one site
    ("Corporate", "Support Svc"), into the system label.

The same tables write the stored-row change list (facility_scale_2026-10-29/
phase2/build_changes_oracle_other.py), so the database holds what the
scraper keeps.
"""
from __future__ import annotations

import re

import facility_names as F

# system (scraper emit name) -> its table, see _sys()
SYSTEMS: dict[str, dict] = {}


def _sys(system, *, db=None, hq=("", ""), emit=(), hospitals, sites=None, rules=(),
         rename=None, oracle=False, clean=None, prefixes=(), town_only=()):
    """One system's table.

    hospitals  {clean public name: (town, state, source of the town)}
    sites      {feed spelling: clean name} (an Oracle work location or the
               adapter's own value); a value may instead be {town: clean
               name} for a spelling several hospitals share, read with the
               posting's own town
    rules      ((regex, clean name), ...) searched only when no spelling
               matched
    rename     {feed code: clean name or the system label} plus, per code,
               optional ((title regex, clean name), ...) read first
    oracle     the tenant's Oracle work location is read (ORACLE_SYSTEMS)
    clean      (regex, replacement) applied to an Oracle work location
               before it is looked up
    town_only  hospitals a feed code names only on a posting in the
               hospital's own town: the code is the employing entity, which
               also runs offices and practices in other towns; a posting
               elsewhere (or with no town) takes the system label with its
               own town (owner 2026-10-29, "go with recs": NYU Langone)
    """
    db = db or system
    SYSTEMS[system] = dict(db=db, hq=hq, emit=tuple(emit), hospitals=dict(hospitals), sites=dict(sites or {}),
                           rules=tuple(rules),
                           rename=dict(rename or {}), oracle=oracle,
                           clean=(re.compile(clean[0], re.I), clean[1]) if clean else None,
                           prefixes=tuple(prefixes), town_only=frozenset(town_only))
    for name in town_only:
        if name not in SYSTEMS[system]["hospitals"]:
            raise ValueError(f"{system}: town_only names unknown hospital {name!r}")


# ═════════════════════════════ ORACLE HCM ═══════════════════════════════════
# Work location spellings: the tenants' own workLocationsFacet (one search
# request per tenant, 2026-10-29), the CMS list for the name and the town.

_sys("Baptist Memorial Health Care", hq=("Memphis", "TN"), oracle=True, hospitals={
    "Baptist Memorial Hospital-Memphis":          ("Memphis", "TN", "cms 440048"),
    "Mississippi Baptist Medical Center":         ("Jackson", "MS", "cms 250102"),
    "NEA Baptist Memorial Hospital":              ("Jonesboro", "AR", "cms 040118"),
    "Baptist Memorial Hospital-DeSoto":           ("Southaven", "MS", "cms 250141"),
    "Baptist Memorial Hospital-North Mississippi": ("Oxford", "MS", "cms 250034"),
    "Baptist Memorial Hospital-Golden Triangle":  ("Columbus", "MS", "address 2520 5th St N, Columbus"),
    "Baptist Anderson Regional Medical Center":   ("Meridian", "MS", "cms 250104"),
    "Baptist Memorial Hospital-Union County":     ("New Albany", "MS", "cms 250006"),
    "Baptist Memorial Hospital-Collierville":     ("Collierville", "TN", "address 1500 W Poplar Ave, Collierville"),
}, sites={
    "BMH - Memphis Hospital":                      "Baptist Memorial Hospital-Memphis",
    "MBMC - Hospital":                             "Mississippi Baptist Medical Center",
    "BMH - NEA Hospital":                          "NEA Baptist Memorial Hospital",
    "BMH - Desoto Hospital":                       "Baptist Memorial Hospital-DeSoto",
    "BMH - North Mississippi Oxford Hospital":     "Baptist Memorial Hospital-North Mississippi",
    "BMH - Golden Triangle Hospital":              "Baptist Memorial Hospital-Golden Triangle",
    "BAMC-Baptist Anderson Regional Medical Center North": "Baptist Anderson Regional Medical Center",
    "BMH - Union County Hospital":                 "Baptist Memorial Hospital-Union County",
    "BMH - Collierville Hospital":                 "Baptist Memorial Hospital-Collierville",
})

# Adventist Health: "<site code> - Hospital" is the hospital itself; its other
# work locations ("AHCS - Kailua Professional Center I") are clinics and
# offices and stay on the system. Codes read off the facet counts against the
# stored towns and the review's own reads (AHHF Hanford, AHVO Vallejo, AHSH
# St. Helena, AHMB Montebello). A code the table does not list is read by the
# posting's town, one Adventist hospital per town (the CMS list).
_ADV = {
    "Adventist Health Portland":                  ("Portland", "OR", "cms 380060"),
    "Adventist Health Castle":                    ("Kailua", "HI", "cms 120006"),
    "Adventist Health Bakersfield":               ("Bakersfield", "CA", "cms 050455"),
    "Adventist Health Hanford":                   ("Hanford", "CA", "cms 050121"),
    "Adventist Health Glendale":                  ("Glendale", "CA", "cms 050239"),
    "Adventist Health Simi Valley":               ("Simi Valley", "CA", "cms 050236"),
    "Adventist Health Twin Cities":               ("Templeton", "CA", "cms 050633"),
    "Adventist Health Lodi Memorial":             ("Lodi", "CA", "cms 050336"),
    "Adventist Health Sierra Vista":              ("San Luis Obispo", "CA", "cms 050506"),
    "Adventist Health St. Helena":                ("Saint Helena", "CA", "cms 050013"),
    "Adventist Health White Memorial Montebello": ("Montebello", "CA", "cms 050350"),
    "Adventist Health Vallejo":                   ("Vallejo", "CA", "cms 054074"),
    "Adventist Health White Memorial":            ("Los Angeles", "CA", "cms 050103"),
    "Adventist Health Clear Lake":                ("Clearlake", "CA", "cms 051317"),
    "Adventist Health Delano":                    ("Delano", "CA", "cms 050608"),
    "Adventist Health Mendocino Coast":           ("Fort Bragg", "CA", "cms 051325"),
    "Adventist Health Rideout":                   ("Marysville", "CA", "cms 050133"),
    "Adventist Health Reedley":                   ("Reedley", "CA", "cms 050192"),
    "Adventist Health Sonora":                    ("Sonora", "CA", "cms 050335"),
    "Adventist Health Tehachapi Valley":          ("Tehachapi", "CA", "cms 051301"),
    "Adventist Health Tulare":                    ("Tulare", "CA", "cms 050784"),
    "Adventist Health Ukiah Valley":              ("Ukiah", "CA", "cms 050301"),
    "Adventist Health Howard Memorial":           ("Willits", "CA", "cms 051310"),
    "Adventist Health Tillamook":                 ("Tillamook", "OR", "cms 381317"),
}
_sys("Adventist Health", hq=("Roseville", "CA"), oracle=True, hospitals=_ADV, sites={
    "AHPL - Hospital": "Adventist Health Portland",
    "AHCS - Hospital": "Adventist Health Castle",
    "AHBD - Hospital": "Adventist Health Bakersfield",
    "AHHF - Hospital": "Adventist Health Hanford",
    "AHGL - Hospital": "Adventist Health Glendale",
    "AHSL - Hospital": "Adventist Health Simi Valley",
    "AHTC - Hospital": "Adventist Health Twin Cities",
    "AHLM - Hospital": "Adventist Health Lodi Memorial",
    "AHSV - Hospital": "Adventist Health Sierra Vista",
    "AHSH - Hospital": "Adventist Health St. Helena",
    "AHMB - Hospital": "Adventist Health White Memorial Montebello",
    "AHVO - Hospital": "Adventist Health Vallejo",
    # any other "<code> - Hospital": the posting's town (clean below)
    "Adventist Health site hospital": {
        "los angeles": "Adventist Health White Memorial", "clearlake": "Adventist Health Clear Lake",
        "delano": "Adventist Health Delano", "fort bragg": "Adventist Health Mendocino Coast",
        "marysville": "Adventist Health Rideout", "reedley": "Adventist Health Reedley",
        "sonora": "Adventist Health Sonora", "tehachapi": "Adventist Health Tehachapi Valley",
        "tulare": "Adventist Health Tulare", "ukiah": "Adventist Health Ukiah Valley",
        "willits": "Adventist Health Howard Memorial", "tillamook": "Adventist Health Tillamook",
        "portland": "Adventist Health Portland", "kailua": "Adventist Health Castle",
        "bakersfield": "Adventist Health Bakersfield", "hanford": "Adventist Health Hanford",
        "glendale": "Adventist Health Glendale", "simi valley": "Adventist Health Simi Valley",
        "templeton": "Adventist Health Twin Cities", "lodi": "Adventist Health Lodi Memorial",
        "san luis obispo": "Adventist Health Sierra Vista", "saint helena": "Adventist Health St. Helena",
        "st helena": "Adventist Health St. Helena", "montebello": "Adventist Health White Memorial Montebello",
        "vallejo": "Adventist Health Vallejo",
    },
}, clean=(r"^AH(?!PL|CS|BD|HF|GL|SL|TC|LM|SV|SH|MB|VO)[A-Z]{2} - Hospital$", "Adventist Health site hospital"))

_sys("Providence Health", hq=("Renton", "WA"), oracle=True, hospitals={
    "Providence Sacred Heart Medical Center":     ("Spokane", "WA", "address 101 W 8th Ave, Spokane"),
    "Providence Alaska Medical Center":           ("Anchorage", "AK", "cms 020001"),
    "Providence Portland Medical Center":         ("Portland", "OR", "cms 380061"),
    "Covenant Medical Center":                    ("Lubbock", "TX", "cms 450040"),
    "Providence St. Vincent Medical Center":      ("Portland", "OR", "cms 380004"),
    "Providence St. Peter Hospital":              ("Olympia", "WA", "cms 500024"),
    "Swedish First Hill":                         ("Seattle", "WA", "cms 500027"),
    "Providence St. Mary Medical Center":         ("Walla Walla", "WA", "cms 500002"),
    "Providence Regional Medical Center Everett": ("Everett", "WA", "cms 500014"),
    "Providence Mission Hospital Mission Viejo":  ("Mission Viejo", "CA", "cms 050567"),
    "Providence St. Joseph Hospital Eureka":      ("Eureka", "CA", "cms 050006"),
    "Providence Saint Joseph Medical Center":     ("Burbank", "CA", "cms 050235"),
}, sites={
    "Sacred Heart Medical Center-Spokane(HR)":       "Providence Sacred Heart Medical Center",
    "Providence Alaska Medical Ctr-Anchorage(HR)":   "Providence Alaska Medical Center",
    "Providence Portland Medical Ctr-Portland(HR)":  "Providence Portland Medical Center",
    "Covenant Medical Center(HR)":                   "Covenant Medical Center",
    "Providence St Vincent Medical Ctr-Portland(HR)": "Providence St. Vincent Medical Center",
    "Providence St Peter Hospital-Olympia(HR)":      "Providence St. Peter Hospital",
    "Swedish First Hill 747 Broadway-Seattle(HR)":   "Swedish First Hill",
    "St Mary Medical Center-Walla Walla(HR)":        "Providence St. Mary Medical Center",
    "Everett Medical Ctr Colby-Everett(HR)":         "Providence Regional Medical Center Everett",
    "Mission Hospital Mission Viejo(HR)":            "Providence Mission Hospital Mission Viejo",
    "St Joseph Hospital Eureka(HR)":                 "Providence St. Joseph Hospital Eureka",
    "Providence Saint Joseph Medical Ctr-Burbank(HR)": "Providence Saint Joseph Medical Center",
})

_sys("Indiana University Health", hq=("Indianapolis", "IN"), oracle=True, hospitals={
    "IU Health Methodist Hospital":               ("Indianapolis", "IN", "cms 150056"),
    "IU Health University Hospital":              ("Indianapolis", "IN", "address 550 University Blvd, Indianapolis"),
    "Riley Hospital for Children at IU Health":   ("Indianapolis", "IN", "address 705 Riley Hospital Dr, Indianapolis"),
    "IU Health Bloomington Hospital":             ("Bloomington", "IN", "cms 150051"),
    "IU Health Arnett Hospital":                  ("Lafayette", "IN", "cms 150173"),
    "IU Health Ball Memorial Hospital":           ("Muncie", "IN", "cms 150089"),
    "IU Health West Hospital":                    ("Avon", "IN", "cms 150158"),
    "IU Health North Hospital":                   ("Carmel", "IN", "cms 150161"),
    "IU Health Bedford Hospital":                 ("Bedford", "IN", "cms 151328"),
    "IU Health Tipton Hospital":                  ("Tipton", "IN", "cms 151311"),
    "IU Health Frankfort Hospital":               ("Frankfort", "IN", "cms 151316"),
}, sites={
    "Methodist Hospital": "IU Health Methodist Hospital",
    "IU Health Methodist Hospital": "IU Health Methodist Hospital",
    "Bloomington Hospital": "IU Health Bloomington Hospital",
    "Riley Hospital": "Riley Hospital for Children at IU Health",
    "Riley Hospital for Children": "Riley Hospital for Children at IU Health",
    "Arnett Hospital": "IU Health Arnett Hospital",
    "University Hospital": "IU Health University Hospital",
    "IU Health University Hospital": "IU Health University Hospital",
    "Ball Memorial Hospital": "IU Health Ball Memorial Hospital",
    "Ball Memorial": "IU Health Ball Memorial Hospital",
    "West Hospital": "IU Health West Hospital",
    "North Hospital": "IU Health North Hospital",
    "Bedford Hospital": "IU Health Bedford Hospital",
    "Tipton Hospital": "IU Health Tipton Hospital",
    "Frankfort Hospital": "IU Health Frankfort Hospital",
})

_sys("Inova Health System", oracle=True, hospitals={
    "Inova Fairfax Hospital":       ("Falls Church", "VA", "cms 490063"),
    "Inova Alexandria Hospital":    ("Alexandria", "VA", "cms 490040"),
    "Inova Loudoun Hospital":       ("Leesburg", "VA", "cms 490043"),
    "Inova Mount Vernon Hospital":  ("Alexandria", "VA", "cms 490122"),
    "Inova Fair Oaks Hospital":     ("Fairfax", "VA", "cms 490101"),
}, sites={})

_sys("Parkview Health", hq=("Fort Wayne", "IN"), oracle=True, hospitals={
    "Parkview Regional Medical Center": ("Fort Wayne", "IN", "cms 150021"),
    "Parkview Hospital Randallia":      ("Fort Wayne", "IN", "address 2200 Randallia Dr, Fort Wayne"),
    "Parkview Huntington Hospital":     ("Huntington", "IN", "cms 150091"),
    "Parkview LaGrange Hospital":       ("LaGrange", "IN", "cms 151323"),
    "Parkview DeKalb Hospital":         ("Auburn", "IN", "cms 150045"),
}, sites={
    "Parkview Regional Medical Ctr": "Parkview Regional Medical Center",
    "Parkview Randallia": "Parkview Hospital Randallia",
    "Parkview Lagrange Hospital": "Parkview LaGrange Hospital",
})

_sys("WellSpan Health", hq=("York", "PA"), oracle=True, hospitals={
    "WellSpan York Hospital":                         ("York", "PA", "cms 390046"),
    "WellSpan Chambersburg Hospital":                 ("Chambersburg", "PA", "cms 390151"),
    "WellSpan Ephrata Community Hospital":            ("Ephrata", "PA", "cms 390225"),
    "WellSpan Evangelical Community Hospital":        ("Lewisburg", "PA", "cms 390013"),
    "WellSpan Good Samaritan Hospital":               ("Lebanon", "PA", "cms 390066"),
    "WellSpan Gettysburg Hospital":                   ("Gettysburg", "PA", "cms 390065"),
    "WellSpan Waynesboro Hospital":                   ("Waynesboro", "PA", "cms 390138"),
    "WellSpan Philhaven":                             ("Mount Gretna", "PA", "cms 394020"),
    "WellSpan Surgery and Rehabilitation Hospital":   ("York", "PA", "cms 390327"),
}, sites={
    "1001 S George St-York Hospital-EMP":              "WellSpan York Hospital",
    "112 N 7th St-Chambersburg Hospital-EMP":          "WellSpan Chambersburg Hospital",
    "169 Martin Ave-Ephrata Community Hospital-EMP":   "WellSpan Ephrata Community Hospital",
    "1 Hospital Dr-Evangelical Community Hospital-EMP": "WellSpan Evangelical Community Hospital",
    "252 S 4th St-Good Samaritan Hospital-EMP":        "WellSpan Good Samaritan Hospital",
    "147 Gettys St-Gettysburg Hospital-EMP":           "WellSpan Gettysburg Hospital",
    "501 E Main St-Waynesboro Hospital-EMP":           "WellSpan Waynesboro Hospital",
    "283 Butler Rd-Philhaven Mt Gretna-EMP":           "WellSpan Philhaven",
    "55 Monument Rd-Surgery & Rehab Hospital-EMP":     "WellSpan Surgery and Rehabilitation Hospital",
})

_sys("Franciscan Missionaries of Our Lady Health System", hq=("Baton Rouge", "LA"), oracle=True, hospitals={
    "St. Dominic Hospital":                              ("Jackson", "MS", "cms 250048"),
    "Our Lady of the Lake Regional Medical Center":      ("Baton Rouge", "LA", "cms 190064"),
    "Our Lady of the Lake Children's Hospital":          ("Baton Rouge", "LA", "address 8300 Constantin Blvd, Baton Rouge"),
    "Our Lady of Lourdes Regional Medical Center":       ("Lafayette", "LA", "cms 190102"),
    "Our Lady of Lourdes Women's & Children's Hospital": ("Lafayette", "LA", "address 4600 Ambassador Caffery Pkwy, Lafayette"),
    "Our Lady of Lourdes Heart Hospital":                ("Lafayette", "LA", "address 1105 Kaliste Saloom Rd, Lafayette"),
    "St. Francis Medical Center":                        ("Monroe", "LA", "cms 190125"),
    "Our Lady of the Angels Hospital":                   ("Bogalusa", "LA", "cms 190312"),
    "St. Elizabeth Hospital":                            ("Gonzales", "LA", "address 1125 W Hwy 30, Gonzales"),
}, sites={
    "HR_SDH Main Campus":                 "St. Dominic Hospital",
    "HR_Lake Main Campus":                "Our Lady of the Lake Regional Medical Center",
    "HR_Lake Childrens Hospital Campus":  "Our Lady of the Lake Children's Hospital",
    "HR_Lourdes Main Campus":             "Our Lady of Lourdes Regional Medical Center",
    "HR_Lourdes WCH Campus":              "Our Lady of Lourdes Women's & Children's Hospital",
    "HR_Lourdes HH Campus":               "Our Lady of Lourdes Heart Hospital",
    "HR_SFMC Main Campus":                "St. Francis Medical Center",
    "HR_OLOA Main Campus":                "Our Lady of the Angels Hospital",
    "HR_St Elizabeth Campus":             "St. Elizabeth Hospital",
})

_sys("HealthPartners", hq=("St. Paul", "MN"), oracle=True, hospitals={
    "Regions Hospital":                   ("Saint Paul", "MN", "cms 240106"),
    "Park Nicollet Methodist Hospital":   ("Saint Louis Park", "MN", "cms 240053"),
    "Hutchinson Health Hospital":         ("Hutchinson", "MN", "cms 240187"),
    "Lakeview Hospital":                  ("Stillwater", "MN", "cms 240066"),
    "Olivia Hospital & Clinic":           ("Olivia", "MN", "cms 241306"),
    "Hudson Hospital & Clinic":           ("Hudson", "WI", "cms 521335"),
    "Westfields Hospital & Clinic":       ("New Richmond", "WI", "cms 521345"),
    "Amery Hospital & Clinic":            ("Amery", "WI", "cms 521308"),
}, sites={
    "Regions Hospital & Clinics": "Regions Hospital",
    "Methodist Hospital": "Park Nicollet Methodist Hospital",
    "Hudson Hospital & Clinics": "Hudson Hospital & Clinic",
})

_sys("UChicago Medicine", hq=("Chicago", "IL"), oracle=True, hospitals={
    "University of Chicago Medical Center":  ("Chicago", "IL", "cms 140088"),
    "UChicago Medicine Ingalls Memorial":    ("Harvey", "IL", "cms 140191"),
}, sites={
    "HR_Chicago - Hyde Park": "University of Chicago Medical Center",
    "HR_Harvey-CHHD": "UChicago Medicine Ingalls Memorial",
})

_sys("UC Health", hq=("Cincinnati", "OH"), oracle=True, hospitals={
    "University of Cincinnati Medical Center": ("Cincinnati", "OH", "cms 360003"),
    "West Chester Hospital":                   ("West Chester", "OH", "cms 360354"),
}, sites={
    "UC Medical Center": "University of Cincinnati Medical Center",
})

_sys("Baptist Health Care (Pensacola)", oracle=True, hospitals={
    "Baptist Hospital":     ("Pensacola", "FL", "cms 100093"),
    "Gulf Breeze Hospital": ("Gulf Breeze", "FL", "cms 100266"),
    "Jay Hospital":         ("Jay", "FL", "cms 101315"),
}, sites={"HR_BH": "Baptist Hospital", "HR_GBH": "Gulf Breeze Hospital", "HR_JH": "Jay Hospital"})

# Lifepoint: the work location is the hospital's own name; the board's ten
# biggest are listed (the facet stops at ten). Its "Facility Name:" body
# lines also name partner hospitals where Lifepoint Rehabilitation runs a
# unit (Kindred, Mosaic, Mercy); those are not read.
_sys("Lifepoint Health", oracle=True, hospitals={
    "Haywood Regional Medical Center":    ("Clyde", "NC", "cms 340184"),
    "Ottumwa Regional Health Center":     ("Ottumwa", "IA", "cms 160089"),
    "Raleigh General Hospital":           ("Beckley", "WV", "cms 510070"),
    "Conemaugh Memorial Medical Center":  ("Johnstown", "PA", "cms 390110"),
    "Frye Regional Medical Center":       ("Hickory", "NC", "cms 340116"),
    "North Alabama Medical Center":       ("Florence", "AL", "cms 010006"),
    "Harris Regional Hospital":           ("Sylva", "NC", "cms 340016"),
    "Lake Cumberland Regional Hospital":  ("Somerset", "KY", "cms 180132"),
    "Havasu Regional Medical Center":     ("Lake Havasu City", "AZ", "cms 030069"),
    "UP Health System-Marquette":         ("Marquette", "MI", "cms 230054"),
}, sites={
    "UP Health System - Marquette General Hospital": "UP Health System-Marquette",
})

_sys("Atlantic Health System", oracle=True, hospitals={
    "Morristown Medical Center":  ("Morristown", "NJ", "cms 310015"),
    "Overlook Medical Center":    ("Summit", "NJ", "cms 310051"),
    "Chilton Medical Center":     ("Pompton Plains", "NJ", "cms 310017"),
    "Newton Medical Center":      ("Newton", "NJ", "cms 310028"),
    "Hackettstown Medical Center": ("Hackettstown", "NJ", "cms 310115"),
}, sites={
    # the work location is the hospital's own street address
    "100 Madison Ave": "Morristown Medical Center",
    "99 Beauvoir Ave": "Overlook Medical Center",
    "97 West Pkwy": "Chilton Medical Center",
    "175 High St": "Newton Medical Center",
    "651 Willow Grove St": "Hackettstown Medical Center",
})

_sys("St. Joseph's Health", oracle=True, hospitals={
    "St. Joseph's University Medical Center": ("Paterson", "NJ", "cms 310019"),
    "St. Joseph's Wayne Medical Center":      ("Wayne", "NJ", "address 224 Hamburg Tpke, Wayne"),
}, sites={})

_sys("Cottage Health", oracle=True, hospitals={
    "Santa Barbara Cottage Hospital":     ("Santa Barbara", "CA", "cms 050396"),
    "Goleta Valley Cottage Hospital":     ("Goleta", "CA", "address 351 S Patterson Ave, Goleta"),
    "Santa Ynez Valley Cottage Hospital": ("Solvang", "CA", "cms 051331"),
    "Cottage Rehabilitation Hospital":    ("Santa Barbara", "CA", "address 2415 De La Vina St, Santa Barbara"),
}, sites={"Cottage Rehab Hospital": "Cottage Rehabilitation Hospital"})

_sys("NorthBay Health", oracle=True, hospitals={
    "NorthBay Medical Center":         ("Fairfield", "CA", "cms 050367"),
    "NorthBay VacaValley Hospital":    ("Vacaville", "CA", "address 1000 Nut Tree Rd, Vacaville"),
}, sites={"Vaca Valley Hospital": "NorthBay VacaValley Hospital"})

_sys("Centra Health", oracle=True, hospitals={
    "Centra Lynchburg General Hospital":    ("Lynchburg", "VA", "cms 490021"),
    "Centra Virginia Baptist Hospital":     ("Lynchburg", "VA", "address 3300 Rivermont Ave, Lynchburg"),
    "Centra Southside Community Hospital":  ("Farmville", "VA", "cms 490090"),
    "Centra Bedford Memorial Hospital":     ("Bedford", "VA", "cms 490088"),
}, sites={
    "Lynchburg General Hospital": "Centra Lynchburg General Hospital",
    "Virginia Baptist Hospital": "Centra Virginia Baptist Hospital",
    "Southside Community Hospital": "Centra Southside Community Hospital",
    "Bedford Memorial Hospital": "Centra Bedford Memorial Hospital",
})

_sys("Adena Health", oracle=True, hospitals={
    "Adena Regional Medical Center":   ("Chillicothe", "OH", "cms 360159"),
    "Adena Greenfield Medical Center": ("Greenfield", "OH", "address 550 Mirabeau St, Greenfield"),
    "Adena Pike Medical Center":       ("Waverly", "OH", "cms 361334"),
    "Adena Fayette Medical Center":    ("Washington Court House", "OH", "cms 361331"),
}, sites={
    "Adena Greenfield Medical Ctr": "Adena Greenfield Medical Center",
    "Pike Community Hospital": "Adena Pike Medical Center",
})

_sys("AtlantiCare", oracle=True, hospitals={
    "AtlantiCare Regional Medical Center, Mainland Campus": ("Pomona", "NJ", "address 65 W Jimmie Leeds Rd, Pomona"),
    "AtlantiCare Regional Medical Center, City Campus":     ("Atlantic City", "NJ", "cms 310064"),
}, sites={
    "Mainland Hosp": "AtlantiCare Regional Medical Center, Mainland Campus",
    "City Hospital": "AtlantiCare Regional Medical Center, City Campus",
})

_sys("Hoag", oracle=True, hospitals={
    "Hoag Hospital Newport Beach": ("Newport Beach", "CA", "cms 050224"),
    "Hoag Hospital Irvine":        ("Irvine", "CA", "address 16200 Sand Canyon Ave, Irvine"),
}, sites={"Hoag Newport Beach": "Hoag Hospital Newport Beach", "Hoag Irvine Campus": "Hoag Hospital Irvine"})

_sys("Mosaic Life Care", hq=("Saint Joseph", "MO"), oracle=True, hospitals={
    "Mosaic Life Care at St. Joseph":   ("Saint Joseph", "MO", "cms 260006"),
    "Mosaic Medical Center - Maryville": ("Maryville", "MO", "cms 261340"),
    "Mosaic Medical Center - Albany":   ("Albany", "MO", "cms 261328"),
}, sites={
    "Medical Center - St. Joseph (HRMC Legal Entity)": "Mosaic Life Care at St. Joseph",
    "Medical Center - Maryville": "Mosaic Medical Center - Maryville",
    "Medical Center - Albany": "Mosaic Medical Center - Albany",
})

_sys("Texas Children's", oracle=True, hospitals={
    "Texas Children's Hospital":              ("Houston", "TX", "cms 453304"),
    "Texas Children's Pavilion for Women":    ("Houston", "TX", "address 6651 Main St, Houston"),
    "Texas Children's Hospital West Campus":  ("Houston", "TX", "address 18200 Katy Fwy, Houston"),
    "Texas Children's Hospital The Woodlands": ("The Woodlands", "TX", "address 17580 Interstate 45 S, The Woodlands"),
    "Texas Children's Hospital North Austin": ("Austin", "TX", "cms 453319"),
}, sites={
    # the main campus's hospital towers
    "West Tower": "Texas Children's Hospital",
    "Abercrombie Building": "Texas Children's Hospital",
    "Legacy Tower": "Texas Children's Hospital",
    "TCH - Legacy Tower": "Texas Children's Hospital",
    "Pavilion for Women": "Texas Children's Pavilion for Women",
    "West Campus": "Texas Children's Hospital West Campus",
    "The Woodlands": "Texas Children's Hospital The Woodlands",
    "North Austin Campus": "Texas Children's Hospital North Austin",
})

_sys("McLeod Health", oracle=True, hospitals={
    "McLeod Health Seacoast":         ("Little River", "SC", "address 4000 Hwy 9 E, Little River"),
    "McLeod Health Clarendon":        ("Manning", "SC", "cms 420109"),
    "McLeod Health Carolina Forest":  ("Myrtle Beach", "SC", "address 4555 Carolina Forest Blvd, Myrtle Beach"),
    "McLeod Health Cheraw":           ("Cheraw", "SC", "cms 420107"),
    "McLeod Health Loris":            ("Loris", "SC", "cms 420105"),
    "McLeod Health Dillon":           ("Dillon", "SC", "cms 420005"),
}, sites={
    "McLeod Seacoast Hospital": "McLeod Health Seacoast",
    "Clarendon": "McLeod Health Clarendon",
    "Carolina Forest - ED": "McLeod Health Carolina Forest",
    "McLeod Medical Cheraw": "McLeod Health Cheraw",
    "McLeod Loris Hospital": "McLeod Health Loris",
    "McLeod Medical Center - Dillon": "McLeod Health Dillon",
})

_sys("INTEGRIS Health", oracle=True, hospitals={
    "INTEGRIS Health Baptist Medical Center":   ("Oklahoma City", "OK", "cms 370028"),
    "INTEGRIS Health Southwest Medical Center": ("Oklahoma City", "OK", "cms 370106"),
    "INTEGRIS Jim Thorpe Rehabilitation":       ("Oklahoma City", "OK", "address 4219 S Western Ave, Oklahoma City"),
}, sites={
    "Baptist Medical Center of OK-HCM": "INTEGRIS Health Baptist Medical Center",
    "BMC 1st Floor-HCM": "INTEGRIS Health Baptist Medical Center",
    "Southwest Medical Center-HCM": "INTEGRIS Health Southwest Medical Center",
    "Jim Thorpe Rehab Hospital-HCM": "INTEGRIS Jim Thorpe Rehabilitation",
})

_sys("TriHealth", hq=("Cincinnati", "OH"), oracle=True, hospitals={
    "Bethesda North Hospital":           ("Cincinnati", "OH", "cms 360179"),
    "Good Samaritan Hospital":           ("Cincinnati", "OH", "cms 360134"),
    "Clinton Memorial Hospital":         ("Wilmington", "OH", "cms 360175"),
    "McCullough-Hyde Memorial Hospital": ("Oxford", "OH", "cms 360046"),
}, sites={
    "NORTH HOSPITAL": "Bethesda North Hospital",
    "GS HOSPITAL": "Good Samaritan Hospital",
    "McCullough Hyde": "McCullough-Hyde Memorial Hospital",
})

_sys("Inspira Health Network", hq=("Vineland", "NJ"), oracle=True, hospitals={
    "Inspira Medical Center Mullica Hill": ("Mullica Hill", "NJ", "address 700 Mullica Hill Rd, Mullica Hill"),
    "Inspira Medical Center Vineland":     ("Vineland", "NJ", "cms 310032"),
    "Inspira Medical Center Elmer":        ("Elmer", "NJ", "address 501 W Front St, Elmer"),
}, sites={
    "Medical Center Mullica Hill": "Inspira Medical Center Mullica Hill",
    "Medical Center Vineland": "Inspira Medical Center Vineland",
    "Medical Center Elmer": "Inspira Medical Center Elmer",
})

_sys("Cedars-Sinai", oracle=True, hospitals={
    "Cedars-Sinai Medical Center":           ("Los Angeles", "CA", "cms 050625"),
    "Cedars-Sinai Marina del Rey Hospital":  ("Marina Del Rey", "CA", "cms 050740"),
}, sites={"Main Medical Center": "Cedars-Sinai Medical Center",
          "Marina Del Rey Hospital": "Cedars-Sinai Marina del Rey Hospital"})

_sys("Northwell Health", hq=("New Hyde Park", "NY"), emit=("Northwell Health (CX_1)", "Northwell Health (CX_3)"),
     oracle=True, hospitals={
    "North Shore University Hospital":      ("Manhasset", "NY", "cms 330106"),
    "South Shore University Hospital":      ("Bay Shore", "NY", "cms 330043"),
    "Long Island Jewish Forest Hills":      ("Forest Hills", "NY", "address 102-01 66th Rd, Forest Hills"),
    "Long Island Jewish Valley Stream":     ("Valley Stream", "NY", "address 900 Franklin Ave, Valley Stream"),
    "Long Island Jewish Medical Center":    ("New Hyde Park", "NY", "cms 330195"),
    "Vassar Brothers Medical Center":       ("Poughkeepsie", "NY", "cms 330023"),
    "Syosset Hospital":                     ("Syosset", "NY", "address 221 Jericho Tpke, Syosset"),
}, sites={
    "North Shore University Hospitl": "North Shore University Hospital",
    "Southside Hospital": "South Shore University Hospital",
    "Forest Hills Hospital": "Long Island Jewish Forest Hills",
    "LIJ Valley Stream": "Long Island Jewish Valley Stream",
    "Long Island Jewish Medical Ctr": "Long Island Jewish Medical Center",
    "Vassar Hospital": "Vassar Brothers Medical Center",
})

# Loma Linda: "Campus/Building/Floor"; the campus names the hospital.
_sys("Loma Linda University Health", hq=("Loma Linda", "CA"), oracle=True, hospitals={
    "Loma Linda University Medical Center":             ("Loma Linda", "CA", "cms 050327"),
    "Loma Linda University Medical Center - Murrieta":  ("Murrieta", "CA", "cms 050770"),
    "Loma Linda University Medical Center East Campus": ("Loma Linda", "CA", "address 25333 Barton Rd, Loma Linda"),
    "Loma Linda University Children's Hospital":        ("Loma Linda", "CA", "cms 050778"),
}, sites={
    "Medical-University Campus": "Loma Linda University Medical Center",
    "Dennis and Carol Troesh Medical Campus": "Loma Linda University Medical Center",
    "East Campus": "Loma Linda University Medical Center East Campus",
    "Children's Hospital": "Loma Linda University Children's Hospital",
}, clean=(r"\s*/.*$", ""))

# UW Health: the work location is the building's street address.
_sys("UW Health", hq=("Madison", "WI"), oracle=True, hospitals={
    "UW Health University Hospital":                  ("Madison", "WI", "address 600 Highland Ave, Madison"),
    "UW Health American Family Children's Hospital":  ("Madison", "WI", "address 1675 Highland Ave, Madison"),
    "UW Health East Madison Hospital":                ("Madison", "WI", "address 4602 Eastpark Blvd, Madison"),
    "UW Health SwedishAmerican Hospital":             ("Rockford", "IL", "cms 140228"),
}, sites={
    "600 HIGHLAND AVE": "UW Health University Hospital",
    "1675 HIGHLAND AVE": "UW Health American Family Children's Hospital",
    "4602 EASTPARK BLVD": "UW Health East Madison Hospital",
    "1401 E STATE ST": "UW Health SwedishAmerican Hospital",
}, clean=(r"^(?:Building:\s*)?(\d+ (?:[A-Z]+ )+?(?:AVE|BLVD|ST))\b.*$", r"\1"))

_sys("UCSF Health", oracle=True, hospitals={
    "Saint Francis Memorial Hospital":          ("San Francisco", "CA", "address 900 Hyde St, San Francisco"),
    "St. Mary's Medical Center":                ("San Francisco", "CA", "address 450 Stanyan St, San Francisco"),
    "UCSF Benioff Children's Hospital Oakland": ("Oakland", "CA", "address 747 52nd St, Oakland"),
}, sites={
    "Saint Francis Memorial Hosp": "Saint Francis Memorial Hospital",
    "Saint Marys Medical Center": "St. Mary's Medical Center",
    "Children's Hospital Oakland Re": "UCSF Benioff Children's Hospital Oakland",
    "BENIOFF CHILDREN'S HOSPITAL OAKLAND": "UCSF Benioff Children's Hospital Oakland",
}, clean=(r"\s+-\s+FL\b.*$", ""))

# ═════════════════════════════ FEED CODES ═══════════════════════════════════
# Jackson Health System (Phenom list over Infor): the facility field is an
# internal unit name. Hospitals take their public name; a unit that is not
# one site (corporate, support services, the corrections health service, the
# medical group, clinics) takes the system label. "Women Children" is the
# Holtz hospitals on the Jackson Memorial campus: Holtz Women's when the
# title says so, else Holtz Children's.
_JHS = "Jackson Health System"
# hq Miami: the feed gives every Jackson posting the town "Miami" (517 of 517
# active rows on 2026-10-29, Doral, North Miami Beach and Cutler Bay sites
# included), so a named hospital's own town replaces it.
_sys(_JHS, hq=("Miami", "FL"), hospitals={
    "Jackson Memorial Hospital":                ("Miami", "FL", "cms 100022"),
    "Jackson South Medical Center":             ("Miami", "FL", "address 9333 SW 152nd St, Miami"),
    "Jackson North Medical Center":             ("North Miami Beach", "FL", "address 160 NW 170th St, North Miami Beach"),
    "Jackson West Medical Center":              ("Doral", "FL", "address 2801 NW 79th Ave, Doral"),
    "Holtz Children's Hospital":                ("Miami", "FL", "address 1611 NW 12th Ave, Miami"),
    "Holtz Women's Hospital":                   ("Miami", "FL", "address 1611 NW 12th Ave, Miami"),
    "Christine E. Lynn Rehabilitation Center":  ("Miami", "FL", "address 1611 NW 12th Ave, Miami"),
    "Jackson Behavioral Health Hospital":       ("Miami", "FL", "address 1695 NW 9th Ave, Miami"),
    "Jackson Memorial Perdue Medical Center":   ("Cutler Bay", "FL", "address 19590 Old Cutler Rd, Cutler Bay"),
    "Jackson Memorial Long Term Care Center":   ("Miami", "FL", "address 2500 NW 22nd Ave, Miami"),
}, rename={
    "Jackson Main": "Jackson Memorial Hospital",
    "South": "Jackson South Medical Center",
    "North": "Jackson North Medical Center",
    "Jackson West Medical Ctr": "Jackson West Medical Center",
    "Women Children": ("Holtz Children's Hospital", ((r"\bwomen'?s\b|mother[- ]baby", "Holtz Women's Hospital"),)),
    "Jackson Rehab": "Christine E. Lynn Rehabilitation Center",
    "Behavioral Health": (_JHS, ((r"\bJackson Behavioral Health\b(?! Center)", "Jackson Behavioral Health Hospital"),)),
    "JM PMC": "Jackson Memorial Perdue Medical Center",
    "JM LTC": "Jackson Memorial Long Term Care Center",
    "Corporate": _JHS, "Support Svc": _JHS, "G and A": _JHS, "Design and Construction": _JHS,
    "Other Satellite": _JHS, "Clinics": _JHS, "Urgent Care Ctrs": _JHS, "OCC Health": _JHS,
    "Corrections": _JHS, "Private Practices": _JHS, "ACC CH": _JHS,
})

# NYU Langone: the Findly department names the hospital in the employer's
# own spelling; one clean spelling each (no em dash: "Hospital - Suffolk").
# Owner 2026-10-29 ("go with recs"): "NYU Langone Hospital--Long Island" is
# the employing entity (the former Winthrop), not only the Mineola hospital:
# 178 active rows of it sat in Garden City, Huntington, Lake Success,
# Bethpage, Manhasset, Patchogue and 15 more towns (faculty group practices,
# ambulatory sites, offices: "Medical Assistant - Urology - Bethpage",
# "Practice Manager - Ortho - East Patchogue"). It names the hospital only on a
# posting in Mineola; elsewhere the row shows "NYU Langone Health" with its own
# town (town_only).
_sys("NYU Langone Health", town_only=("NYU Langone Hospital - Long Island",), hospitals={
    "NYU Langone Hospital - Long Island": ("Mineola", "NY", "address 259 First St, Mineola"),
    "NYU Langone Hospital - Suffolk":     ("Patchogue", "NY", "address 101 Hospital Rd, Patchogue"),
    "NYU Langone Hospital - Brooklyn":    ("Brooklyn", "NY", "address 150 55th St, Brooklyn"),
    "NYU Langone Orthopedic Hospital":    ("New York", "NY", "address 301 E 17th St, New York"),
}, rename={
    "NYU Langone Hospital--Long Island": "NYU Langone Hospital - Long Island",
    "NYU Langone Hospital—Long Island": "NYU Langone Hospital - Long Island",
    "NYU Langone Hospital—Suffolk": "NYU Langone Hospital - Suffolk",
    "NYU Langone Hospital--Suffolk": "NYU Langone Hospital - Suffolk",
    "NYU Langone Hospital—Brooklyn": "NYU Langone Hospital - Brooklyn",
})

# Renown Health (HealthcareSource): the facility value is cut at 30
# characters ("Renown Regional Med Cntr"). The physician-services and
# medical-school units are not one site: the system label. The health plan
# ("Hometown Health Plan") and the imaging unit keep their own names.
_RENOWN = "Renown Health"
_sys(_RENOWN, hospitals={
    "Renown Regional Medical Center":      ("Reno", "NV", "cms 290001"),
    "Renown South Meadows Medical Center": ("Reno", "NV", "cms 290049"),
    "Renown Rehabilitation Hospital":      ("Reno", "NV", "address 1495 Mill St, Reno"),
}, rename={
    "Renown Regional Med Cntr": "Renown Regional Medical Center",
    "Renown S Meadows Med Cntr": "Renown South Meadows Medical Center",
    "Renown Rehab Hospital": "Renown Rehabilitation Hospital",
    "Renown Health Physician Servic": _RENOWN,
    "Renown Med Schl Assoc Nor": _RENOWN,
    "Renown Transitional Care Servi": _RENOWN,
})

# Hawaii Pacific Health (Infor): the facility value carries its own town
# ("Straub Medical Center, Honolulu, HI"); the name alone.
_sys("Hawaii Pacific Health", hospitals={
    "Straub Medical Center":                         ("Honolulu", "HI", "cms 120022"),
    "Kapiolani Medical Center for Women & Children": ("Honolulu", "HI", "cms 123300"),
    "Wilcox Medical Center":                         ("Lihue", "HI", "cms 120014"),
    "Pali Momi Medical Center":                      ("Aiea", "HI", "cms 120026"),
    "Kauai Medical Clinic":                          ("Lihue", "HI", "board: the feed value names Lihue, HI"),
}, rename={
    "Straub Medical Center, Honolulu, HI": "Straub Medical Center",
    "Kapiolani Medical Center for Women & Children, Honolulu, HI": "Kapiolani Medical Center for Women & Children",
    "Wilcox Medical Center, Lihue, HI": "Wilcox Medical Center",
    "Pali Momi Medical Center, Aiea, HI": "Pali Momi Medical Center",
    "Kauai Medical Clinic, Lihue, HI": "Kauai Medical Clinic",
})

# Deaconess Health (UKG): the facility value is the system's short name for
# each hospital ("Midtown Hospital", "IL Crossroads Hospital"). Jasper's
# "Memorial Medical Center" is left as it is: the CMS list still names it
# Memorial Hospital and Health Care Center, and its current public name is
# not certain.
_sys("Deaconess Health", hospitals={
    "Deaconess Midtown Hospital":                ("Evansville", "IN", "cms 150082"),
    "Deaconess Gateway Hospital":                ("Newburgh", "IN", "address 4011 Gateway Blvd, Newburgh"),
    "Deaconess Women's Hospital":                ("Newburgh", "IN", "cms 150149"),
    "Deaconess Henderson Hospital":              ("Henderson", "KY", "cms 180056"),
    "Deaconess Gibson Hospital":                 ("Princeton", "IN", "cms 151319"),
    "Deaconess Illinois Medical Center":         ("Marion", "IL", "address 3333 W DeYoung St, Marion"),
    "Deaconess Illinois Crossroads Hospital":    ("Mount Vernon", "IL", "cms 140294"),
    "Deaconess Illinois Union County Hospital":  ("Anna", "IL", "cms 141342"),
    "Red Bud Regional Hospital":                 ("Red Bud", "IL", "cms 141348"),
    "Jennie Stuart Medical Center":              ("Hopkinsville", "KY", "cms 180051"),
    "Baptist Health Deaconess Madisonville":     ("Madisonville", "KY", "cms 180093"),
}, rename={
    "Midtown Hospital": "Deaconess Midtown Hospital",
    "Gateway Hospital": "Deaconess Gateway Hospital",
    "Deaconess Womens Hospital": "Deaconess Women's Hospital",
    "Henderson Hospital": "Deaconess Henderson Hospital",
    "Gibson Hospital": "Deaconess Gibson Hospital",
    "Deaconess IL Med Center": "Deaconess Illinois Medical Center",
    "IL Crossroads Hospital": "Deaconess Illinois Crossroads Hospital",
    "IL Union County Hospital": "Deaconess Illinois Union County Hospital",
    "Red Bud Reg Hospital": "Red Bud Regional Hospital",
    "Jennie Stuart Med Center": "Jennie Stuart Medical Center",
    "Baptist Hlth Madisonville": "Baptist Health Deaconess Madisonville",
})

# CHS (jobs.chs.net cards): every row names its hospital already; the
# corporate and shared-service offices take the system label, the "(hub)"
# tag on two medical groups is dropped. No town is added: those rows keep the
# town the card gives (often none).
_CHS = "Community Health Systems"     # the label the CHS adapter gives a card that names no hospital
_sys(_CHS, db="CHS", hospitals={}, rename={
    "CHS Corporate": _CHS,
    "CHS Shared Business Operations, LLC": _CHS,
    "Shared Services Center - Nashville": _CHS,
    "HIM Central Services": _CHS,
    "Merit Health Medical Group (hub)": "Merit Health Medical Group",
    "Tennova Medical Group (hub)": "Tennova Medical Group",
})


# ═════════════════════════════ WIRING ═══════════════════════════════════════
def _key(s) -> str:
    return F.fkey(s)


ORACLE_SYSTEMS = frozenset(name for s, t in SYSTEMS.items() if t["oracle"] for name in (s, *t["emit"]))

_RENAMES: dict = {}     # (emit system or db label) -> {fkey(code): (default, ((rx, name), ...))}
_TOWN_ONLY: dict = {}   # (emit system or db label) -> (system label, {hospital: (town, state)})
_SPECS: dict = {}       # emit system -> facility_names.Spec


def _build() -> None:
    for system, t in SYSTEMS.items():
        hospitals = {n: (c, st) for n, (c, st, _src) in t["hospitals"].items()}
        aliases = {}
        for raw, target in t["sites"].items():
            aliases[raw] = target
        labels = {system, t["db"], *t["emit"]}
        spec = F.Spec(system, t["db"], labels, t["hq"], hospitals, aliases,
                      prefixes=t["prefixes"], rules=t["rules"]) if hospitals else None
        if spec is not None:
            F.register(spec, emit_names=t["emit"])
            _SPECS[system] = spec
        if t["rename"]:
            ren = {}
            for raw, v in t["rename"].items():
                default, by_title = (v, ()) if isinstance(v, str) else v
                for name in (default, *(n for _, n in by_title)):
                    if name not in labels and name not in t["hospitals"] and t["hospitals"]:
                        raise ValueError(f"{system}: rename {raw!r} names unknown hospital {name!r}")
                ren[_key(raw)] = (default, tuple((re.compile(rx, re.I), n) for rx, n in by_title))
            for name in labels:
                _RENAMES[name] = ren
        if t["town_only"]:
            places = {n: t["hospitals"][n][:2] for n in t["town_only"]}
            for name in labels:
                _TOWN_ONLY[name] = (system, places)


# 2026-10-29 (facility scale phase 3): the work-location spellings of the
# Lifepoint, Providence and Baptist Memorial sites these tables did not list
# yet (facilities_hospital_systems_a.ORACLE_EXTRA), added before the Specs
# are built so scrape_oracle names them from the list payload every night.
# All or nothing, and never fatal: a table that fails its checks leaves these
# tables as they were and the run goes on.
import facilities_hospital_systems_a as _hsa   # noqa: E402

try:
    _hsa.extend_oracle(SYSTEMS)
except Exception as _e:      # pragma: no cover - the tests hold the tables whole
    import logging as _logging
    _logging.getLogger("scraper.facility").warning(f"Oracle tables: phase 3 additions not installed ({_e})")

# 2026-10-29 (facility scale phase 4 B): Northwell's and Parkview's hospitals
# whose work location these tables did not list yet
# (facilities_systems_b.ORACLE_EXTRA). Same rules: before _build, all or
# nothing, never fatal.
try:
    import facilities_systems_b as _fsb   # noqa: E402
    _fsb.extend_oracle(SYSTEMS)
except Exception as _e:      # pragma: no cover - the tests hold the tables whole
    import logging as _logging
    _logging.getLogger("scraper.facility").warning(f"Oracle tables: phase 4 B additions not installed ({_e})")
_build()


def entries(system) -> dict:
    """{feed spelling or code: (clean public name, town, state, source of the
    town)} for one system: the shared shape every family's tables take."""
    t = SYSTEMS.get(system)
    if t is None:
        return {}
    out = {}
    for raw, target in t["sites"].items():
        if isinstance(target, dict):
            for town, name in target.items():
                out[f"{raw} @ {town}"] = (name, *t["hospitals"][name])
        else:
            out[raw] = (target, *t["hospitals"][target])
    for raw, v in t["rename"].items():
        default = v if isinstance(v, str) else v[0]
        out[raw] = (default, *t["hospitals"].get(default, ("", "", "system label")))
    return out


def oracle_location(system: str, name: str) -> str:
    """An Oracle work location as the system's table spells it: the tenant's
    clean rule (Loma Linda "East Campus/East Campus Hosp/1flr" -> "East
    Campus"), or the name unchanged."""
    t = _table(system)
    if t is None or not t["clean"]:
        return name
    rx, repl = t["clean"]
    out = rx.sub(repl, name or "").strip()
    return out or name


def _table(system):
    t = SYSTEMS.get(system)
    if t is None:
        t = next((tt for tt in SYSTEMS.values() if system in tt["emit"]), None)
    return t


def oracle_site_town(system: str, j: dict, name: str):
    """(town, state) for an Oracle row named `name` by its work location, when
    that work location's own address (TownOrCity, Region2) is the hospital's
    town in the table; None otherwise. The requisition's PrimaryLocation is a
    recruiting geography and can name a neighbouring town (NorthBay: a
    NorthBay Medical Center posting, work location 1200 B Gale Wilson Blvd,
    Fairfield, listed at "Vacaville, CA"); the work location is the site.
    Nothing is taken when the work location gives no town or another one."""
    t = _table(system)
    if t is None or not t["oracle"] or name not in t["hospitals"]:
        return None
    town, st, _src = t["hospitals"][name]
    wl = j.get("workLocation", j.get("WorkLocation"))
    items = wl.get("items") if isinstance(wl, dict) else wl
    for w in items if isinstance(items, list) else []:
        if not isinstance(w, dict) or not str(w.get("LocationName") or "").strip():
            continue
        wt = str(w.get("TownOrCity") or "").strip()
        ws = str(w.get("Region2") or "").strip().upper()
        if wt and ws == st and _key(wt) == _key(town):
            return town, st
        return None
    return None


def rename(job) -> str:
    """A feed code on one Job turned into the hospital's clean name or the
    system label. Returns the new name ("" when nothing changed)."""
    ren = _RENAMES.get(getattr(job, "hospital_system", "") or "")
    if not ren:
        return ""
    cur = F.clean_text(getattr(job, "hospital_name", ""))
    hit = ren.get(_key(cur)) if cur else None
    if hit is None:
        return ""
    default, by_title = hit
    name = default
    title = getattr(job, "title", "") or ""
    for rx, n in by_title:
        if rx.search(title):
            name = n
            break
    name = _town_only(job, name)
    if name == cur:
        return ""
    job.hospital_name = name
    return name


def _town_only(job, name) -> str:
    """`name`, or the system label when `name` is a town_only hospital and the
    posting is not in that hospital's own town (no town counts as elsewhere:
    a town is never guessed)."""
    hit = _TOWN_ONLY.get(getattr(job, "hospital_system", "") or "")
    if not hit or name not in hit[1]:
        return name
    label, places = hit
    town, st = places[name]
    jc = F.clean_text(getattr(job, "city", ""))
    js = F.clean_text(getattr(job, "state", "")).upper()
    if jc and _key(jc) == _key(town) and js in ("", st):
        return name
    return label


def rename_jobs(jobs) -> dict:
    """rename() over a run's rows. Returns {system: rows renamed}."""
    out: dict = {}
    for j in jobs:
        if rename(j):
            s = j.hospital_system
            out[s] = out.get(s, 0) + 1
    return out
