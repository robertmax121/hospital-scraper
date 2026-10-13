"""Name-mismatch relabels (2026-10-13, sql/131_name_mismatch_relabels.sql).

Fourteen boards wrote their postings under another employer's name, and two
shared labels mixed unrelated systems. Each board now carries the employer it
really is (evidence: the board's own address, links and posting bodies, read
against the stored rows on 2026-10-13); the stored rows, their
hospital_cms_alias rows and the wage-source rows are moved by sql/131, keyed on
the board code or host. Offline checks only: config keys, the feed map, the
aliases and the pure row builders."""
import scraper

# board -> (config table, key, base url or tenant, the label it used to carry)
RELABELLED = [
    ("UKG_ORGS", "Heritage Valley Health System", "/HER1004HERIT", "Heritage Health"),
    ("UKG_ORGS", "Northwestern Medical Center", "/NOR1080NWMC", "Northwest Medical Center"),
    ("UKG_ORGS", "Augusta Health", "/AUG1000AUG", "Augusta University Health"),
    ("UKG_ORGS", "Colquitt Regional Medical Center", "/COL1042CRME", "Columbia Regional Medical"),
    ("UKG_ORGS", "Alliance Health (NC)", "/ALL1034ABHC", "Alliant Health"),
    ("UKG_ORGS", "Self Regional Healthcare", "/GRE1050GNHP", "Granite Hills Medical"),
    ("UKG_ORGS", "Meadville Medical Center", "/MEA1004MEVM", "Medical Associates"),
    ("UKG_ORGS", "Chesapeake Regional Healthcare", "/CHE1503CHPE", "Cape Regional Health"),
    ("UKG_ORGS", "Southeast Georgia Health System", "/SOU1076SOUG", "South Georgia Health"),
    ("UKG_ORGS", "Independence Health System", "/EXC1005EXCEH", "Excela Health"),
    ("ORACLE_ORGS", "Centra Health", "https://ejid.fa.us6.oraclecloud.com", "Jackson Hospital"),
    ("ORACLE_ORGS", "NorthBay Health", "https://erou.fa.us2.oraclecloud.com", "Flagler Health"),
    ("ORACLE_ORGS", "Good Samaritan (Vincennes)", "https://fa-exgl-saasfaprod1.fa.ocs.oraclecloud.com",
     "Southwest Health"),
]


def _base(table, key):
    v = getattr(scraper, table)[key]
    return v if isinstance(v, str) else v[0]


def test_each_board_carries_its_real_employer():
    for table, key, base, old in RELABELLED:
        got = _base(table, key)
        assert got.endswith(base) or got == base, (key, got)
        assert old not in getattr(scraper, table), old
        # exactly one config points at the board
        bases = [_base(table, k) for k in getattr(scraper, table)]
        assert sum(1 for b in bases if b.endswith(base) or b == base) == 1, base
    assert scraper.HEALTHCARESOURCE_ORGS["Powers Health"] == "comhs"
    assert "Community Memorial" not in scraper.HEALTHCARESOURCE_ORGS
    assert list(scraper.HEALTHCARESOURCE_ORGS.values()).count("comhs") == 1


def test_old_labels_are_gone_from_feeds_and_aliases():
    feeds = scraper.configured_feed_map()
    gone = [old for _, _, _, old in RELABELLED] + ["Community Memorial", "Chapman Global Medical Center"]
    for old in gone:
        assert old not in feeds, old
        assert old not in scraper.HOSPITAL_SYSTEM_ALIASES, old
        assert old not in scraper.HOSPITAL_SYSTEM_ALIASES.values(), old
    # "Southwest Healthcare" is still a real feed: the UHS brand in Temecula CA
    # (UHS_BRANDS), which the Good Samaritan site was being folded into.
    assert scraper.UHS_BRANDS["Southwest Healthcare"] == "southwest-healthcare"
    assert feeds["Southwest Healthcare"] == {"UHS_BRANDS"}
    # every new label is its own canonical name
    for _, key, _, _ in RELABELLED:
        assert scraper._canon_system(key) == key
    for key in ("Powers Health", "KPC Health", "Memorial Health", "Memorial Healthcare System",
                "Saint Luke's Health System"):
        assert scraper._canon_system(key) == key


def test_shared_labels_split_by_feed():
    # Memorial: Springfield IL (Workday wd108), Hollywood FL (Workday wd1) and
    # Marietta OH (Paycom) were three systems under one label.
    assert scraper.WORKDAY_TENANTS["Memorial Health"][0] == "memorialhealth"
    assert scraper.WORKDAY_TENANTS["Memorial Healthcare System"][0] == "memorialhealthcare"
    assert "Memorial Health System" in scraper.PAYCOM_ORGS
    assert scraper._canon_system("Memorial Healthcare System") == "Memorial Healthcare System"
    assert scraper.WD_TENANT_DEFAULT["Memorial Health"] == ("Springfield", "IL")
    assert "Memorial Health System" not in scraper.WD_TENANT_DEFAULT
    assert "Memorial Health System" not in scraper.WD_FACILITY_MAP
    assert scraper.WD_FACILITY_MAP["Memorial Health"]["mmc"][0] == "Springfield Memorial Hospital"
    # St. Luke's: Kansas City (saintlukes.wd1) is not Boise (Avature + iCIMS).
    assert scraper.WORKDAY_TENANTS["Saint Luke's Health System"][0] == "saintlukes"
    assert scraper._canon_system("Saint Luke's Health System") == "Saint Luke's Health System"
    assert scraper._canon_system("St. Luke's Health System (Boise)") == "St. Luke's Health System"


# ── KPC Health: two ADP career centers ─────────────────────────────────────

KPC_SECOND = "a45074b6-b45e-424e-9e71-c45a4d73c34b"


def _req(city, head=None, item="9201"):
    loc = {"address": {"cityName": city, "countrySubdivisionLevel1": {"codeValue": "CA"}} if city else {}}
    if head is not None:
        loc["nameCode"] = {"shortName": f"{head}, {city}, CA, US" if city else head}
    return {"itemID": item, "requisitionTitle": "Registered Nurse", "requisitionLocations": [loc]}


def test_kpc_health_has_both_career_centers():
    centers = scraper._adp_centers(scraper.ADP_ORGS["KPC Health"])
    assert [c[0] for c in centers] == ["1a214979-2739-4245-a1d1-38dc8531018f", KPC_SECOND]
    assert all(c[2] == "CA" for c in centers)
    assert "Chapman Global Medical Center" not in scraper.ADP_ORGS
    assert "chapman global medical center" not in scraper.SYSTEM_LOCATION_DEFAULTS


def test_kpc_facility_comes_from_the_posting_then_the_city():
    cid, cc, st, by_city = scraper._adp_centers(scraper.ADP_ORGS["KPC Health"])[1]
    assert cid == KPC_SECOND and isinstance(by_city, dict)

    def name(city, head=None):
        j = scraper._adp_job(_req(city, head), "KPC Health", cid, cc, st, by_city)
        assert j.hospital_system == "KPC Health" and j.state == "CA"
        return j.hospital_name

    # the posting names its facility: that wins
    assert name("Anaheim", "Anaheim Global Medical Center") == "Anaheim Global Medical Center"
    assert name("Santa Ana", "South Coast Global Medical Center") == "South Coast Global Medical Center"
    # the posting names only its city: that city's KPC hospital
    assert name("Anaheim", "Anaheim") == "Anaheim Global Medical Center"
    assert name("Orange", "Orange") == "Chapman Global Medical Center"
    # Santa Ana has two KPC hospitals (Orange County Global, South Coast
    # Global): no guess, the system name
    assert name("Santa Ana", "Santa Ana") == "KPC Health"
    # no location at all: the system name and the center's state
    j = scraper._adp_job({"itemID": "9", "requisitionTitle": "RN"}, "KPC Health", cid, cc, st, by_city)
    assert (j.hospital_name, j.state, j.city) == ("KPC Health", "CA", "")
    # the URL still carries this center's cid (the detail pass and sql/131 key on it)
    assert f"cid={KPC_SECOND}" in j.url


def test_a_fixed_adp_hospital_name_still_overrides_the_posting():
    # The dict form is new; the string form keeps its old meaning.
    cid, cc, st, fixed = scraper._adp_centers(scraper.ADP_ORGS["Catalina Island Medical Center"])[0]
    j = scraper._adp_job(_req("Avalon", "Some Clinic"), "Catalina Island Medical Center", cid, cc, st, fixed)
    assert j.hospital_name == "Catalina Island Medical Center"


# ── Good Samaritan (Vincennes): rows with no location get a state ─────────

def test_good_samaritan_rows_without_a_location_are_placed_in_vincennes():
    j = scraper.Job(title="Staff Nurse - Surgical Unit", hospital_system="Good Samaritan (Vincennes)",
                    hospital_name="Good Samaritan (Vincennes)", city="", state="", location="",
                    specialty="", job_type="", url="https://fa-exgl-saasfaprod1.fa.ocs.oraclecloud.com/x/1",
                    job_id="1", posted_date="", description="", ats_platform="Oracle HCM")
    d = scraper.normalize_job(j)
    assert (d["city"], d["state"]) == ("Vincennes", "IN")
    # a located row keeps its own town
    j.city, j.state = "Washington", "IN"
    d = scraper.normalize_job(j)
    assert (d["city"], d["state"]) == ("Washington", "IN")


# ── St. Luke's split: the Kansas City town map follows the Kansas City name ─

def _stlukes_row(system, city, state=""):
    return scraper.Job(title="Registered Nurse", hospital_system=system, hospital_name=system,
                       city=city, state=state, location="", specialty="", job_type="",
                       url="https://saintlukes.wd1.myworkdayjobs.com/x/1", job_id="1",
                       posted_date="", description="", ats_platform="Workday")


def test_kansas_city_towns_key_on_the_kansas_city_name():
    assert scraper.SYSTEM_CITY_STATE["saint luke's health system"]["overland park"] == "KS"
    assert "st. luke's health system" not in scraper.SYSTEM_CITY_STATE
    assert scraper.SYSTEM_LOCATION_DEFAULTS["saint luke's health system"] == ("Kansas City", "MO")
    assert scraper.SYSTEM_LOCATION_DEFAULTS["st. luke's health system"] == ("Boise", "ID")
    # a bare Kansas town on the Kansas City feed is KS, not the MO default
    d = scraper.normalize_job(_stlukes_row("Saint Luke's Health System", "Overland Park"))
    assert (d["city"], d["state"]) == ("Overland Park", "KS")
    # a row with no location falls back to Kansas City MO
    d = scraper.normalize_job(_stlukes_row("Saint Luke's Health System", ""))
    assert (d["city"], d["state"]) == ("Kansas City", "MO")
    # the Boise name never lands in Kansas City
    d = scraper.normalize_job(_stlukes_row("St. Luke's Health System", ""))
    assert (d["city"], d["state"]) == ("Boise", "ID")
