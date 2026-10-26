# 2026-10-25 coverage stage 3: the v2 fingerprinter's offline rules (site
# verification, careers links, platform + tenant extraction, tenant scoring).
# No network: every check runs on literal HTML / JSON.
import fingerprint_hospitals as F

KIMBLE = {"ccn": "451306", "name": "KIMBLE HOSPITAL", "city": "Junction", "state": "TX", "zip": "76849",
          "address": "349 REID ROAD", "phone": "(325) 446-3321", "type": "Critical Access Hospitals"}
BIG_BEND = {"ccn": "451378", "name": "BIG BEND REGIONAL MEDICAL CENTER", "city": "Alpine", "state": "TX",
            "zip": "79830", "address": "2600 HIGHWAY 118 NORTH", "phone": "(432) 837-3447"}


# -- websites ----------------------------------------------------------------------
def test_phone_on_the_page_verifies_the_site():
    html = "<html><title>Kimble Hospital</title><body>Call us (325) 446-3321</body></html>" + " " * 300
    ev = F.verify_site(KIMBLE, "https://kimblehospital.org/", html=html)
    assert ev["phone"] and ev["ok"]
    assert ev["name_title"] == 1.0


def test_street_number_and_name_verify_the_site():
    html = "<title>Home</title><p>349 Reid Road, Junction, Texas</p>" + " " * 300
    ev = F.verify_site(KIMBLE, "https://example.org/", html=html)
    assert ev["street"] and ev["ok"] and not ev["phone"]


def test_town_alone_does_not_verify_a_site():
    # a county or chamber page that mentions the town but not the hospital
    html = "<title>Junction Texas Chamber</title><p>Welcome to Junction, TX 76849</p>" + " " * 300
    ev = F.verify_site(KIMBLE, "https://junctiontexas.net/", html=html)
    assert not ev["ok"]


def test_directory_pages_are_flagged():
    phones = " ".join(f"(325) 446-{3300 + i}" for i in range(8))
    html = (f"<title>Kimble Hospital - Junction TX - Reviews and Phone Number</title><p>Kimble Hospital "
            f"(325) 446-3321 {phones}</p>") + " " * 300
    ev = F.verify_site(KIMBLE, "https://hospitalsineachstate.com/x", html=html)
    assert ev["directory"]
    assert F.is_aggregator("https://www.hospitalsineachstate.com/hospital/kimble")


def test_street_parts():
    assert F.street_parts("349 REID ROAD") == ("349", "reid")
    assert F.street_parts("2600 HIGHWAY 118 NORTH") == ("2600", "118")
    assert F.street_parts("PO BOX 12") == ("", "")
    assert F.street_parts("100 N FIRST STREET") == ("100", "1st")


def test_osm_phone_match_beats_name_match():
    osm = [{"name": "Kimble Hospital", "city": "Junction", "website": "https://wrong.example/", "phone": ""},
           {"name": "KH", "city": "", "website": "https://kimblehospital.org/", "phone": "+1 325-446-3321"}]
    c = F.osm_candidates(KIMBLE, osm)
    assert c[0] == ("https://kimblehospital.org/", "osm-phone")
    assert ("https://wrong.example/", "osm-name") in c


def test_generic_names_need_the_town_for_an_osm_match():
    h = {"name": "MEMORIAL HOSPITAL", "city": "Seminole", "state": "TX", "zip": "79360", "phone": ""}
    osm = [{"name": "Memorial Hospital", "city": "Gonzales", "website": "https://gonzales.example/", "phone": ""}]
    assert F.osm_candidates(h, osm) == []


# -- careers links ------------------------------------------------------------------
def test_career_links_rank_and_skip_noise():
    html = """
      <a href="/volunteer">Volunteer</a>
      <a href="/pay-my-bill">Pay My Bill</a>
      <a href="/about/careers">Careers</a>
      <a href="https://www.facebook.com/kimble">Join our team on Facebook</a>
      <a href="https://recruiting.paylocity.com/recruiting/jobs/All/1bcf0c7d-43e3-4c68-89fc-d02935d1b57e/Kimble-Hospital">Apply</a>
    """
    links = F.career_links(html, "https://kimblehospital.org/")
    urls = [u for _, u, _ in links]
    assert urls[0].startswith("https://recruiting.paylocity.com/")
    assert "https://kimblehospital.org/about/careers" in urls
    assert not any("volunteer" in u or "pay-my-bill" in u or "facebook" in u for u in urls)


# -- platforms and tenants ------------------------------------------------------------
def _one(text):
    found = F.detect_ats(text)
    assert found, text
    return found[0][0], found[0][1]


def test_tenant_keys_are_extracted():
    assert _one("https://recruiting.paylocity.com/Recruiting/Jobs/All/1bcf0c7d-43e3-4c68-89fc-d02935d1b57e/Kimble-Hospital") == \
        ("Paylocity", {"guid": "1bcf0c7d-43e3-4c68-89fc-d02935d1b57e", "slug": "Kimble-Hospital"})
    assert _one("https://www.paycomonline.net/v4/ats/web.php/jobs?clientkey=4f9268b125c765e56921bd62380f6fda") == \
        ("Paycom", {"clientkey": "4F9268B125C765E56921BD62380F6FDA"})
    assert _one("https://recruiting2.ultipro.com/QHC1000QHCS/JobBoard/b3394170-e409-475c-a2dd-95d53697602f/?q=") == \
        ("UKG Pro", {"base": "https://recruiting2.ultipro.com/QHC1000QHCS", "guid": "b3394170-e409-475c-a2dd-95d53697602f"})
    assert _one("https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=76d82eed-ed91-46db-82b7-9e829fb2467b&ccId=19000101_000001&lang=en_US") == \
        ("ADP", {"cid": "76d82eed-ed91-46db-82b7-9e829fb2467b", "ccId": "19000101_000001"})
    assert _one("https://elitekingwood.applicantpro.com/jobs/") == ("ApplicantPro", {"sub": "elitekingwood"})
    assert _one("https://pm.healthcaresource.com/CS/medicalcenterhealth/#/search") == \
        ("HealthcareSource", {"slug": "medicalcenterhealth"})
    assert _one("https://umchealthsystem.wd1.myworkdayjobs.com/en-US/External") == \
        ("Workday", {"tenant": "umchealthsystem", "wd": "1", "site": "External"})
    assert _one("https://careers-obmc.icims.com/jobs/intro") == ("iCIMS", {"domain": "careers-obmc.icims.com"})
    assert _one("https://prd01-hcm01.prd.mykronos.com/ta/6070232.careers?CareersSearch") == \
        ("UKG Ready", {"sub": "prd01-hcm01.prd", "company": "6070232"})
    assert _one("https://phf.tbe.taleo.net/phf02/ats/careers/v2/jobSearch?act=redirectCwsV2&cws=38&org=BHST") == \
        ("Taleo BE", {"base": "https://phf.tbe.taleo.net/phf02", "org": "BHST", "cws": "38"})
    assert _one("https://recruitingbypaycor.com/career/CareerHome.action?clientId=8a7883c65f99acc5015fc5bb4d5073ef") == \
        ("Paycor", {"clientId": "8a7883c65f99acc5015fc5bb4d5073ef"})
    assert _one("https://apply.workable.com/yoakum-community/") == ("Workable", {"account": "yoakum-community"})


def test_generic_hosts_name_no_tenant():
    # v1 reported these as tenants; with no company key there is nothing to configure
    assert F.detect_ats("https://prd01-hcm01.prd.mykronos.com/") == []
    assert F.detect_ats("https://www.paylocity.com/") == []
    assert F.detect_ats("https://www.icims.com/") == []
    assert F.detect_ats("https://www.applicantpro.com/") == []


def test_unsupported_platforms_are_named_for_the_adapter_tally():
    assert _one("https://kch.bamboohr.com/careers")[0] == "BambooHR"
    assert _one("https://stmarys.applytojob.com/apply")[0] == "JazzHR"
    assert _one("https://jobs.dayforcehcm.com/en-US/ochsner/CANDIDATEPORTAL")[0] == "Dayforce"
    assert _one("https://xyz.isolvedhire.com/jobs/")[0] == "iSolved"
    assert _one("https://mcleodhealth.applicantstack.com/x/openings")[0] == "ApplicantStack"
    for p in ("BambooHR", "JazzHR", "Dayforce", "iSolved", "ApplicantStack"):
        assert p not in F.SUPPORTED


# -- tenant scoring (the junk filter) --------------------------------------------------------
def test_a_vendor_board_with_postings_elsewhere_is_low():
    # Big Bend's site links R1 RCM's Workday board (revenue-cycle vendor): 412 jobs, none in Alpine
    res = {"count": 412, "locs": ["Hancock, MI", "Ottumwa, IA", "Remote, USA", "Destin, FL"]}
    s = F.score_tenant(BIG_BEND, "Workday", {"tenant": "r1rcm", "wd": "1", "site": "R1RCM"}, res, "careers")
    assert s["confidence"] == "low"


def test_postings_in_town_on_the_careers_page_are_high():
    res = {"count": 12, "locs": ["Alpine, TX"] * 12}
    s = F.score_tenant(BIG_BEND, "UKG Pro", {"base": "x", "guid": "y"}, res, "careers")
    assert s["confidence"] == "high" and s["city_share"] == 1.0


def test_board_named_for_the_hospital_with_no_locations_is_high():
    res = {"count": 8, "locs": [], "company": "Kimble Hospital"}
    s = F.score_tenant(KIMBLE, "Paylocity", {"guid": "g", "slug": "Kimble-Hospital"}, res, "careers")
    assert s["name_match"] and s["confidence"] == "high"


def test_a_search_result_board_is_at_most_medium():
    res = {"count": 8, "locs": ["Junction, TX"] * 8}
    s = F.score_tenant(KIMBLE, "Paylocity", {"guid": "g", "slug": "x"}, res, "search")
    assert s["confidence"] == "medium"


def test_an_empty_board_is_low():
    s = F.score_tenant(KIMBLE, "Paycom", {"clientkey": "K"}, {"count": 0, "locs": []}, "careers")
    assert s["confidence"] == "low"


def test_pick_best_prefers_the_verified_tenant():
    low = {"platform": "Workday", "context": "careers", "res": {"count": 412}, "score": {"confidence": "low"}}
    high = {"platform": "UKG Pro", "context": "careers", "res": {"count": 12}, "score": {"confidence": "high"}}
    assert F.pick_best([low, high]) is high
    assert F.pick_best([]) is None


def test_no_board_classification():
    assert F.classify_none({"careers_page": False}) == "no careers page found"
    assert F.classify_none({"careers_page": True, "pdf_application": True}) == "none (PDF / email application)"
    assert F.classify_none({"careers_page": True, "job_words": 12}) == "custom HTML"
    assert F.classify_none({"careers_page": True, "no_openings_text": True}) == "none (no openings listed)"


# -- page analysis stays linear on malformed pages (the first live run stalled on one) ------------
def test_unclosed_blocks_and_anchors_are_fast():
    import time
    html = ("<script>var a=1;" * 4000) + ("<a href='/x'>" * 4000) + ("<!-- c " * 4000) + "x" * 200_000
    t0 = time.time()
    F.visible_text(html)
    F.anchors(html, "https://example.org/")
    F.detect_ats(html)
    assert time.time() - t0 < 5


def test_visible_text_drops_scripts_and_comments():
    html = "<p>Open <b>RN</b> roles</p><script>var x='<p>hidden</p>';</script><!-- note --><style>p{}</style>"
    assert F.visible_text(html) == "Open RN roles"


def test_white_label_and_portal_urls():
    assert _one("https://secure6.entertimeonline.com/ta/6144973.careers?rnd=QHV&") == \
        ("UKG Ready", {"sub": "secure6.entertimeonline.com", "company": "6144973"})
    assert _one("https://secure7.saashr.com/ta/6200563.careers?CareersSearch") == \
        ("UKG Ready", {"sub": "secure7.saashr.com", "company": "6200563"})
    assert _one("https://www.paycomonline.net/v4/ats/web.php/portal/898BD2386405070392D5E532BA263B62/career-page") == \
        ("Paycom", {"clientkey": "898BD2386405070392D5E532BA263B62"})
    assert _one("https://www.indeed.com/viewjob?jk=ad50d1eac4e2bfd4&from=shareddesktop_copy")[0] == "Indeed only"
    assert _one("https://form.jotform.com/212096069918161")[0] == "Form only"
    assert _one("https://cta.cadienttalent.com/index.jsp?locale=en_US&applicationName=CitizensMedicalCenterReqExt") == \
        ("Cadient", {"app": "CitizensMedicalCenterReqExt"})
    assert _one("https://gpch.catsone.com/careers/")[0] == "CATS"


def test_saint_and_st_are_the_same_town():
    h = {"name": "RIVER'S EDGE HOSPITAL & CLINIC", "city": "St Peter", "state": "MN"}
    s = F.score_tenant(h, "UKG Ready", {"sub": "x", "company": "1"}, {"count": 7, "locs": ["Saint Peter, MN"] * 7}, "careers")
    assert s["in_city"] == 7 and s["confidence"] == "high"


def test_a_name_made_of_the_town_cannot_verify_a_site_by_name():
    # "GREAT FALLS CLINIC HOSPITAL": every name token is the town, so a Great Falls page
    # (benefis.org, reached from a guessed domain) must not pass on ZIP + "name"
    h = {"name": "GREAT FALLS CLINIC HOSPITAL", "city": "Great Falls", "state": "MT", "zip": "59405",
         "address": "3010 15TH AVENUE SOUTH", "phone": "(406) 216-8000"}
    assert F.distinct_tokens(h) == []
    html = "<title>Benefis Health System</title><p>Great Falls, MT 59405 (406) 455-5000</p>" + " " * 300
    ev = F.verify_site(h, "https://www.benefis.org/", html=html)
    assert not ev["ok"]


def test_a_town_word_in_the_tenant_is_not_a_name_match():
    # Access Hospital Dayton's page linked Dayton Children's Workday tenant
    h = {"name": "ACCESS HOSPITAL DAYTON", "city": "Dayton", "state": "OH"}
    s = F.score_tenant(h, "Workday", {"tenant": "daytonchildrens", "wd": "1", "site": "dayton_childrens_career_site"},
                       {"count": 158, "locs": []}, "careers")
    assert not s["name_match"] and s["confidence"] == "low"
