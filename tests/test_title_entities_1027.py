# 2026-10-27 (title entities): 380 active titles and 2 facility names were
# stored with HTML character references ("RN L&amp;D Nights", "PACU RN
# 3-12&#x27;s", "UGH Women&apos;s Health"), and the cards printed them as
# written. The TalentBrew page parsers (Kaiser, UHG, Maxim, Enhabit), CHS,
# AFC (CareerPlug) and Concentra read titles out of raw HTML without
# decoding; ApplicantPro and ADP send them encoded. normalize_job now decodes
# complete references in title and hospital_name (decode_text_entities).
# No network, no database.
import pytest

import scraper


def _job(title, name="Kaiser Permanente", system="Kaiser Permanente", job_id="1234567",
         description="", ats="TalentBrew"):
    return scraper.Job(
        title=title, hospital_system=system, hospital_name=name, city="Oakland", state="CA",
        location="Oakland, CA", specialty="", job_type="", url=f"https://example.org/job/{job_id}",
        job_id=job_id, posted_date="", description=description, ats_platform=ats)


@pytest.mark.parametrize("raw,want", [
    # the shapes stored on 2026-10-26 (one per reference seen live)
    ("RN L&amp;D Nights", "RN L&D Nights"),
    ("PACU RN 3-12&#x27;s Capitol Hill", "PACU RN 3-12's Capitol Hill"),
    ("Mammo Tech - Women&#039;s Care Specialists", "Mammo Tech - Women's Care Specialists"),
    ("BXMO &#39;Basic X-Ray Machine Operator&#39; - Fulltime &amp; PRN Opportunities",
     "BXMO 'Basic X-Ray Machine Operator' - Fulltime & PRN Opportunities"),
    ("Clinical Manager RN, STAR&#x2B;PLUS - El Paso, TX - Remote",
     "Clinical Manager RN, STAR+PLUS - El Paso, TX - Remote"),
    ("School Nurse &#x2013; Bus Runs - Worcester County| $52&#x2013;$55/hr",
     "School Nurse – Bus Runs - Worcester County| $52–$55/hr"),
    ("UGH Women&apos;s Health", "UGH Women's Health"),
    ("Nanny&rsquo;s", "Nanny’s"),
    # the Instagram landing-page card (Maxim, RN / FL): stored single-encoded
    ("Pediatric Home Care RN | Specialized Trach &amp; Vent Nursing",
     "Pediatric Home Care RN | Specialized Trach & Vent Nursing"),
    # double and triple encoding decode until stable
    ("Trach &amp;amp; Vent", "Trach & Vent"),
    ("Saint Mary&amp;#x27;s Homecare", "Saint Mary's Homecare"),
    ("L&amp;amp;amp;D", "L&D"),
    # spaces a reference carries become plain spaces, and runs collapse
    ("RN&#x202F;-&#x202F;ICU", "RN - ICU"),
    ("Medical Assistant&#x9;- Float", "Medical Assistant - Float"),
    ("Charge RN&nbsp;&nbsp;Nights", "Charge RN Nights"),
])
def test_references_decode(raw, want):
    assert scraper.decode_text_entities(raw) == want


@pytest.mark.parametrize("text", [
    "R&D Program Manager", "AT&T Account Nurse", "Med&Surg RN", "Q&A Specialist",
    "L&D RN", "Labor & Delivery RN", "Fish & Chips; Cook",
    # HCA CareNow bodies say "I&Ds; suturing": not a reference, left alone
    "pediatric and adult care; I&Ds; suturing",
    # a legacy name without its semicolon is text, not a reference
    "Clinic&notes Coordinator", "Lab &copy Tech", "RN &amp Charge",
    # an unknown name keeps its text
    "Unit &foo; Lead", "Nurse &; Aide", "", "Registered Nurse",
])
def test_real_text_is_untouched(text):
    assert scraper.decode_text_entities(text) == text


def test_unchanged_value_is_the_same_object():
    s = "Registered Nurse - Med/Surg & Tele"
    assert scraper.decode_text_entities(s) is s
    assert scraper.decode_text_entities(None) is None
    assert scraper.decode_text_entities(42) == 42


def test_whitespace_of_a_title_without_references_is_kept():
    # only a value that decoded is re-spaced: other titles are byte-identical
    s = "RN  -  ICU "
    assert scraper.decode_text_entities(s) is s


def test_normalize_job_decodes_title_and_facility_name():
    d = scraper.normalize_job(_job("Specialty Unit Staff RN - LABOR &amp; DELIVERY",
                                   name="UGH Women&apos;s Health", system="Union General Health System",
                                   ats="ADP"))
    assert d["title"] == "Specialty Unit Staff RN - LABOR & DELIVERY"
    assert d["hospital_name"] == "UGH Women's Health"
    assert d["job_id"] == "1234567"


def test_decoded_title_classifies_like_its_plain_spelling():
    # "L&amp;D" hid the L&D unit: 56 RN rows sat under Float Pool / General RN
    enc = scraper.normalize_job(_job("RN L&amp;D Nights"))
    plain = scraper.normalize_job(_job("RN L&D Nights"))
    assert enc["specialty"] == plain["specialty"] == "Labor & Delivery"
    assert enc["title"] == plain["title"]
    assert enc["job_type"] == plain["job_type"]
    assert enc["has_signon"] == plain["has_signon"]


def test_normalize_job_keeps_the_first_line_rule():
    d = scraper.normalize_job(_job("Registered Nurse RN - ED&#xa;&#xa;Req ID: 49092"))
    assert d["title"] == "Registered Nurse RN - ED"


def test_a_title_that_would_decode_to_nothing_is_kept():
    d = scraper.normalize_job(_job("&nbsp;"))
    assert d["title"] == "&nbsp;"


def test_rows_without_references_hash_exactly_as_before(monkeypatch):
    # The decode must not move any other row's content hash (no mass rewrite).
    titles = ["Registered Nurse", "R&D Analyst", "RN  -  ICU", "Labor & Delivery RN - Nights",
              "Med&Surg; Tech"]
    body = "Body text " * 40
    with_decode = [scraper.content_hash_for(scraper.normalize_job(_job(t, job_id=str(i), description=body)))
                   for i, t in enumerate(titles)]
    monkeypatch.setattr(scraper, "decode_text_entities", lambda s: s)
    without = [scraper.content_hash_for(scraper.normalize_job(_job(t, job_id=str(i), description=body)))
               for i, t in enumerate(titles)]
    assert with_decode == without


def test_encoded_row_hashes_like_its_plain_spelling():
    body = "Body text " * 40
    a = scraper.content_hash_for(scraper.normalize_job(_job("RN L&amp;D Nights", description=body)))
    b = scraper.content_hash_for(scraper.normalize_job(_job("RN L&D Nights", description=body)))
    assert a == b


# The cause, pinned on card markup in the live shape: the Kaiser / UHG card
# regexes read the anchor text raw, so the reference reached the Job; the
# decode in normalize_job is what cleans it.
KAISER_CARD = ('<li><a href="/job/oakland/rn-l-and-d-nights/641/98765432" data-job-id="98765432">'
               '<h2>RN L&amp;D Nights</h2></a></li>')
UHG_CARD = ('<li><a href="/job/spokane/np/34088/12345678" data-job-id="12345678">'
            '<h2>Nurse Practitioner - Spokane, WA &amp; Kootenai County, ID</h2></a></li>')


def test_kaiser_card_title_reaches_normalize_encoded_and_leaves_decoded():
    got = dict(scraper.KAISER_TITLE_NEAR_HREF.findall(KAISER_CARD))
    assert got["98765432"] == "RN L&amp;D Nights"
    d = scraper.normalize_job(_job(got["98765432"], job_id="98765432"))
    assert d["title"] == "RN L&D Nights"
    assert d["job_id"] == "98765432"


def test_uhg_card_title_reaches_normalize_encoded_and_leaves_decoded():
    got = dict(scraper.UHG_TITLE_NEAR_HREF.findall(UHG_CARD))
    assert got["12345678"] == "Nurse Practitioner - Spokane, WA &amp; Kootenai County, ID"
    d = scraper.normalize_job(_job(got["12345678"], system="UnitedHealth Group",
                                   name="UnitedHealth Group", job_id="12345678"))
    assert d["title"] == "Nurse Practitioner - Spokane, WA & Kootenai County, ID"
