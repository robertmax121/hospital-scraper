"""Posted pay the parser missed (Bounce QA, 2026-10-25): job pages ChatGPT
visitors bounced from said "Pay not listed" although the posting printed a
range. Three spellings, each from real active rows (read-only SQL,
2026-10-25); the extractor is replayed on "title\\nbody" as normalize_job
passes it."""
from scraper import extract_posted_wage


def test_comma_less_annual_range_ecu():
    # ECU Health 38570510 (Phenom body)
    body = "Executive Director of Pharmacy Operations\nIntegrated Delivery Network or Academic Medical Center Experience Preferred.\nPay Range\n$173076.80 - $252263.44\n\n#LI-JW1\nECU Health"
    assert extract_posted_wage(body) == (173076.8, 252263.44, "year")


def test_comma_less_annual_range_kaiser():
    # Kaiser Permanente 38362868 (TalentBrew body)
    body = "Senior Manager, Investigative Science\nDepartment: Oakland Reg - 1950 Franklin\nPay Range: $206300 - $266970 / year\nTravel: No"
    assert extract_posted_wage(body) == (206300.0, 266970.0, "year")


def test_bare_range_heading_minimum_maximum_bon_secours():
    # Bon Secours Mercy Health 20960917 (Workday body)
    body = "Rehabilitation Technician (Rehab Tech)\nprofessionals.\n\nRange\n\nMinimum :$16.32\n\nMaximum: $23.15\n\nBon Secours is an equal opportunity employer."
    assert extract_posted_wage(body) == (16.32, 23.15, "hour")


def test_between_and_range():
    assert extract_posted_wage("Medical Assistant\nThe pay range for this position is between $16.49 and $20.21 per hour.") == (16.49, 20.21, "hour")


def test_comma_figures_and_existing_forms_unchanged():
    assert extract_posted_wage("RN\n$19.55 - $34.25 /Hr") == (19.55, 34.25, "hour")
    assert extract_posted_wage("RN\n$117K-$132,600") == (117000.0, 132600.0, "year")
    assert extract_posted_wage("RN\nPay Range Minimum:\n$45,288.00 annual\nPay Range Maximum:\n$53,400.00 annual") == (45288.0, 53400.0, "year")


def test_comma_less_bonus_and_implausible_pairs_stay_out():
    assert extract_posted_wage("RN\n$1500 - $2000 sign-on bonus") is None
    assert extract_posted_wage("RN\nSign-on bonus $5000 - $10000 for experienced RNs. Pay $32.00 - $45.00/hr") == (32.0, 45.0, "hour")
    assert extract_posted_wage("RN\nBonus between $5000 and $10000 for nights") is None
    # an eight-digit run is never a figure
    assert extract_posted_wage("RN\nReq $12345678 - $23456789") is None
