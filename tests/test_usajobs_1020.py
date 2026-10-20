"""USAJOBS coverage (2026-10-20, "that's it for the VA jobs?"). No network,
no database: the search API needs a key we do not hold locally, so the
payloads are built here in the shape of developer.usajobs.gov's
api-reference/get-api-search (SearchResult.SearchResultItems[].
MatchedObjectDescriptor.PositionLocation[]: LocationName, CountryCode,
CountrySubDivisionCode, CityName) and the public code lists
(data.usajobs.gov/api/codelist/agencysubelements, /occupationalseries,
/countrysubdivisions), fetched 2026-10-20.

1. Org codes: Indian Health Service is HE37 and NIH HE38 (the adapter read
   HE38 for IHS and HE06, no such code, for NIH).
"""
import scraper


# ── 1. Org codes ─────────────────────────────────────────────────────────────

def test_usajobs_org_codes_are_the_agency_subelement_codes():
    assert dict(scraper.USAJOBS_ORGS) == {
        "VA Hospitals": "VATA",              # Veterans Health Administration
        "Indian Health Service": "HE37",     # Indian Health Service
        "Military Health System": "DD",      # Department of Defense
        "NIH Clinical Center": "HE38",       # National Institutes of Health
    }
