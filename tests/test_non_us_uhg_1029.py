"""Overseas UnitedHealth Group rows with no state (owner 2026-10-29: "yes" to
taking them off). Its TalentBrew board writes the town alone: "Makati City",
"Dublin" (Optum Ireland), "United Kingdom". Rows read from the DATA project
2026-10-29."""
import scraper

UHG = "https://careers.unitedhealthgroup.com/job/{}/x/34088/1"


def test_overseas_towns_with_no_state_are_dropped():
    for city in ("Makati City", "City Of Muntinglupa", "Cebu City", "Davao City",
                 "Letterkenny", "Hong Kong Sar"):
        assert scraper.non_us_reason(city, city, "", UHG.format(city.lower())), city


def test_a_country_or_uk_nation_as_the_only_place():
    for city in ("United Kingdom", "England", "India"):
        assert scraper.non_us_reason(city, city, "", UHG.format(city.lower())) == "lone_country", city


def test_shared_town_names_need_the_country_in_the_posting():
    body = "Optum is a global organisation... This role is based in our Dublin, Ireland office."
    assert scraper.non_us_reason("Dublin", "Dublin", "", UHG.format("dublin"), body) == "body_country"
    assert scraper.non_us_reason("Vancouver", "Vancouver", "", UHG.format("vancouver"),
                                 "Principal Quality Engineer, Vancouver, British Columbia") == "body_country"
    # no country in the posting, or a US state on the row: kept
    assert scraper.non_us_reason("Dublin", "Dublin", "", UHG.format("dublin"), "Join our team in Dublin.") is None
    assert scraper.non_us_reason("Dublin, OH", "Dublin", "OH", UHG.format("dublin"), body) is None
    assert scraper.non_us_reason("London, KY", "London", "KY", "", "London, England trained") is None


def test_us_rows_with_no_state_are_kept():
    for city in ("San Juan", "United States", "Richmond", "Middletown", "Auburn"):
        assert scraper.non_us_reason(city, city, "", UHG.format(city.lower()), "Ireland") is None, city
