"""2026-10-12: externalPath location fallback for every Workday tenant.

Segments are real ones from the 2026-10-12 live sample (150 tenants)."""
import scraper as S


class J:
    def __init__(self, city, state, loc=""):
        self.city, self.state, self.location = city, state, loc


def seg(s, spellings=None):
    return S._wd_segment_location(s, spellings)


def test_city_state_forms():
    assert seg("Hayward-CA") == ("Hayward", "CA")
    assert seg("Englewood-Colorado") == ("Englewood", "CO")
    assert seg("US-FL-Daytona-Beach") == ("Daytona Beach", "FL")
    assert seg("Charleston-West-Virginia") == ("Charleston", "WV")
    assert seg("PARK-RIDGE-IL") == ("", "IL")          # 2026-10-13: two words, no known town
    S.set_cms_lookup([{"hospital_name": "Lutheran General", "city": "Park Ridge", "state": "IL"}])
    assert seg("PARK-RIDGE-IL") == ("Park Ridge", "IL")


def test_state_only_segment_never_splits_the_state_name():
    assert seg("West-Virginia") == ("", "WV")
    assert seg("South-Carolina") == ("", "SC")
    assert seg("Washington") == ("", "")


def test_remote_and_home_words_give_no_city():
    assert seg("Remote---South-Carolina") == ("", "SC")
    assert seg("Remote-Texas") == ("", "TX")
    assert seg("PA-Working-at-Home---Pennsylvania") == ("", "PA")
    assert seg("Virtual-Arizona") == ("", "AZ")
    assert seg("USA---MN---Remote") == ("", "MN")
    assert seg("WORK-AT-HOME---OHIO") == ("", "OH")


def test_triple_dash_chunks():
    assert seg("Raynham-MA---General-Area") == ("Raynham", "MA")        # Elara
    assert seg("Granby---Granby-CT") == ("Granby", "CT")                # no doubled city
    assert seg("Saint-Lukes-Hospital------4401-Wornall-Rd------Kansas-City------MO") == ("Kansas City", "MO")
    assert seg("Meridian---Central-Mississippi-Region---Mississippi") == ("", "MS")


def test_junk_city_gives_state_only_or_nothing():
    assert seg("Carolinas-Medical-Center---1000-Blythe-Blvd-Charlotte-NC") == ("", "NC")
    assert seg("Richmond-University-Medical-Center-Staten-Island-NY") == ("", "NY")
    assert seg("Decatur-319-W-Delaware") == ("", "")                     # a street, not the state
    assert seg("Primary-Care---6th--Maine") == ("", "")
    assert seg("NE-Torresdale-Main-Hosp") == ("", "")
    assert seg("NESF---95-Woodland-Street") == ("", "")
    assert seg("SE-Health-Hospital") == ("", "")


def test_tenant_spelling_restores_punctuation_and_tail_town():
    sp = {(S._wd_city_key("St. Louis"), "MO"): "St. Louis",
          (S._wd_city_key("Charlotte"), "NC"): "Charlotte",
          (S._wd_city_key("Bismarck"), "ND"): "Bismarck"}
    assert seg("St-Louis-MO", sp) == ("St. Louis", "MO")
    assert seg("Saint-Louis-MO", sp) == ("St. Louis", "MO")
    assert seg("Carolinas-Medical-Center---1000-Blythe-Blvd-Charlotte-NC", sp) == ("Charlotte", "NC")
    assert seg("ND-Bismarck", sp) == ("Bismarck", "ND")


def test_punct_table_when_tenant_has_no_spelling():
    assert seg("Winston-Salem-NC") == ("Winston-Salem", "NC")
    assert seg("Wilkes-Barre-PA") == ("Wilkes-Barre", "PA")
    assert seg("St-Louis-MO") == ("St. Louis", "MO")
    assert seg("Ft-Worth-TX") == ("Fort Worth", "TX")


def test_n_locations_is_never_a_city():
    assert S._WD_N_LOCATIONS_RE.match("2 Locations")
    assert S._WD_N_LOCATIONS_RE.match("51 Locations")
    assert not S._WD_N_LOCATIONS_RE.match("Locations Of Care")


def _apply(rows, default=None, site_default=False, path_state=None):
    jobs = [J(c, s, loc) for c, s, loc, _ in rows]
    ext = {id(j): e for j, (_, _, _, e) in zip(jobs, rows)}
    S._wd_apply_locations(jobs, ext, {}, path_state or {}, default, site_default)
    return [(j.city, j.state) for j in jobs]


def test_apply_multi_site_row_takes_path_before_default():
    out = _apply([("", "", "2 Locations", "/job/Novato-CA/RN_R1")], default=("Greenbrae", "CA"))
    assert out == [("Novato", "CA")]


def test_apply_sibling_row_spelling_wins():
    rows = [("St. Louis", "MO", "St. Louis, MO", "/job/St-Louis-MO/A_R1"),
            ("", "", "3 Locations", "/job/St-Louis-MO/B_R2")]
    assert _apply(rows)[1] == ("St. Louis", "MO")


def test_apply_state_known_city_blank_takes_town_in_same_state_only():
    rows = [("", "MA", "Raynham, MA - General Area", "/job/Raynham-MA---General-Area/HHA_R1"),
            ("", "NM", "Lincoln County", "/job/Hayward-CA/X_R2")]
    assert _apply(rows) == [("Raynham", "MA"), ("", "NM")]


def test_apply_facet_state_wins_over_path():
    out = _apply([("", "", "", "/job/Hayward-CA/X_R1")], path_state={"/job/Hayward-CA/X_R1": "OR"})
    assert out[0][1] == "OR"


def test_apply_remote_row_gets_state_not_hq_town():
    out = _apply([("51 Locations", "", "51 Locations", "/job/PA-Working-at-Home---Pennsylvania/X_R1")],
                 default=("Pittsburgh", "PA"))
    assert out == [("", "PA")]


def test_apply_site_default_city_only_on_listed_tenants():
    rows = [("SE Health Hospital", "", "SE Health Hospital", "/job/SE-Health-Hospital/RN_R1")]
    assert _apply(rows, default=("Dothan", "AL"), site_default=True) == [("Dothan", "AL")]
    rows = [("Prattville Baptist Hospital", "", "Prattville Baptist Hospital", "/job/Prattville-Baptist-Hospital/RN_R1")]
    assert _apply(rows, default=("Montgomery", "AL")) == [("Prattville Baptist Hospital", "AL")]
    assert "Southeast Health (Dothan)" in S.WD_SITE_DEFAULT_CITY
