"""2026-10-13: review findings on 7b140b5 (Workday externalPath locations).

Segments are the live examples the review named plus made-up ones of the
same shape. No network; CMS towns come from set_cms_lookup like a real run.
"""
import os
import re

import pytest

import scraper as S


class J:
    def __init__(self, city, state, loc=""):
        self.city, self.state, self.location = city, state, loc


def seg(s, spellings=None):
    return S._wd_segment_location(s, spellings)


def _apply(rows, default=None, site_default=False, path_state=None):
    jobs = [J(c, s, loc) for c, s, loc, _ in rows]
    ext = {id(j): e for j, (_, _, _, e) in zip(jobs, rows)}
    S._wd_apply_locations(jobs, ext, {}, path_state or {}, default, site_default)
    return [(j.city, j.state) for j in jobs]


# ── finding 2: site, street and system names are not towns ────────────────

def test_site_and_street_names_give_the_state_alone():
    assert seg("Sacred-Heart-Lavonia-GA") == ("", "GA")
    assert seg("External-Name-Muskegon-MI") == ("", "MI")
    assert seg("Wheat-Ridge-Wadsworth-CO") == ("", "CO")
    assert seg("Princeton-Jct-Hightstown-NJ") == ("", "NJ")
    assert seg("Kansas-City-Mulberry-Drive-MO") == ("", "MO")
    assert seg("Wheat-Ridge-Wadsworth------CO") == ("", "CO")      # chunk-before-state form too


def test_system_names_give_the_state_alone():
    assert seg("Ascension-Seton-TX") == ("", "TX")
    assert seg("Baptist-Health-KY") == ("", "KY")
    assert seg("Bon-Secours-VA") == ("", "VA")
    assert seg("Sutter-Roseville-CA") == ("", "CA")


def test_stoplist_words_are_never_a_town():
    for word in ("Health", "Hospital", "Medical", "Center", "Clinic", "Care", "Community",
                 "University", "Campus", "Drive", "Street", "Avenue", "Blvd", "Road", "Lane",
                 "Way", "Suite", "Building", "Name", "External", "Internal", "General", "Area",
                 "Region", "System", "Services"):
        assert seg(f"{word}-TX") == ("", "TX"), word
        assert not S._wd_town_ok(word), word


def test_a_bare_prefix_is_never_a_town():
    for word in ("Fort", "Ft", "Port", "Mount", "Mt", "Lake", "West", "East", "North", "South",
                 "New", "Saint", "St", "Main", "Mary"):
        assert not S._wd_town_ok(word), word
        assert seg(f"US-TX-{word}") == ("", "TX"), word


def test_a_state_name_after_a_prefix_is_a_place_not_a_state():
    assert seg("Fort-Washington") == ("", "")
    assert seg("Port-Washington") == ("", "")
    assert seg("Mount-Washington") == ("", "")
    assert seg("Mary-Washington") == ("", "")
    assert seg("Lake-Delaware") == ("", "")
    assert seg("Main-OR") == ("", "")                 # the main operating room, not Oregon
    # a direction still names the state; a state-named place with a code is a two-word town
    assert seg("East-Texas") == ("", "TX")
    assert seg("West-Virginia") == ("", "WV")
    assert seg("Main-St-TX") == ("", "TX")
    assert seg("Fort-Washington-PA") == ("", "PA")


def test_known_two_word_towns_still_resolve():
    assert seg("Kansas-City-MO") == ("Kansas City", "MO")
    assert seg("Wheat-Ridge-CO") == ("Wheat Ridge", "CO")
    assert seg("Fort-Worth-TX") == ("Fort Worth", "TX")
    assert seg("Virginia-Beach-VA") == ("Virginia Beach", "VA")
    assert seg("Saint-Lukes-Hospital------4401-Wornall-Rd------Kansas-City------MO") == ("Kansas City", "MO")
    # one plain word is still a town
    assert seg("Lavonia-GA") == ("Lavonia", "GA")
    assert seg("Muskegon-MI") == ("Muskegon", "MI")


def test_cms_towns_count_as_known_and_override_the_stoplist():
    assert seg("Medical-Lake-WA") == ("", "WA")
    assert seg("Rockville-Centre-NY") == ("", "NY")
    assert seg("Federal-Way-WA") == ("", "WA")
    S.set_cms_lookup([
        {"hospital_name": "Eastern State Hospital", "city": "Medical Lake", "state": "WA"},
        {"hospital_name": "Mercy Hospital", "city": "Rockville Centre", "state": "NY"},
        {"hospital_name": "Mercy Hospital", "city": "Federal Way", "state": "WA"},   # shared name: still a town
    ])
    assert seg("Medical-Lake-WA") == ("Medical Lake", "WA")
    assert seg("Rockville-Centre-NY") == ("Rockville Centre", "NY")
    assert seg("Federal-Way-WA") == ("Federal Way", "WA")
    assert seg("Medical-Lake-OR") == ("", "OR")       # a known town only in its own state
    S.set_cms_lookup([])
    assert seg("Medical-Lake-WA") == ("", "WA")


def test_tenant_spelling_still_wins_for_a_multi_word_town():
    sp = {(S._wd_city_key("Sacred Heart Lavonia"), "GA"): "Sacred Heart Lavonia",
          (S._wd_city_key("Camp Hill"), "PA"): "Camp Hill"}
    assert seg("Camp-Hill-PA", sp) == ("Camp Hill", "PA")
    assert seg("Sutter-Roseville-CA", {(S._wd_city_key("Roseville"), "CA"): "Roseville"}) == ("Roseville", "CA")


def test_parsed_site_names_do_not_become_tenant_spellings():
    # A row whose locationsText parse left a street as the city must not spell
    # the path town of its siblings.
    rows = [("Kansas City Mulberry Drive", "MO", "Kansas City Mulberry Drive, MO", "/job/KC-MO/A_R1"),
            ("", "", "2 Locations", "/job/Kansas-City-Mulberry-Drive-MO/B_R2")]
    assert _apply(rows)[1] == ("", "MO")
    assert not S._wd_parsed_town_ok("Kansas City Mulberry Drive", "MO")
    assert not S._wd_parsed_town_ok("NY)", "NY")
    assert not S._wd_parsed_town_ok("Main", "OR")
    for town, st in (("St. Louis", "MO"), ("Española", "NM"), ("Winston-Salem", "NC"),
                     ("Coeur d'Alene", "ID"), ("Camp Hill", "PA")):
        assert S._wd_parsed_town_ok(town, st), town


# ── finding 3: abbreviations, Mount, Mc / De casing ───────────────────────

def test_abbreviated_towns_need_a_spelling():
    assert seg("St-Marys-GA") == ("", "GA")
    assert seg("N-Little-Rock-AR") == ("", "AR")
    assert seg("Ste-Marie-IL") == ("", "IL")
    assert seg("Mt-Airy-NC") == ("", "NC")
    assert seg("Ft-Thomas-KY") == ("", "KY")
    assert seg("W-Monroe-LA") == ("", "LA")
    sp = {(S._wd_city_key("St. Marys"), "GA"): "St. Marys",
          (S._wd_city_key("North Little Rock"), "AR"): "North Little Rock"}
    assert seg("St-Marys-GA", sp) == ("St. Marys", "GA")
    # the punctuation table is a spelling too
    assert seg("St-Louis-MO") == ("St. Louis", "MO")
    assert seg("Ft-Worth-TX") == ("Fort Worth", "TX")


def test_mount_vernon_table():
    for st in ("WA", "IL", "TX", "NY", "OH"):
        assert seg(f"Mt-Vernon-{st}") == ("Mount Vernon", st)
        assert seg(f"Mount-Vernon-{st}") == ("Mount Vernon", st)
    assert seg("Mt-Vernon-KY") == ("", "KY")
    # the abbreviated towns of the 2026-10-12 live sample
    assert seg("Mt-Pleasant-Iowa") == ("Mount Pleasant", "IA")
    assert seg("Mt-Pleasant---MI") == ("Mount Pleasant", "MI")
    assert seg("St-Johns---MI") == ("St. Johns", "MI")
    assert seg("St-Helena-CA") == ("St. Helena", "CA")


def test_mc_and_de_title_case():
    assert seg("MCALLEN-TX") == ("McAllen", "TX")
    assert seg("Mcallen-TX") == ("McAllen", "TX")
    assert seg("MCKINNEY-TX") == ("McKinney", "TX")
    assert seg("DEKALB-IL") == ("DeKalb", "IL")
    assert seg("DELAND-FL") == ("DeLand", "FL")
    assert seg("DENVER-CO") == ("Denver", "CO")      # not every De town
    assert S._wd_title("O'FALLON") == "O'Fallon"


# ── finding 4: a non-ASCII city already counts as a city ──────────────────

def test_non_ascii_existing_city_is_kept():
    rows = [("Española", "NM", "Española, NM", "/job/Santa-Fe-NM/A_R1"),
            ("Cañon City", "CO", "Cañon City, CO", "/job/Pueblo-CO/B_R2")]
    assert S._wd_path_fill([J(c, s, l) for c, s, l, _ in rows], {}, {}) == {}
    assert _apply(rows) == [("Española", "NM"), ("Cañon City", "CO")]


def test_non_ascii_tenant_spelling_restores_the_accent():
    rows = [("Española", "NM", "Española, NM", "/job/Espanola-NM/A_R1"),
            ("", "", "2 Locations", "/job/Espanola-NM---Clinic/B_R2")]
    assert _apply(rows)[1] == ("Española", "NM")
    assert S._wd_city_key("Española") == S._wd_city_key("Espanola")


def test_state_only_path_keeps_a_non_ascii_city():
    rows = [("Española", "", "Española", "/job/Remote---New-Mexico/A_R1")]
    assert _apply(rows) == [("Española", "NM")]


def test_path_candidates_stay_ascii():
    assert not S._wd_town_ok("Española")
    assert S._wd_path_city(["Española"], "NM", None) == ""


# ── finding 1: sql/118 keeps the old city only in the same state ──────────

SQL_DIR = os.environ.get("WAYPOINT_SQL_DIR",
                         r"C:/Users/19403/OneDrive/claud_outputinput/redesign_2026-09-09/sql")
_OLD = "if coalesce(btrim(new.city), '') = '' and coalesce(btrim(old.city), '') <> '' then new.city := old.city; end if;"
_NEW = ("if coalesce(btrim(new.city), '') = '' and coalesce(btrim(old.city), '') <> '' "
        "and upper(coalesce(btrim(new.state), '')) in ('', upper(coalesce(btrim(old.state), ''))) "
        "then new.city := old.city; end if;")


def _sql(name):
    p = os.path.join(SQL_DIR, name)
    if not os.path.exists(p):
        pytest.skip(f"{name} not on this machine")
    with open(p, encoding="ascii") as f:
        return f.read()


def _fn(text):
    first = text.split("\n")[0]
    prefix = "set lock_timeout = '3s'; "
    return first[len(prefix):] if first.startswith(prefix) else first


def test_sql_118_changes_only_the_city_clause_of_113():
    base = _fn(_sql("113_preserve_req_id.sql"))
    fwd = _sql("118_preserve_city_same_state.sql")
    rb = _sql("118r_preserve_city_same_state_rollback.sql")
    assert base.count(_OLD) == 1
    assert _fn(fwd) == base.replace(_OLD, _NEW)
    assert _fn(rb) == base
    for text in (fwd, rb):
        assert not any(line.lstrip().startswith("--") for line in text.splitlines())
        assert text.splitlines()[0].startswith("set lock_timeout = '3s'; CREATE OR REPLACE FUNCTION public.preserve_scraped_enrichment()")
    # the state clause still runs first, so a blank incoming state is restored before the city test
    f = _fn(fwd)
    assert f.index("new.state := old.state") < f.index(_NEW)


def _emulate_city_clause(new_city, new_state, old_city, old_state):
    """The 118 city clause, after the function's own state clause."""
    def ok_code(s):
        return bool(re.fullmatch(r"[A-Za-z]{2}", (s or "").strip()))
    if not ok_code(new_state) and ok_code(old_state):
        new_state = old_state
    if (new_city or "").strip() == "" and (old_city or "").strip() != "" \
            and (new_state or "").strip().upper() in ("", (old_state or "").strip().upper()):
        new_city = old_city
    return new_city, new_state


def test_city_clause_semantics():
    assert _emulate_city_clause("", "TX", "Phoenix", "AZ") == ("", "TX")         # no "Phoenix, TX"
    assert _emulate_city_clause("", "AZ", "Phoenix", "AZ") == ("Phoenix", "AZ")
    assert _emulate_city_clause("", "", "Phoenix", "AZ") == ("Phoenix", "AZ")
    assert _emulate_city_clause("", "az", "Phoenix", "AZ") == ("Phoenix", "az")
    assert _emulate_city_clause("Tempe", "AZ", "Phoenix", "AZ") == ("Tempe", "AZ")
