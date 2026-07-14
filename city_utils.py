# city_utils.py
# Server-side city sanitization for the scraper write path.
#
# ATS feeds routinely dump non-city junk into the location/city field:
#   - "2 Locations" / "Multiple Locations"  (multi-site Workday reqs)
#   - "Remote" / "Work From Home" / "Nationwide"
#   - street addresses            ("Atrium Health Mercy - 2001 Vail Ave")
#   - pipe-delimited facility rows ("Saint Luke's Hospital | 4401 Wornall Rd | Kansas City | MO")
#   - newline facility blocks      ("Location\nMedical Center Alpena\nAlpena")
#   - bare facility names          ("Akron General Medical Center")
#
# clean_city() first tries to EXTRACT the real city out of the known embedded
# patterns (pipe, newline, "Facility - City"), then validates the result. If it
# still isn't a plausible city name, it returns "" so normalize_job()'s
# FACILITY_LOCATION_MAP / SYSTEM_LOCATION_DEFAULTS fallback can fill it from the
# facility/system name instead.
#
# Mirrors the front-end guard in waypoint-jobs/lib/clean-city.js, but adds
# extraction (the front-end only needs to hide junk from dropdowns; here we want
# to recover the real value before it's stored). Validated against all distinct
# live city values (see _validate_city_clean.py).

import re

# 2-char USPS codes (+ territories) — used to locate the state segment when
# extracting a city from pipe/comma-delimited facility strings.
STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR", "GU", "VI",
}

# Facility / non-place phrases that never occur in a real US city name. Matched
# as whole words/phrases so real cities survive: "Center" -> Centerville,
# "Health" -> Healdsburg, "Mansfield Center" (a real CT town) are all kept
# because none of them contain one of these multi-word phrases (or the
# standalone "hospital"/"clinic" tokens, which no US city name contains).
_FACILITY_RE = re.compile(
    r"\b("
    r"medical center|medical ctr|regional medical|memorial hospital|"
    r"care center|care ctr|health center|health system|health services|"
    r"hospital|clinic|outpatient|surgery center|cancer center|"
    r"main campus|campus|all locations|flexible work|corporate"
    r")\b",
    re.IGNORECASE,
)
_LOCATIONS_RE = re.compile(r"\b(multiple|various|two|three|four|several)\s+locations?\b", re.IGNORECASE)
_LOCATION_PREFIX_RE = re.compile(r"^locations?\b", re.IGNORECASE)
_REMOTE_RE = re.compile(r"\b(remote|work\s*from\s*home|virtual|nationwide|statewide|telework|telecommute)\b", re.IGNORECASE)


def is_plausible_city(c: str) -> bool:
    """True only if `c` looks like a real US city name (no extraction)."""
    if not c:
        return False
    c = c.strip()
    if len(c) < 2 or len(c) > 40:
        return False
    if c.upper() in STATE_CODES:                # bare "MO"/"KY" is a state, not a city
        return False
    if any(ch.isdigit() for ch in c):          # street addresses, "N Locations"
        return False
    if "|" in c or "\n" in c or "\r" in c or "\t" in c:
        return False
    if _LOCATION_PREFIX_RE.match(c):            # "Location...", "Locations..."
        return False
    if _LOCATIONS_RE.search(c):                 # "Multiple Locations"
        return False
    if _REMOTE_RE.search(c):                    # "Remote", "Work From Home"
        return False
    if _FACILITY_RE.search(c):                  # "... Medical Center", "... Hospital"
        return False
    return True


def _pick_from_segments(segs) -> str:
    """From delimited facility segments, return the real city or ''.
    Facility strings are ordered Facility [| Addr] | City [| ST], so the city
    is either the segment right before a trailing state code, or the FINAL
    segment. We never fall back to an earlier segment: those hold the facility
    name, and returning one just writes fresh junk ("Atrium Health Cabarrus -
    920 Church St N" must NOT yield "Atrium Health Cabarrus")."""
    segs = [s.strip() for s in segs if s and s.strip()]
    if not segs:
        return ""
    # Segment immediately before a trailing 2-char state code (most reliable).
    for i, s in enumerate(segs):
        if s.upper() in STATE_CODES and i > 0 and is_plausible_city(segs[i - 1]):
            return segs[i - 1]
    # Otherwise only the final segment can be the city.
    return segs[-1] if is_plausible_city(segs[-1]) else ""


def clean_city(raw: str) -> str:
    """Return a plausible city name, extracting it from embedded facility/address
    strings where possible; '' when no usable city can be recovered."""
    if not raw:
        return ""
    c = str(raw).strip()

    # Pipe-delimited: "Facility | Addr | City | ST"
    if "|" in c:
        return _pick_from_segments(c.split("|"))

    # Newline facility blocks: "Location\nMedical Center Alpena\nAlpena"
    # (the real city is typically the last line).
    if "\n" in c or "\r" in c:
        return _pick_from_segments(re.split(r"[\n\r]+", c))

    # Already clean — the common case, checked before the dash split so cities
    # that legitimately contain " - " (none known, but be safe) aren't mangled.
    if is_plausible_city(c):
        return c

    # "Facility - City" / "City - Facility": recover the plausible side.
    if " - " in c:
        return _pick_from_segments(c.split(" - "))

    return ""
