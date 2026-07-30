# Canonical specialty classifier — the single source of truth, shared by the
# scraper and the backfill dry-run.
#
# TWO FIXES over the old scraper logic:
#
# 1. PROFESSION BEFORE SETTING. The old SPECIALTY_MAP was ordered so that
#    setting/population buckets (Med/Surg via "acute care", Pediatrics, Home
#    Health) were tested BEFORE profession buckets (PT, OT, Respiratory, Lab).
#    First match wins, so "Physical Therapist - Inpatient - Acute Care" was
#    filed under Med / Surg and "Physical Therapist (Pediatrics)" under
#    Pediatrics. A PT working in peds is a PT. Professions are now tested first.
#
# 2. ALWAYS CANONICALISE. The scraper only classified when the ATS supplied no
#    category, so raw ATS strings were stored verbatim — 5 spellings of
#    "Advanced Practice", 7 of "Administrative", plus "Rehabilitation Services",
#    "Physical Therapist Assistant" and hundreds more. Now: classify the title
#    first; fall back to mapping the ATS string; else leave null.
#
# Also splits the old "Physical / Occupational Therapy" bucket into Physical
# Therapy, Occupational Therapy and Speech Language Pathology — three distinct
# professions that were sharing one filter (and SLP would otherwise be orphaned
# by the split).

import re

# ── Tier 1: PROFESSIONS ────────────────────────────────────────────────────
# Unambiguous role words. Checked first, in this order.
PROFESSION_MAP = {
    "CRNA / Anesthesia": ["crna", "certified registered nurse anesthetist",
                          "nurse anesthetist", "anesthesiolog", "anesthesia tech"],
    "Nurse Practitioner / PA": [
        "nurse practitioner", "physician assistant", "physician's assistant",
        "advanced practice", "advanced practitioner", "aprn", "acnp", " fnp",
        "agacnp", "pmhnp", " np ", " np-", "-np ", " pa-c", " pa ", " pa-",
    ],
    "Physical Therapy": [
        "physical therap", "physiotherap", " dpt ", "pt assistant", "pta ",
        " pt aide", "physical therapist",
    ],
    "Occupational Therapy": [
        "occupational therap", " cota", "ot assistant", " ota ", "occupational therapist",
        " otr", "hand therapist",
    ],
    # "speech lang" catches the abbreviated "Speech Lang Pathologist" forms,
    # which otherwise fell through to Laboratory on the bare "patholog" stem.
    "Speech Language Pathology": [
        "speech language", "speech-language", "speech lang", "speech patholog",
        "speech therap", "speech clinician", " slp ", "slp-", "audiolog",
    ],
    "Respiratory Therapy": [
        "respiratory therap", "respiratory care", " rrt", " crt ", "pulmonary function",
        "respiratory therapist",
    ],
    "Pharmacy": ["pharmacist", "pharmacy tech", "pharmacy assistant", "pharmacy ",
                 " pharm d", "pharmd"],
    "Laboratory": [
        "laboratory", "lab tech", "lab scientist", "lab assistant", "medical technologist",
        "phlebotom", "histolog", "cytolog", "microbiolog", "blood bank", "patholog",
        "specimen",
    ],
    "Radiology / Imaging": [
        "radiolog", "x-ray", "xray", " mri", "magnetic resonance", " ct tech", "ct scan",
        "computed tomography", "ultrasound", "sonograph", "mammograph", "nuclear medicine",
        "fluoroscop", "interventional radiology", "imaging", "echo tech", "echocardiograph",
        "rad tech", "dosimetrist", "radiation therap",
    ],
    "Surgical Tech": ["surgical technolog", "surgical tech", "scrub tech", " cst ",
                      "sterile processing", "central sterile", "sterile tech"],
    "EMS / Paramedic": ["paramedic", " emt", "emergency medical tech", " ems ",
                        "ambulance", "flight medic"],
    # -IST forms only. "cardiolog"/"neurolog" also match "Cardiology
    # Sonographer" and "Pediatric Neurology Social Worker", which are not
    # physician roles; and "family medicine" / "internal medicine" are
    # department names that appear on NP, PA and RN postings just as often
    # ("Family Medicine Nurse Practitioner"). Requiring the practitioner noun
    # keeps this bucket to actual physicians. (2026-07-30 audit.)
    "Physician": [
        "physician", " md ", "m.d.", " do ", "hospitalist", "intensivist",
        "neonatologist", "cardiologist", "neurologist", "oncologist", "surgeon",
        "psychiatrist", "pulmonologist", "gastroenterologist", "nephrologist",
        "endocrinologist", "rheumatologist", "urologist", "ophthalmologist",
        "dermatologist", "physiatrist", "podiatrist", "resident physician",
    ],
    "Dietary / Nutrition": ["dietit", "dietary", "nutrition"],
    "Social Work / Case Management": [
        "social work", " lcsw", " msw ", "case manager", "case management",
        "care coordinator", "utilization review", "utilization management",
        "discharge planner",
    ],
}

# ── Tier 2: NURSING SETTINGS / POPULATIONS ─────────────────────────────────
# Only reached when no profession matched, so these stay nursing-flavoured.
SETTING_MAP = {
    "ICU / Critical Care": ["icu", "intensive care", "critical care", "micu", "sicu",
                            "cvicu", "neuro icu", "picu", "nicu", "ccu", "coronary care",
                            "trauma icu", "burn icu", "stepdown", "step-down"],
    "Emergency / Trauma": ["emergency department", "emergency room", "emergency care",
                           "emergency medicine", " ed rn", " er rn", "trauma nurse",
                           "trauma rn", " er nurse", "emergency nurse", "triage",
                           "trauma"],
    "Labor & Delivery": ["labor and delivery", "labor & delivery", "l&d", "ldrp",
                         "obstetric", " ob nurse", " ob rn", "mother baby", "antepartum",
                         "postpartum", "maternal", "perinatal", "birth center",
                         "women and infant", "women & infant", "midwife", "lactation"],
    "Operating Room / Surgery": ["operating room", " or rn", " or nurse", "perioperative",
                                 "surgical services", "surgery rn", "circulator",
                                 "scrub nurse", "pacu", "post anesthesia", "pre-op",
                                 "preoperative", "post-op", "ambulatory surgery",
                                 "endoscopy"],
    "Cardiac / Cardiovascular": ["cardiac", "cardiovascular", "cath lab",
                                 "catheterization", "cardiothoracic", "electrophysiology",
                                 " ep lab", "telemetry", "heart failure", "cardiac rehab"],
    "Oncology": ["oncolog", "cancer", "chemo", "hematolog", "infusion"],
    "Pediatrics": ["pediatric", " peds", " pedi ", "newborn", "children", "child life"],
    "Behavioral Health / Psych": ["behavioral health", "behavioral medicine", "psychiatric",
                                  "psych ", "mental health", "addiction", "substance abuse",
                                  "detox", "counselor", "chemical dependency"],
    "Home Health": ["home health", "home care", "visiting nurse", "hospice", "palliative"],
    "Wound Care / Dialysis": ["wound care", "ostomy", "dialysis", "hemodialysis", "renal",
                              "nephrology tech"],
    "Med / Surg": ["med surg", "med-surg", "medsurg", "medical surgical",
                   "medical-surgical", "acute care", " imc ", "progressive care", " pcu "],
    "Travel Nursing": ["travel nurse", "travel rn", "travel assignment", "travel contract",
                       "13-week", "13 week"],
    "Float Pool / General RN": [
        "float pool", "float rn", "staff nurse", "staff rn", "registered nurse", " rn ",
        "rn -", "rn,", "clinical nurse", "nurse resident", "nurse extern", "nurse intern",
        "charge nurse", "nurse manager", "nursing supervisor", "nursing assistant",
        "nurse aide", " cna ", "licensed practical nurse", " lpn", " lvn",
        "licensed vocational nurse", "patient care tech", " pct ", "medical assistant",
        "telemetry tech", "monitor tech", "nursing",
    ],
}

# ── Tier 3: NON-CLINICAL ───────────────────────────────────────────────────
NONCLINICAL_MAP = {
    "Healthcare Administration": [
        "director", "administrator", "chief ", " vp ", "vice president", "manager",
        "supervisor", "coordinator", "analyst", "compliance", "revenue cycle", "coding",
        "billing", "health information", "medical records", "human resources",
        "accounts payable", "accounts receivable", "accounting", "finance", "payroll",
        "recruiter", "marketing", "project ", "administrative", "executive",
    ],
    "Support Staff": [
        "patient transport", "unit secretary", "unit clerk", "patient registrar",
        "patient access", "admitting", "scheduling", "scheduler", "front desk",
        "receptionist", "food service", "housekeeping", "environmental services",
        " evs ", "security", "groundskeeper", "maintenance", "supply chain", "driver",
        "chaplain", "office assistant", "registrar", "greeter", "valet", "courier",
        "laundry", "cook", "server", "porter", "custodian", "engineer", "technician i",
    ],
}

TIERS = [PROFESSION_MAP, SETTING_MAP, NONCLINICAL_MAP]

# ── Raw ATS category -> canonical, used only when the title says nothing ────
# Keys are lowercased and stripped of punctuation. Deliberately conservative:
# genuinely broad buckets ("Allied Health", "Rehabilitation Services") map to
# None so the title decides rather than us guessing.
ATS_ALIASES = {
    "physical therapist": "Physical Therapy",
    "physical therapist assistant": "Physical Therapy",
    "physical therapy": "Physical Therapy",
    "physical therapy tech aide": "Physical Therapy",
    "occupational therapist": "Occupational Therapy",
    "occupational therapy": "Occupational Therapy",
    "speech language pathologist": "Speech Language Pathology",
    "speech therapy": "Speech Language Pathology",
    "respiratory therapy": "Respiratory Therapy",
    "respiratory therapist": "Respiratory Therapy",
    "advanced practice": "Nurse Practitioner / PA",
    "advanced practice provider": "Nurse Practitioner / PA",
    "advanced practice providers": "Nurse Practitioner / PA",
    "advanced practice clinician": "Nurse Practitioner / PA",
    "advanced practitioners": "Nurse Practitioner / PA",
    "advanced practice nursing": "Nurse Practitioner / PA",
    "nurse practitioner": "Nurse Practitioner / PA",
    "physician assistant": "Nurse Practitioner / PA",
    "pharmacy": "Pharmacy",
    "laboratory": "Laboratory",
    "laboratory services": "Laboratory",
    "imaging": "Radiology / Imaging",
    "imaging services": "Radiology / Imaging",
    "radiology": "Radiology / Imaging",
    "diagnostic imaging": "Radiology / Imaging",
    "surgical services": "Operating Room / Surgery",
    "perioperative": "Operating Room / Surgery",
    "emergency services": "Emergency / Trauma",
    "emergency": "Emergency / Trauma",
    "critical care": "ICU / Critical Care",
    "behavioral health": "Behavioral Health / Psych",
    "behavioral social work services": "Behavioral Health / Psych",
    "mental health": "Behavioral Health / Psych",
    "home health": "Home Health",
    "hospice": "Home Health",
    "oncology": "Oncology",
    "cardiology": "Cardiac / Cardiovascular",
    "cardiovascular": "Cardiac / Cardiovascular",
    "pediatrics": "Pediatrics",
    "womens health": "Labor & Delivery",
    "dietary": "Dietary / Nutrition",
    "nutrition services": "Dietary / Nutrition",
    "food and nutrition services": "Dietary / Nutrition",
    "social work": "Social Work / Case Management",
    "case management": "Social Work / Case Management",
    "care management": "Social Work / Case Management",
    "physician": "Physician",
    "physicians": "Physician",
    "nursing": "Float Pool / General RN",
    "nursing support": "Float Pool / General RN",
    "patient care": "Float Pool / General RN",
    "administrative": "Healthcare Administration",
    "administrative clerical": "Healthcare Administration",
    "administrative clerical support": "Healthcare Administration",
    "administrative support": "Healthcare Administration",
    "administrative executive services": "Healthcare Administration",
    "administration and support services": "Healthcare Administration",
    "administration careers": "Healthcare Administration",
    "administration": "Healthcare Administration",
    "accounting finance": "Healthcare Administration",
    "finance": "Healthcare Administration",
    "information technology": "Healthcare Administration",
    "human resources": "Healthcare Administration",
    "environmental services": "Support Staff",
    "food services": "Support Staff",
    "facilities": "Support Staff",
    "security": "Support Staff",
    "support services": "Support Staff",

    # ── Travel-agency category strings ────────────────────────────────────
    # travel_jobs carries agency taxonomies (588 distinct values), which are
    # far more granular than hospital ATS categories. These are the ones that
    # actually occur at volume; measured against 20k live rows 2026-07-30.
    "pt outpatient": "Physical Therapy",
    "pt inpatient rehab": "Physical Therapy",
    "pt snf": "Physical Therapy",
    "pt home health": "Physical Therapy",
    "ot inpatient rehab": "Occupational Therapy",
    "ot outpatient": "Occupational Therapy",
    "ot snf": "Occupational Therapy",
    "cvor": "Operating Room / Surgery",
    "cvor technologist": "Operating Room / Surgery",
    "or circulate": "Operating Room / Surgery",
    "or scrub": "Operating Room / Surgery",
    "first assist": "Operating Room / Surgery",
    "perfusionist": "Operating Room / Surgery",
    "intermediate care": "Med / Surg",
    "transitional care unit": "Med / Surg",
    "long term care": "Med / Surg",
    "post partum": "Labor & Delivery",
    "postpartum": "Labor & Delivery",
    "medical physicist": "Radiology / Imaging",
    "physicist": "Radiology / Imaging",
    "eeg technologist": "Radiology / Imaging",
    "eeg tech": "Radiology / Imaging",
    "polysomnographer sleep tech": "Radiology / Imaging",
    "intraoperative neuromonitoring technologist": "Radiology / Imaging",
    "orthopedic technologist": "Surgical Tech",
    "new graduate": "Float Pool / General RN",
    "float": "Float Pool / General RN",
    "vaccination": "Float Pool / General RN",
    "trauma": "Emergency / Trauma",
    "rehabilitation": None,   # ambiguous PT/OT/SLP — let the title decide
}

_PUNCT = re.compile(r"[^a-z0-9 ]+")


def _norm_ats(s):
    if not s:
        return ""
    return re.sub(r"\s+", " ", _PUNCT.sub(" ", str(s).lower())).strip()


def classify_title(title):
    """Canonical specialty from a job title, or None.

    Two different rules, because the two kinds of bucket behave differently
    (2026-07-30 accuracy audit).

    PROFESSIONS — earliest keyword wins. A title leads with the role and
    qualifies it afterwards, so the left-most match is the actual job. Plain
    map-order mis-filed every title naming two professions:
      "Occupational Therapist - Physical Therapy"  -> Physical Therapy  (wrong)
      "Speech Pathologist - Physical Therapy"      -> Physical Therapy  (wrong)
      "Dietitian - Pediatric Gastroenterology"     -> Physician         (wrong)
      "Surgical Technician - Radiology"            -> Radiology         (wrong)

    SETTINGS / NON-CLINICAL — map order wins. Here the leading words are
    boilerplate ("Registered Nurse - ICU" starts with the credential, not the
    unit), and the generic catch-alls (Float Pool / General RN, Support Staff)
    are deliberately last so specific units are tested first. Position-matching
    these would send every "Registered Nurse - X" to the float pool.

    Tier precedence always holds: profession beats setting beats non-clinical.
    """
    if not title:
        return None
    t = f" {str(title).lower()} "
    for tier in TIERS:
        if tier is PROFESSION_MAP:
            best = None  # (position, map_order, specialty)
            for order, (specialty, keywords) in enumerate(tier.items()):
                pos = min((t.find(kw) for kw in keywords if kw in t), default=-1)
                if pos >= 0 and (best is None or (pos, order) < (best[0], best[1])):
                    best = (pos, order, specialty)
            if best:
                return best[2]
        else:
            for specialty, keywords in tier.items():
                for kw in keywords:
                    if kw in t:
                        return specialty
    return None


# "Travel Nursing" is a contract type, not a clinical specialty, and every
# travel title begins "Travel Nurse - ...". Treating it as a normal title match
# would bury the agency's real specialty ("CVOR", "PT Outpatient"), so it only
# wins when nothing more specific is available.
WEAK_MATCHES = {"Travel Nursing"}


def canonical_specialty(title, raw_specialty=None, keep_raw=True):
    """Title first, then the ATS/agency category, else the raw value unchanged.

    keep_raw exists so this can NEVER make a row worse. travel_jobs already has
    a specialty on 100% of rows (agency taxonomies, 588 distinct values);
    nulling the ones we can't map would trade a messy-but-present value for
    nothing. Unmapped values simply stay as they are — they're absent from the
    filter dropdowns either way, exactly as today, just far fewer of them.
    """
    hit = classify_title(title)
    if hit and hit not in WEAK_MATCHES:
        return hit
    key = _norm_ats(raw_specialty)
    if key in ATS_ALIASES:
        alias = ATS_ALIASES[key]
        if alias:
            return alias
        # Explicit None = "known but ambiguous"; fall through to raw/None.
    if hit:                      # weak match, but better than nothing
        return hit
    return (raw_specialty or None) if keep_raw else None


ALL_SPECIALTIES = sorted(
    set(PROFESSION_MAP) | set(SETTING_MAP) | set(NONCLINICAL_MAP)
)

if __name__ == "__main__":
    for t in ["Physical Therapist - Inpatient - Acute Care",
              "Physical Therapist (Pediatrics)",
              "PRN Home Health Physical Therapist",
              "Occupational Therapist - Hand Therapy",
              "Speech Language Pathologist PRN",
              "Rehabilitation Technician - Physical Therapy",
              "Certified Nursing Assistant - Physical Therapy Rehab",
              "Registered Nurse - ICU",
              "Nurse Practitioner Urgent Care",
              "Chief Financial Officer"]:
        print(f"  {classify_title(t)!s:32} <- {t}")
