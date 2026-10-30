"""Intermountain Health's sites by name on its Workday rows (2026-10-30, facility scale phase 7).

The owner, 2026-10-30, on a Billings posting filed under the label
("Patient Service Representative - Non Oncology Infusion Center", Workday
site /job/Billings-Downtown-Clinic/): "innermountain needs billings clinic
added". His two standing rules (2026-10-29): (1) cards and job pages show the
HOSPITAL or site ("Apply to <site>"); the system name and logo stay on the job
page and the system hub (hospital_system is never touched here: the upsert
key, the hub, the filters, the logo); (2) named sites (hospitals, named
clinics and centers) show their own name; generic offices, home-based and
remote jobs and multi-site pools show the system name with the town; a site
or a town is never guessed.

Intermountain (imh.wd108, emit name "Intermountain Health (IMH)", stored under
hospital_system "Intermountain Healthcare" through HOSPITAL_SYSTEM_ALIASES):
WD_FACILITY_MAP names its hospitals from the list's location descriptor, and
facilities_workday's table (phase 2) names the hospitals the board renamed.
Every other site fell through to the label: 418 of 1,473 active rows on
2026-10-30 (UT 202, CO 119, MT 45, NV 41, ID 4 ...), clinics, centers,
offices, home services and Life Flight bases alike. Phases 2 to 6 counted
completeness by hospital_name = hospital_system, so a label that differs from
the system name ("Intermountain Health (IMH)" under "Intermountain
Healthcare") was never counted.

Every Intermountain posting names its site twice (facilities_workday): the
list's locationsText (the descriptor, "Billings Downtown Clinic"; a posting
at several sites lists them, "Intermountain Health St Vincent Regional
Hospital; Billings Downtown Clinic") and the first segment of its
externalPath (the primary site). Its bulletFields are [req, Work State, Work
City]. The stored body repeats them: "Location:" then the site (several,
comma separated, for a posting at several), then "Work City:" and "Work
State:" (the town of the Workday location record).

This module adds the rest of the board's sites to the facilities_workday
table (one place, read every night at list time through WD_SITE_FACILITY):

  hospitals           the hospital segments (the primary site of a two-site
                      posting, which WD_FACILITY_MAP cannot read); names as the
                      rows already carry them, towns from the CMS list
  named clinics and   "Intermountain Health <site>", the way the phase 2 table
  centers             wrote Budge, Badura and Mile High and the way
                      intermountainhealth.org writes its locations
                      ("intermountain-health-heights-clinic-family-medicine",
                      "intermountain-health-downtown-clinic"); a department
                      after the clinic is dropped ("Billings Downtown Clinic
                      Gastroenterology", "Central Park Clinic - OB-GYN");
                      Workday's "Clinic <x>" sites in Nevada are written the
                      way the clinic is called ("Clinic Pediatrics - Tenaya"
                      is the Tenaya Clinic). The brand also keeps a site from
                      reading as another employer: Intermountain's Billings
                      Downtown Clinic is not the separate system Billings
                      Clinic.
  offices, buildings, the system label with the site's town ("-"): regional
  system services     and central offices, towers, medical office buildings
                      and pavilions, the supply chain, laboratory, laundry and
                      print centers, SelectHealth (the health plan), Life
                      Flight bases, home services, homecare, hospice and home
                      medical equipment, the employee child care centers, the
                      medical group's own offices; a site whose name is only a
                      hospital campus or a street ("St George Regional at 400
                      E") or a planned hospital (Nevada Children's Hospital)
                      also keeps the system
Towns: the rows' own "Work City" / "Work State" (source "rows"), the CMS list
for a hospital ("cms <id>"), or the town the site's name gives ("site").

Two rules the table cannot carry (decision 2, multi-site pools):
  site(loc, path)     the WD_SITE_FACILITY resolver. The board lists a posting
                      at several sites as "A; B; C" (in no fixed order; never
                      "N Locations" on the 2026-10-30 pages): three or more
                      sites keep the system label, in the primary site's town
                      (else the bulletFields' Work City); two take the primary
                      site (the path segment) when it is one of them, as
                      facilities_workday
                      and advocate_facilities do (on this board two sites are a
                      clinic and its own hospital, or two Life Flight bases);
                      WD_FACILITY_MAP used to name whichever hospital ended the
                      descriptor ("Saint Joseph Hospital; ...; Lutheran Medical
                      Center" was Lutheran Medical Center)
  apply_facilities(jobs)  finalize_jobs: a clinic or center name this module
                      gives goes back to the label when the title says a float
                      pool, a market or a remote or hybrid role ("Medical
                      Assistant Float Pool", "Registered Nurse Central Market
                      Clinic Float Pool"); a hospital keeps its own float pool
stored_decision(row)  the same rules over a stored row (its body's Location
                      block in place of the list's descriptor), for the change
                      list built from the database, so the database holds what
                      the nightly keeps.

hospital_system, job_id, url and slug are never touched.
"""
from __future__ import annotations

import re

import facilities_workday as FW
import facilities_town as FT
from city_utils import known_town

SYSTEM = "Intermountain Health (IMH)"      # the Workday emit name and the label rows carry
DB_SYSTEM = "Intermountain Healthcare"     # hospital_system (HOSPITAL_SYSTEM_ALIASES)
MULTI_SITE_MIN = 3                         # sites on one posting that make it a pool

# ── The table (site | name or - | city | ST | source) ─────────────────────
# Read off the stored links and bodies of every "Intermountain Health (IMH)"
# row on 2026-10-30 (418 active, 7,475 expired) and two live list pages. The
# phase 2 block in facilities_workday (St Marys, St Vincent, Lutheran, Good
# Samaritan, St George, Platte Valley, St James, Holy Rosary, Saint Joseph,
# Utah Valley, Park City, Budge, Badura, Mile High, Superior) stays there.
# Primary Children's at Taylorsville: WD_FACILITY_MAP filed the campus as
# "Primary Children's Hospital" in Taylorsville (the flagship's name in a
# second town); intermountainhealth.org calls it "Primary Children's Hospital
# - Taylorsville", written here as the rows already write Lehi.
_HOSPITALS = """
Intermountain-Health-Intermountain-Medical-Center | Intermountain Medical Center | Murray | UT | cms 460010
Intermountain-Health-Primary-Childrens-Hospital | Primary Children's Hospital | Salt Lake City | UT | cms 463301
Intermountain-Health-McKay-Dee-Hospital | McKay-Dee Hospital | Ogden | UT | cms 460004
Intermountain-Health-LDS-Hospital | LDS Hospital | Salt Lake City | UT | cms 460006
Intermountain-Health-Logan-Regional-Hospital | Logan Regional Hospital | Logan | UT | cms 460015
Intermountain-Health-Alta-View-Hospital | Alta View Hospital | Sandy | UT | cms 460044
Intermountain-Health-Layton-Hospital | Layton Hospital | Layton | UT | cms 460061
Intermountain-Health-Riverton-Hospital | Riverton Hospital | Riverton | UT | cms 460058
Intermountain-Health-American-Fork-Hospital | American Fork Hospital | American Fork | UT | cms 460023
Intermountain-Health-Spanish-Fork-Hospital | Spanish Fork Hospital | Spanish Fork | UT | cms 460062
Intermountain-Health-Cedar-City-Hospital | Cedar City Hospital | Cedar City | UT | cms 460007
Intermountain-Health-Orem-Community-Hospital | Orem Community Hospital | Orem | UT | cms 460043
Intermountain-Health-Sevier-Valley-Hospital | Sevier Valley Hospital | Richfield | UT | cms 460026
Intermountain-Health-Heber-Valley-Hospital | Heber Valley Hospital | Heber City | UT | cms 461307
Intermountain-Health-Bear-River-Valley-Hospital | Bear River Valley Hospital | Tremonton | UT | cms 460039
Intermountain-Health-Sanpete-Valley-Hospital | Sanpete Valley Hospital | Mount Pleasant | UT | cms 461303
Intermountain-Health-Garfield-Memorial-Hospital | Garfield Memorial Hospital | Panguitch | UT | cms 461333
Intermountain-Health-Delta-Community-Hospital | Delta Community Hospital | Delta | UT | cms 461300
Intermountain-Health-Fillmore-Community-Hospital | Fillmore Community Hospital | Fillmore | UT | cms 461301
Intermountain-Health-Cassia-Regional-Hospital | Cassia Regional Hospital | Burley | ID | cms 131326
Lutheran-Medical-Center | Lutheran Medical Center | Wheat Ridge | CO | cms 060009
Primary-Childrens-at-Lehi | Primary Children's Hospital Lehi | Lehi | UT | rows (the name WD_FACILITY_MAP gives "primary childrens at lehi")
Primary-Childrens-at-Taylorsville | Primary Children's Hospital Taylorsville | Taylorsville | UT | address 5770 S 1500 W, Taylorsville (intermountainhealth.org "Primary Children's Hospital - Taylorsville")
St-Vincent-Regional-Hospital---Cancer-Centers-of-Montana | St. Vincent Regional Hospital | Billings | MT | cms 270049
TOSH | TOSH The Orthopedic Specialty Hospital | Murray | UT | address 5848 S 300 E, Murray (intermountainhealth.org "TOSH Campus"; the site rule reads "The Orthopedic Specialty Hospital" alone as service words)
"""

_SITES = """
# Montana (St. Vincent Regional, St. James, Holy Rosary regions). Billings
# Downtown Clinic: intermountainhealth.org "Downtown Clinic", 1233 N 30th St.
Billings-Downtown-Clinic | Intermountain Health Billings Downtown Clinic | Billings | MT | rows; address 1233 N 30th St, Billings (intermountainhealth.org Downtown Clinic)
Billings-Downtown-Clinic-Gastroenterology | Intermountain Health Billings Downtown Clinic | Billings | MT | rows (the clinic's gastroenterology department)
St-Vincent-Regional-Neuroscience-Center | Intermountain Health St. Vincent Regional Neuroscience Center | Billings | MT | rows
Broadwater-Clinic | Intermountain Health Broadwater Clinic | Billings | MT | rows
Heights-Clinic | Intermountain Health Heights Clinic | Billings | MT | rows
Billings-OB-GYN | Intermountain Health Billings OB-GYN | Billings | MT | rows
Billings-Medical-Group | - | Billings | MT | rows (the medical group, no one site)
MGBL-West-End-MOB---Billings | - | Billings | MT | rows (a medical office building)
CSC-Warehouse-Montana---Billings | - | Billings | MT | site
Laurel-Clinic--Family-Medicine | Intermountain Health Laurel Clinic | Laurel | MT | rows
Red-Lodge-Clinic---Family-Medicine | Intermountain Health Red Lodge Clinic | Red Lodge | MT | rows
Rocky-Mountain-Clinic | Intermountain Health Rocky Mountain Clinic | Butte | MT | rows
Butte-Clinic | Intermountain Health Butte Clinic | Butte | MT | rows
Butte-OB-and-Urology | Intermountain Health Butte OB and Urology | Butte | MT | rows
Butte-Flats-Clinic | Intermountain Health Butte Flats Clinic | Butte | MT | site
Bozeman-Midtown-Clinic | Intermountain Health Bozeman Midtown Clinic | Bozeman | MT | rows
Gallatin-Clinic | Intermountain Health Gallatin Clinic | Bozeman | MT | rows
# Colorado (Peaks region)
Heart-and-Vascular---Denver | Intermountain Health Heart & Vascular Denver | Denver | CO | rows
Intermountain-Health-Heart--Vascular-Wheat-Ridge | Intermountain Health Heart & Vascular Wheat Ridge | Wheat Ridge | CO | rows
Intermountain-Health-Heart-and-Vascular---Grand-Junction-Vascular-Surgery | Intermountain Health Heart & Vascular Grand Junction | Grand Junction | CO | rows
Intermountain-Health-Heart-and-Vascular---Grand-Junction-Cardiothoracic-Surgery | Intermountain Health Heart & Vascular Grand Junction | Grand Junction | CO | rows
Intermountain-Health-Heart-and-Vascular-West-Park-Cardiology | Intermountain Health Heart & Vascular West Park | Grand Junction | CO | rows
Denver-Midtown-Clinic | Intermountain Health Denver Midtown Clinic | Denver | CO | rows
Denver-Midtown-Clinic-Primary-Care | Intermountain Health Denver Midtown Clinic | Denver | CO | rows
Lutheran-Clinic | Intermountain Health Lutheran Clinic | Wheat Ridge | CO | rows
Lutheran-Family-Practices | Intermountain Health Lutheran Family Practices | Wheat Ridge | CO | rows
Lutheran-Orthopedic-Trauma | Intermountain Health Lutheran Orthopedic Trauma | Wheat Ridge | CO | rows
Wheat-Ridge-Clinic | Intermountain Health Wheat Ridge Clinic | Wheat Ridge | CO | rows
Rocky-Mountain-Orthopedics | Intermountain Health Rocky Mountain Orthopedics | Grand Junction | CO | rows
Grand-Junction-Downtown-Clinic | Intermountain Health Grand Junction Downtown Clinic | Grand Junction | CO | rows
Grand-Junction-Neurology-Clinic | Intermountain Health Grand Junction Neurology Clinic | Grand Junction | CO | rows
Patterson-OB-GYN-and-Midwifery-Clinic | Intermountain Health Patterson OB-GYN and Midwifery Clinic | Grand Junction | CO | rows
Patterson-Primary-Care-Clinic | Intermountain Health Patterson Primary Care Clinic | Grand Junction | CO | rows
Wellington-OB-GYN-Clinic | Intermountain Health Wellington OB-GYN Clinic | Grand Junction | CO | rows
Wellington-Lung-and-Sleep-Clinic | Intermountain Health Wellington Lung and Sleep Clinic | Grand Junction | CO | rows
San-Juan-Cancer-Center-JV | San Juan Cancer Center | Montrose | CO | rows (a joint venture: its own name)
Central-Park-Clinic---Occupational-Medicine-and-Physical-Therapy | Intermountain Health Central Park Clinic | Denver | CO | rows
Central-Park-Clinic---OB-GYN | Intermountain Health Central Park Clinic | Denver | CO | rows
Central-Park-Occ-Med | Intermountain Health Central Park Clinic | Denver | CO | rows (the clinic's occupational medicine)
Lowry-Clinic | Intermountain Health Lowry Clinic | Denver | CO | rows
Northfield-Clinic---Primary-Care | Intermountain Health Northfield Clinic | Denver | CO | rows
Rampart-Clinic-Neurology--Neuro-Ophthalmology | Intermountain Health Rampart Clinic | Denver | CO | rows
Denver-Sports-Medicine-Clinic | Intermountain Health Denver Sports Medicine Clinic | Denver | CO | rows
Birth-Center-of-Denver | Intermountain Health Birth Center of Denver | Denver | CO | rows
Mount-St-Vincent-Home | Intermountain Health Mount St. Vincent Home | Denver | CO | rows
St-Josephs-Oncology-Center | Intermountain Health St. Joseph's Oncology Center | Denver | CO | rows
Cherry-Creek-Primary-Care | Intermountain Health Cherry Creek Primary Care | Glendale | CO | rows
Platte-Valley-Clinic | Intermountain Health Platte Valley Clinic | Brighton | CO | rows
Platte-Valley-Hospital-OB-GYN | Platte Valley Hospital OB-GYN | Brighton | CO | rows (the hospital's OB-GYN practice, as the board names it)
Brighton-Family-Medicine | Intermountain Health Brighton Family Medicine | Brighton | CO | rows
Reunion-Clinic---OB-GYN | Intermountain Health Reunion Clinic | Commerce City | CO | rows
Fort-Lupton-Clinic | Intermountain Health Fort Lupton Clinic | Fort Lupton | CO | rows
Thornton-Clinic | Intermountain Health Thornton Clinic | Thornton | CO | rows
AmberCreek | Intermountain Health Amber Creek Clinic | Thornton | CO | rows (intermountainhealth.org amber-creek-clinic)
Candelas-Clinic | Intermountain Health Candelas Clinic | Arvada | CO | rows
Highlands-Ranch-Clinic | Intermountain Health Highlands Ranch Clinic | Highlands Ranch | CO | rows
Broomfield-Clinic | Intermountain Health Broomfield Clinic | Broomfield | CO | rows
Firestone-Clinic | Intermountain Health Firestone Clinic | Firestone | CO | rows
Lafayette-Clinic | Intermountain Health Lafayette Clinic | Lafayette | CO | rows
Lafayette-Front-Range-Spine-Center | Intermountain Health Lafayette Front Range Spine Center | Lafayette | CO | rows
Belmar-Clinic---Primary-Care | Intermountain Health Belmar Clinic | Lakewood | CO | rows
Peaks-Regional-Office | - | Broomfield | CO | rows (regional office)
Good-Samaritan-Medical-Office-Pavilion | - | Lafayette | CO | rows (a medical office building)
Foothills-Medical-Office-Bldg | - | Wheat Ridge | CO | rows (a medical office building)
Northglenn-Healthcare-Pavilion | - | Northglenn | CO | rows (a building)
Advanced-Medical-Pavilion | - | Grand Junction | CO | rows (a building)
Hospice---Denver | - | Wheat Ridge | CO | rows (hospice)
Homecare---Denver-Metro | - | Denver | CO | rows (home care)
Cortez-Air-Base | - | Cortez | CO | rows (Life Flight base)
Steamboat-Air-Base | - | Steamboat Springs | CO | rows (Life Flight base)
# Utah (and Idaho)
Utah-Valley-Clinic | Intermountain Health Utah Valley Clinic | Provo | UT | rows
Utah-Valley-Outpatient-Center | Intermountain Health Utah Valley Outpatient Center | Provo | UT | rows
Utah-Valley-InstaCare | Intermountain Health Utah Valley InstaCare | Provo | UT | rows
Central-Orem-Clinic | Intermountain Health Central Orem Clinic | Orem | UT | rows
American-Fork-Specialty-Clinic | Intermountain Health American Fork Specialty Clinic | American Fork | UT | rows
American-Fork-InstaCare | Intermountain Health American Fork InstaCare | American Fork | UT | rows
Saratoga-Springs-Clinic | Intermountain Health Saratoga Springs Clinic | Saratoga Springs | UT | rows
Saratoga-Springs-ED | Intermountain Health Saratoga Springs Emergency Facility | Saratoga Springs | UT | rows (the posting's own words; Workday "ED")
Salt-Lake-Clinic | Intermountain Health Salt Lake Clinic | Salt Lake City | UT | rows
North-Temple-Clinic | Intermountain Health North Temple Clinic | Salt Lake City | UT | rows
Holladay-Clinic | Intermountain Health Holladay Clinic | Salt Lake City | UT | rows (Work City Salt Lake City)
Memorial-Clinic | Intermountain Health Memorial Clinic | Salt Lake City | UT | rows (stored town)
Avenues-Womens-Center | Intermountain Health Avenues Women's Center | Salt Lake City | UT | rows
Primary-Childrens-Outpatient-Clinic | Primary Children's Outpatient Clinic | Salt Lake City | UT | rows
West-Temple-Dialysis-Center | Intermountain Health West Temple Dialysis Center | Salt Lake City | UT | rows
Cottonwood-Medical-Clinic | Intermountain Health Cottonwood Medical Clinic | Murray | UT | rows
Murray-WorkMed | Intermountain Health Murray WorkMed | Murray | UT | rows
Alta-View-Clinic | Intermountain Health Alta View Clinic | Sandy | UT | rows
Alta-View-Specialty-Clinic | Intermountain Health Alta View Specialty Clinic | Sandy | UT | rows
Draper-Clinic | Intermountain Health Draper Clinic | Draper | UT | site
Southridge-Clinic-at-Riverton | Intermountain Health Southridge Clinic | Riverton | UT | rows (Workday "Southridge Clinic at Riverton")
West-Valley-Clinic | Intermountain Health West Valley Clinic | West Valley City | UT | rows
Taylorsville-Clinic | Intermountain Health Taylorsville Clinic | Taylorsville | UT | rows
West-Jordan-Clinic | Intermountain Health West Jordan Clinic | West Jordan | UT | rows
Copper-Hills-Clinic | Intermountain Health Copper Hills Clinic | West Jordan | UT | rows
Lehi-Clinic | Intermountain Health Lehi Clinic | Lehi | UT | site
Bountiful-Clinic | Intermountain Health Bountiful Clinic | Bountiful | UT | rows
Kaysville-Creekside-Clinic | Intermountain Health Kaysville Creekside Clinic | Kaysville | UT | site
Centerville-Day-Treatment | Intermountain Health Centerville Day Treatment | Centerville | UT | rows
Layton-Clinic | Intermountain Health Layton Clinic | Layton | UT | rows
Layton-Workmed | Intermountain Health Layton WorkMed | Layton | UT | site (the posting: "Layton Workmed ... Layton, UT"; its Work City reads Fairfield)
Roy-Clinic | Intermountain Health Roy Clinic | Roy | UT | site
South-Ogden-Clinic | Intermountain Health South Ogden Clinic | Ogden | UT | rows (Work City Ogden)
North-Ogden-Clinic | Intermountain Health North Ogden Clinic | North Ogden | UT | site
McKay-Dee-Surgical-Center | Intermountain Health McKay-Dee Surgical Center | Ogden | UT | rows
Snowbasin-Clinic | Intermountain Health Snowbasin Clinic | Huntsville | UT | rows
Logan-Clinic | Intermountain Health Logan Clinic | Logan | UT | rows
Logan-Dialysis-Center | Intermountain Health Logan Dialysis Center | Logan | UT | site
Gossner-Cancer-Center---Logan | Intermountain Health Gossner Cancer Center | Logan | UT | rows
South-Cache-Valley-Clinic | Intermountain Health South Cache Valley Clinic | Providence | UT | rows
Bear-River-Clinic | Intermountain Health Bear River Clinic | Tremonton | UT | rows
Park-City-Clinic | Intermountain Health Park City Clinic | Park City | UT | rows
Park-City-Instacare | Intermountain Health Park City InstaCare | Park City | UT | rows
Jerry-and-Kathleen-Grundhofer-Clinic | Intermountain Health Jerry and Kathleen Grundhofer Clinic | Park City | UT | rows
Mountain-Family-Health | Intermountain Health Mountain Family Health Clinic | Park City | UT | rows (intermountainhealth.org mountain-family-health-clinic)
Heber-Valley-Clinic | Intermountain Health Heber Valley Clinic | Heber City | UT | site
Heber-Valley-Specialty-Clinic | Intermountain Health Heber Valley Specialty Clinic | Heber City | UT | site
Manti-Clinic | Intermountain Health Manti Clinic | Manti | UT | rows
Fillmore-Clinic | Intermountain Health Fillmore Clinic | Fillmore | UT | rows
River-Road-Clinic | Intermountain Health River Road Clinic | St George | UT | rows
Washington-Fields-Clinic | Intermountain Health Washington Fields Clinic | St George | UT | rows
Sunset-Clinic | Intermountain Health Sunset Clinic | St George | UT | rows
St-George-Cancer-Center | Intermountain Health St. George Cancer Center | St George | UT | rows
Workmed---St-George | Intermountain Health St. George WorkMed | St George | UT | site
Hurricane-Valley-Clinic | Intermountain Health Hurricane Valley Clinic | Hurricane | UT | rows
Tooele-Valley-Dialysis-Center | Intermountain Health Tooele Valley Dialysis Center | Tooele | UT | site
St-George-Regional-at-Foremaster | - | St George | UT | rows (a hospital campus, no site of its own name)
St-George-Regional-at-400-E | - | St George | UT | rows (a hospital campus by its street)
St-George-Cardio | - | St George | UT | rows (a service line, no site name)
Psychiatry-and-Counseling---St-George | - | St George | UT | rows (a service line and a town)
Physical-Therapy---Bountiful | - | Bountiful | UT | rows (a service line and a town)
Tellica-Orem | - | Orem | UT | rows (imaging joint venture, by its town)
Home-Services---Salt-Lake-City | - | South Jordan | UT | rows (home services; Work City South Jordan)
Home-Services---Provo | - | Provo | UT | rows (home services)
Home-Services---Heber | - | Heber City | UT | rows (home services)
Home-Services---Fillmore | - | Fillmore | UT | site (home services)
Home-Services---Cedar-City | - | Cedar City | UT | site (home services)
Home-Services---Logan | - | Logan | UT | site (home services)
Home-Services---Mount-Pleasant | - | Mount Pleasant | UT | site (home services)
Homecare---Ogden | - | Ogden | UT | rows (home care)
Home-Health-and-Hospice---St-George | - | St George | UT | rows (home health and hospice)
Home-Health-and-Hospice---Cassia | - | Burley | ID | rows (home health and hospice)
Home-Medical-Equipment---Logan | - | Logan | UT | rows (home medical equipment)
Home-Medical-Equipment---Provo | - | Provo | UT | site (home medical equipment)
Home-Medical-Equipment---Ogden | - | Ogden | UT | site (home medical equipment)
HCS-Provo-DME | - | Provo | UT | rows (home medical equipment)
R-Kids-Child-Development-Center---Provo | - | Provo | UT | rows (employee child care)
Child-Development-Center---Salt-Lake | - | Salt Lake City | UT | site (employee child care)
Key-Bank-Tower | - | Salt Lake City | UT | rows (office tower)
Valley-Center-Tower | - | Murray | UT | rows (office tower; Work City Murray)
SelectHealth---Murray | - | Murray | UT | rows (SelectHealth, the health plan)
SelectHealth---Idaho | - | Meridian | ID | rows (SelectHealth, the health plan)
Supply-Chain-Center | - | Midvale | UT | rows (supply chain)
Central-Laboratory | - | Murray | UT | rows (the system laboratory)
Central-Laundry | - | Woods Cross | UT | rows (laundry)
Lake-Park-Building | - | West Valley City | UT | rows (office building)
Employee-Service-Center | - | Murray | UT | rows (office)
Transformation-Center | - | Murray | UT | rows (office)
Vine-Street-Office-Building | - | Murray | UT | rows (office building; stored town)
Biorepository | - | Salt Lake City | UT | rows (research; stored town)
Medical-Group-Administration | - | Orem | UT | rows (the medical group's office)
Avenues-Physician-Tower | - | Salt Lake City | UT | rows (a building)
McKay-Dee-Medical-Building | - | Ogden | UT | rows (a building)
TOSH-Medical-Tower | - | Murray | UT | rows (a building)
Copper-Peak-Medical-Building | - | American Fork | UT | rows (a building)
Intermountain-Print-Center--Salt-Lake-City | - | Salt Lake City | UT | rows (print center)
Life-Flight-Operations-Center | - | Salt Lake City | UT | rows (Life Flight)
# Nevada
Pahrump-Clinic | Intermountain Health Pahrump Clinic | Pahrump | NV | rows
La-Canada-Clinic | Intermountain Health La Canada Clinic | Las Vegas | NV | rows
La-Canada-Endocrinology | Intermountain Health La Canada Clinic | Las Vegas | NV | rows (intermountainhealth.org la-canada-clinic-endocrinology)
Clinic-Pediatrics---Tenaya | Intermountain Health Tenaya Clinic | Las Vegas | NV | rows (intermountainhealth.org tenaya-clinic-pediatrics)
Clinic-Oncology---San-Martin | Intermountain Health San Martin Clinic | Las Vegas | NV | rows (Workday "Clinic Oncology - San Martin")
Clinic-Wynn | Intermountain Health Wynn Clinic | Las Vegas | NV | rows (Workday "Clinic Wynn")
Clinic-West-Sunset | Intermountain Health West Sunset Clinic | Las Vegas | NV | rows (Workday "Clinic West Sunset")
Sunset-Multi-Specialty-Clinic | Intermountain Health Sunset Multi-Specialty Clinic | Las Vegas | NV | rows
Blue-Diamond-Clinic | Intermountain Health Blue Diamond Clinic | Las Vegas | NV | rows
Rancho-Clinic | Intermountain Health Rancho Clinic | Las Vegas | NV | rows
Centennial-Clinic---Primary-Care | Intermountain Health Centennial Clinic | Las Vegas | NV | rows
Elkhorn-Pediatrics | Intermountain Health Elkhorn Pediatrics | Las Vegas | NV | rows
District-Oncology | Intermountain Health District Oncology | Henderson | NV | rows
District-Clinic---Pediatrics | Intermountain Health District Clinic | Henderson | NV | rows (stored town; intermountainhealth.org district-clinic)
Siena-Cardiology-Clinic | Intermountain Health Siena Cardiology Clinic | Henderson | NV | rows (stored town)
Clinic-Mountain-View-MOB-Suite-330 | - | Las Vegas | NV | rows (a suite in a medical office building)
Nevada-Childrens-Hospital | - | Las Vegas | NV | rows (a planned hospital, not in the CMS list)
Nevada-Central-Office | - | Las Vegas | NV | rows (office)
Central-Office---Las-Vegas | - | Las Vegas | NV | rows (the home health office)
Intermountain-Health-Home-Services---Home-Health-at-Silverado-Ranch | - | Las Vegas | NV | rows (home health)
Elko-Air-Base | - | Elko | NV | rows (Life Flight base)
# Life Flight bases outside the four states
Page-Air-Base | - | Page | AZ | rows (Life Flight base)
Hartford-Air-Base | - | Hartford | WI | rows (Life Flight base)
Waupaca-Air-Base | - | Waupaca | WI | rows (Life Flight base)
Neenah-Air-Base | - | Neenah | WI | rows (Life Flight base)
Martinsville-Air-Base | - | Martinsville | VA | rows (Life Flight base)
Concord-Air-Base | - | Concord | NC | rows (Life Flight base)
"""

FW._t(SYSTEM, _HOSPITALS, also=DB_SYSTEM)
FW._t(SYSTEM, _SITES, also=DB_SYSTEM)


def _names(block: str) -> set:
    out = set()
    for line in block.strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            name = line.split("|")[1].strip()
            if name != "-":
                out.add(name)
    return out


HOSPITAL_NAMES = _names(_HOSPITALS) | {
    # the phase 2 block's hospitals (facilities_workday) and WD_FACILITY_MAP's
    "St. Mary's Regional Hospital", "St. Vincent Regional Hospital", "Lutheran Medical Center",
    "Good Samaritan Medical Center", "St. George Regional Hospital", "Platte Valley Hospital", "St. James Hospital",
    "Holy Rosary Hospital", "Saint Joseph Hospital", "Utah Valley Hospital", "Park City Hospital"}
# The clinic and center names this module gives (the float / remote rule reads them).
SITE_NAMES = _names(_SITES) | {"Intermountain Health Budge Clinic", "Intermountain Health Badura Clinic",
                               "Intermountain Health Mile High Clinic", "Intermountain Health Superior Clinic"}

_N_LOCATIONS_RX = re.compile(r"^\s*(\d+)\s+locations?\s*$", re.I)
# A clinic float pool, a market-wide role ("Float Covering South Market",
# "Registered Nurse Central Market Clinic Float Pool", "Resort Triage North
# Market Clinics"): no one clinic is its site.
POOL_RX = re.compile(r"\bfloat(?:ing|er|s)?\b|\bpool\b|\bmarket\b|\bcovering\b|\btravel(?:er|ing)?\b|\biflex\b", re.I)


def _is_system(system: str) -> bool:
    return system in (SYSTEM, DB_SYSTEM)


def parts(loc: str) -> list:
    """The sites a descriptor lists: the board writes a posting at several
    sites as "A; B; C" ("Intermountain Health St Vincent Regional Hospital;
    Billings Downtown Clinic", list pages read 2026-10-30), in no fixed order;
    [] for a "N Locations" descriptor or none."""
    if _N_LOCATIONS_RX.match(loc or ""):
        return []
    return [p.strip() for p in (loc or "").split(";") if p.strip()]


def n_sites(loc: str) -> int:
    """How many sites a descriptor names ("N Locations" or "A; B; C")."""
    m = _N_LOCATIONS_RX.match(loc or "")
    return int(m.group(1)) if m else len(parts(loc))


def site(loc: str, external_path: str = ""):
    """WD_SITE_FACILITY resolver for Intermountain: (hospital_name, city,
    state) or None (placed as before: WD_FACILITY_MAP).
      three or more sites  the system label (decision 2, a multi-site pool)
                           in the primary site's town (the externalPath
                           segment, when it is one of the listed sites and
                           the table knows it); else with no town of its
                           own, and the adapter takes the posting's Work City
                           and Work State from its bulletFields ([req,
                           "Utah", "Salt Lake City"])
      two sites            the primary site when it is one of the two, else
                           the label as above; WD_FACILITY_MAP used to name
                           whichever hospital ended the descriptor
      one site             the table (the descriptor)"""
    n = n_sites(loc)
    if n >= 2:
        listed = {FW.key(p) for p in parts(loc)}
        seg = FW.key(FW.path_segment(external_path))
        primary = FW.facility(SYSTEM, "", external_path) if seg and (not listed or seg in listed) else None
        if n >= MULTI_SITE_MIN:
            # the label, in the primary site's own town (or the bullets' when
            # the primary is not listed or the table does not know it)
            return (SYSTEM, primary[1], primary[2]) if primary else (SYSTEM, "", "")
        return primary or (SYSTEM, "", "")
    return FW.facility(SYSTEM, loc, external_path)


def unknown(loc: str, external_path: str = "") -> str:
    if n_sites(loc) >= 2:
        return ""
    return FW.unknown_site(SYSTEM, loc, external_path)


def pool_title(title: str) -> bool:
    """The title says a clinic float pool, a market-wide role, a remote or a
    hybrid role, or several sites (facilities_town's own rules)."""
    t = FT.clean_text(title or "")
    return bool(POOL_RX.search(t) or FT.REMOTE_RX.search(t) or FT.MULTI_SITE_RX.search(t))


def keep_label(name: str, title: str) -> bool:
    """A clinic or center this module names goes back to the label on a pool,
    market, remote or hybrid title (a hospital keeps its own float pool)."""
    return name in SITE_NAMES and pool_title(title)


def apply_facilities(jobs) -> dict:
    """finalize_jobs pass: the float / remote rule over tonight's Intermountain
    rows. Returns {SYSTEM: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        if not _is_system(getattr(job, "hospital_system", "")):
            continue
        name = job.hospital_name or ""
        if keep_label(name, job.title):
            job.hospital_name = SYSTEM
            how = "pool or remote title: label"
        elif name in SITE_NAMES:
            how = "clinic or center"
        elif name in HOSPITAL_NAMES:
            how = "hospital"
        elif name in (SYSTEM, DB_SYSTEM, ""):
            how = "label"
        else:
            how = "other name"
        d = stats.setdefault(SYSTEM, {})
        d[how] = d.get(how, 0) + 1
    return stats


# ── Stored rows (the change list) ─────────────────────────────────────────
# the sites sit on one line, then a blank line, then "Work City:" (a
# recruiter's free text after "Location:" spans paragraphs and is not read)
_LOC_BLOCK_RX = re.compile(r"Location:[ \t]*\n[ \t]*([^\n]+?)[ \t]*\n[ \t]*\n[ \t]*Work City:")
_WORK_CITY_RX = re.compile(r"Work City:[ \t]*\n[ \t]*([^\n]+)")
_WORK_STATE_RX = re.compile(r"Work State:[ \t]*\n[ \t]*([^\n]+)")


def body_sites(description: str) -> list:
    """The sites of the body's structured Location block ("Location:", the
    sites comma separated, a blank line, "Work City:"); [] for a body without
    one (a free-text "Location: ..." line is the recruiter's, not the board's)."""
    m = _LOC_BLOCK_RX.search(description or "")
    if not m:
        return []
    return [s.strip() for s in m.group(1).split(",") if s.strip()]


def work_town(description: str):
    """(town, ST) from the body's Work City / Work State, the town checked
    against the known-town list; None otherwise."""
    d = description or ""
    c, s = _WORK_CITY_RX.search(d), _WORK_STATE_RX.search(d)
    if not c or not s:
        return None
    st = FW._STATES.get(s.group(1).strip().lower())
    town = known_town(c.group(1).strip(), st) if st else None
    return (town, st) if town else None


def stored_decision(url: str, title: str, description: str):
    """(hospital_name, city, state, how) for one stored Intermountain row: the
    nightly's rules (site above) with the body's Location block standing in
    for the list's descriptor and its Work City / Work State for the
    bulletFields. A body without the block uses the url's site segment. City
    and state are "" when the row keeps its stored town; the name is None
    when the table does not know the site."""
    sites = body_sites(description)
    if len(sites) >= 2:
        loc = "; ".join(sites)
    elif sites:
        loc = sites[0]
    else:
        loc = FW._site_text("", url)
    hit = site(loc, url)
    if hit is None:
        return None, None, None, "site the table does not know"
    name, city, state = hit
    if not city:
        wt = work_town(description)
        city, state = wt if wt else ("", "")
    if len(sites) >= MULTI_SITE_MIN:
        return SYSTEM, city, state, f"multi-site posting ({len(sites)} sites)"
    seg = FW.key(FW.path_segment(url))
    if name == SYSTEM and len(sites) == 2 and seg not in {FW.key(x) for x in sites}:
        return SYSTEM, city, state, "two sites, the primary site not among them"
    if name == SYSTEM and len(sites) == 2 and seg not in FW.TABLES[SYSTEM]:
        return SYSTEM, city, state, "two sites, a primary site the table does not name"
    if keep_label(name, title):
        return SYSTEM, city, state, "pool, market, remote or hybrid title"
    if name == SYSTEM:
        return SYSTEM, city, state, "office, building, home services or system service"
    return name, city, state, "hospital" if name in HOSPITAL_NAMES else "clinic or center"
