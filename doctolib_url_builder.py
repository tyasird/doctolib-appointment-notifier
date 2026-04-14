"""
doctolib_url_builder.py

Parses a Doctolib booking URL and reconstructs availabilities.json URLs.

Pipeline:
  booking URL
    → extract profile_slug, motive_ids, practice_id, practitioner_id,
               telehealth, insurance_sector
    → GET /online_booking/api/slot_selection_funnel/v1/info.json?profile_slug=<slug>
    → for each requested motive: find agendas that serve it
         — filtered by practitioner_id if present (group practices)
         — filtered by practice_id if present and not NO_PREFERENCE
    → build availabilities.json URL(s)

WINDOWS CLI: always use double quotes around the URL.
"""

import sys
from datetime import date
from urllib.parse import urlparse, parse_qs, urlencode
import requests

BASE_URL = "https://www.doctolib.de"
INFO_JSON_PATH = "/online_booking/api/slot_selection_funnel/v1/info.json"
AVAILABILITY_PATH = "/availabilities.json"
DEFAULT_LIMIT = 7

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Referer": BASE_URL,
}


# ---------------------------------------------------------------------------
# Step 1: Parse the booking URL
# ---------------------------------------------------------------------------

def parse_booking_url(booking_url: str) -> dict:
    """
    Extracts all available parameters from a Doctolib booking URL.

    Confirmed parameters and their sources:
      profile_slug      path segment before /booking
                        individual:  irmgard-schuppert-cd818906-...
                        group:       hno-praxis-bonn
      motive_ids        motiveIds[] or vmids[]
      telehealth        telehealth=
      insurance_sector  insuranceSector= or insurance_sector=
      practice_id       numeric part of placeId=practice-XXXX
                        None when placeId=NO_PREFERENCE or absent
      practitioner_id   practitionerId=  (present in group practice URLs)
                        used to filter agendas in info.json

    Not in the URL (must come from info.json):
      agenda_ids
    """
    parsed = urlparse(booking_url)
    path_parts = [p for p in parsed.path.split("/") if p]

    profile_slug = None
    for i, part in enumerate(path_parts):
        if part == "booking" and i > 0:
            profile_slug = path_parts[i - 1]
            break
    if not profile_slug:
        for part in reversed(path_parts):
            if "-" in part and part not in ("booking", "availabilities"):
                profile_slug = part
                break

    qs = parse_qs(parsed.query, keep_blank_values=True)

    motive_ids = (
        qs.get("motiveIds[]") or qs.get("motiveIds") or
        qs.get("vmids[]") or qs.get("vmids") or []
    )
    telehealth = (qs.get("telehealth") or ["false"])[0].lower()
    insurance_sector = (
        qs.get("insuranceSector") or qs.get("insurance_sector") or ["public"]
    )[0].lower()

    # placeId=practice-446499 → "446499"
    # placeId=NO_PREFERENCE   → None
    # absent                  → None
    practice_id = None
    for key in ("placeId", "pid"):
        val = (qs.get(key) or [None])[0]
        if val and "practice-" in val:
            practice_id = val.split("practice-")[-1]
            break

    # practitionerId present in group practice booking URLs
    practitioner_id = (qs.get("practitionerId") or [None])[0]

    return {
        "profile_slug":     profile_slug,
        "motive_ids":       motive_ids,
        "telehealth":       telehealth,
        "insurance_sector": insurance_sector,
        "practice_id":      practice_id,       # None if NO_PREFERENCE
        "practitioner_id":  practitioner_id,   # None for solo practitioners
    }


# ---------------------------------------------------------------------------
# Step 2: Fetch info.json
# ---------------------------------------------------------------------------

def fetch_info_json(profile_slug: str, locale: str = "en") -> dict:
    """
    GET /online_booking/api/slot_selection_funnel/v1/info.json?profile_slug=<slug>

    Returns 410 for nonexistent slugs (not because the endpoint is retired).
    """
    url = BASE_URL + INFO_JSON_PATH
    params = {"profile_slug": profile_slug, "locale": locale}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Step 3: Extract motive→agenda→practice mappings
# ---------------------------------------------------------------------------

def extract_motive_mappings(
    info: dict,
    filter_motive_ids: list | None = None,
    practice_id_hint: str | None = None,
    practitioner_id: str | None = None,
) -> list:
    """
    Builds motive → agenda → practice mappings from info.json.

    Key structural facts (CONFIRMED from real response):
      - Motives do NOT list their agendas.
      - Agendas list which motives they serve via visit_motive_ids[].
      - Each agenda belongs to one practitioner (practitioner_id field).
      - Group practices return agendas for all practitioners in the practice.

    Filtering logic:
      1. Skip agendas with booking_disabled=True or booking_temporary_disabled=True.
      2. If practitioner_id is given, only keep agendas for that practitioner.
         This is necessary for group practices where info.json returns agendas
         from multiple doctors.
      3. If practice_id is given (placeId=practice-XXXX), only keep agendas
         for that practice. Skipped when placeId=NO_PREFERENCE.
    """
    data = info.get("data") or info

    raw_motives = data.get("visit_motives") or []
    raw_agendas = data.get("agendas") or []
    raw_places  = data.get("places") or []

    # Build motive_id -> [(agenda_id, practice_id), ...]
    motive_to_agendas: dict[str, list] = {}
    for ag in raw_agendas:
        if ag.get("booking_disabled") or ag.get("booking_temporary_disabled"):
            continue

        ag_id   = str(ag.get("id", ""))
        prac_id = str(ag.get("practice_id", ""))
        ag_practitioner_id = str(ag.get("practitioner_id", ""))

        # Filter by practitioner when specified (group practices)
        if practitioner_id and ag_practitioner_id != str(practitioner_id):
            continue

        # Filter by practice when a specific one was requested
        if practice_id_hint and prac_id != str(practice_id_hint):
            continue

        for mid in (ag.get("visit_motive_ids") or []):
            key = str(mid)
            motive_to_agendas.setdefault(key, [])
            motive_to_agendas[key].append((ag_id, prac_id))

    # Collect all practice IDs from places as fallback
    fallback_practice_ids = []
    for place in raw_places:
        for pid in (place.get("practice_ids") or []):
            sid = str(pid)
            if sid not in fallback_practice_ids:
                fallback_practice_ids.append(sid)

    filter_set = {str(m) for m in filter_motive_ids} if filter_motive_ids else None

    results = []
    for m in raw_motives:
        motive_id = str(m.get("id", ""))
        if not motive_id or (filter_set and motive_id not in filter_set):
            continue

        motive_name = m.get("name") or ""
        pairs = motive_to_agendas.get(motive_id, [])
        agenda_ids   = list(dict.fromkeys(p[0] for p in pairs if p[0]))
        practice_ids = list(dict.fromkeys(p[1] for p in pairs if p[1]))

        if not practice_ids:
            practice_ids = fallback_practice_ids[:1]

        results.append({
            "motive_id":    motive_id,
            "motive_name":  motive_name,
            "agenda_ids":   agenda_ids,
            "practice_ids": practice_ids,
        })
    return results


# ---------------------------------------------------------------------------
# Step 4: Build availabilities.json URLs
# ---------------------------------------------------------------------------

def build_availability_urls(
    motive_mappings: list,
    insurance_sector: str = "public",
    telehealth: str = "false",
    start_date: str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> list:
    """
    agenda_ids joined with "-"  (CONFIRMED from working examples).
    practice_ids: first entry   (almost always one per motive/practitioner).
    """
    if start_date is None:
        start_date = date.today().isoformat()
    urls = []
    for m in motive_mappings:
        if not m["agenda_ids"]:
            print(f"  [WARN] Motive {m['motive_id']} ({m['motive_name']!r}) — no agendas, skipping")
            continue
        if not m["practice_ids"]:
            print(f"  [WARN] Motive {m['motive_id']} ({m['motive_name']!r}) — no practice, skipping")
            continue
        params = {
            "visit_motive_ids": m["motive_id"],
            "agenda_ids":       "-".join(m["agenda_ids"]),
            "practice_ids":     m["practice_ids"][0],
            "insurance_sector": insurance_sector,
            "telehealth":       telehealth,
            "start_date":       start_date,
            "limit":            limit,
        }
        urls.append({
            "motive_id":   m["motive_id"],
            "motive_name": m["motive_name"],
            "url":         BASE_URL + AVAILABILITY_PATH + "?" + urlencode(params),
        })
    return urls


# ---------------------------------------------------------------------------
# Optional: fetch and summarise slots
# ---------------------------------------------------------------------------

def fetch_availability(url: str) -> dict:
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()

def summarise_slots(data: dict) -> str:
    total = sum(len(a.get("slots") or []) for a in (data.get("availabilities") or []))
    next_slot = data.get("next_slot")
    return f"{total} slot(s); next: {next_slot}" if total else "no slots in this window"


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def build_from_booking_url(
    booking_url: str,
    start_date: str | None = None,
    limit: int = DEFAULT_LIMIT,
    fetch_slots: bool = False,
) -> list:
    print(f"\n{'='*60}")
    print(f"Input: {booking_url[:90]}{'...' if len(booking_url) > 90 else ''}")

    parsed = parse_booking_url(booking_url)
    print(f"\n[1] From booking URL:")
    print(f"    profile_slug:     {parsed['profile_slug']!r}")
    print(f"    motive_ids:       {parsed['motive_ids']}")
    print(f"    practice_id:      {parsed['practice_id']!r}  (None = NO_PREFERENCE)")
    print(f"    practitioner_id:  {parsed['practitioner_id']!r}")
    print(f"    telehealth:       {parsed['telehealth']}")
    print(f"    insurance_sector: {parsed['insurance_sector']}")

    if not parsed["profile_slug"]:
        raise ValueError("Could not extract profile_slug from URL")

    print(f"\n[2] Fetching info.json for slug={parsed['profile_slug']!r} …")
    info = fetch_info_json(parsed["profile_slug"])
    data_block = info.get("data") or info
    print(f"    OK — {len(data_block.get('visit_motives') or [])} motive(s), "
          f"{len(data_block.get('agendas') or [])} agenda(s)")

    mappings = extract_motive_mappings(
        info,
        filter_motive_ids=parsed["motive_ids"] or None,
        practice_id_hint=parsed["practice_id"],      # None when NO_PREFERENCE → no filter
        practitioner_id=parsed["practitioner_id"],   # None for solo practitioners
    )
    if not mappings:
        print("    [WARN] No motives matched — returning all motives without filters")
        mappings = extract_motive_mappings(info)

    print(f"\n[3] Motive → agenda → practice:")
    for m in mappings:
        print(f"    [{m['motive_id']}] {m['motive_name']!r}")
        print(f"      agenda_ids:   {m['agenda_ids']}")
        print(f"      practice_ids: {m['practice_ids']}")

    urls = build_availability_urls(
        mappings,
        insurance_sector=parsed["insurance_sector"],
        telehealth=parsed["telehealth"],
        start_date=start_date,
        limit=limit,
    )
    print(f"\n[4] availabilities.json URLs:")
    for u in urls:
        print(f"    [{u['motive_id']}] {u['motive_name']}")
        print(f"    {u['url']}")

    if fetch_slots:
        print(f"\n[5] Fetching slots …")
        for u in urls:
            try:
                summary = summarise_slots(fetch_availability(u["url"]))
                u["slots_summary"] = summary
                print(f"    [{u['motive_id']}] {summary}")
            except requests.HTTPError as e:
                u["slots_summary"] = f"ERROR {e}"
                print(f"    [{u['motive_id']}] HTTP error: {e}")

    return urls


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python doctolib_url_builder.py <booking_url> [--fetch] [--start YYYY-MM-DD]")
        print()
        print('  python doctolib_url_builder.py "https://www.doctolib.de/.../booking/availabilities?..."')
        print('  python doctolib_url_builder.py "<url>" --fetch --start 2026-04-15')
        sys.exit(0)

    url_arg = sys.argv[1]
    fetch_flag = "--fetch" in sys.argv

    start_arg = None
    if "--start" in sys.argv:
        idx = sys.argv.index("--start")
        if idx + 1 < len(sys.argv):
            start_arg = sys.argv[idx + 1]

    results = build_from_booking_url(url_arg, start_date=start_arg, fetch_slots=fetch_flag)
    print(f"\nDone. {len(results)} URL(s) generated.")