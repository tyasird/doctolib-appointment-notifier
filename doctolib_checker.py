import argparse
import datetime
import json
import pathlib
import re
import smtplib
import sys
import time
import urllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable
from email.message import EmailMessage

import requests
from doctolib_url_builder import (
    build_availability_urls,
    extract_motive_mappings,
    fetch_info_json,
    parse_booking_url,
)

SETTINGS_FILENAME = "settings.json"


def load_runtime_settings():
    """Load and minimally validate settings from settings.json."""

    settings_path = pathlib.Path(__file__).parent.resolve() / SETTINGS_FILENAME
    with open(settings_path, "r", encoding="utf-8") as settings_file:
        loaded = json.load(settings_file)

    required_roots = {"execution", "checking_window", "doctors", "notifications"}
    missing_roots = [section for section in required_roots if section not in loaded]
    if missing_roots:
        raise ValueError(
            f"Missing required settings section(s): {', '.join(missing_roots)}"
        )
    return loaded


runtime_settings = load_runtime_settings()

# yyyy-mm-dd
today_date = datetime.date.today()
raw_window_start = runtime_settings["checking_window"].get("start")
raw_window_end = runtime_settings["checking_window"].get("end")

if raw_window_start:
    parsed_window_start = datetime.datetime.strptime(raw_window_start, "%Y-%m-%d").date()
    window_start_date = max(parsed_window_start, today_date)
else:
    window_start_date = today_date

if raw_window_end:
    window_end_date = datetime.datetime.strptime(raw_window_end, "%Y-%m-%d").date()
else:
    window_end_date = window_start_date + datetime.timedelta(days=15)

search_start_day = window_start_date.strftime("%Y-%m-%d")
search_end_day = window_end_date.strftime("%Y-%m-%d")

# max number of days in advance
search_horizon_days = int(runtime_settings["checking_window"]["look_for_n_days"])

# URL templates from settings. Each entry can be:
# - a direct availabilities.json URL
# - a booking URL (auto-resolved)
# - a page URL that contains availabilities.json URLs in HTML
doctor_sources = runtime_settings["doctors"]
execution_settings = runtime_settings["execution"]
check_interval_seconds = int(execution_settings["check_in_n_seconds"])
delivery_settings = runtime_settings["notifications"]

# Track which URLs have already notified (to only notify once per URL)
already_alerted_urls = set()


def publish_ntfy_alert(alert_text):
    """Send a notification via ntfy.sh."""

    ntfy_config = delivery_settings.get("ntfy") or {}
    topic_name = ntfy_config.get("topic")
    if not topic_name:
        raise ValueError("Missing notifications.ntfy.topic in settings.json")

    ntfy_server = (ntfy_config.get("server") or "https://ntfy.sh").rstrip("/")
    response = requests.post(
        f"{ntfy_server}/{topic_name}",
        data=alert_text.encode("utf-8"),
        headers={"Title": "Doctolib checker", "Priority": "default"},
        timeout=15,
    )
    response.raise_for_status()


def send_gmail_alert(alert_text):
    """Send an email via Gmail SMTP using App Password."""

    email_config = delivery_settings.get("email") or {}
    sender_mailbox = email_config.get("sender")
    app_secret = email_config.get("app_password")
    recipient_mailbox = email_config.get("recipient")
    smtp_host = email_config.get("smtp_host") or "smtp.gmail.com"
    smtp_port = int(email_config.get("smtp_port", 587))

    if not sender_mailbox or not app_secret or not recipient_mailbox:
        raise ValueError(
            "Missing notifications.email sender/app_password/recipient in settings.json"
        )

    email_message = EmailMessage()
    email_message["Subject"] = "Doctolib checker alert"
    email_message["From"] = sender_mailbox
    email_message["To"] = recipient_mailbox
    email_message.set_content(alert_text)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp_session:
        smtp_session.starttls()
        smtp_session.login(sender_mailbox, app_secret)
        smtp_session.send_message(email_message)


def dispatch_alert(alert_text, via_ntfy=False, via_email=False):
    """Send alerts via selected channels."""

    if via_ntfy:
        publish_ntfy_alert(alert_text)
    if via_email:
        send_gmail_alert(alert_text)


def normalize_availabilities_url(url):
    """Normalize an availabilities URL and enforce start_date/limit placeholders."""

    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        return ""

    if not parsed.path.endswith("availabilities.json"):
        return ""

    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    filtered_items = [
        (key, value)
        for key, value in query_items
        if key not in {"start_date", "limit", "master_patient_signed_id", "masterPatientSignedId"}
    ]
    filtered_items.append(("start_date", "%(start_date)s"))
    filtered_items.append(("limit", "%(limit)s"))

    normalized_query = urllib.parse.urlencode(filtered_items)
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, normalized_query, "")
    )


def first_query_value(query, keys):
    """Get first non-empty query value for any key in keys."""

    for key in keys:
        value = query.get(key, [None])[0]
        if value:
            return value
    return None


def extract_booking_params(url):
    """Extract booking parameters from booking URL query."""

    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    motive_id = first_query_value(query, ["motiveIds[]", "vmids[]"])
    speciality_id = first_query_value(query, ["specialityId", "speciality_ids[]"])
    place_or_pid = first_query_value(query, ["placeId", "pid"])
    insurance = first_query_value(query, ["insuranceSector", "insurance_sector"])
    telehealth = first_query_value(query, ["telehealth"])

    practice_id = None
    if place_or_pid:
        practice_id = (
            place_or_pid.split("practice-", 1)[1]
            if place_or_pid.startswith("practice-")
            else place_or_pid
        )

    return {
        "motive_id": motive_id,
        "speciality_id": speciality_id,
        "practice_id": practice_id,
        "insurance": insurance,
        "telehealth": telehealth,
        "parsed_url": parsed,
        "query": query,
    }


def collect_agenda_values(data):
    """Collect candidate agenda values recursively from dict/list JSON structures."""

    agenda_values = []
    if isinstance(data, dict):
        for key, value in data.items():
            if key in {"agenda_ids", "agenda_id", "agendaIds", "agendaId"}:
                if isinstance(value, list):
                    agenda_values.extend(str(item) for item in value if str(item))
                elif value is not None:
                    agenda_values.append(str(value))
            agenda_values.extend(collect_agenda_values(value))
    elif isinstance(data, list):
        for item in data:
            agenda_values.extend(collect_agenda_values(item))

    return agenda_values


def normalize_agenda_ids(values):
    """Normalize agenda IDs into single dash-separated format expected by availabilities.json."""

    normalized = []
    for value in values:
        if not value:
            continue
        if isinstance(value, str) and "-" in value:
            parts = value.split("-")
        elif isinstance(value, Iterable) and not isinstance(value, str):
            parts = [str(item) for item in value]
        else:
            parts = [str(value)]

        for part in parts:
            part = part.strip()
            if part and part not in normalized:
                normalized.append(part)

    return "-".join(normalized)


def fetch_funnel_info_json(booking_url, booking_params):
    """Fetch slot selection funnel info JSON to derive agenda IDs when missing."""

    parsed_url = booking_params["parsed_url"]
    original_query_items = urllib.parse.parse_qsl(
        parsed_url.query,
        keep_blank_values=True,
    )
    sanitized_query_items = [
        (key, value)
        for key, value in original_query_items
        if key != "masterPatientSignedId"
    ]

    if not any(key == "placeId" for key, _ in sanitized_query_items):
        sanitized_query_items.append(("placeId", f"practice-{booking_params['practice_id']}"))

    info_url = urllib.parse.urlunsplit(
        (
            parsed_url.scheme or "https",
            parsed_url.netloc or "www.doctolib.de",
            "/slot_selection_funnel/v1/info.json",
            urllib.parse.urlencode(sanitized_query_items, doseq=True),
            "",
        )
    )

    req = urllib.request.Request(info_url, headers={"User-Agent": "Magic Browser"})
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read())


def capture_availability_template_with_playwright(booking_url):
    """Capture availabilities.json URL from booking flow via browser network events."""

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as error:
        raise ValueError(
            "Playwright is required for booking URL auto-capture. "
            "Install with: pip install playwright && playwright install chromium"
        ) from error

    captured_templates = []
    click_selectors = [
        "[data-test*='visit-motive'] button",
        "[data-test*='visit-motive-card']",
        "[data-test*='motive'] button",
        "button[aria-pressed='false']",
        "button",
    ]

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def on_response(response):
            if "availabilities.json" not in response.url:
                return
            if response.request.method != "GET":
                return

            normalized = normalize_availabilities_url(response.url)
            if normalized and normalized not in captured_templates:
                captured_templates.append(normalized)

        page.on("response", on_response)

        try:
            page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            for selector in click_selectors:
                if captured_templates:
                    break
                locator = page.locator(selector)
                if locator.count() == 0:
                    continue
                try:
                    locator.first.click(timeout=2000)
                    page.wait_for_timeout(2500)
                except PlaywrightTimeoutError:
                    continue

            if captured_templates:
                return captured_templates[0]

            raise ValueError("Could not capture availabilities.json from booking page.")
        finally:
            browser.close()


def build_availability_template(booking_params, agenda_ids):
    """Build availabilities.json URL template from booking parameters."""

    query_items = [
        ("visit_motive_ids", booking_params["motive_id"]),
        ("practice_ids", booking_params["practice_id"]),
        ("insurance_sector", booking_params["insurance"]),
        ("telehealth", booking_params["telehealth"]),
    ]

    query_items.append(("agenda_ids", agenda_ids))

    query_items.extend(
        [
            ("start_date", "%(start_date)s"),
            ("limit", "%(limit)s"),
        ]
    )

    return urllib.parse.urlunsplit(
        (
            "https",
            booking_params["parsed_url"].netloc or "www.doctolib.de",
            "/availabilities.json",
            urllib.parse.urlencode(query_items),
            "",
        )
    )


def resolve_booking_url_to_availability_template(booking_url, forced_agenda_ids=None):
    """Resolve a booking URL into a normalized availabilities.json template."""

    booking_params = extract_booking_params(booking_url)
    required = ["motive_id", "practice_id", "insurance", "telehealth"]
    missing = [field for field in required if not booking_params[field]]
    if missing:
        raise ValueError(f"Booking URL is missing required fields: {', '.join(missing)}")

    query = booking_params["query"]
    agenda_ids = normalize_agenda_ids(
        query.get("agenda_ids", [])
        + query.get("agenda_ids[]", [])
        + query.get("agendaIds", [])
        + query.get("agendaIds[]", [])
    )
    if forced_agenda_ids:
        agenda_ids = normalize_agenda_ids([forced_agenda_ids])

    if not agenda_ids:
        funnel_error = None
        try:
            funnel_json = fetch_funnel_info_json(booking_url, booking_params)
            agenda_ids = normalize_agenda_ids(collect_agenda_values(funnel_json))
        except urllib.error.HTTPError as error:
            funnel_error = f"Could not fetch agenda_ids from funnel endpoint ({error.code})."
        except urllib.error.URLError as error:
            funnel_error = (
                f"Could not fetch agenda_ids from funnel endpoint ({error.reason})."
            )

    if not agenda_ids:
        try:
            return capture_availability_template_with_playwright(booking_url)
        except ValueError as capture_error:
            prefix = f"{funnel_error} " if funnel_error else ""
            raise ValueError(
                f"{prefix}Playwright capture also failed: {capture_error}"
            ) from capture_error

    return build_availability_template(booking_params, agenda_ids)


def parse_url_entry(url_entry):
    """Normalize settings URL entry into (url, agenda_override)."""

    if isinstance(url_entry, str):
        return url_entry, None

    if isinstance(url_entry, dict):
        url = url_entry.get("url")
        agenda_override = (
            url_entry.get("agenda_ids")
            or url_entry.get("agenda_id")
            or url_entry.get("agendaIds")
        )
        if not url:
            raise ValueError("URL entry object must include a 'url' field.")
        return url, agenda_override

    raise ValueError("Each entry in settings 'doctors' must be a string or object.")


def extract_availabilities_urls_from_page(page_url):
    """Fetch a Doctolib page and extract unique availabilities.json URLs."""

    req = urllib.request.Request(page_url, headers={"User-Agent": "Magic Browser"})
    with urllib.request.urlopen(req) as response:
        html = response.read().decode("utf-8", errors="ignore")

    # Doctolib often stores URLs in escaped form (e.g. https:\/\/... or \/availabilities.json?...).
    normalized_html = html.replace("\\/", "/")
    candidates = []
    candidates.extend(
        re.findall(
            r"https?://[^\"'\s<>]+availabilities\.json[^\"'\s<>]*",
            normalized_html,
        )
    )
    candidates.extend(
        re.findall(
            r"/[^\"'\s<>]*availabilities\.json[^\"'\s<>]*",
            normalized_html,
        )
    )

    unique_urls = []
    seen = set()
    for candidate in candidates:
        if candidate.startswith("/"):
            candidate = urllib.parse.urljoin(page_url, candidate)
        normalized = normalize_availabilities_url(candidate)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_urls.append(normalized)

    return unique_urls


def resolve_url_entry_to_templates(url_entry, agenda_override=None):
    """Resolve one settings URL entry into one or multiple availabilities templates."""

    parsed = urllib.parse.urlsplit(url_entry)

    if parsed.path.endswith("availabilities.json"):
        normalized = normalize_availabilities_url(url_entry)
        return [normalized] if normalized else []

    if "/booking/availabilities" in parsed.path:
        parsed_booking = parse_booking_url(url_entry)
        if not parsed_booking["profile_slug"]:
            raise ValueError("Could not extract profile_slug from booking URL")

        info = fetch_info_json(parsed_booking["profile_slug"])
        mappings = extract_motive_mappings(
            info,
            filter_motive_ids=parsed_booking["motive_ids"] or None,
            practice_id_hint=parsed_booking["practice_id"],
            practitioner_id=parsed_booking["practitioner_id"],
        )
        if not mappings:
            mappings = extract_motive_mappings(info)

        built_urls = build_availability_urls(
            mappings,
            insurance_sector=parsed_booking["insurance_sector"],
            telehealth=parsed_booking["telehealth"],
            start_date=search_start_day,
            limit=search_horizon_days,
        )
        templates = []
        for item in built_urls:
            normalized = normalize_availabilities_url(item["url"])
            if normalized:
                templates.append(normalized)
        if not templates:
            raise ValueError("Builder did not return any usable availabilities URLs")
        return templates

    return extract_availabilities_urls_from_page(url_entry)


def resolve_source_urls(url_entries_to_resolve):
    """Resolve all settings URL entries into final API URLs with concrete start/limit values."""

    resolved_templates = []
    seen_templates = set()

    for entry in url_entries_to_resolve:
        try:
            normalized_entry, agenda_override = parse_url_entry(entry)
            templates = resolve_url_entry_to_templates(normalized_entry, agenda_override)
        except (
            ValueError,
            urllib.error.HTTPError,
            urllib.error.URLError,
            requests.RequestException,
        ) as error:
            print(f"WARNING: Could not resolve URL entry '{entry}': {error}")
            continue

        for template in templates:
            if template and template not in seen_templates:
                seen_templates.add(template)
                resolved_templates.append(template)

    if not resolved_templates:
        raise ValueError(
            "No URL entries could be resolved. Please verify your settings 'doctors' values."
        )

    resolved_urls = []
    for template in resolved_templates:
        parsed = urllib.parse.urlsplit(template)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        query["start_date"] = [search_start_day]
        query["limit"] = [str(search_horizon_days)]
        final_query = urllib.parse.urlencode(query, doseq=True)
        resolved_urls.append(
            urllib.parse.urlunsplit(
                (parsed.scheme, parsed.netloc, parsed.path, final_query, "")
            )
        )

    return resolved_urls


def find_earliest_slot_before_deadline(json_data):
    """Get the closest available time slot"""

    availabilities = json_data["availabilities"]
    closest_slot = ""

    for slot in availabilities:
        if slot["slots"] == []:
            continue

        if datetime.datetime.strptime(
            slot["date"][:10], "%Y-%m-%d"
        ) <= datetime.datetime.strptime(search_end_day, "%Y-%m-%d"):
            closest_slot = slot["slots"][0]
            break

    return closest_slot


def parse_slot_timestamp(date):
    """Get a proper date string
    Input will be something like "2024-11-07T11:20:00.000+01:00"
    Output will be something like "2024-11-07 11:20:00"
    """
    return str(datetime.datetime.strptime(date[:-10], "%Y-%m-%dT%H:%M:%S"))


def main(use_ntfy, use_email):
    global already_alerted_urls

    try:
        urls = resolve_source_urls(doctor_sources)
    except ValueError as error:
        print(f"ERROR: {error}")
        print(
            "Hint: add at least one working availabilities.json URL in settings.json under 'doctors'."
        )
        return 1

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] Starting Doctolib checker...")
    print(f"  Checking {len(urls)} doctor(s) for appointments between {search_start_day} and {search_end_day}")
    print(f"  Interval: {check_interval_seconds}s")
    dispatch_alert(
        f"Doctolib checker started.\nChecking {len(urls)} doctor(s) for appointments up to {search_end_day}.",
        via_ntfy=use_ntfy,
        via_email=use_email,
    )

    iteration = 0
    while True:
        try:
            iteration += 1
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now}] Check #{iteration}")

            for i, url in enumerate(urls, 1):
                try:
                    print(f"  Doctor {i}/{len(urls)}...", end=" ", flush=True)

                    req = urllib.request.Request(url, headers={"User-Agent": "Magic Browser"})
                    con = urllib.request.urlopen(req)
                    json_data = json.loads(con.read())

                    # there is a free slot in the next <limit> days
                    if json_data["total"] > 0:
                        closest_slot = find_earliest_slot_before_deadline(json_data)
                        if closest_slot != "":
                            print(f"FOUND {json_data['total']} slot(s)! Earliest: {parse_slot_timestamp(closest_slot)}")
                            # Only notify if we haven't notified for this URL before
                            if url not in already_alerted_urls:
                                already_alerted_urls.add(url)
                                dispatch_alert(
                                    f"New appointment available on Doctolib (Doctor {i})! \nNumber of available appointments: {json_data['total']} \nEarliest appointment: {parse_slot_timestamp(closest_slot)}",
                                    via_ntfy=use_ntfy,
                                    via_email=use_email,
                                )
                        else:
                            print(f"total={json_data['total']} but no slot before limit date.")

                    # if next available slot is before the limit date
                    elif datetime.datetime.strptime(
                        json_data["next_slot"][:10], "%Y-%m-%d"
                    ) <= datetime.datetime.strptime(search_end_day, "%Y-%m-%d"):
                        print(f"Slot found via next_slot: {parse_slot_timestamp(json_data['next_slot'])}")
                        # Only notify if we haven't notified for this URL before
                        if url not in already_alerted_urls:
                            already_alerted_urls.add(url)
                            dispatch_alert(
                                f"New appointment available on Doctolib (Doctor {i}) within your limit time! \nEarliest appointment: {parse_slot_timestamp(json_data['next_slot'])}",
                                via_ntfy=use_ntfy,
                                via_email=use_email,
                            )

                    else:
                        next_slot = json_data.get("next_slot", "unknown")
                        print(f"No slot. Next available: {next_slot}")

                    # 1 second delay between each doctor check to avoid getting banned
                    if i < len(urls):
                        time.sleep(1)

                except Exception as e:
                    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"\n[{now}] ERROR checking doctor {i}: {e}")
                    # Don't send notification for individual doctor errors, continue with next

            time.sleep(check_interval_seconds)

        except Exception as e:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{now}] ERROR: {e}")
            dispatch_alert(
                f"An error occured while running the Doctolib script: {e}",
                via_ntfy=use_ntfy,
                via_email=use_email,
            )

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--notify", action="store_true", help="Send alerts through ntfy.sh")
    parser.add_argument("--email", action="store_true", help="Send alerts through Gmail SMTP")
    parser.add_argument("--notify-test", action="store_true", help="Send a test ntfy.sh notification")
    parser.add_argument("--email-test", action="store_true", help="Send a test Gmail email")
    args = parser.parse_args()

    if args.notify_test:
        print("Sending test ntfy notification...")
        publish_ntfy_alert("Test notification from Doctolib checker. If you see this, ntfy.sh is working!")
        print("Done.")
    elif args.email_test:
        print("Sending test email...")
        send_gmail_alert("Test email from Doctolib checker. If you see this, Gmail SMTP is working!")
        print("Done.")
    else:
        if not args.notify and not args.email:
            print("ERROR: choose at least one alert channel: --notify and/or --email")
            sys.exit(1)
        sys.exit(main(use_ntfy=args.notify, use_email=args.email))
