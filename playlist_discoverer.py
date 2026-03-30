import gspread
import json
import os
import re
import time
from datetime import datetime
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# ============================================================
#  SETTINGS — only edit SPREADSHEET_ID
# ============================================================

SPREADSHEET_ID     = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
PROFILE_DUMP_SHEET = "profile link dump"
PLAYLIST_OUT_SHEET = "playlist urls"

# ============================================================
#  CONNECT TO GOOGLE SHEETS
# ============================================================

def connect_to_sheets():
    print("🔗 Connecting to Google Sheets...")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
        )
    else:
        creds = Credentials.from_service_account_file(
            "service_account.json",
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
        )
    client      = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    print("   ✅ Connected")
    return spreadsheet

# ============================================================
#  READ PROFILE URLS
# ============================================================

def get_profile_urls(spreadsheet):
    print("📋 Reading profile URLs...")
    sheet  = spreadsheet.worksheet(PROFILE_DUMP_SHEET)
    values = sheet.col_values(1)
    urls   = [u.strip() for u in values[1:] if u.strip() and "/user/" in u]
    print(f"   Found {len(urls)} profile URLs")
    return urls

# ============================================================
#  LOGIN TO SPOTIFY WITH PLAYWRIGHT
# ============================================================

def login_to_spotify(page):
    email    = os.environ.get("SPOTIFY_EMAIL")
    password = os.environ.get("SPOTIFY_PASSWORD")

    if not email or not password:
        raise Exception(
            "❌ SPOTIFY_EMAIL or SPOTIFY_PASSWORD not found in GitHub Secrets"
        )

    print("🔐 Logging into Spotify...")
    page.goto("https://accounts.spotify.com/en/login", wait_until="networkidle")
    page.wait_for_load_state("networkidle")
    time.sleep(3)

    # ── STEP 1: Fill email and click Continue ──────────────────
    print("   Step 1: Filling email...")
    email_selectors = [
        'input[data-testid="login-username"]',
        'input[name="username"]',
        'input[type="email"]',
        'input[type="text"]',
    ]
    email_filled = False
    for sel in email_selectors:
        try:
            page.wait_for_selector(sel, timeout=5000, state="visible")
            page.click(sel)
            time.sleep(0.5)
            page.fill(sel, email)
            time.sleep(0.5)
            if page.input_value(sel):
                print(f"   ✓ Email filled ({sel})")
                email_filled = True
                break
        except:
            continue
    if not email_filled:
        raise Exception("❌ Could not fill email field")

    # Click Continue button
    continue_selectors = [
        'button:has-text("Continue")',
        'button[data-testid="login-button"]',
        'button[type="submit"]',
    ]
    for sel in continue_selectors:
        try:
            page.wait_for_selector(sel, timeout=5000, state="visible")
            page.click(sel)
            print(f"   ✓ Clicked Continue ({sel})")
            break
        except:
            continue

    # Wait for next page to load (OTP screen)
    time.sleep(4)
    page.wait_for_load_state("networkidle")
    print(f"   Current URL after Continue: {page.url[:80]}")

    # ── STEP 2: Click "Log in with a password" ─────────────────
    print("   Step 2: Clicking Log in with a password...")
    password_link_selectors = [
        'button:has-text("Log in with a password")',
        'a:has-text("Log in with a password")',
        '[data-testid="login-with-password-button"]',
        'button:has-text("password")',
        'a:has-text("password")',
        'span:has-text("Log in with a password")',
    ]
    link_clicked = False
    for sel in password_link_selectors:
        try:
            page.wait_for_selector(sel, timeout=6000, state="visible")
            page.click(sel)
            print(f"   ✓ Clicked password link ({sel})")
            link_clicked = True
            break
        except:
            continue
    if not link_clicked:
        raise Exception("❌ Could not find 'Log in with a password' link")

    # Wait for password field to appear
    time.sleep(3)
    page.wait_for_load_state("networkidle")

    # ── STEP 3: Fill password and log in ───────────────────────
    print("   Step 3: Filling password...")
    password_selectors = [
        'input[data-testid="login-password"]',
        'input[name="password"]',
        'input[type="password"]',
    ]
    password_filled = False
    for sel in password_selectors:
        try:
            page.wait_for_selector(sel, timeout=5000, state="visible")
            page.click(sel)
            time.sleep(0.5)
            page.fill(sel, password)
            time.sleep(0.5)
            if page.input_value(sel):
                print(f"   ✓ Password filled ({sel})")
                password_filled = True
                break
        except:
            continue
    if not password_filled:
        raise Exception("❌ Could not fill password field")

    # Click Log in button
    login_selectors = [
        'button[data-testid="login-button"]',
        'button:has-text("Log in")',
        'button[type="submit"]',
    ]
    for sel in login_selectors:
        try:
            page.wait_for_selector(sel, timeout=5000, state="visible")
            page.click(sel)
            print(f"   ✓ Clicked Log in ({sel})")
            break
        except:
            continue

    # Wait for successful redirect
    try:
        page.wait_for_url(
            lambda url: "accounts.spotify.com/en/login" not in url,
            timeout=15000
        )
    except:
        print("   ⚠️ Redirect timeout — checking current URL...")

    time.sleep(2)
    print(f"   ✅ Login complete — URL: {page.url[:80]}")

# ============================================================
#  SCRAPE ALL PLAYLISTS FROM ONE PROFILE
# ============================================================

def scrape_profile_playlists(page, profile_url, user_id):
    print(f"   🌐 Opening profile page...")
    page.goto(profile_url.split("?")[0], wait_until="domcontentloaded")
    time.sleep(3)

    # Scroll down to load all playlists (lazy loaded)
    print("   📜 Scrolling to load all playlists...")
    prev_height = 0
    for _ in range(15):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1.5)
        curr_height = page.evaluate("document.body.scrollHeight")
        if curr_height == prev_height:
            break
        prev_height = curr_height

    # Get page HTML after JS has fully rendered
    html    = page.content()
    content = page.inner_text("body")

    playlist_ids = []

    # Method 1 — extract from __NEXT_DATA__ JSON (most reliable)
    nd_match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html, re.DOTALL
    )
    if nd_match:
        try:
            nd   = json.loads(nd_match.group(1))
            text = json.dumps(nd)
            ids  = re.findall(r'spotify:playlist:([A-Za-z0-9]{22})', text)
            ids += re.findall(r'"id"\s*:\s*"([A-Za-z0-9]{22})"', text)
            playlist_ids = list(dict.fromkeys(ids))
            print(f"   Found {len(playlist_ids)} IDs in __NEXT_DATA__")
        except Exception as e:
            print(f"   ⚠️ __NEXT_DATA__ parse error: {e}")

    # Method 2 — scan all href links on page
    if not playlist_ids:
        links = page.query_selector_all('a[href*="/playlist/"]')
        for link in links:
            href  = link.get_attribute("href") or ""
            match = re.search(r'/playlist/([A-Za-z0-9]{22})', href)
            if match:
                playlist_ids.append(match.group(1))
        playlist_ids = list(dict.fromkeys(playlist_ids))
        print(f"   Found {len(playlist_ids)} IDs from href links")

    # Method 3 — regex scan entire HTML
    if not playlist_ids:
        ids  = re.findall(r'spotify:playlist:([A-Za-z0-9]{22})', html)
        ids += re.findall(r'/playlist/([A-Za-z0-9]{22})', html)
        playlist_ids = list(dict.fromkeys(ids))
        print(f"   Found {len(playlist_ids)} IDs from raw HTML scan")

    # Method 4 — intercept Spotify's own API calls via network requests
    if not playlist_ids:
        print("   🔄 Trying network intercept method...")
        collected_ids = []

        def handle_response(response):
            if f"/users/{user_id}/playlists" in response.url or \
               f"/user/{user_id}" in response.url:
                try:
                    data  = response.json()
                    items = data.get("items", [])
                    for item in items:
                        if item and item.get("id"):
                            collected_ids.append(item["id"])
                except:
                    pass

        page.on("response", handle_response)
        page.reload(wait_until="networkidle")
        time.sleep(4)

        for _ in range(10):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)

        playlist_ids = list(dict.fromkeys(collected_ids))
        print(f"   Found {len(playlist_ids)} IDs via network intercept")

    return playlist_ids

# ============================================================
#  GET PLAYLIST NAME + OWNER FROM PAGE
# ============================================================

def get_playlist_info_from_page(page, playlist_id, user_id):
    try:
        url = f"https://open.spotify.com/playlist/{playlist_id}"
        page.goto(url, wait_until="domcontentloaded")
        time.sleep(2)

        # Get title from page
        try:
            title = page.title().replace(" | Spotify", "").replace(" - Spotify", "").strip()
        except:
            title = playlist_id

        # Check owner from page content
        html  = page.content()
        owner = ""
        nd    = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html, re.DOTALL
        )
        if nd:
            try:
                data  = json.loads(nd.group(1))
                text  = json.dumps(data)
                match = re.search(r'"owner"\s*:\s*\{[^}]*"id"\s*:\s*"([^"]+)"', text)
                if match:
                    owner = match.group(1)
            except:
                pass

        return title, owner

    except Exception as e:
        print(f"      ⚠️ Error getting playlist info: {e}")
        return playlist_id, ""

# ============================================================
#  WRITE DISCOVERED PLAYLISTS TO GOOGLE SHEETS
# ============================================================

def write_to_sheet(spreadsheet, all_playlists):
    print(f"\n📝 Writing {len(all_playlists)} playlists to '{PLAYLIST_OUT_SHEET}' tab...")

    try:
        sheet = spreadsheet.worksheet(PLAYLIST_OUT_SHEET)
        print("   Tab exists — adding new playlists only")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=PLAYLIST_OUT_SHEET, rows=2000, cols=5)
        print("   Created new tab")

    existing = sheet.get_all_values()

    # Set headers if new
    if not existing or existing[0][:4] != ["Playlist Name", "Playlist URL", "Owner ID", "Profile URL"]:
        sheet.update(values=[["Playlist Name", "Playlist URL", "Owner ID", "Profile URL"]], range_name="A1:D1")
        sheet.format("A1:D1", {"textFormat": {"bold": True}})
        existing = sheet.get_all_values()

    # Build set of existing URLs to avoid duplicates
    existing_urls = set()
    for row in existing[1:]:
        if len(row) > 1 and row[1]:
            existing_urls.add(row[1].strip())

    next_row = len(existing) + 1
    added    = 0

    for p in all_playlists:
        clean_url = p["url"].split("?")[0]
        if clean_url not in existing_urls:
            sheet.update(
                values=[[p["name"], clean_url, p["owner_id"], p["profile_url"]]],
                range_name=f"A{next_row}:D{next_row}"
            )
            existing_urls.add(clean_url)
            next_row += 1
            added    += 1

    skipped = len(all_playlists) - added
    print(f"   ✅ Added {added} new playlists ({skipped} already existed)")

# ============================================================
#  MAIN
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY PLAYLIST DISCOVERER")
    print("  Browser-based — no API restrictions")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    spreadsheet  = connect_to_sheets()
    profile_urls = get_profile_urls(spreadsheet)

    if not profile_urls:
        print("❌ No profile URLs found. Exiting.")
        return

    all_playlists = []

    with sync_playwright() as p:
        # Launch real Chrome browser (headless = no screen needed)
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled"
            ]
        )

        # Create browser context with realistic settings
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            locale="en-US"
        )

        page = context.new_page()

        # Hide automation flags so Spotify doesn't detect the bot
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        """)

        # Login once — reuse session for all profiles
        login_to_spotify(page)

        # Process each profile
        for i, profile_url in enumerate(profile_urls, 1):
            match = re.search(r"/user/([^/?]+)", profile_url)
            if not match:
                print(f"\n[{i}/{len(profile_urls)}] ⚠️ Can't extract user ID — skipping")
                continue

            user_id = match.group(1)
            print(f"\n[{i}/{len(profile_urls)}] 👤 {user_id}")

            playlist_ids = scrape_profile_playlists(page, profile_url, user_id)

            if not playlist_ids:
                print("   ⚠️ No playlists found")
                continue

            print(f"   Getting details for {len(playlist_ids)} playlists...")

            for j, pid in enumerate(playlist_ids, 1):
                name, owner_id = get_playlist_info_from_page(page, pid, user_id)

                # Only keep playlists owned by this user
                if owner_id and owner_id != user_id:
                    continue

                purl = f"https://open.spotify.com/playlist/{pid}"
                all_playlists.append({
                    "name":        name,
                    "url":         purl,
                    "owner_id":    user_id,
                    "profile_url": profile_url
                })
                print(f"      [{j}/{len(playlist_ids)}] ✓ {name}")
                time.sleep(1)

        context.close()
        browser.close()

    if all_playlists:
        write_to_sheet(spreadsheet, all_playlists)
        print(f"\n✅ Done! Found {len(all_playlists)} playlists across {len(profile_urls)} profiles")
        print(f"   Check '{PLAYLIST_OUT_SHEET}' tab in your Google Sheet")
        print(f"   Now run scraper.py to start tracking follower counts")
    else:
        print("\n❌ No playlists found across any profile")

    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
