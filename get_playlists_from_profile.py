import requests
import gspread
import json
import os
import re
import time
from google.oauth2.service_account import Credentials

# ============================================================
#  SETTINGS — edit these
# ============================================================

SPREADSHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE"
# Same Sheet ID as your scraper.py

PROFILE_DUMP_SHEET = "profile link dump"
# Tab that has your profile URLs in column A

PLAYLIST_OUTPUT_SHEET = "playlist urls"
# Tab where this script will write all discovered playlist URLs
# It will CREATE this tab automatically if it doesn't exist

DELAY = 1  # seconds between requests

# ============================================================
#  HOW TO USE THIS SCRIPT
# ============================================================
#
#  Run this script ONCE manually whenever you want to
#  discover all playlists from the profiles in your dump sheet.
#
#  It will:
#  1. Read profile URLs from "profile link dump" tab
#  2. For each profile, find all their public playlists
#  3. Write all playlist URLs into "playlist urls" tab
#
#  After this runs, your daily scraper.py uses "playlist urls"
#  to track follower counts every day automatically.
#
#  Run it again anytime to pick up newly added playlists.
# ============================================================

# ============================================================
#  GET FREE TOKEN FROM SPOTIFY WEB PLAYER
#  No account, no API key, no TOTP needed
#  Uses the same token Spotify's own search page uses
# ============================================================

def get_free_token():
    print("🔑 Getting free Spotify token...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    try:
        # Hit the search page — it sends back a token in the HTML
        response = requests.get("https://open.spotify.com/search", headers=headers, timeout=15)

        if response.status_code != 200:
            print(f"   ⚠️ Search page status: {response.status_code}")
            return None

        # Extract the token from the page's __NEXT_DATA__ JSON blob
        match = re.search(r'"accessToken":"([^"]+)"', response.text)
        if match:
            token = match.group(1)
            print(f"   ✅ Got token: {token[:20]}...")
            return token

        # Fallback: look for it in a different format
        match2 = re.search(r'accessToken%22%3A%22([^%]+)%22', response.text)
        if match2:
            token = match2.group(1)
            print(f"   ✅ Got token (fallback): {token[:20]}...")
            return token

        print("   ❌ Could not find token in page")
        return None

    except Exception as e:
        print(f"   ❌ Token fetch error: {e}")
        return None

# ============================================================
#  GET ALL PUBLIC PLAYLISTS FOR A USER
# ============================================================

def get_user_playlists(user_id, token):
    all_playlists = []
    url = f"https://api.spotify.com/v1/users/{user_id}/playlists?limit=50&offset=0"
    headers = {"Authorization": f"Bearer {token}"}

    while url:
        try:
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 3))
                print(f"   ⏳ Rate limited — waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            if response.status_code == 401:
                print(f"   ❌ Token expired or invalid (401)")
                return None  # Signal to refresh token

            if response.status_code != 200:
                print(f"   ⚠️ Response {response.status_code} for {user_id}")
                break

            data = response.json()
            items = data.get("items", [])

            for item in items:
                if not item:
                    continue
                owner_id = item.get("owner", {}).get("id", "")
                # Only include playlists OWNED by this user (not just followed)
                if owner_id == user_id:
                    ext_urls = item.get("external_urls", {})
                    playlist_url = ext_urls.get("spotify", "")
                    if playlist_url:
                        all_playlists.append({
                            "name": item.get("name", "Unknown"),
                            "url": playlist_url,
                            "id": item.get("id", ""),
                            "owner_id": owner_id
                        })

            url = data.get("next")  # pagination — None when done
            if url:
                time.sleep(DELAY)

        except Exception as e:
            print(f"   ❌ Error fetching playlists for {user_id}: {e}")
            break

    return all_playlists

# ============================================================
#  CONNECT TO GOOGLE SHEETS
# ============================================================

def connect_to_sheets():
    print("🔗 Connecting to Google Sheets...")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")

    if creds_json:
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(
            creds_dict,
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

    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    print("✅ Connected")
    return spreadsheet

# ============================================================
#  READ PROFILE URLS
# ============================================================

def get_profile_urls(spreadsheet):
    print("📋 Reading profile URLs...")
    sheet = spreadsheet.worksheet(PROFILE_DUMP_SHEET)
    all_values = sheet.col_values(1)
    urls = []
    for url in all_values[1:]:
        url = url.strip()
        if "/user/" in url:
            urls.append(url)
        elif url and "/playlist/" not in url:
            print(f"   ⚠️ Skipping non-user URL: {url[:50]}")
    print(f"   Found {len(urls)} profile URLs")
    return urls

# ============================================================
#  WRITE PLAYLIST URLS TO OUTPUT SHEET
# ============================================================

def write_playlists_to_sheet(spreadsheet, all_playlists):
    print(f"\n📝 Writing {len(all_playlists)} playlists to '{PLAYLIST_OUTPUT_SHEET}' tab...")

    try:
        sheet = spreadsheet.worksheet(PLAYLIST_OUTPUT_SHEET)
        print("   Tab exists — will add new playlists only")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=PLAYLIST_OUTPUT_SHEET, rows=2000, cols=5)
        print("   Created new tab")

    existing_data = sheet.get_all_values()

    # Set headers if new
    if not existing_data or existing_data[0] != ["Playlist Name", "Playlist URL", "Owner ID", "Added"]:
        sheet.update("A1:D1", [["Playlist Name", "Playlist URL", "Owner ID", "Added"]])
        sheet.format("A1:D1", {"textFormat": {"bold": True}})
        existing_data = sheet.get_all_values()

    # Build set of existing URLs to avoid duplicates
    existing_urls = set()
    for row in existing_data[1:]:
        if len(row) > 1 and row[1]:
            existing_urls.add(row[1].strip())

    # Find next empty row
    next_row = len(existing_data) + 1
    added = 0

    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")

    for p in all_playlists:
        clean_url = p["url"].split("?")[0]  # remove tracking params
        if clean_url not in existing_urls:
            sheet.update(f"A{next_row}:D{next_row}", [[
                p["name"],
                clean_url,
                p["owner_id"],
                today
            ]])
            existing_urls.add(clean_url)
            next_row += 1
            added += 1

    print(f"   ✅ Added {added} new playlists ({len(all_playlists) - added} already existed)")
    return added

# ============================================================
#  MAIN
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY PROFILE → PLAYLIST DISCOVERER")
    print("  Run this once to find all playlist URLs")
    print("=" * 55)

    spreadsheet = connect_to_sheets()
    profile_urls = get_profile_urls(spreadsheet)

    if not profile_urls:
        print("\n❌ No profile URLs found.")
        print("   Add Spotify profile URLs to column A of your dump sheet")
        print("   Format: https://open.spotify.com/user/XXXXXXX")
        return

    # Get free token
    token = get_free_token()
    if not token:
        print("\n❌ Could not get Spotify token. Try again later.")
        return

    all_playlists = []

    for i, profile_url in enumerate(profile_urls, 1):
        match = re.search(r"/user/([^/?]+)", profile_url)
        if not match:
            print(f"\n[{i}/{len(profile_urls)}] ⚠️ Can't extract user ID from: {profile_url}")
            continue

        user_id = match.group(1)
        print(f"\n[{i}/{len(profile_urls)}] 👤 {user_id}")

        playlists = get_user_playlists(user_id, token)

        if playlists is None:
            # Token expired — get a new one and retry
            print("   🔄 Refreshing token...")
            token = get_free_token()
            if token:
                playlists = get_user_playlists(user_id, token)

        if playlists:
            print(f"   Found {len(playlists)} playlists owned by this user")
            for p in playlists:
                print(f"      • {p['name']}")
            all_playlists.extend(playlists)
        else:
            print(f"   ⚠️ No public playlists found for {user_id}")

        time.sleep(DELAY)

    if all_playlists:
        write_playlists_to_sheet(spreadsheet, all_playlists)
        print(f"\n✅ Done! {len(all_playlists)} total playlists found across {len(profile_urls)} profiles")
        print(f"   Check the '{PLAYLIST_OUTPUT_SHEET}' tab in your Google Sheet")
        print(f"   Now update scraper.py to read from '{PLAYLIST_OUTPUT_SHEET}' tab")
    else:
        print("\n❌ No playlists found across any profile")

if __name__ == "__main__":
    main()
