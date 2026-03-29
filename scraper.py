import gspread
import time
import json
import os
import re
import requests
from datetime import datetime
from google.oauth2.service_account import Credentials
from spotify_scraper import SpotifyClient

# ============================================================
# SETTINGS
# ============================================================

SPREADSHEET_ID = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
PROFILE_DUMP_SHEET = "profile link dump"
DELAY_BETWEEN_REQUESTS = 2

# ============================================================
# GOOGLE SHEETS
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
    print("✅ Connected to Google Sheets")
    return spreadsheet

# ============================================================
# GET URLS
# ============================================================

def get_profile_urls(spreadsheet):
    sheet = spreadsheet.worksheet(PROFILE_DUMP_SHEET)
    values = sheet.col_values(1)
    return [v.strip() for v in values[1:] if v.strip()]

def extract_user_id(url):
    m = re.search(r"/user/([^/?]+)", url)
    return m.group(1) if m else None

# ============================================================
# 🔥 FETCH ALL PLAYLISTS (UNLIMITED)
# ============================================================

def get_all_playlists(user_id):
    print("   🔄 Fetching ALL playlists...")

    url = f"https://spclient.wg.spotify.com/user-profile-view/v3/profile/{user_id}/playlists"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "App-platform": "WebPlayer"
    }

    playlists = []
    offset = 0
    limit = 50

    while True:
        params = {"offset": offset, "limit": limit}

        try:
            res = requests.get(url, headers=headers, params=params, timeout=15)

            if res.status_code != 200:
                print(f"   ❌ API error: {res.status_code}")
                break

            data = res.json()
            items = data.get("public_playlists", [])

            if not items:
                break

            for item in items:
                uri = item.get("uri", "")
                pid = uri.split(":")[-1] if uri else None

                if pid:
                    playlists.append({
                        "name": item.get("name", "Unknown"),
                        "url": f"https://open.spotify.com/playlist/{pid}"
                    })

            offset += limit
            time.sleep(1)

        except Exception as e:
            print(f"   ❌ Error fetching playlists: {e}")
            break

    print(f"   ✅ Total playlists found: {len(playlists)}")
    return playlists

# ============================================================
# SCRAPE PROFILE
# ============================================================

def scrape_profile(profile_url, client):
    user_id = extract_user_id(profile_url)

    if not user_id:
        print("   ❌ Invalid profile URL")
        return None, []

    print(f"   🎵 Profile: {user_id}")

    playlists = []
    display_name = user_id

    # 🔥 Get ALL playlists
    all_playlists = get_all_playlists(user_id)

    for p in all_playlists:
        try:
            data = client.get_playlist_info(p["url"])

            if not data:
                continue

            followers_raw = data.get("followers", 0)
            followers = followers_raw.get("total", 0) if isinstance(followers_raw, dict) else int(followers_raw or 0)

            playlists.append({
                "name": p["name"],
                "url": p["url"],
                "followers": followers
            })

            print(f"      ✓ {p['name']} — {followers}")
            time.sleep(DELAY_BETWEEN_REQUESTS)

        except Exception as e:
            print(f"      ⚠️ Error: {e}")

    return display_name, playlists

# ============================================================
# GOOGLE SHEETS WRITE
# ============================================================

def update_followers_sheet(spreadsheet, profile_url, display_name, playlists):
    if not playlists:
        return

    name = re.sub(r'[^a-zA-Z0-9]', '_', display_name)
    sheet_name = name + "_Followers"

    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=50)

    data = sheet.get_all_values()

    if not data:
        sheet.update("A1:C1", [["Profile", display_name, profile_url]])
        sheet.update("A2:B2", [["Playlist", "URL"]])

    col = len(sheet.row_values(2)) + 1
    timestamp = datetime.now().strftime("%d %b %H:%M")
    sheet.update_cell(2, col, timestamp)

    for i, p in enumerate(playlists, start=3):
        sheet.update_cell(i, 1, p["name"])
        sheet.update_cell(i, 2, p["url"])
        sheet.update_cell(i, col, p["followers"])

    print(f"   📊 Updated sheet: {sheet_name}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 50)
    print("SPOTIFY TRACKER — FULL PLAYLIST MODE")
    print("=" * 50)

    sheet = connect_to_sheets()
    urls = get_profile_urls(sheet)

    if not urls:
        print("❌ No URLs")
        return

    client = SpotifyClient()

    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] {url}")

        name, playlists = scrape_profile(url, client)

        if playlists:
            update_followers_sheet(sheet, url, name, playlists)
        else:
            print("   ⚠️ No data")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    if hasattr(client, "close"):
        client.close()

    print("\n✅ DONE")

if __name__ == "__main__":
    main()
