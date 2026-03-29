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
#  SETTINGS — only edit this section
# ============================================================

SPREADSHEET_ID = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
PROFILE_DUMP_SHEET = "profile link dump"
DELAY_BETWEEN_REQUESTS = 2

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
    print("✅ Connected to Google Sheets")
    return spreadsheet

# ============================================================
#  READ PROFILE URLS
# ============================================================

def get_profile_urls(spreadsheet):
    print("📋 Reading profile URLs from dump sheet...")
    sheet = spreadsheet.worksheet(PROFILE_DUMP_SHEET)
    all_values = sheet.col_values(1)
    urls = [url.strip() for url in all_values[1:] if url.strip()]
    print(f"   Found {len(urls)} URLs")
    return urls

def extract_user_id(url):
    match = re.search(r"/user/([^/?]+)", url)
    return match.group(1) if match else None

def extract_playlist_id(url):
    match = re.search(r"/playlist/([^/?]+)", url)
    return match.group(1) if match else None

# ============================================================
#  SCRAPE PROFILE
# ============================================================

def scrape_profile(profile_url, client):
    user_id = extract_user_id(profile_url)

    if not user_id:
        playlist_id = extract_playlist_id(profile_url)

        if playlist_id:
            print("   ℹ️ Direct playlist URL")

            try:
                purl = f"https://open.spotify.com/playlist/{playlist_id}"
                playlist = client.get_playlist_info(purl)

                if not playlist:
                    return None, []

                followers_raw = playlist.get("followers", 0)
                followers = followers_raw.get("total", 0) if isinstance(followers_raw, dict) else int(followers_raw or 0)

                return "Direct Playlist", [{
                    "name": playlist.get("name", "Unknown"),
                    "url": purl,
                    "followers": followers
                }]

            except Exception as e:
                print(f"   ❌ Error: {e}")
                return None, []

        return None, []

    print(f"   🎵 Scraping profile: {user_id}")
    playlists = []
    display_name = user_id

    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(
            f"https://open.spotify.com/user/{user_id}",
            headers=headers,
            timeout=15
        )

        if response.status_code != 200:
            print(f"   ⚠️ Profile page error: {response.status_code}")
            return display_name, []

        # Extract playlist IDs
        playlist_ids = re.findall(r'spotify:playlist:([A-Za-z0-9]+)', response.text)
        playlist_ids += re.findall(r'"/playlist/([A-Za-z0-9]{22})"', response.text)
        playlist_ids = list(dict.fromkeys(playlist_ids))

        print(f"   Found {len(playlist_ids)} playlists")

        # Extract display name
        nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.DOTALL)
        if nd:
            try:
                nd_data = json.loads(nd.group(1))
                display_name = nd_data.get("props", {}).get("pageProps", {}).get("profile", {}).get("name", user_id)
            except:
                pass

        for pid in playlist_ids:
            try:
                purl = f"https://open.spotify.com/playlist/{pid}"
                playlist = client.get_playlist_info(purl)

                if not playlist:
                    continue

                followers_raw = playlist.get("followers", 0)
                followers = followers_raw.get("total", 0) if isinstance(followers_raw, dict) else int(followers_raw or 0)

                owner = playlist.get("owner", {})
                owner_id = owner.get("id", "") if isinstance(owner, dict) else ""

                if owner_id and owner_id != user_id:
                    continue

                playlists.append({
                    "name": playlist.get("name", "Unknown"),
                    "url": purl,
                    "followers": followers
                })

                print(f"      ✓ {playlist.get('name')} — {followers}")
                time.sleep(DELAY_BETWEEN_REQUESTS)

            except Exception as e:
                print(f"      ⚠️ Error: {e}")

    except Exception as e:
        print(f"   ❌ Error: {e}")

    return display_name, playlists

# ============================================================
#  WRITE TO SHEETS
# ============================================================

def update_followers_sheet(spreadsheet, profile_url, display_name, playlists):
    if not playlists:
        print("   ⚠️ No data to write")
        return

    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', display_name)
    sheet_name = clean_name + "_Followers"

    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=50)

    existing_data = sheet.get_all_values()

    if not existing_data:
        sheet.update("A1:C1", [["Profile Name", display_name, profile_url]])
        sheet.update("A2:B2", [["Playlist Name", "Playlist URL"]])

    today = datetime.now().strftime("%d %b %H:%M")
    col = len(sheet.row_values(2)) + 1

    sheet.update_cell(2, col, today)

    for i, p in enumerate(playlists, start=3):
        sheet.update_cell(i, 1, p["name"])
        sheet.update_cell(i, 2, p["url"])
        sheet.update_cell(i, col, p["followers"])

    print(f"   ✅ Updated sheet: {sheet_name}")

# ============================================================
#  MAIN
# ============================================================

def main():
    print("=" * 55)
    print("SPOTIFY FOLLOWER TRACKER")
    print("=" * 55)

    spreadsheet = connect_to_sheets()
    profile_urls = get_profile_urls(spreadsheet)

    if not profile_urls:
        print("❌ No URLs found")
        return

    # ✅ FIXED
    spotify_client = SpotifyClient()

    for i, url in enumerate(profile_urls, 1):
        print(f"\n[{i}] {url}")

        name, playlists = scrape_profile(url, spotify_client)

        if playlists:
            update_followers_sheet(spreadsheet, url, name, playlists)
        else:
            print("   ⚠️ No playlists found")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # ✅ SAFE CLOSE
    if hasattr(spotify_client, "close"):
        spotify_client.close()

    print("\n✅ DONE")

if __name__ == "__main__":
    main()
