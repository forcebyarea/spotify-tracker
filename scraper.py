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
            print(f"   ℹ️ Direct playlist URL — scraping it directly")
            try:
                purl = f"https://open.spotify.com/playlist/{playlist_id}"
                playlist = client.get_playlist_info(purl)
                name = playlist.get("name", "Unknown Playlist")
                followers_raw = playlist.get("followers", 0)
                followers = followers_raw.get("total", 0) if isinstance(followers_raw, dict) else int(followers_raw or 0)
                return "Direct Playlist", [{"name": name, "url": purl, "followers": followers}]
            except Exception as e:
                print(f"   ❌ Error: {e}")
                return None, []
        print(f"   ⚠️ Not a valid user or playlist URL")
        return None, []

    print(f"   🎵 Scraping profile: {user_id}")
    playlists = []
    display_name = user_id

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.get(f"https://open.spotify.com/user/{user_id}", headers=headers, timeout=15)

        if response.status_code == 200:
            # Extract playlist IDs from page
            playlist_ids = re.findall(r'spotify:playlist:([A-Za-z0-9]+)', response.text)
            playlist_ids += re.findall(r'"/playlist/([A-Za-z0-9]{22})"', response.text)
            playlist_ids = list(dict.fromkeys(playlist_ids))

            # Try to get display name from __NEXT_DATA__
            nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.DOTALL)
            if nd:
                try:
                    nd_data = json.loads(nd.group(1))
                    profile = nd_data.get("props", {}).get("pageProps", {}).get("profile", {})
                    display_name = profile.get("name", user_id)
                except:
                    pass

            print(f"   Found {len(playlist_ids)} playlists on page")

            for pid in playlist_ids:
                try:
                    purl = f"https://open.spotify.com/playlist/{pid}"
                    playlist = client.get_playlist_info(purl)
                    if not playlist:
                        continue
                    name = playlist.get("name", "Unknown")
                    followers_raw = playlist.get("followers", 0)
                    followers = followers_raw.get("total", 0) if isinstance(followers_raw, dict) else int(followers_raw or 0)
                    owner = playlist.get("owner", {})
                    owner_id = owner.get("id", "") if isinstance(owner, dict) else ""
                    if owner_id and owner_id != user_id:
                        continue
                    playlists.append({"name": name, "url": purl, "followers": followers})
                    print(f"      ✓ {name}: {followers:,} followers")
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                except Exception as e:
                    print(f"      ⚠️ Error on playlist {pid}: {e}")
        else:
            print(f"   ⚠️ Profile page returned: {response.status_code}")

    except Exception as e:
        print(f"   ❌ Error: {e}")

    return display_name, playlists

# ============================================================
#  WRITE TO SHEETS
# ============================================================

def update_followers_sheet(spreadsheet, profile_url, display_name, playlists):
    if not playlists:
        print(f"   ⚠️ Nothing to write for {display_name}")
        return

    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', display_name)
    sheet_name = clean_name + "_Followers"

    try:
        sheet = spreadsheet.worksheet(sheet_name)
        print(f"   📄 Updating: {sheet_name}")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=50)
        print(f"   📄 Created: {sheet_name}")

    existing_data = sheet.get_all_values()

    if not existing_data or len(existing_data) < 2:
        sheet.update("A1:C1", [["Profile Name", display_name, profile_url]])
        sheet.format("A1:C1", {"textFormat": {"bold": True}})
        sheet.update("A2:B2", [["Playlist Name", "Playlist URL"]])
        sheet.format("A2:B2", {"textFormat": {"bold": True}})
        existing_data = sheet.get_all_values()

    today = datetime.now().strftime("%d %b %H:%M")
    last_col = len(existing_data[1]) if len(existing_data) > 1 else 2
    new_col_index = last_col + 1

    sheet.update_cell(2, new_col_index, today)
    sheet.format(gspread.utils.rowcol_to_a1(2, new_col_index), {"textFormat": {"bold": True}})

    url_to_row = {}
    for i, row in enumerate(existing_data[2:], start=3):
        if len(row) > 1 and row[1]:
            url_to_row[row[1]] = i

    next_new_row = len(existing_data) + 1

    for playlist in playlists:
        row_num = url_to_row.get(playlist["url"], next_new_row)
        if playlist["url"] not in url_to_row:
            url_to_row[playlist["url"]] = next_new_row
            next_new_row += 1

        sheet.update_cell(row_num, 1, playlist["name"])
        sheet.update_cell(row_num, 2, playlist["url"])
        sheet.update_cell(row_num, new_col_index, playlist["followers"])

        if new_col_index > 3:
            try:
                prev_val = sheet.cell(row_num, new_col_index - 1).value
                if prev_val and str(prev_val).isdigit():
                    diff = playlist["followers"] - int(prev_val)
                    color = {"red": 1.0, "green": 1.0, "blue": 0.0} if diff > 0 else \
                            {"red": 1.0, "green": 0.2, "blue": 0.2} if diff < 0 else \
                            {"red": 1.0, "green": 1.0, "blue": 1.0}
                    sheet.format(gspread.utils.rowcol_to_a1(row_num, new_col_index), {"backgroundColor": color})
            except:
                pass

    print(f"   ✅ Written {len(playlists)} playlists to {sheet_name}")

# ============================================================
#  MAIN
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY FOLLOWER TRACKER — No API, No Limits")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    spreadsheet = connect_to_sheets()
    profile_urls = get_profile_urls(spreadsheet)

    if not profile_urls:
        print("❌ No URLs found. Exiting.")
        return

    spotify_client = SpotifyClient(rate_limit_delay=DELAY_BETWEEN_REQUESTS)

    for i, profile_url in enumerate(profile_urls, 1):
        print(f"\n[{i}/{len(profile_urls)}] {profile_url}")
        display_name, playlists = scrape_profile(profile_url, spotify_client)
        if playlists:
            update_followers_sheet(spreadsheet, profile_url, display_name, playlists)
        else:
            print(f"   ⚠️ Skipping — no data found")
        if i < len(profile_urls):
            time.sleep(DELAY_BETWEEN_REQUESTS)

    spotify_client.close()
    print("\n" + "=" * 55)
    print("  ✅ Done!")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

if __name__ == "__main__":
    main()
