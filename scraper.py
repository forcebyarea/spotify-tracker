import gspread
import requests
import time
import json
import os
from datetime import datetime
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup

# ============================================================
#  SETTINGS — only edit this section
# ============================================================

SPREADSHEET_ID = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
# How to find it: open your Google Sheet, look at the URL:
# https://docs.google.com/spreadsheets/d/THIS_PART_HERE/edit
# Copy the long string between /d/ and /edit

PROFILE_DUMP_SHEET = "profile link dump"
# This must match your sheet tab name exactly

DELAY_BETWEEN_REQUESTS = 3
# Seconds to wait between Spotify requests (be polite, avoid blocks)

# ============================================================
#  STEP 1 — Connect to Google Sheets
# ============================================================

def connect_to_sheets():
    print("🔗 Connecting to Google Sheets...")

    # GitHub Actions stores the key as an environment variable
    # When running locally, it reads from service_account.json file
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")

    if creds_json:
        # Running on GitHub Actions
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
        )
    else:
        # Running locally — reads service_account.json from same folder
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
#  STEP 2 — Read profile URLs from your dump sheet
# ============================================================

def get_profile_urls(spreadsheet):
    print("📋 Reading profile URLs from dump sheet...")
    sheet = spreadsheet.worksheet(PROFILE_DUMP_SHEET)
    all_values = sheet.col_values(1)  # Column A
    # Skip header row, remove empty cells
    urls = [url.strip() for url in all_values[1:] if url.strip()]
    print(f"   Found {len(urls)} profile URLs")
    return urls

# ============================================================
#  STEP 3 — Scrape a Spotify profile page
#  Gets all playlist names, URLs, and follower counts
#  No API key. No login. Just HTTP requests.
# ============================================================

def scrape_profile(profile_url):
    user_id = extract_user_id(profile_url)
    if not user_id:
        print(f"   ⚠️ Could not extract user ID from: {profile_url}")
        return None, []

    print(f"   🎵 Scraping profile: {user_id}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://open.spotify.com/"
    }

    playlists = []
    display_name = user_id

    try:
        # Spotify's internal API endpoint — no auth needed for public profiles
        api_url = f"https://spclient.wg.spotify.com/user-profile-view/v3/profile/{user_id}/playlists"
        
        response = requests.get(api_url, headers=headers, timeout=15)

        if response.status_code == 200:
            data = response.json()
            
            # Get display name
            display_name = data.get("name", user_id)

            # Get playlists
            public_playlists = data.get("public_playlists", [])
            
            for item in public_playlists:
                playlist_uri = item.get("uri", "")  # e.g. spotify:playlist:37i9dQZF1DX...
                playlist_id = playlist_uri.split(":")[-1] if ":" in playlist_uri else ""
                playlist_name = item.get("name", "Unknown")
                playlist_url = f"https://open.spotify.com/playlist/{playlist_id}"

                if playlist_id:
                    # Get follower count for this specific playlist
                    followers = scrape_playlist_followers(playlist_id, headers)
                    playlists.append({
                        "name": playlist_name,
                        "url": playlist_url,
                        "followers": followers
                    })
                    print(f"      ✓ {playlist_name}: {followers:,} followers")
                    time.sleep(DELAY_BETWEEN_REQUESTS)
        else:
            print(f"   ⚠️ Profile response status: {response.status_code}")

    except Exception as e:
        print(f"   ❌ Error scraping {user_id}: {e}")

    return display_name, playlists

# ============================================================
#  STEP 4 — Scrape follower count for a single playlist
# ============================================================

def scrape_playlist_followers(playlist_id, headers):
    try:
        # Use Spotify's embed page — publicly available, no auth
        embed_url = f"https://open.spotify.com/embed/playlist/{playlist_id}"
        response = requests.get(embed_url, headers=headers, timeout=15)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # The page bakes data into a <script id="__NEXT_DATA__"> tag
            next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if next_data_tag:
                next_data = json.loads(next_data_tag.string)
                # Navigate the JSON tree to find follower count
                props = next_data.get("props", {})
                page_props = props.get("pageProps", {})
                state = page_props.get("state", {})
                data = state.get("data", {})
                attributes = data.get("attributes", {})
                followers_obj = attributes.get("followers", {})
                total = followers_obj.get("total", 0)
                if total:
                    return total

        # Fallback: try direct API endpoint (sometimes works without token)
        api_url = f"https://api.spotify.com/v1/playlists/{playlist_id}?fields=followers"
        api_response = requests.get(api_url, headers=headers, timeout=15)
        if api_response.status_code == 200:
            data = api_response.json()
            return data.get("followers", {}).get("total", 0)

        return 0

    except Exception as e:
        print(f"      ⚠️ Could not get followers for {playlist_id}: {e}")
        return 0

# ============================================================
#  STEP 5 — Write data into Google Sheets
# ============================================================

def update_followers_sheet(spreadsheet, profile_url, display_name, playlists):
    if not playlists:
        print(f"   ⚠️ No playlists to write for {display_name}")
        return

    # Clean sheet name
    clean_name = "".join(c if c.isalnum() else "_" for c in display_name)
    sheet_name = clean_name + "_Followers"

    # Get or create the sheet
    try:
        sheet = spreadsheet.worksheet(sheet_name)
        print(f"   📄 Updating existing sheet: {sheet_name}")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=50)
        print(f"   📄 Created new sheet: {sheet_name}")

    # Read existing data
    existing_data = sheet.get_all_values()

    # Set up header rows if sheet is new
    if not existing_data or len(existing_data) < 2:
        sheet.update("A1:C1", [["Profile Name", display_name, profile_url]])
        sheet.format("A1:C1", {"textFormat": {"bold": True}})
        sheet.update("A2:B2", [["Playlist Name", "Playlist URL"]])
        sheet.format("A2:B2", {"textFormat": {"bold": True}})
        existing_data = sheet.get_all_values()

    # Add today's date as new column header
    today = datetime.now().strftime("%d %b %H:%M")
    last_col = len(existing_data[1]) if len(existing_data) > 1 else 2
    new_col_index = last_col + 1  # 1-based for gspread

    # Write date header in row 2
    sheet.update_cell(2, new_col_index, today)

    # Build URL → row map from existing data
    url_to_row = {}
    for i, row in enumerate(existing_data[2:], start=3):  # row 3 onwards, 1-based
        if len(row) > 1 and row[1]:
            url_to_row[row[1]] = i

    next_new_row = len(existing_data) + 1

    # Write each playlist
    updates = []
    for playlist in playlists:
        if playlist["url"] in url_to_row:
            row_num = url_to_row[playlist["url"]]
        else:
            row_num = next_new_row
            next_new_row += 1
            url_to_row[playlist["url"]] = row_num

        # Write name and URL
        sheet.update_cell(row_num, 1, playlist["name"])
        sheet.update_cell(row_num, 2, playlist["url"])

        # Write follower count in new column
        sheet.update_cell(row_num, new_col_index, playlist["followers"])

        # Colour cell based on growth vs previous column
        if new_col_index > 3:
            prev_val = sheet.cell(row_num, new_col_index - 1).value
            if prev_val and prev_val.isdigit():
                prev = int(prev_val)
                curr = playlist["followers"]
                if curr > prev:
                    color = {"red": 1.0, "green": 1.0, "blue": 0.0}   # yellow
                elif curr < prev:
                    color = {"red": 1.0, "green": 0.0, "blue": 0.0}   # red
                else:
                    color = {"red": 1.0, "green": 1.0, "blue": 1.0}   # white
                sheet.format(
                    gspread.utils.rowcol_to_a1(row_num, new_col_index),
                    {"backgroundColor": color}
                )

    print(f"   ✅ Written {len(playlists)} playlists to {sheet_name}")

# ============================================================
#  HELPERS
# ============================================================

def extract_user_id(profile_url):
    import re
    match = re.search(r"(?:user|profile)/([^/?]+)", profile_url)
    return match.group(1) if match else None

# ============================================================
#  MAIN — runs everything
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY FOLLOWER TRACKER — No API, No Limits")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # Connect to Sheets
    spreadsheet = connect_to_sheets()

    # Get all profile URLs from dump sheet
    profile_urls = get_profile_urls(spreadsheet)

    if not profile_urls:
        print("❌ No profile URLs found in dump sheet. Exiting.")
        return

    # Process each profile
    for i, profile_url in enumerate(profile_urls, 1):
        print(f"\n[{i}/{len(profile_urls)}] Processing: {profile_url}")
        display_name, playlists = scrape_profile(profile_url)

        if playlists:
            update_followers_sheet(spreadsheet, profile_url, display_name, playlists)
        else:
            print(f"   ⚠️ Skipping — no playlists found")

        # Wait between profiles to be respectful
        if i < len(profile_urls):
            print(f"   ⏳ Waiting {DELAY_BETWEEN_REQUESTS}s before next profile...")
            time.sleep(DELAY_BETWEEN_REQUESTS)

    print("\n" + "=" * 55)
    print("  ✅ All profiles processed successfully!")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

if __name__ == "__main__":
    main()
