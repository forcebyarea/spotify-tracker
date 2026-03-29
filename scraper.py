import gspread
import time
import json
import os
import re
from datetime import datetime
from google.oauth2.service_account import Credentials
from spotify_scraper import SpotifyClient

# ============================================================
#  SETTINGS — only edit this section
# ============================================================

SPREADSHEET_ID = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
# Find this in your Sheet URL:
# https://docs.google.com/spreadsheets/d/THIS_IS_YOUR_ID/edit

PLAYLIST_DUMP_SHEET = "playlist urls"
# The tab name in your Google Sheet with playlist URLs in column A

DELAY_BETWEEN_REQUESTS = 2  # seconds between Spotify requests

# ============================================================
#  HOW THIS WORKS
#  -------------------------------------------------------
#  Instead of scraping a profile page (which requires auth),
#  paste individual PLAYLIST URLs directly into your dump sheet.
#  The scraper fetches follower count for each playlist directly.
#
#  Your dump sheet column A should look like:
#  Row 1: (header — anything)
#  Row 2: https://open.spotify.com/playlist/37i9dQZF1DX...
#  Row 3: https://open.spotify.com/playlist/6GIVyMIW8Ji...
#  Row 4: https://open.spotify.com/playlist/4rzfv06Th31...
#  etc.
# ============================================================

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
#  READ PLAYLIST URLS FROM DUMP SHEET
# ============================================================

def get_playlist_urls(spreadsheet):
    print("📋 Reading playlist URLs from playlist urls tab...")
    sheet = spreadsheet.worksheet(PLAYLIST_DUMP_SHEET)
    all_values = sheet.get_all_values()
    urls = []
    for row in all_values[1:]:  # skip header
        if len(row) > 1 and row[1].strip() and "/playlist/" in row[1]:
            urls.append(row[1].strip())
    print(f"   Found {len(urls)} playlist URLs")
    return urls

# ============================================================
#  EXTRACT PLAYLIST ID FROM URL
# ============================================================

def extract_playlist_id(url):
    match = re.search(r"/playlist/([^/?]+)", url)
    return match.group(1) if match else None

def clean_playlist_url(url):
    # Remove tracking parameters like ?si=xxxxx
    return url.split("?")[0]

# ============================================================
#  SCRAPE FOLLOWER COUNT FOR ONE PLAYLIST
# ============================================================

def get_playlist_followers(playlist_url, client):
    try:
        clean_url = clean_playlist_url(playlist_url)
        playlist = client.get_playlist_info(clean_url)

        if not playlist:
            return None, 0

        name = playlist.get("name", "Unknown Playlist")

        # followers is returned as {"total": 1234, "href": null}
        followers_raw = playlist.get("followers", {})
        if isinstance(followers_raw, dict):
            followers = followers_raw.get("total", 0)
        elif isinstance(followers_raw, int):
            followers = followers_raw
        else:
            followers = 0

        owner = playlist.get("owner", {})
        owner_name = owner.get("display_name") or owner.get("id", "Unknown") if isinstance(owner, dict) else "Unknown"

        return name, followers, owner_name

    except Exception as e:
        print(f"      ❌ Error: {e}")
        return None, 0, "Unknown"

# ============================================================
#  WRITE ALL PLAYLIST DATA INTO ONE TRACKING SHEET
#  All playlists go into a single sheet called "Playlist Tracker"
# ============================================================

def update_tracking_sheet(spreadsheet, playlist_data):
    sheet_name = "Playlist Tracker"

    try:
        sheet = spreadsheet.worksheet(sheet_name)
        print(f"\n📄 Updating existing sheet: {sheet_name}")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=100)
        print(f"\n📄 Created new sheet: {sheet_name}")

    existing_data = sheet.get_all_values()

    # Set up header if brand new
    if not existing_data or len(existing_data) < 2:
        sheet.update("A1:C1", [["Playlist Name", "Playlist URL", "Owner"]])
        sheet.format("A1:C1", {"textFormat": {"bold": True}})
        existing_data = sheet.get_all_values()

    # Add today's date as new column header in row 1
    today = datetime.now().strftime("%d %b %H:%M")
    last_col = len(existing_data[0]) if existing_data else 3
    new_col_index = last_col + 1

    sheet.update_cell(1, new_col_index, today)
    sheet.format(
        gspread.utils.rowcol_to_a1(1, new_col_index),
        {"textFormat": {"bold": True}}
    )

    # Build URL → row map from existing data
    url_to_row = {}
    for i, row in enumerate(existing_data[1:], start=2):
        if len(row) > 1 and row[1]:
            url_to_row[row[1]] = i

    next_new_row = len(existing_data) + 1

    written = 0
    for item in playlist_data:
        clean_url = clean_playlist_url(item["url"])

        if clean_url in url_to_row:
            row_num = url_to_row[clean_url]
        else:
            row_num = next_new_row
            next_new_row += 1
            url_to_row[clean_url] = row_num

        # Write name, url, owner
        sheet.update_cell(row_num, 1, item["name"])
        sheet.update_cell(row_num, 2, clean_url)
        sheet.update_cell(row_num, 3, item["owner"])

        # Write follower count in new column
        sheet.update_cell(row_num, new_col_index, item["followers"])

        # Colour based on growth vs previous column
        if new_col_index > 4:
            try:
                prev_val = sheet.cell(row_num, new_col_index - 1).value
                if prev_val and str(prev_val).strip().isdigit():
                    diff = item["followers"] - int(prev_val)
                    if diff > 0:
                        color = {"red": 1.0, "green": 1.0, "blue": 0.0}   # yellow = growth
                    elif diff < 0:
                        color = {"red": 1.0, "green": 0.2, "blue": 0.2}   # red = decline
                    else:
                        color = {"red": 1.0, "green": 1.0, "blue": 1.0}   # white = no change
                    sheet.format(
                        gspread.utils.rowcol_to_a1(row_num, new_col_index),
                        {"backgroundColor": color}
                    )
            except:
                pass

        written += 1

    print(f"   ✅ Written {written} playlists to '{sheet_name}'")

# ============================================================
#  MAIN
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY PLAYLIST FOLLOWER TRACKER")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    spreadsheet = connect_to_sheets()
    playlist_urls = get_playlist_urls(spreadsheet)

    if not playlist_urls:
        print("\n❌ No playlist URLs found in dump sheet.")
        print("   Make sure column A has Spotify playlist URLs like:")
        print("   https://open.spotify.com/playlist/XXXXXXX")
        return

    print(f"\n🎵 Scraping {len(playlist_urls)} playlists...")
    client = SpotifyClient()

    playlist_data = []

    for i, url in enumerate(playlist_urls, 1):
        playlist_id = extract_playlist_id(url)
        if not playlist_id:
            print(f"[{i}/{len(playlist_urls)}] ⚠️ Invalid URL: {url}")
            continue

        print(f"[{i}/{len(playlist_urls)}] Fetching: {url[:60]}...")

        result = get_playlist_followers(url, client)
        if result[0]:  # name is not None
            name, followers, owner = result
            playlist_data.append({
                "name": name,
                "url": url,
                "followers": followers,
                "owner": owner
            })
            print(f"   ✓ {name} — {followers:,} followers (by {owner})")
        else:
            print(f"   ⚠️ Could not fetch data for this playlist")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    client.close()

    if playlist_data:
        update_tracking_sheet(spreadsheet, playlist_data)
    else:
        print("\n❌ No data fetched — nothing written to sheet")

    print("\n" + "=" * 55)
    print(f"  ✅ Done! {len(playlist_data)}/{len(playlist_urls)} playlists tracked")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

if __name__ == "__main__":
    main()
