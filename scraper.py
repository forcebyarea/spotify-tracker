import gspread
import requests
import time
import json
import os
import re
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
#  SETTINGS — only edit this section
# ============================================================

SPREADSHEET_ID = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
# Find in your Sheet URL:
# https://docs.google.com/spreadsheets/d/THIS_PART/edit

PROFILE_DUMP_SHEET = "profile link dump"
# Tab name with your Spotify profile URLs in column A

DELAY = 1  # seconds between Spotify API calls

# ============================================================
#  HOW THIS WORKS — one script does everything
#  -------------------------------------------------------
#  Every run it:
#  1. Gets a Spotify token using your Client ID + Secret
#  2. Reads profile URLs from your dump sheet
#  3. For each profile — fetches ALL their public playlists
#  4. For each playlist — fetches current follower count
#  5. Creates/updates a _Followers sheet per profile
#  6. Adds a new date column with today's counts
#  7. Colours cells yellow (growth) or red (decline)
#  8. Sorts playlists by follower count
#
#  Client ID + Secret are stored as GitHub Secrets —
#  never hardcoded, never visible in your code.
# ============================================================


# ============================================================
#  STEP 1 — GET SPOTIFY TOKEN
#  Uses Client Credentials — works for ANY public profile
# ============================================================

def get_spotify_token():
    print("🔑 Getting Spotify token...")

    # Read from GitHub Secrets (environment variables)
    client_id     = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise Exception(
            "❌ SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not found.\n"
            "   Go to GitHub → Settings → Secrets and add both secrets."
        )

    response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        headers={
            "Authorization": "Basic " + __import__("base64").b64encode(
                f"{client_id}:{client_secret}".encode()
            ).decode()
        },
        timeout=15
    )

    if response.status_code != 200:
        raise Exception(f"❌ Token request failed: {response.status_code} — {response.text}")

    token = response.json().get("access_token")
    if not token:
        raise Exception("❌ No access_token in response")

    print("   ✅ Token obtained")
    return token


# ============================================================
#  STEP 2 — CONNECT TO GOOGLE SHEETS
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

    client   = gspread.authorize(creds)
    sheet    = client.open_by_key(SPREADSHEET_ID)
    print("   ✅ Connected")
    return sheet


# ============================================================
#  STEP 3 — READ PROFILE URLS FROM DUMP SHEET
# ============================================================

def get_profile_urls(spreadsheet):
    print("📋 Reading profile URLs...")
    sheet  = spreadsheet.worksheet(PROFILE_DUMP_SHEET)
    values = sheet.col_values(1)
    urls   = [u.strip() for u in values[1:] if u.strip()]

    valid = []
    for url in urls:
        if "/user/" in url or "/profile/" in url:
            valid.append(url)
        elif url:
            print(f"   ⚠️ Skipping non-profile URL: {url[:60]}")

    print(f"   Found {len(valid)} profile URLs")
    return valid


# ============================================================
#  STEP 4 — FETCH ALL PLAYLISTS FOR A PROFILE
# ============================================================

def get_user_playlists(user_id, token):
    playlists = []
    url       = f"https://api.spotify.com/v1/users/{user_id}/playlists?limit=50"
    headers   = {"Authorization": f"Bearer {token}"}

    while url:
        try:
            resp = requests.get(url, headers=headers, timeout=15)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 3))
                print(f"      ⏳ Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                print("      ❌ Token expired")
                return None

            if resp.status_code != 200:
                print(f"      ⚠️ Status {resp.status_code} for {user_id}")
                break

            data  = resp.json()
            items = data.get("items", [])

            for item in items:
                if not item:
                    continue
                # Only playlists OWNED by this user
                owner_id = item.get("owner", {}).get("id", "")
                if owner_id != user_id:
                    continue
                ext_url = item.get("external_urls", {}).get("spotify", "")
                if ext_url:
                    playlists.append({
                        "id":   item.get("id", ""),
                        "name": item.get("name", "Unknown"),
                        "url":  ext_url
                    })

            url = data.get("next")
            if url:
                time.sleep(DELAY)

        except Exception as e:
            print(f"      ❌ Error: {e}")
            break

    return playlists


# ============================================================
#  STEP 5 — FETCH FOLLOWER COUNT FOR ONE PLAYLIST
# ============================================================

def get_playlist_followers(playlist_id, token):
    url     = f"https://api.spotify.com/v1/playlists/{playlist_id}?fields=followers"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 3))
            time.sleep(wait)
            resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code != 200:
            return 0

        data = resp.json()
        followers = data.get("followers", {})
        return followers.get("total", 0) if isinstance(followers, dict) else 0

    except Exception as e:
        print(f"      ⚠️ Followers error: {e}")
        return 0


# ============================================================
#  STEP 6 — WRITE DATA INTO A _Followers SHEET
# ============================================================

def update_followers_sheet(spreadsheet, profile_url, display_name, playlists_with_counts):
    if not playlists_with_counts:
        return

    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', display_name)
    sheet_name = clean_name + "_Followers"

    # Get or create the sheet
    try:
        sheet = spreadsheet.worksheet(sheet_name)
        print(f"   📄 Updating: {sheet_name}")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=100)
        print(f"   📄 Created:  {sheet_name}")

    existing_data = sheet.get_all_values()

    # Set up headers if brand new sheet
    if not existing_data or len(existing_data) < 2:
        sheet.update("A1:C1", [["Profile Name", display_name, profile_url]])
        sheet.format("A1:C1", {"textFormat": {"bold": True}})
        sheet.update("A2:B2", [["Playlist Name", "Playlist URL"]])
        sheet.format("A2:B2", {"textFormat": {"bold": True}})
        existing_data = sheet.get_all_values()

    # Always update display name in B1 in case it changed
    sheet.update_cell(1, 2, display_name)

    # Add today's date as new column header in row 2
    today         = datetime.now().strftime("%d %b %H:%M")
    last_col      = len(existing_data[1]) if len(existing_data) > 1 else 2
    new_col_index = last_col + 1

    sheet.update_cell(2, new_col_index, today)
    sheet.format(
        gspread.utils.rowcol_to_a1(2, new_col_index),
        {"textFormat": {"bold": True}}
    )

    # Build URL → row number lookup from existing data
    url_to_row  = {}
    for i, row in enumerate(existing_data[2:], start=3):
        if len(row) > 1 and row[1]:
            url_to_row[row[1].strip()] = i

    next_new_row = len(existing_data) + 1
    written      = 0

    for playlist in playlists_with_counts:
        clean_url = playlist["url"].split("?")[0]

        if clean_url in url_to_row:
            row_num = url_to_row[clean_url]
        else:
            row_num = next_new_row
            next_new_row += 1
            url_to_row[clean_url] = row_num

        # Write name and URL
        sheet.update_cell(row_num, 1, playlist["name"])
        sheet.update_cell(row_num, 2, clean_url)

        # Write follower count
        sheet.update_cell(row_num, new_col_index, playlist["followers"])

        # Colour cell based on growth vs previous column
        if new_col_index > 3:
            try:
                prev_val = sheet.cell(row_num, new_col_index - 1).value
                if prev_val and str(prev_val).strip().lstrip('-').isdigit():
                    diff = playlist["followers"] - int(prev_val)
                    if diff > 0:
                        color = {"red": 1.0, "green": 1.0, "blue": 0.0}   # yellow
                    elif diff < 0:
                        color = {"red": 1.0, "green": 0.2, "blue": 0.2}   # red
                    else:
                        color = {"red": 1.0, "green": 1.0, "blue": 1.0}   # white
                    sheet.format(
                        gspread.utils.rowcol_to_a1(row_num, new_col_index),
                        {"backgroundColor": color}
                    )
            except:
                pass

        written += 1

    # Sort rows by follower count descending (latest column)
    sort_data = sheet.get_all_values()
    if len(sort_data) > 3:
        data_rows   = sort_data[2:]  # everything from row 3 down
        sorted_rows = sorted(
            data_rows,
            key=lambda r: int(r[new_col_index - 1]) if len(r) >= new_col_index and str(r[new_col_index - 1]).strip().lstrip('-').isdigit() else 0,
            reverse=True
        )
        if sorted_rows != data_rows:
            sheet.update(
                f"A3:{gspread.utils.rowcol_to_a1(2 + len(sorted_rows), new_col_index)}",
                sorted_rows
            )

    print(f"   ✅ {written} playlists written — sorted by followers")


# ============================================================
#  STEP 7 — UPDATE MASTER TABLE
# ============================================================

def update_master_table(spreadsheet, profile_links):
    sheet_name   = "Master Table"
    base_url     = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"

    try:
        master = spreadsheet.worksheet(sheet_name)
        master.clear()
    except gspread.WorksheetNotFound:
        master = spreadsheet.add_worksheet(title=sheet_name, rows=200, cols=5)

    headers = ["Display Name", "Profile URL", "Followers Sheet", "Last Updated"]
    master.update("A1:D1", [headers])
    master.format("A1:D1", {"textFormat": {"bold": True}})

    # Build a lookup of all _Followers sheets
    all_sheets    = spreadsheet.worksheets()
    url_to_sheet  = {}
    for s in all_sheets:
        if s.title.endswith("_Followers"):
            try:
                c1_val = s.cell(1, 3).value  # profile URL in C1
                if c1_val:
                    url_to_sheet[c1_val.strip()] = s
            except:
                pass

    rows = []
    for url in profile_links:
        fsheet = url_to_sheet.get(url)
        if fsheet:
            display_name  = fsheet.cell(1, 2).value or url
            sheet_link    = f'=HYPERLINK("{base_url}#gid={fsheet.id}","{fsheet.title}")'
        else:
            display_name  = url
            sheet_link    = "Not created yet"

        rows.append([display_name, url, sheet_link, datetime.now().strftime("%Y-%m-%d %H:%M")])

    if rows:
        master.update(f"A2:C{1 + len(rows)}", [[r[0], r[1], ""] for r in rows])
        for i, row in enumerate(rows, start=2):
            if row[2].startswith("="):
                master.update_cell(i, 3, row[2])
            else:
                master.update_cell(i, 3, row[2])
            master.update_cell(i, 4, row[3])

    print(f"   ✅ Master Table updated — {len(rows)} profiles")


# ============================================================
#  MAIN — runs everything in one go
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY FOLLOWER TRACKER — Full Auto")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # Connect
    token       = get_spotify_token()
    spreadsheet = connect_to_sheets()
    profile_urls = get_profile_urls(spreadsheet)

    if not profile_urls:
        print("\n❌ No profile URLs found in dump sheet. Exiting.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    for i, profile_url in enumerate(profile_urls, 1):
        print(f"\n[{i}/{len(profile_urls)}] {profile_url}")

        # Extract user ID
        match = re.search(r"/(?:user|profile)/([^/?]+)", profile_url)
        if not match:
            print("   ⚠️ Can't extract user ID — skipping")
            continue

        user_id = match.group(1)

        # Get display name
        try:
            user_resp    = requests.get(
                f"https://api.spotify.com/v1/users/{user_id}",
                headers=headers, timeout=15
            )
            display_name = user_resp.json().get("display_name") or user_id
        except:
            display_name = user_id

        print(f"   👤 {display_name}")

        # Get all playlists
        playlists = get_user_playlists(user_id, token)

        if playlists is None:
            # Token expired — refresh and retry
            print("   🔄 Refreshing token...")
            token    = get_spotify_token()
            headers  = {"Authorization": f"Bearer {token}"}
            playlists = get_user_playlists(user_id, token)

        if not playlists:
            print(f"   ⚠️ No public playlists found")
            continue

        print(f"   Found {len(playlists)} playlists — fetching follower counts...")

        # Get follower count for each playlist
        playlists_with_counts = []
        for j, playlist in enumerate(playlists, 1):
            followers = get_playlist_followers(playlist["id"], token)
            playlists_with_counts.append({**playlist, "followers": followers})
            print(f"      [{j}/{len(playlists)}] {playlist['name']}: {followers:,}")
            time.sleep(DELAY)

        # Write to sheet
        update_followers_sheet(spreadsheet, profile_url, display_name, playlists_with_counts)

    # Update master table
    print("\n📊 Updating Master Table...")
    update_master_table(spreadsheet, profile_urls)

    print("\n" + "=" * 55)
    print(f"  ✅ All done!")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)


if __name__ == "__main__":
    main()
