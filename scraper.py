import gspread
import requests
import time
import json
import os
import re
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
#  SETTINGS
# ============================================================

SPREADSHEET_ID     = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
PLAYLIST_TAB       = "playlist urls"   # written by playlist_discoverer.py
DELAY              = 1.5

# ============================================================
#  GET SPOTIFY TOKEN
# ============================================================

def get_spotify_token():
    print("🔑 Getting Spotify token...")
    client_id     = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise Exception("❌ SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET missing in GitHub Secrets")

    import base64
    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        headers={"Authorization": "Basic " + base64.b64encode(
            f"{client_id}:{client_secret}".encode()).decode()},
        timeout=15
    )
    if resp.status_code != 200:
        raise Exception(f"❌ Token failed: {resp.status_code}")
    print("   ✅ Token obtained")
    return resp.json().get("access_token")

# ============================================================
#  CONNECT TO GOOGLE SHEETS
# ============================================================

def connect_to_sheets():
    print("🔗 Connecting to Google Sheets...")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=["https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        creds = Credentials.from_service_account_file(
            "service_account.json",
            scopes=["https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"]
        )
    client = gspread.authorize(creds)
    ss     = client.open_by_key(SPREADSHEET_ID)
    print("   ✅ Connected")
    return ss

# ============================================================
#  READ PLAYLIST URLS FROM DISCOVERER TAB
#  Reads column B (playlist URL) and column C (owner/profile)
# ============================================================

def get_playlists(spreadsheet):
    print("📋 Reading playlist URLs...")
    try:
        sheet = spreadsheet.worksheet(PLAYLIST_TAB)
    except gspread.WorksheetNotFound:
        print(f"   ❌ '{PLAYLIST_TAB}' tab not found.")
        print("   Run playlist_discoverer.py first to populate this tab.")
        return {}

    rows = sheet.get_all_values()
    # Group by profile URL (column D)
    profile_to_playlists = {}
    for row in rows[1:]:
        if len(row) < 2 or not row[1]:
            continue
        name        = row[0].strip() if row[0] else "Unknown"
        url         = row[1].strip()
        owner_id    = row[2].strip() if len(row) > 2 else ""
        profile_url = row[3].strip() if len(row) > 3 else "Unknown"

        if "/playlist/" not in url:
            continue

        if profile_url not in profile_to_playlists:
            profile_to_playlists[profile_url] = []
        profile_to_playlists[profile_url].append({
            "name":     name,
            "url":      url,
            "owner_id": owner_id
        })

    total = sum(len(v) for v in profile_to_playlists.values())
    print(f"   Found {total} playlists across {len(profile_to_playlists)} profiles")
    return profile_to_playlists

# ============================================================
#  GET FOLLOWER COUNT FOR ONE PLAYLIST
# ============================================================

def get_followers(playlist_url, token):
    pid     = re.search(r"/playlist/([^/?]+)", playlist_url)
    if not pid:
        return 0
    url     = f"https://api.spotify.com/v1/playlists/{pid.group(1)}?fields=followers,name"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 3))
            time.sleep(wait)
            resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return 0
        f = resp.json().get("followers", {})
        return f.get("total", 0) if isinstance(f, dict) else int(f or 0)
    except:
        return 0

# ============================================================
#  WRITE TO _Followers SHEET
# ============================================================

def update_sheet(spreadsheet, profile_url, display_name, playlists):
    clean      = re.sub(r'[^a-zA-Z0-9]', '_', display_name)
    sheet_name = clean + "_Followers"

    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=100)

    data = sheet.get_all_values()

    if not data or len(data) < 2:
        sheet.update(values=[["Profile Name", display_name, profile_url]], range_name="A1:C1")
        sheet.format("A1:C1", {"textFormat": {"bold": True}})
        sheet.update(values=[["Playlist Name", "Playlist URL"]], range_name="A2:B2")
        sheet.format("A2:B2", {"textFormat": {"bold": True}})
        data = sheet.get_all_values()

    today     = datetime.now().strftime("%d %b %H:%M")
    last_col  = len(data[1]) if len(data) > 1 else 2
    new_col   = last_col + 1

    sheet.update_cell(2, new_col, today)
    sheet.format(gspread.utils.rowcol_to_a1(2, new_col), {"textFormat": {"bold": True}})

    url_to_row   = {row[1].strip(): i + 3 for i, row in enumerate(data[2:]) if len(row) > 1 and row[1]}
    next_new_row = len(data) + 1

    for pl in playlists:
        clean_url = pl["url"].split("?")[0]
        row_num   = url_to_row.get(clean_url, next_new_row)
        if clean_url not in url_to_row:
            url_to_row[clean_url] = next_new_row
            next_new_row += 1

        sheet.update_cell(row_num, 1, pl["name"])
        sheet.update_cell(row_num, 2, clean_url)
        sheet.update_cell(row_num, new_col, pl["followers"])

        if new_col > 3:
            try:
                prev = sheet.cell(row_num, new_col - 1).value
                if prev and str(prev).strip().lstrip('-').isdigit():
                    diff  = pl["followers"] - int(prev)
                    color = ({"red":1.0,"green":1.0,"blue":0.0} if diff > 0 else
                             {"red":1.0,"green":0.2,"blue":0.2} if diff < 0 else
                             {"red":1.0,"green":1.0,"blue":1.0})
                    sheet.format(gspread.utils.rowcol_to_a1(row_num, new_col), {"backgroundColor": color})
            except:
                pass

    # Sort by latest followers descending
    all_data = sheet.get_all_values()
    if len(all_data) > 3:
        rows = all_data[2:]
        sorted_rows = sorted(rows,
            key=lambda r: int(r[new_col-1]) if len(r) >= new_col and str(r[new_col-1]).strip().lstrip('-').isdigit() else 0,
            reverse=True)
        if sorted_rows != rows:
            sheet.update(values=sorted_rows, range_name=f"A3:{gspread.utils.rowcol_to_a1(2+len(sorted_rows), new_col)}")

    print(f"   ✅ {len(playlists)} playlists → {sheet_name}")

# ============================================================
#  MAIN
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY FOLLOWER TRACKER")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    token               = get_spotify_token()
    spreadsheet         = connect_to_sheets()
    profile_to_playlists = get_playlists(spreadsheet)

    if not profile_to_playlists:
        print("❌ No playlists to track. Run playlist_discoverer.py first.")
        return

    for i, (profile_url, playlists) in enumerate(profile_to_playlists.items(), 1):
        display_name = playlists[0]["owner_id"] if playlists else profile_url
        print(f"\n[{i}/{len(profile_to_playlists)}] 👤 {display_name} — {len(playlists)} playlists")

        for j, pl in enumerate(playlists, 1):
            pl["followers"] = get_followers(pl["url"], token)
            print(f"   [{j}/{len(playlists)}] {pl['name']}: {pl['followers']:,}")
            time.sleep(DELAY)

        update_sheet(spreadsheet, profile_url, display_name, playlists)

    print("\n" + "=" * 55)
    print(f"  ✅ Done! — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

if __name__ == "__main__":
    main()
