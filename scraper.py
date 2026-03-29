import gspread
import time
import json
import os
import re
import requests
import base64
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
# SETTINGS
# ============================================================

SPREADSHEET_ID = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
PROFILE_DUMP_SHEET = "profile link dump"
DELAY_BETWEEN_REQUESTS = 1

# ============================================================
# GOOGLE SHEETS
# ============================================================

def connect_to_sheets():
    print("🔗 Connecting to Google Sheets...")

    creds_dict = json.loads(os.environ.get("GOOGLE_CREDENTIALS"))

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID)

    print("✅ Connected to Google Sheets")
    return sheet

# ============================================================
# SPOTIFY TOKEN (OFFICIAL)
# ============================================================

def get_spotify_token():
    print("🔑 Getting Spotify API token...")

    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")

    print("CLIENT_ID exists:", bool(client_id))
    print("CLIENT_SECRET exists:", bool(client_secret))

    if not client_id or not client_secret:
        print("❌ Missing Spotify credentials")
        return None

    import base64

    auth_str = f"{client_id}:{client_secret}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {b64_auth}"
    }

    data = {
        "grant_type": "client_credentials"
    }

    res = requests.post(
        "https://accounts.spotify.com/api/token",
        headers=headers,
        data=data
    )

    print("Status:", res.status_code)
    print("Response:", res.text)

    if res.status_code != 200:
        print("❌ Token request failed")
        return None

    print("✅ Token received")
    return res.json().get("access_token")

# ============================================================
# EXTRACT USER ID
# ============================================================

def extract_user_id(url):
    m = re.search(r"/user/([^/?]+)", url)
    return m.group(1) if m else None

# ============================================================
# GET USER PLAYLISTS (API)
# ============================================================

def get_user_playlists(user_id, token):
    print("   📡 Fetching playlists via API...")

    url = f"https://api.spotify.com/v1/users/{user_id}/playlists"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    playlists = []

    while url:
        res = requests.get(url, headers=headers)

        if res.status_code != 200:
            print(f"   ❌ API error: {res.status_code}")
            break

        data = res.json()

        for item in data.get("items", []):
            playlists.append({
                "name": item["name"],
                "url": item["external_urls"]["spotify"],
                "followers": item["followers"]["total"]
            })

        url = data.get("next")
        time.sleep(0.5)

    print(f"   ✅ Found {len(playlists)} playlists")
    return playlists

# ============================================================
# GOOGLE SHEETS WRITE
# ============================================================

def update_sheet(sheet, profile_url, name, playlists):
    if not playlists:
        return

    clean = re.sub(r'[^a-zA-Z0-9]', '_', name)
    sheet_name = clean + "_Followers"

    try:
        ws = sheet.worksheet(sheet_name)
    except:
        ws = sheet.add_worksheet(title=sheet_name, rows=500, cols=50)

    data = ws.get_all_values()

    if not data:
        ws.update("A1:C1", [["Profile", name, profile_url]])
        ws.update("A2:B2", [["Playlist", "URL"]])

    col = len(ws.row_values(2)) + 1
    timestamp = datetime.now().strftime("%d %b %H:%M")
    ws.update_cell(2, col, timestamp)

    for i, p in enumerate(playlists, start=3):
        ws.update_cell(i, 1, p["name"])
        ws.update_cell(i, 2, p["url"])
        ws.update_cell(i, col, p["followers"])

    print(f"   📊 Updated: {sheet_name}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 50)
    print("SPOTIFY TRACKER — API VERSION")
    print("=" * 50)

    sheet = connect_to_sheets()

    urls = sheet.worksheet(PROFILE_DUMP_SHEET).col_values(1)[1:]

    token = get_spotify_token()

    if not token:
        print("❌ Cannot continue without token")
        return

    for i, url in enumerate(urls, 1):
        if not url.strip():
            continue

        print(f"\n[{i}] {url}")

        user_id = extract_user_id(url)

        if not user_id:
            print("   ❌ Invalid URL")
            continue

        playlists = get_user_playlists(user_id, token)

        if playlists:
            update_sheet(sheet, url, user_id, playlists)
        else:
            print("   ⚠️ No playlists found")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    print("\n✅ DONE")

if __name__ == "__main__":
    main()
