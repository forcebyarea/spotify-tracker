import gspread
import requests
import time
import json
import os
import re
from datetime import datetime
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup

# ============================================================
#  SETTINGS — only edit SPREADSHEET_ID
# ============================================================

SPREADSHEET_ID     = "1dIjl5darXJ678ftBALLK-vqWkXopWRryvUlPGRdLJ9Q"
PROFILE_DUMP_SHEET = "profile link dump"
DELAY              = 1.5

# ============================================================
#  HOW THIS WORKS
#  1. Gets Spotify token via Client Credentials (your secrets)
#  2. Reads profile URLs from your dump sheet
#  3. Scrapes each profile's PUBLIC page to find playlist IDs
#     (no auth needed — just HTML from open.spotify.com)
#  4. Uses Client Credentials token to get follower count
#     for each playlist via GET /playlists/{id}  ← this works!
#  5. Writes everything into _Followers sheets + Master Table
# ============================================================


# ============================================================
#  GET SPOTIFY TOKEN — Client Credentials
# ============================================================

def get_spotify_token():
    print("🔑 Getting Spotify token...")
    client_id     = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise Exception(
            "❌ SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET missing.\n"
            "   Add them in GitHub → Settings → Secrets → Actions"
        )

    import base64
    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        headers={
            "Authorization": "Basic " + base64.b64encode(
                f"{client_id}:{client_secret}".encode()
            ).decode()
        },
        timeout=15
    )

    if resp.status_code != 200:
        raise Exception(f"❌ Token failed: {resp.status_code} — {resp.text}")

    token = resp.json().get("access_token")
    print("   ✅ Token obtained")
    return token


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
    urls   = []
    for u in values[1:]:
        u = u.strip()
        if "/user/" in u or "/profile/" in u:
            urls.append(u)
        elif u:
            print(f"   ⚠️ Skipping: {u[:60]}")
    print(f"   Found {len(urls)} profile URLs")
    return urls


# ============================================================
#  SCRAPE PLAYLIST IDs FROM PUBLIC PROFILE PAGE
#  This works without any auth — just reads public HTML
# ============================================================

def get_playlist_ids_from_profile(user_id):
    url     = f"https://open.spotify.com/user/{user_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"   ⚠️ Profile page status: {resp.status_code}")
            return []

        html = resp.text

        # Method 1 — find playlist IDs in __NEXT_DATA__ JSON blob
        ids = []
        nd_match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html, re.DOTALL
        )
        if nd_match:
            try:
                nd = json.loads(nd_match.group(1))
                # Walk the JSON to find playlist URIs
                text = json.dumps(nd)
                ids  = re.findall(r'spotify:playlist:([A-Za-z0-9]{22})', text)
                ids  = list(dict.fromkeys(ids))  # deduplicate
            except:
                pass

        # Method 2 — find in raw HTML if __NEXT_DATA__ had nothing
        if not ids:
            ids  = re.findall(r'spotify:playlist:([A-Za-z0-9]{22})', html)
            ids += re.findall(r'"/playlist/([A-Za-z0-9]{22})"', html)
            ids  = list(dict.fromkeys(ids))

        # Method 3 — BeautifulSoup href scan
        if not ids:
            soup  = BeautifulSoup(html, "html.parser")
            links = soup.find_all("a", href=re.compile(r"/playlist/[A-Za-z0-9]{22}"))
            ids   = list(dict.fromkeys([
                re.search(r"/playlist/([A-Za-z0-9]{22})", a["href"]).group(1)
                for a in links if re.search(r"/playlist/([A-Za-z0-9]{22})", a["href"])
            ]))

        print(f"   Found {len(ids)} playlist IDs on profile page")
        return ids

    except Exception as e:
        print(f"   ❌ Error scraping profile page: {e}")
        return []


# ============================================================
#  GET PLAYLIST DETAILS + FOLLOWERS
#  Uses Client Credentials — works for ANY public playlist ✅
# ============================================================

def get_playlist_details(playlist_id, token):
    url     = f"https://api.spotify.com/v1/playlists/{playlist_id}?fields=id,name,external_urls,followers,owner"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 3))
            print(f"      ⏳ Rate limited — waiting {wait}s...")
            time.sleep(wait)
            resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code == 401:
            return None  # token expired signal

        if resp.status_code != 200:
            return {}

        data      = resp.json()
        followers = data.get("followers", {})
        if isinstance(followers, dict):
            follower_count = followers.get("total", 0)
        else:
            follower_count = int(followers or 0)

        owner    = data.get("owner", {})
        owner_id = owner.get("id", "") if isinstance(owner, dict) else ""

        return {
            "id":        data.get("id", playlist_id),
            "name":      data.get("name", "Unknown"),
            "url":       data.get("external_urls", {}).get("spotify", f"https://open.spotify.com/playlist/{playlist_id}"),
            "followers": follower_count,
            "owner_id":  owner_id
        }

    except Exception as e:
        print(f"      ⚠️ Error: {e}")
        return {}


# ============================================================
#  WRITE TO _Followers SHEET
# ============================================================

def update_followers_sheet(spreadsheet, profile_url, display_name, playlists):
    if not playlists:
        return

    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', display_name)
    sheet_name = clean_name + "_Followers"

    try:
        sheet = spreadsheet.worksheet(sheet_name)
        print(f"   📄 Updating:  {sheet_name}")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=100)
        print(f"   📄 Created:   {sheet_name}")

    existing_data = sheet.get_all_values()

    # Headers if new sheet
    if not existing_data or len(existing_data) < 2:
        sheet.update(values=[["Profile Name", display_name, profile_url]], range_name="A1:C1")
        sheet.format("A1:C1", {"textFormat": {"bold": True}})
        sheet.update(values=[["Playlist Name", "Playlist URL"]], range_name="A2:B2")
        sheet.format("A2:B2", {"textFormat": {"bold": True}})
        existing_data = sheet.get_all_values()

    # New date column
    today         = datetime.now().strftime("%d %b %H:%M")
    last_col      = len(existing_data[1]) if len(existing_data) > 1 else 2
    new_col_index = last_col + 1

    sheet.update_cell(2, new_col_index, today)
    sheet.format(gspread.utils.rowcol_to_a1(2, new_col_index), {"textFormat": {"bold": True}})

    # URL → row lookup
    url_to_row   = {}
    for i, row in enumerate(existing_data[2:], start=3):
        if len(row) > 1 and row[1]:
            url_to_row[row[1].strip()] = i
    next_new_row = len(existing_data) + 1

    for playlist in playlists:
        clean_url = playlist["url"].split("?")[0]
        row_num   = url_to_row.get(clean_url, next_new_row)
        if clean_url not in url_to_row:
            url_to_row[clean_url] = next_new_row
            next_new_row += 1

        sheet.update_cell(row_num, 1, playlist["name"])
        sheet.update_cell(row_num, 2, clean_url)
        sheet.update_cell(row_num, new_col_index, playlist["followers"])

        # Colour: yellow = growth, red = decline, white = no change
        if new_col_index > 3:
            try:
                prev = sheet.cell(row_num, new_col_index - 1).value
                if prev and str(prev).strip().lstrip('-').isdigit():
                    diff  = playlist["followers"] - int(prev)
                    color = ({"red": 1.0, "green": 1.0, "blue": 0.0} if diff > 0 else
                             {"red": 1.0, "green": 0.2, "blue": 0.2} if diff < 0 else
                             {"red": 1.0, "green": 1.0, "blue": 1.0})
                    sheet.format(
                        gspread.utils.rowcol_to_a1(row_num, new_col_index),
                        {"backgroundColor": color}
                    )
            except:
                pass

    # Sort by latest follower count descending
    all_data = sheet.get_all_values()
    if len(all_data) > 3:
        data_rows = all_data[2:]
        sorted_rows = sorted(
            data_rows,
            key=lambda r: int(r[new_col_index - 1]) if (
                len(r) >= new_col_index and
                str(r[new_col_index - 1]).strip().lstrip('-').isdigit()
            ) else 0,
            reverse=True
        )
        if sorted_rows != data_rows:
            end_cell = gspread.utils.rowcol_to_a1(2 + len(sorted_rows), new_col_index)
            sheet.update(values=sorted_rows, range_name=f"A3:{end_cell}")

    print(f"   ✅ {len(playlists)} playlists written to {sheet_name}")


# ============================================================
#  UPDATE MASTER TABLE
# ============================================================

def update_master_table(spreadsheet, profile_urls):
    print("\n📊 Updating Master Table...")
    base_url   = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"

    try:
        master = spreadsheet.worksheet("Master Table")
        master.clear()
    except gspread.WorksheetNotFound:
        master = spreadsheet.add_worksheet(title="Master Table", rows=200, cols=5)

    master.update(values=[["Display Name", "Profile URL", "Followers Sheet", "Last Updated"]], range_name="A1:D1")
    master.format("A1:D1", {"textFormat": {"bold": True}})

    # Build lookup of all _Followers sheets by their C1 URL value
    url_to_sheet = {}
    for s in spreadsheet.worksheets():
        if s.title.endswith("_Followers"):
            try:
                c1 = s.cell(1, 3).value
                if c1:
                    url_to_sheet[c1.strip()] = s
            except:
                pass

    for i, url in enumerate(profile_urls, start=2):
        fs           = url_to_sheet.get(url)
        display_name = fs.cell(1, 2).value if fs else url
        link         = (f'=HYPERLINK("{base_url}#gid={fs.id}","{fs.title}")'
                        if fs else "Not created yet")
        master.update_cell(i, 1, display_name)
        master.update_cell(i, 2, url)
        if fs:
            master.update_cell(i, 3, link)
        else:
            master.update_cell(i, 3, link)
        master.update_cell(i, 4, datetime.now().strftime("%Y-%m-%d %H:%M"))

    print(f"   ✅ Master Table updated — {len(profile_urls)} profiles")


# ============================================================
#  MAIN
# ============================================================

def main():
    print("=" * 55)
    print("  SPOTIFY FOLLOWER TRACKER")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    token       = get_spotify_token()
    spreadsheet = connect_to_sheets()
    profiles    = get_profile_urls(spreadsheet)

    if not profiles:
        print("❌ No profile URLs found. Exiting.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    for i, profile_url in enumerate(profiles, 1):
        print(f"\n[{i}/{len(profiles)}] {profile_url}")

        match = re.search(r"/(?:user|profile)/([^/?]+)", profile_url)
        if not match:
            print("   ⚠️ Can't extract user ID — skipping")
            continue
        user_id = match.group(1)

        # Get display name via API (this endpoint works with Client Credentials)
        try:
            user_resp    = requests.get(
                f"https://api.spotify.com/v1/users/{user_id}",
                headers=headers, timeout=15
            )
            display_name = user_resp.json().get("display_name") or user_id
        except:
            display_name = user_id
        print(f"   👤 {display_name}")

        # Step 1 — scrape playlist IDs from public profile page (no auth)
        playlist_ids = get_playlist_ids_from_profile(user_id)
        if not playlist_ids:
            print("   ⚠️ No playlists found on profile page")
            continue

        # Step 2 — get details + followers for each playlist via API
        print(f"   Fetching details for {len(playlist_ids)} playlists...")
        playlists = []
        for j, pid in enumerate(playlist_ids, 1):
            details = get_playlist_details(pid, token)

            if details is None:
                # Token expired — refresh
                print("   🔄 Token expired — refreshing...")
                token   = get_spotify_token()
                headers = {"Authorization": f"Bearer {token}"}
                details = get_playlist_details(pid, token)

            if not details:
                continue

            # Only include playlists owned by this user
            if details.get("owner_id") and details["owner_id"] != user_id:
                continue

            playlists.append(details)
            print(f"      [{j}/{len(playlist_ids)}] {details['name']}: {details['followers']:,} followers")
            time.sleep(DELAY)

        # Step 3 — write to sheet
        update_followers_sheet(spreadsheet, profile_url, display_name, playlists)

    update_master_table(spreadsheet, profiles)

    print("\n" + "=" * 55)
    print(f"  ✅ Done!")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

if __name__ == "__main__":
    main()
