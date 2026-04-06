import os
import json
import time
import gspread
import requests
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
#  SPOTIFY FOLLOWER TRACKER
#  Reads playlist URLs from all _Followers tabs in a sheet
#  Fetches follower counts from Spotify
#  Writes data back to the sheet
# ============================================================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

DELAY_BETWEEN_REQUESTS = 1  # seconds between Spotify API calls
MAX_RETRIES = 3              # retries on 429 rate limit


# ── Auth ────────────────────────────────────────────────────

def get_spotify_token():
    client_id     = os.environ['SPOTIFY_CLIENT_ID']
    client_secret = os.environ['SPOTIFY_CLIENT_SECRET']

    response = requests.post(
        'https://accounts.spotify.com/api/token',
        data={'grant_type': 'client_credentials'},
        auth=(client_id, client_secret)
    )
    if response.status_code != 200:
        raise Exception(f'Spotify token failed: {response.text}')
    
    token = response.json().get('access_token')
    print(f'   ✅ Spotify token obtained')
    return token


def get_gspread_client():
    creds_json = os.environ['GOOGLE_CREDENTIALS']
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Spotify ─────────────────────────────────────────────────

def get_playlist_followers(playlist_id, token):
    url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    headers = {'Authorization': f'Bearer {token}'}
    params  = {'fields': 'followers.total,name'}

    for attempt in range(MAX_RETRIES):
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            return data.get('followers', {}).get('total', 0)

        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            print(f'   ⏳ Rate limited. Waiting {retry_after}s...')
            time.sleep(retry_after)

        elif response.status_code == 401:
            print(f'   ❌ Token expired for playlist {playlist_id}')
            return None

        else:
            print(f'   ⚠️ Status {response.status_code} for {playlist_id}')
            return None

    return None


def extract_playlist_id(url):
    # Handles: https://open.spotify.com/playlist/37i9dQZF1DX...
    try:
        if '/playlist/' in url:
            return url.split('/playlist/')[1].split('?')[0].strip()
    except:
        pass
    return None


# ── Sheet logic ──────────────────────────────────────────────

def get_or_create_column(sheet, today_str):
    """Find today's column or create a new one. Returns column index (1-based)."""
    headers = sheet.row_values(2)  # Row 2 has date headers

    # Check if today's column already exists
    for i, h in enumerate(headers):
        if today_str in str(h):
            return i + 1  # 1-based

    # Add new column at the end
    new_col = len(headers) + 1
    sheet.update_cell(2, new_col, today_str)
    return new_col


def process_followers_sheet(sheet, token, today_str):
    print(f'   📋 Processing: {sheet.title}')

    all_values = sheet.get_all_values()
    if len(all_values) < 3:
        print(f'   ⚠️ Not enough rows — skipping')
        return

    # Find today's column
    col_index = get_or_create_column(sheet, today_str)

    # Process each playlist row (rows 3 onwards, index 2+)
    updates      = []
    color_updates = []

    for row_idx in range(2, len(all_values)):  # 0-based, row 3 = index 2
        row = all_values[row_idx]

        if len(row) < 2:
            continue

        playlist_url = row[1].strip() if len(row) > 1 else ''
        if not playlist_url or 'spotify' not in playlist_url:
            continue

        playlist_id = extract_playlist_id(playlist_url)
        if not playlist_id:
            continue

        followers = get_playlist_followers(playlist_id, token)
        if followers is None:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            continue

        sheet_row = row_idx + 1  # 1-based
        updates.append({
            'range': gspread.utils.rowcol_to_a1(sheet_row, col_index),
            'values': [[followers]]
        })

        # Determine colour based on previous column
        prev_followers = None
        if col_index > 3 and len(row) >= col_index - 1:
            try:
                prev_val = row[col_index - 2]
                if prev_val:
                    prev_followers = int(str(prev_val).replace(',', ''))
            except:
                pass

        if prev_followers is not None:
            if followers > prev_followers:
                color = {'red': 1, 'green': 0.95, 'blue': 0.2}   # yellow
            elif followers < prev_followers:
                color = {'red': 1, 'green': 0.4, 'blue': 0.4}    # red
            else:
                color = {'red': 1, 'green': 1, 'blue': 1}         # white
        else:
            color = {'red': 1, 'green': 1, 'blue': 1}

        color_updates.append({
            'row': sheet_row,
            'col': col_index,
            'color': color
        })

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Batch write all follower counts at once
    if updates:
        sheet.batch_update(updates)
        print(f'   ✅ Wrote {len(updates)} follower counts')

    # Apply colours
    spreadsheet = sheet.spreadsheet
    requests_body = []
    for cu in color_updates:
        requests_body.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet.id,
                    'startRowIndex': cu['row'] - 1,
                    'endRowIndex':   cu['row'],
                    'startColumnIndex': cu['col'] - 1,
                    'endColumnIndex':   cu['col']
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': cu['color']
                    }
                },
                'fields': 'userEnteredFormat.backgroundColor'
            }
        })

    if requests_body:
        spreadsheet.batch_update({'requests': requests_body})
        print(f'   🎨 Applied colours')


# ── Main ─────────────────────────────────────────────────────

def main():
    today_str  = datetime.now().strftime('%d %b %Y')
    sheet_ids  = json.loads(os.environ['SHEET_IDS'])
    sheet_index = int(os.environ.get('SHEET_INDEX', '0'))
    sheet_id   = sheet_ids[sheet_index]

    print(f'''
=======================================================
  SPOTIFY FOLLOWER TRACKER
  Sheet {sheet_index + 1} of {len(sheet_ids)}
  Sheet ID: {sheet_id}
  Date: {today_str}
=======================================================''')

    print('🔑 Getting Spotify token...')
    token = get_spotify_token()

    print('🔗 Connecting to Google Sheets...')
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(sheet_id)
    print(f'   ✅ Connected to: {spreadsheet.title}')

    # Find all _Followers tabs
    follower_sheets = [
        s for s in spreadsheet.worksheets()
        if s.title.endswith('_Followers')
    ]
    print(f'📋 Found {len(follower_sheets)} _Followers tabs')

    for sheet in follower_sheets:
        try:
            process_followers_sheet(sheet, token, today_str)
        except Exception as e:
            print(f'   ❌ Error on {sheet.title}: {e}')

    print(f'''
=======================================================
  ✅ Sheet {sheet_index + 1} complete!
=======================================================''')


if __name__ == '__main__':
    main()
