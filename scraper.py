import os
import json
import time
import gspread
import requests
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
#  SPOTIFY FOLLOWER TRACKER
#  Processes all 17 sheets one by one — no parallel jobs
#  Slower but reliable — no rate limit issues
# ============================================================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

DELAY_BETWEEN_REQUESTS = 3   # 3 seconds between Spotify calls — safe
MAX_RETRIES = 3


# ── Spotify Auth ─────────────────────────────────────────────

def get_spotify_token():
    multi = os.environ.get('SPOTIFY_CREDENTIALS')
    if multi:
        try:
            creds = json.loads(multi)
            if isinstance(creds, list):
                for cred in creds:
                    r = requests.post(
                        'https://accounts.spotify.com/api/token',
                        data={'grant_type': 'client_credentials'},
                        auth=(cred['id'], cred['secret'])
                    )
                    if r.status_code == 200:
                        print(f'   ✅ Token obtained')
                        return r.json().get('access_token')
        except:
            pass

    # Single key fallback
    r = requests.post(
        'https://accounts.spotify.com/api/token',
        data={'grant_type': 'client_credentials'},
        auth=(os.environ['SPOTIFY_CLIENT_ID'], os.environ['SPOTIFY_CLIENT_SECRET'])
    )
    if r.status_code != 200:
        raise Exception(f'Token failed: {r.text}')
    print(f'   ✅ Token obtained')
    return r.json().get('access_token')


# ── Google Sheets ────────────────────────────────────────────

def get_gspread_client():
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Spotify Data ─────────────────────────────────────────────

def get_playlist_followers(playlist_id, token):
    url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    headers = {'Authorization': f'Bearer {token}'}
    params  = {'fields': 'followers.total'}

    for attempt in range(MAX_RETRIES):
        r = requests.get(url, headers=headers, params=params)

        if r.status_code == 200:
            return r.json().get('followers', {}).get('total', 0)

        elif r.status_code == 429:
            wait = int(r.headers.get('Retry-After', 30))
            print(f'   ⏳ Rate limited — waiting {wait}s...')
            time.sleep(wait)

        elif r.status_code == 401:
            print(f'   🔄 Token expired — refreshing...')
            token = get_spotify_token()

        else:
            print(f'   ⚠️ Status {r.status_code} for {playlist_id}')
            return None

    return None


def extract_playlist_id(url):
    try:
        if '/playlist/' in url:
            return url.split('/playlist/')[1].split('?')[0].strip()
    except:
        pass
    return None


# ── Sheet Logic ──────────────────────────────────────────────

def find_or_create_column(sheet, today_str):
    row2 = sheet.row_values(2)
    print(f'   📊 Row 2 has {len(row2)} columns')

    # Check if today already exists
    for i, h in enumerate(row2):
        if today_str in str(h):
            print(f'   📅 Today column exists at col {i+1}')
            return i + 1

    # Find last non-empty column
    last_col = 0
    for i, h in enumerate(row2):
        if str(h).strip():
            last_col = i + 1

    new_col = last_col + 1
    print(f'   📅 New column at {new_col}')

    # Expand if needed
    sheet_meta = sheet.spreadsheet.fetch_sheet_metadata()
    for s in sheet_meta['sheets']:
        if s['properties']['sheetId'] == sheet.id:
            current_cols = s['properties']['gridProperties']['columnCount']
            if new_col >= current_cols:
                sheet.spreadsheet.batch_update({
                    'requests': [{'appendDimension': {
                        'sheetId': sheet.id,
                        'dimension': 'COLUMNS',
                        'length': 50
                    }}]
                })
                print(f'   📐 Expanded columns by 50')
            break

    sheet.update_cell(2, new_col, today_str)
    print(f'   ✅ Header written at col {new_col}')
    return new_col


def process_followers_sheet(sheet, token, today_str):
    print(f'\n   📋 Processing: {sheet.title}')

    all_values = sheet.get_all_values()
    if len(all_values) < 3:
        print(f'   ⚠️ Not enough rows — skipping')
        return token

    col_index = find_or_create_column(sheet, today_str)
    print(f'   📝 Writing to column {col_index}')

    written = 0
    skipped = 0

    for row_idx in range(2, len(all_values)):
        row = all_values[row_idx]
        if len(row) < 2:
            continue

        playlist_url = str(row[1]).strip()
        if not playlist_url or 'spotify' not in playlist_url:
            continue

        playlist_id = extract_playlist_id(playlist_url)
        if not playlist_id:
            continue

        followers = get_playlist_followers(playlist_id, token)
        time.sleep(DELAY_BETWEEN_REQUESTS)

        if followers is None:
            skipped += 1
            continue

        sheet_row = row_idx + 1

        try:
            sheet.update_cell(sheet_row, col_index, followers)
            written += 1

            # Colour
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
                    color = {'red': 1, 'green': 0.95, 'blue': 0.2}
                elif followers < prev_followers:
                    color = {'red': 1, 'green': 0.4, 'blue': 0.4}
                else:
                    color = {'red': 1, 'green': 1, 'blue': 1}
            else:
                color = {'red': 1, 'green': 1, 'blue': 1}

            sheet.spreadsheet.batch_update({'requests': [{'repeatCell': {
                'range': {
                    'sheetId': sheet.id,
                    'startRowIndex': sheet_row - 1,
                    'endRowIndex': sheet_row,
                    'startColumnIndex': col_index - 1,
                    'endColumnIndex': col_index
                },
                'cell': {'userEnteredFormat': {'backgroundColor': color}},
                'fields': 'userEnteredFormat.backgroundColor'
            }}]})

            if written % 10 == 0:
                print(f'   ✍️  {written} written so far...')

        except Exception as e:
            print(f'   ❌ Write error row {sheet_row}: {e}')

    print(f'   ✅ Done: {written} written, {skipped} skipped')
    return token


# ── Main ─────────────────────────────────────────────────────

def main():
    today_str = datetime.now().strftime('%d %b %Y %H:%M')
    sheet_ids = json.loads(os.environ['SHEET_IDS'])

    print(f'''
=======================================================
  SPOTIFY FOLLOWER TRACKER
  Processing all {len(sheet_ids)} sheets one by one
  Date: {today_str}
=======================================================''')

    print('🔑 Getting Spotify token...')
    token = get_spotify_token()

    print('🔗 Connecting to Google Sheets...')
    gc = get_gspread_client()
    print('   ✅ Connected')

    for i, sheet_id in enumerate(sheet_ids):
        print(f'''
-------------------------------------------------------
  Sheet {i+1}/{len(sheet_ids)}: {sheet_id}
-------------------------------------------------------''')
        try:
            spreadsheet = gc.open_by_key(sheet_id)
            print(f'   ✅ Opened: {spreadsheet.title}')

            follower_sheets = [
                s for s in spreadsheet.worksheets()
                if s.title.endswith('_Followers')
            ]
            print(f'   📋 Found {len(follower_sheets)} _Followers tabs')

            for sheet in follower_sheets:
                try:
                    token = process_followers_sheet(sheet, token, today_str)
                except Exception as e:
                    print(f'   ❌ Error on {sheet.title}: {e}')

        except Exception as e:
            print(f'   ❌ Could not open sheet {sheet_id}: {e}')

        # Small pause between sheets
        if i < len(sheet_ids) - 1:
            print(f'   ⏸️  Pausing 10s before next sheet...')
            time.sleep(10)

    print(f'''
=======================================================
  ✅ All sheets complete!
=======================================================''')


if __name__ == '__main__':
    main()
