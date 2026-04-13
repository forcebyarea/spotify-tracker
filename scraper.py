import os
import json
import time
import gspread
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from google.oauth2.service_account import Credentials

# ============================================================
#  SPOTIFY FOLLOWER TRACKER — PARALLEL + FAST VERSION
#  - Concurrent Spotify requests (5 at a time)
#  - Batch writes to Google Sheets
#  - 1 dedicated API key per job
#  - Skips Link Sheet 2
# ============================================================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

CONCURRENT_REQUESTS = 5  # fetch 5 playlists at once
MAX_RETRIES = 3


# ── Spotify Auth ─────────────────────────────────────────────

def get_spotify_token(client_id, client_secret):
    r = requests.post(
        'https://accounts.spotify.com/api/token',
        data={'grant_type': 'client_credentials'},
        auth=(client_id, client_secret)
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

def fetch_single_playlist(args):
    playlist_id, token, client_id, client_secret, row_idx, prev_followers = args

    url    = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    params = {'fields': 'followers.total'}

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(
                url,
                headers={'Authorization': f'Bearer {token}'},
                params=params,
                timeout=10
            )

            if r.status_code == 200:
                followers = r.json().get('followers', {}).get('total', 0)
                return (row_idx, followers, prev_followers, None)

            elif r.status_code == 429:
                wait = int(r.headers.get('Retry-After', 30))
                print(f'   ⏳ Rate limited — waiting {wait}s...')
                time.sleep(wait)
                # Refresh token and retry
                token = get_spotify_token(client_id, client_secret)

            elif r.status_code == 404:
                return (row_idx, None, prev_followers, 'deleted')

            elif r.status_code == 401:
                token = get_spotify_token(client_id, client_secret)

            else:
                return (row_idx, None, prev_followers, f'status_{r.status_code}')

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return (row_idx, None, prev_followers, str(e))

    return (row_idx, None, prev_followers, 'max_retries')


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

    for i, h in enumerate(row2):
        if today_str in str(h):
            print(f'   📅 Today column exists at col {i+1}')
            return i + 1

    last_col = 0
    for i, h in enumerate(row2):
        if str(h).strip():
            last_col = i + 1

    new_col = last_col + 1
    print(f'   📅 New column at {new_col}')

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
    sheet.spreadsheet.batch_update({'requests': [{'repeatCell': {
        'range': {
            'sheetId': sheet.id,
            'startRowIndex': 1, 'endRowIndex': 2,
            'startColumnIndex': new_col - 1, 'endColumnIndex': new_col
        },
        'cell': {'userEnteredFormat': {'backgroundColor': {'red': 1, 'green': 1, 'blue': 1}}},
        'fields': 'userEnteredFormat.backgroundColor'
    }}]})
    print(f'   ✅ Header written at col {new_col}')
    return new_col


def process_followers_sheet(sheet, token, client_id, client_secret, today_str):
    print(f'\n   📋 Processing: {sheet.title}')

    all_values = sheet.get_all_values()
    if len(all_values) < 3:
        print(f'   ⚠️ Not enough rows — skipping')
        return token

    col_index = find_or_create_column(sheet, today_str)

    # Skip if already tracked today
    existing = sheet.col_values(col_index)
    already_written = sum(1 for v in existing[2:] if v)
    if already_written > 0:
        print(f'   ⏭️  Already tracked today ({already_written} values) — skipping')
        return token

    print(f'   📝 Writing to column {col_index}')

    # Build task list
    tasks = []
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

        prev_followers = None
        if col_index > 3:
            try:
                prev_col_idx = col_index - 2
                if len(row) > prev_col_idx and row[prev_col_idx]:
                    prev_followers = int(str(row[prev_col_idx]).replace(',', ''))
            except:
                pass

        tasks.append((playlist_id, token, client_id, client_secret, row_idx + 1, prev_followers))

    print(f'   🎯 Fetching {len(tasks)} playlists ({CONCURRENT_REQUESTS} at a time)...')

    # Fetch concurrently
    results = {}
    deleted = []
    completed = 0

    with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
        futures = {executor.submit(fetch_single_playlist, task): task for task in tasks}

        for future in as_completed(futures):
            row_idx, followers, prev_followers, error = future.result()
            completed += 1

            if followers is not None:
                results[row_idx] = (followers, prev_followers)
            elif error == 'deleted':
                deleted.append(row_idx)

            if completed % 50 == 0:
                print(f'   ✍️  {completed}/{len(tasks)} fetched...')

    print(f'   ✅ Fetched {len(results)} | Deleted/private: {len(deleted)}')

    if not results:
        return token

    # Batch write values
    value_updates = []
    color_requests = []

    for sheet_row, (followers, prev_followers) in sorted(results.items()):
        value_updates.append({
            'range': gspread.utils.rowcol_to_a1(sheet_row, col_index),
            'values': [[followers]]
        })

        if prev_followers is not None:
            if followers > prev_followers:
                color = {'red': 1.0, 'green': 1.0, 'blue': 0.0}   # yellow
            elif followers < prev_followers:
                color = {'red': 1.0, 'green': 0.0, 'blue': 0.0}   # red
            else:
                color = {'red': 1.0, 'green': 1.0, 'blue': 1.0}   # white
        else:
            color = {'red': 1.0, 'green': 1.0, 'blue': 1.0}

        color_requests.append({'repeatCell': {
            'range': {
                'sheetId': sheet.id,
                'startRowIndex': sheet_row - 1,
                'endRowIndex': sheet_row,
                'startColumnIndex': col_index - 1,
                'endColumnIndex': col_index
            },
            'cell': {'userEnteredFormat': {'backgroundColor': color}},
            'fields': 'userEnteredFormat.backgroundColor'
        }})

    sheet.batch_update(value_updates)
    print(f'   ✅ Wrote {len(value_updates)} values')

    sheet.spreadsheet.batch_update({'requests': color_requests})
    print(f'   🎨 Applied colours')

    return token


# ── Main ─────────────────────────────────────────────────────

def main():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today_str = now.strftime('%d %b %H:%M IST')

    all_sheet_ids  = json.loads(os.environ['SHEET_IDS'])
    sheet_indices  = json.loads(os.environ['SHEET_INDICES'])
    all_creds      = json.loads(os.environ['SPOTIFY_CREDENTIALS'])
    key_index      = int(os.environ['KEY_INDEX'])
    cred           = all_creds[key_index]
    client_id      = cred['id']
    client_secret  = cred['secret']

    my_sheets = [all_sheet_ids[i] for i in sheet_indices]

    print(f'''
=======================================================
  SPOTIFY FOLLOWER TRACKER
  Job sheets: {[i+1 for i in sheet_indices]}
  API key: {key_index + 1}/6
  Concurrent requests: {CONCURRENT_REQUESTS}
  Date: {today_str}
=======================================================''')

    print('🔑 Getting Spotify token...')
    token = get_spotify_token(client_id, client_secret)

    print('🔗 Connecting to Google Sheets...')
    gc = get_gspread_client()
    print('   ✅ Connected')

    for i, sheet_id in zip(sheet_indices, my_sheets):
        print(f'''
-------------------------------------------------------
  Sheet {i+1}/17
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
                    token = process_followers_sheet(
                        sheet, token, client_id, client_secret, today_str
                    )
                except Exception as e:
                    print(f'   ❌ Error on {sheet.title}: {e}')

        except Exception as e:
            print(f'   ❌ Could not open sheet {i+1}: {e}')

    print(f'''
=======================================================
  ✅ Job complete!
  Sheets: {[i+1 for i in sheet_indices]}
  Finished: {datetime.now(ist).strftime("%d %b %H:%M IST")}
=======================================================''')


if __name__ == '__main__':
    main()
