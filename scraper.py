import os
import json
import time
import gspread
import requests
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
#  SPOTIFY FOLLOWER TRACKER
#  - Multi API key rotation
#  - Reliable column detection
#  - No formatting on empty columns
# ============================================================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

DELAY_BETWEEN_REQUESTS = 1
MAX_RETRIES = 3


# ── API Key Rotation ─────────────────────────────────────────

class SpotifyTokenManager:
    def __init__(self):
        self.credentials = self._load_credentials()
        self.current_index = 0
        self.tokens = {}
        print(f'   🔑 Loaded {len(self.credentials)} Spotify credential(s)')

    def _load_credentials(self):
        multi = os.environ.get('SPOTIFY_CREDENTIALS')
        if multi:
            try:
                creds = json.loads(multi)
                if isinstance(creds, list) and len(creds) > 0:
                    return creds
            except:
                pass
        client_id     = os.environ.get('SPOTIFY_CLIENT_ID')
        client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')
        if client_id and client_secret:
            return [{'id': client_id, 'secret': client_secret}]
        raise Exception('No Spotify credentials found')

    def _fetch_token(self, index):
        cred = self.credentials[index]
        try:
            r = requests.post(
                'https://accounts.spotify.com/api/token',
                data={'grant_type': 'client_credentials'},
                auth=(cred['id'], cred['secret'])
            )
            if r.status_code == 200:
                return r.json().get('access_token')
        except:
            pass
        return None

    def get_token(self):
        if self.current_index not in self.tokens:
            token = self._fetch_token(self.current_index)
            if token:
                self.tokens[self.current_index] = token
                print(f'   ✅ Token from credential {self.current_index + 1}/{len(self.credentials)}')
            else:
                return self._rotate()
        return self.tokens.get(self.current_index)

    def _rotate(self):
        next_index = self.current_index + 1
        while next_index < len(self.credentials):
            print(f'   🔄 Rotating to credential {next_index + 1}...')
            token = self._fetch_token(next_index)
            if token:
                self.current_index = next_index
                self.tokens[next_index] = token
                print(f'   ✅ Token from credential {next_index + 1}/{len(self.credentials)}')
                return token
            next_index += 1
        print('   ❌ All credentials exhausted')
        return None

    def handle_failure(self):
        if self.current_index in self.tokens:
            del self.tokens[self.current_index]
        return self._rotate()


# ── Google Sheets ────────────────────────────────────────────

def get_gspread_client():
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Spotify ──────────────────────────────────────────────────

def get_playlist_followers(playlist_id, token_manager):
    url    = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    params = {'fields': 'followers.total'}

    for attempt in range(MAX_RETRIES):
        token = token_manager.get_token()
        if not token:
            return None

        r = requests.get(url, headers={'Authorization': f'Bearer {token}'}, params=params)

        if r.status_code == 200:
            return r.json().get('followers', {}).get('total', 0)
        elif r.status_code == 429:
            wait = int(r.headers.get('Retry-After', 10))
            print(f'   ⏳ Rate limited — waiting {wait}s...')
            time.sleep(wait)
            token_manager.handle_failure()
        elif r.status_code == 401:
            print(f'   🔄 Token expired — rotating...')
            if not token_manager.handle_failure():
                return None
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


# ── Sheet logic ──────────────────────────────────────────────

def find_or_create_column(sheet, today_str):
    """
    Reliably find or create today's column.
    Uses row_values with include_tailing_empty=False then checks actual sheet col count.
    """
    # Read row 2 directly — this gives us all headers including recently added ones
    row2 = sheet.row_values(2)

    # Search for today's header
    for i, h in enumerate(row2):
        if today_str in str(h):
            print(f'   📅 Today column already exists at col {i+1}')
            return i + 1, False  # col_index, is_new

    # Find the true last column with data in row 2
    last_data_col = len(row2)  # row_values strips trailing empty, so this is accurate
    new_col = last_data_col + 1

    print(f'   📅 New column at index {new_col} for {today_str}')

    # Expand sheet if needed
    sheet_meta = sheet.spreadsheet.fetch_sheet_metadata()
    for s in sheet_meta['sheets']:
        if s['properties']['sheetId'] == sheet.id:
            current_cols = s['properties']['gridProperties']['columnCount']
            if new_col > current_cols:
                sheet.spreadsheet.batch_update({
                    'requests': [{
                        'appendDimension': {
                            'sheetId': sheet.id,
                            'dimension': 'COLUMNS',
                            'length': 50
                        }
                    }]
                })
                print(f'   📐 Expanded sheet by 50 columns')
            break

    # Write header
    sheet.update_cell(2, new_col, today_str)
    return new_col, True  # col_index, is_new


def process_followers_sheet(sheet, token_manager, today_str):
    print(f'   📋 Processing: {sheet.title}')

    # Read all data once
    all_values = sheet.get_all_values()
    if len(all_values) < 3:
        print(f'   ⚠️ Not enough rows — skipping')
        return

    # Get column index for today
    col_index, is_new_col = find_or_create_column(sheet, today_str)
    print(f'   📊 Writing to column {col_index} ({"new" if is_new_col else "existing"})')

    updates       = []
    color_updates = []
    count         = 0

    for row_idx in range(2, len(all_values)):
        row = all_values[row_idx]
        if len(row) < 2:
            continue

        playlist_url = row[1].strip() if len(row) > 1 else ''
        if not playlist_url or 'spotify' not in playlist_url:
            continue

        playlist_id = extract_playlist_id(playlist_url)
        if not playlist_id:
            continue

        followers = get_playlist_followers(playlist_id, token_manager)
        if followers is None:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            continue

        count += 1
        sheet_row = row_idx + 1  # 1-based

        updates.append({
            'range': gspread.utils.rowcol_to_a1(sheet_row, col_index),
            'values': [[followers]]
        })

        # Only colour if we have a previous value to compare
        prev_followers = None
        if col_index > 3:
            try:
                prev_val = row[col_index - 2] if len(row) >= col_index - 1 else ''
                if str(prev_val).strip():
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
            color_updates.append({'row': sheet_row, 'col': col_index, 'color': color})

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Batch write follower counts
    if updates:
        sheet.batch_update(updates)
        print(f'   ✅ Wrote {count} follower counts to column {col_index}')
    else:
        print(f'   ⚠️ No data written — check playlist URLs in column B')

    # Apply colours only to cells that have data
    if color_updates:
        requests_body = [{
            'repeatCell': {
                'range': {
                    'sheetId':          sheet.id,
                    'startRowIndex':    cu['row'] - 1,
                    'endRowIndex':      cu['row'],
                    'startColumnIndex': cu['col'] - 1,
                    'endColumnIndex':   cu['col']
                },
                'cell': {'userEnteredFormat': {'backgroundColor': cu['color']}},
                'fields': 'userEnteredFormat.backgroundColor'
            }
        } for cu in color_updates]
        sheet.spreadsheet.batch_update({'requests': requests_body})
        print(f'   🎨 Applied colours to {len(color_updates)} cells')


# ── Main ─────────────────────────────────────────────────────

def main():
    today_str   = datetime.now().strftime('%d %b %Y %H:%M')
    sheet_ids   = json.loads(os.environ['SHEET_IDS'])
    sheet_index = int(os.environ.get('SHEET_INDEX', '0'))
    sheet_id    = sheet_ids[sheet_index]

    print(f'''
=======================================================
  SPOTIFY FOLLOWER TRACKER
  Sheet {sheet_index + 1} of {len(sheet_ids)}
  Sheet ID: {sheet_id}
  Date: {today_str}
=======================================================''')

    print('🔑 Initialising Spotify credentials...')
    token_manager = SpotifyTokenManager()
    token_manager.get_token()

    print('🔗 Connecting to Google Sheets...')
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(sheet_id)
    print(f'   ✅ Connected to: {spreadsheet.title}')

    follower_sheets = [
        s for s in spreadsheet.worksheets()
        if s.title.endswith('_Followers')
    ]
    print(f'📋 Found {len(follower_sheets)} _Followers tabs')

    for sheet in follower_sheets:
        try:
            process_followers_sheet(sheet, token_manager, today_str)
        except Exception as e:
            print(f'   ❌ Error on {sheet.title}: {e}')

    print(f'''
=======================================================
  ✅ Sheet {sheet_index + 1} complete!
=======================================================''')


if __name__ == '__main__':
    main()
