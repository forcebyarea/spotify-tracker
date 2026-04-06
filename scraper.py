import os
import json
import time
import gspread
import requests
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
#  SPOTIFY FOLLOWER TRACKER
#  - Multi API key rotation (up to 4 keys)
#  - Auto switches to next key on 401/429
#  - Reads all _Followers tabs in a sheet
#  - Writes follower counts with colours
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
        # Try multi-key JSON first: SPOTIFY_CREDENTIALS = [{"id":"...","secret":"..."},...]
        multi = os.environ.get('SPOTIFY_CREDENTIALS')
        if multi:
            try:
                creds = json.loads(multi)
                if isinstance(creds, list) and len(creds) > 0:
                    return creds
            except:
                pass

        # Fall back to single key
        client_id     = os.environ.get('SPOTIFY_CLIENT_ID')
        client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')
        if client_id and client_secret:
            return [{'id': client_id, 'secret': client_secret}]

        raise Exception('No Spotify credentials found in environment')

    def _fetch_token(self, index):
        cred = self.credentials[index]
        try:
            response = requests.post(
                'https://accounts.spotify.com/api/token',
                data={'grant_type': 'client_credentials'},
                auth=(cred['id'], cred['secret'])
            )
            if response.status_code == 200:
                return response.json().get('access_token')
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
                print(f'   ❌ Credential {self.current_index + 1} failed — trying next...')
                return self._rotate()
        return self.tokens.get(self.current_index)

    def _rotate(self):
        next_index = self.current_index + 1
        while next_index < len(self.credentials):
            print(f'   🔄 Switching to credential {next_index + 1}/{len(self.credentials)}...')
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
        # Clear current token and rotate to next key
        if self.current_index in self.tokens:
            del self.tokens[self.current_index]
        return self._rotate()


# ── Google Sheets ────────────────────────────────────────────

def get_gspread_client():
    creds_json = os.environ['GOOGLE_CREDENTIALS']
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Spotify ──────────────────────────────────────────────────

def get_playlist_followers(playlist_id, token_manager):
    url    = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    params = {'fields': 'followers.total'}

    for attempt in range(MAX_RETRIES):
        token = token_manager.get_token()
        if not token:
            print(f'   ❌ No valid token available')
            return None

        headers  = {'Authorization': f'Bearer {token}'}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json().get('followers', {}).get('total', 0)

        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 10))
            print(f'   ⏳ Rate limited — waiting {retry_after}s then rotating key...')
            time.sleep(retry_after)
            token_manager.handle_failure()

        elif response.status_code == 401:
            print(f'   🔄 Token expired — rotating key...')
            new_token = token_manager.handle_failure()
            if not new_token:
                return None

        else:
            print(f'   ⚠️ Status {response.status_code} for {playlist_id}')
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

def get_or_create_column(sheet, today_str, all_values):
    # Use already-fetched all_values to find headers
    if len(all_values) < 2:
        return 3

    headers = all_values[1]  # row 2 (0-indexed)

    # Check if today column already exists
    for i, h in enumerate(headers):
        if today_str in str(h):
            print(f"   📅 Found existing column for {today_str} at col {i+1}")
            return i + 1

    # Find last non-empty header
    last_col = 0
    for i, h in enumerate(headers):
        if str(h).strip():
            last_col = i + 1

    new_col = last_col + 1
    print(f"   📅 Creating new column {new_col} for {today_str}")

    # Expand columns if needed
    props = sheet.spreadsheet.fetch_sheet_metadata()
    for s in props["sheets"]:
        if s["properties"]["sheetId"] == sheet.id:
            current_cols = s["properties"]["gridProperties"]["columnCount"]
            if new_col >= current_cols:
                sheet.spreadsheet.batch_update({"requests": [{"appendDimension": {"sheetId": sheet.id, "dimension": "COLUMNS", "length": 50}}]})
                print(f"   📐 Expanded columns by 50")
            break

    sheet.update_cell(2, new_col, today_str)
    return new_col



def process_followers_sheet(sheet, token_manager, today_str):
    print(f'   📋 Processing: {sheet.title}')

    all_values = sheet.get_all_values()
    if len(all_values) < 3:
        print(f'   ⚠️ Not enough rows — skipping')
        return

    col_index     = get_or_create_column(sheet, today_str, all_values)
    updates       = []
    color_updates = []

    for row_idx in range(2, len(all_values)):
        row = all_values[row_idx]
        if len(row) < 2:
            continue

        playlist_url = row[1].strip()
        if not playlist_url or 'spotify' not in playlist_url:
            continue

        playlist_id = extract_playlist_id(playlist_url)
        if not playlist_id:
            continue

        followers = get_playlist_followers(playlist_id, token_manager)
        if followers is None:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            continue

        sheet_row = row_idx + 1
        updates.append({
            'range': gspread.utils.rowcol_to_a1(sheet_row, col_index),
            'values': [[followers]]
        })

        # Colour based on previous column
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

        color_updates.append({'row': sheet_row, 'col': col_index, 'color': color})
        time.sleep(DELAY_BETWEEN_REQUESTS)

    if updates:
        sheet.batch_update(updates)
        print(f'   ✅ Wrote {len(updates)} follower counts')

    if color_updates:
        requests_body = [{
            'repeatCell': {
                'range': {
                    'sheetId': sheet.id,
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
        print(f'   🎨 Applied colours')


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
    token_manager.get_token()  # warm up first token

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
