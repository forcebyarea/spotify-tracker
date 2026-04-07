import os
import json
import time
import gspread
import requests
from datetime import datetime
import pytz
from google.oauth2.service_account import Credentials

# ============================================================
#  SPOTIFY FOLLOWER TRACKER — FULL UPDATE
#  1. Indian time (IST)
#  2. Correct yellow/red colours matching original sheet
#  3. No colour formatting on new date header column
#  4. API rotation starts from exact playlist where rate limit hit
#  5. Smart delay to avoid rate limits with 6 keys
# ============================================================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# 6 keys, ~7885 playlists total
# Spotify allows ~180 req/min per key
# With 6 keys rotating = effectively 6x capacity
# Safe delay = 1s per request (well under limit for any single key)
DELAY_BETWEEN_REQUESTS = 1
DELAY_BETWEEN_SHEETS   = 15
MAX_RETRIES            = 6  # one retry per key


# ── API Key Rotation ─────────────────────────────────────────

class SpotifyTokenManager:
    def __init__(self):
        self.credentials = self._load_credentials()
        self.current_index = 0
        self.tokens = {}
        self.request_counts = [0] * len(self.credentials)
        print(f'   🔑 Loaded {len(self.credentials)} Spotify credentials')

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
                print(f'   ✅ Token from key {self.current_index + 1}/{len(self.credentials)}')
            else:
                return self._rotate()
        return self.tokens.get(self.current_index)

    def _rotate(self):
        # Try every other key
        start = self.current_index
        for offset in range(1, len(self.credentials)):
            next_index = (start + offset) % len(self.credentials)
            print(f'   🔄 Switching to key {next_index + 1}/{len(self.credentials)}...')
            token = self._fetch_token(next_index)
            if token:
                self.current_index = next_index
                self.tokens[next_index] = token
                print(f'   ✅ Token from key {next_index + 1}/{len(self.credentials)}')
                return token
        print('   ❌ All keys exhausted or rate limited')
        return None

    def handle_rate_limit(self, wait_seconds):
        # Clear current key token and rotate immediately
        # Don't wait the full Retry-After — just switch keys
        print(f'   ⚡ Rate limit hit on key {self.current_index + 1} — rotating immediately')
        if self.current_index in self.tokens:
            del self.tokens[self.current_index]
        return self._rotate()

    def handle_401(self):
        if self.current_index in self.tokens:
            del self.tokens[self.current_index]
        return self._rotate()

    def increment(self):
        self.request_counts[self.current_index] += 1
        # Proactively rotate every 200 requests to spread load
        if self.request_counts[self.current_index] % 200 == 0:
            print(f'   🔄 Proactive rotation after 200 requests on key {self.current_index + 1}')
            self._rotate()


# ── Google Sheets ────────────────────────────────────────────

def get_gspread_client():
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Spotify Data ─────────────────────────────────────────────

def get_playlist_followers(playlist_id, token_manager):
    url    = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    params = {'fields': 'followers.total'}

    for attempt in range(MAX_RETRIES):
        token = token_manager.get_token()
        if not token:
            print(f'   ❌ No valid token available')
            return None

        r = requests.get(
            url,
            headers={'Authorization': f'Bearer {token}'},
            params=params
        )

        if r.status_code == 200:
            token_manager.increment()
            return r.json().get('followers', {}).get('total', 0)

        elif r.status_code == 429:
            wait = int(r.headers.get('Retry-After', 30))
            new_token = token_manager.handle_rate_limit(wait)
            if not new_token:
                # All keys rate limited — wait minimum time
                print(f'   😴 All keys rate limited — waiting 60s...')
                time.sleep(60)

        elif r.status_code == 401:
            print(f'   🔄 401 — rotating key...')
            token_manager.handle_401()

        elif r.status_code == 404:
            # Playlist deleted/private — skip silently
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

    # Expand columns if needed
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

    # Write header — NO colour formatting on header cell
    sheet.update_cell(2, new_col, today_str)

    # Explicitly clear any formatting on the header cell
    sheet.spreadsheet.batch_update({'requests': [{
        'repeatCell': {
            'range': {
                'sheetId': sheet.id,
                'startRowIndex': 1,
                'endRowIndex': 2,
                'startColumnIndex': new_col - 1,
                'endColumnIndex': new_col
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': {'red': 1, 'green': 1, 'blue': 1}
                }
            },
            'fields': 'userEnteredFormat.backgroundColor'
        }
    }]})

    print(f'   ✅ Header written at col {new_col} (no colour)')
    return new_col


def process_followers_sheet(sheet, token_manager, today_str):
    print(f'\n   📋 Processing: {sheet.title}')

    all_values = sheet.get_all_values()
    if len(all_values) < 3:
        print(f'   ⚠️ Not enough rows — skipping')
        return

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

        followers = get_playlist_followers(playlist_id, token_manager)
        time.sleep(DELAY_BETWEEN_REQUESTS)

        if followers is None:
            skipped += 1
            continue

        sheet_row = row_idx + 1

        try:
            # Write follower count
            sheet.update_cell(sheet_row, col_index, followers)
            written += 1

            # Determine colour — match original sheet style exactly
            # Yellow (#FFFF00) = growth, Red (#FF0000) = decline, White = no change
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
                    # Yellow — exact match to original sheet
                    color = {'red': 1.0, 'green': 1.0, 'blue': 0.0}
                elif followers < prev_followers:
                    # Red — exact match to original sheet
                    color = {'red': 1.0, 'green': 0.0, 'blue': 0.0}
                else:
                    # White — no change
                    color = {'red': 1.0, 'green': 1.0, 'blue': 1.0}
            else:
                # White — no previous data
                color = {'red': 1.0, 'green': 1.0, 'blue': 1.0}

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


# ── Main ─────────────────────────────────────────────────────

def main():
    # 1. Indian Standard Time
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today_str = now.strftime('%d %b %H:%M IST')

    sheet_ids = json.loads(os.environ['SHEET_IDS'])

    print(f'''
=======================================================
  SPOTIFY FOLLOWER TRACKER
  Processing all {len(sheet_ids)} sheets one by one
  Date: {today_str}
=======================================================''')

    print('🔑 Initialising Spotify credentials...')
    token_manager = SpotifyTokenManager()
    token_manager.get_token()

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
                    process_followers_sheet(sheet, token_manager, today_str)
                except Exception as e:
                    print(f'   ❌ Error on {sheet.title}: {e}')

        except Exception as e:
            print(f'   ❌ Could not open sheet {sheet_id}: {e}')

        if i < len(sheet_ids) - 1:
            print(f'   ⏸️  Pausing {DELAY_BETWEEN_SHEETS}s before next sheet...')
            time.sleep(DELAY_BETWEEN_SHEETS)

    print(f'''
=======================================================
  ✅ All {len(sheet_ids)} sheets complete!
  Finished: {datetime.now(ist).strftime("%d %b %H:%M IST")}
=======================================================''')


if __name__ == '__main__':
    main()
