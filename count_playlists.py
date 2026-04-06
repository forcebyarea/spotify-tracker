import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
#  PLAYLIST COUNTER
#  Counts all playlists across all 17 sheets
# ============================================================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def main():
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sheet_ids = json.loads(os.environ['SHEET_IDS'])

    print(f'''
=======================================================
  PLAYLIST COUNTER
  Counting playlists across {len(sheet_ids)} sheets
=======================================================''')

    grand_total = 0
    grand_followers_tabs = 0

    for i, sheet_id in enumerate(sheet_ids):
        try:
            spreadsheet = gc.open_by_key(sheet_id)
            follower_sheets = [
                s for s in spreadsheet.worksheets()
                if s.title.endswith('_Followers')
            ]

            sheet_total = 0
            print(f'\n📊 Sheet {i+1}: {spreadsheet.title}')

            for sheet in follower_sheets:
                all_values = sheet.get_all_values()
                # Count rows from row 3 onwards with a URL in column B
                count = 0
                for row in all_values[2:]:
                    if len(row) > 1 and 'spotify' in str(row[1]):
                        count += 1

                sheet_total += count
                grand_followers_tabs += 1
                print(f'   📋 {sheet.title}: {count} playlists')

            grand_total += sheet_total
            print(f'   ✅ Sheet total: {sheet_total}')

        except Exception as e:
            print(f'   ❌ Error on sheet {i+1}: {e}')

    print(f'''
=======================================================
  📊 FINAL RESULTS
  Total _Followers tabs: {grand_followers_tabs}
  Total playlists: {grand_total}
  
  At 0.5s delay: ~{round(grand_total * 0.5 / 60)} minutes
  At 1s delay:   ~{round(grand_total * 1 / 60)} minutes  
  At 3s delay:   ~{round(grand_total * 3 / 60)} minutes
  
  API keys needed for 1 hour completion:
  ~{max(1, round(grand_total * 1 / 60 / 60))} key(s) at 1s delay
=======================================================''')

if __name__ == '__main__':
    main()
