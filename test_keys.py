import os
import json
import requests

def test_key(index, cred):
    # Get token
    r = requests.post(
        'https://accounts.spotify.com/api/token',
        data={'grant_type': 'client_credentials'},
        auth=(cred['id'], cred['secret'])
    )
    if r.status_code != 200:
        print(f'   Key {index+1}: ❌ Token failed — {r.status_code}')
        return

    token = r.json().get('access_token')

    # Test one playlist request
    test_id = '37i9dQZF1DXcBWIGoYBM5M'  # Today's Top Hits
    r2 = requests.get(
        f'https://api.spotify.com/v1/playlists/{test_id}',
        headers={'Authorization': f'Bearer {token}'},
        params={'fields': 'followers.total'}
    )

    if r2.status_code == 200:
        followers = r2.json().get('followers', {}).get('total', 0)
        print(f'   Key {index+1}: ✅ Working — test playlist has {followers:,} followers')
    elif r2.status_code == 429:
        wait = r2.headers.get('Retry-After', '???')
        mins = int(wait) // 60 if wait != '???' else '???'
        print(f'   Key {index+1}: 🔴 Rate limited — {mins} minutes remaining')
    else:
        print(f'   Key {index+1}: ⚠️ Status {r2.status_code}')

def main():
    creds = json.loads(os.environ['SPOTIFY_CREDENTIALS'])
    print(f'\n🔍 Testing all {len(creds)} API keys...\n')
    for i, cred in enumerate(creds):
        test_key(i, cred)
    print('\nDone!')

if __name__ == '__main__':
    main()
