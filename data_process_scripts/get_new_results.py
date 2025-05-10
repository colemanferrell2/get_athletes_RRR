import os
import requests
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime, timedelta
import glob
import time
import shutil
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--shard', type=int, default=0)
parser.add_argument('--num_shards', type=int, default=1)
args = parser.parse_args()

script_dir = os.path.dirname(os.path.abspath(__file__))
current_script = os.path.basename(__file__)

# Clean working directory except for this script
for file_name in os.listdir(script_dir):
    file_path = os.path.join(script_dir, file_name)
    if file_name == current_script:
        continue
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Removed file: {file_path}")
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
            print(f"Removed folder: {file_path}")
    except Exception as e:
        print(f"Failed to remove {file_path}. Error: {e}")

# List of two-letter state abbreviations
states = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me",
    "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa",
    "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"
]

output_file = os.path.join(script_dir, 'meet-numbers')
meet_numbers = set()
current_date = datetime.now().date()

for state in states:
    url = f"https://{state}.milesplit.com/results"
    print(f"Fetching data from {url}...")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch the page for {state.upper()}. Status code: {response.status_code}")
        continue
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', class_='meets order-table table results')
    if table:
        for row in table.find_all('tr'):
            date_cell = row.find('td', class_='date')
            if not date_cell:
                continue
            try:
                date_text = date_cell.text.strip()
                date_text = re.sub(r'\s+', ' ', date_text).strip()
                if '-' in date_text or ' ' in date_text:
                    date_parts = re.split(r'[-\s]', date_text)
                    date_text = date_parts[-1]
                meet_date = datetime.strptime(date_text, "%m/%d")
                meet_date = meet_date.replace(year=current_date.year).date()
            except ValueError:
                print(f"Failed to parse date: {date_cell.text.strip()}")
                continue
            print(f"Checking: {meet_date} (Range: {current_date - timedelta(days=1)} to {current_date + timedelta(days=1)})")
            if not (current_date - timedelta(days=1) <= meet_date <= current_date + timedelta(days=1)):
                continue
            link_cell = row.find('td', class_='name')
            if not link_cell:
                continue
            a_tag = link_cell.find('a', href=True)
            if not a_tag:
                continue
            match = re.search(r'meets/(\d+)-', a_tag['href'])
            if match:
                meet_numbers.add(match.group(1))
    print(f"Finished processing state: {state.upper()}. Waiting for 2 seconds before the next state...")
    time.sleep(2)

with open(output_file, 'w') as file:
    for number in sorted(meet_numbers):
        file.write(number + '\n')
print(f"All unique meet numbers saved to '{output_file}'.")

meet_data_dir = os.path.join(script_dir, 'meet-data')
os.makedirs(meet_data_dir, exist_ok=True)

api_url_template = (
    "https://www.milesplit.com/api/v1/meets/{}/performances?"
    "ismeetpro=0&fields=id,meetId,meetName,teamId,videoId,teamName,athleteId,firstName,lastName,"
    "gender,genderName,divisionId,divisionName,meetResultsDivisionId,resultsDivisionId,ageGroupName,"
    "gradYear,eventName,eventCode,eventDistance,eventGenreOrder,round,roundName,heat,units,mark,"
    "place,windReading,profileUrl,teamProfileUrl,performanceVideoId,teamLogo,statusCode,dateStart,"
    "dateEnd,season,seasonYear,venueCity,venueState,venueCountry"
)

with open(output_file, 'r') as file:
    meet_numbers = [line.strip() for line in file.readlines()]

for meet_number in meet_numbers:
    print(f"Fetching data for meet number {meet_number}...")
    api_url = api_url_template.format(meet_number)
    response = requests.get(api_url)
    if response.status_code != 200:
        print(f"Failed to fetch data for meet number {meet_number}. Status code: {response.status_code}")
        continue
    output_file_meet = os.path.join(meet_data_dir, f"{meet_number}.json")
    with open(output_file_meet, 'w') as file:
        file.write(response.text)
    time.sleep(1)
    print(f"Data for meet number {meet_number} saved to '{output_file_meet}'.")
print("All meet data has been fetched and saved.")

athlete_numbers_file = os.path.join(script_dir, 'athlete-numbers')
unique_athlete_ids = set()

for meet_file in os.listdir(meet_data_dir):
    meet_file_path = os.path.join(meet_data_dir, meet_file)
    try:
        with open(meet_file_path, 'r') as file:
            data = json.load(file)
            for performance in data.get('data', []):
                athlete_id = performance.get('athleteId')
                if athlete_id:
                    unique_athlete_ids.add(athlete_id)
    except (json.JSONDecodeError, KeyError):
        print(f"Failed to process file: {meet_file_path}")
        continue

with open(athlete_numbers_file, 'w') as file:
    for athlete_id in sorted(unique_athlete_ids):
        file.write(str(athlete_id) + '\n')
print(f"All unique athlete IDs saved to '{athlete_numbers_file}'.")

with open(athlete_numbers_file, 'r') as file:
    all_athletes = [line.strip() for line in file.readlines()]

athlete_metadata_dir = os.path.join(script_dir, 'athlete-metadata')
os.makedirs(athlete_metadata_dir, exist_ok=True)

athlete_numbers = [id for i, id in enumerate(all_athletes) if i % args.num_shards == args.shard]

for athlete_id in athlete_numbers:
    print(f"Fetching metadata for athlete ID {athlete_id}...")
    athlete_url = (
        f"https://www.milesplit.com/api/v1/athletes/{athlete_id}/stats?ismeetpro=0&fields=id,meetId,meetName,teamId,videoId,teamName,athleteId,firstName,lastName,gender,genderName,divisionId,divisionName,meetResultsDivisionId,resultsDivisionId,ageGroupName,gradYear,eventName,eventCode,eventDistance,eventGenreOrder,round,roundName,heat,units,mark,place,windReading,profileUrl,teamProfileUrl,performanceVideoId,teamLogo,statusCode,dateStart,dateEnd,season,seasonYear,venueCity,venueState,venueCountry,siteSubdomain,slug,nickname,birthDate,birthYear,note,honors,specialty,city,state,country,isProfilePhoto,hide,usatf,tfrrsId,lastTouch,profilePhotoUrl"
    )
    response = requests.get(athlete_url)
    if response.status_code != 200:
        print(f"Failed to fetch metadata for athlete ID {athlete_id}. Status code: {response.status_code}")
        continue
    try:
        response_json = response.json()
        data = response_json.get('data', [])
        athlete = response_json.get('_embedded', {}).get('athlete', {})
    except json.JSONDecodeError:
        print(f"Failed to parse JSON for athlete ID {athlete_id}.")
        continue
    grad_year = athlete.get("gradYear")
    weighted_score = athlete.get("weightedScore", 0)
    if (
        grad_year and str(grad_year).isdigit() and int(grad_year) >= 2026
        and isinstance(weighted_score, (int, float)) and weighted_score >= 0
    ):
        output_content = {
            "data": data,
            "athlete": athlete
        }
        output_file = os.path.join(athlete_metadata_dir, f"{athlete_id}.json")
        with open(output_file, 'w') as file:
            json.dump(output_content, file, indent=4)
        print(f"Metadata for athlete ID {athlete_id} saved to '{output_file}'.")
    else:
        print(f"Skipped athlete ID {athlete_id} due to gradYear: {grad_year}")
    time.sleep(1)
print("All athlete metadata has been fetched and saved.")

team_ids = set()
for file_path in glob.glob(os.path.join(athlete_metadata_dir, '*.json')):
    with open(file_path, 'r') as file:
        try:
            content = json.load(file)
            athlete = content.get('athlete', {})
            team_id = athlete.get('teamId')
            if team_id:
                team_ids.add(team_id)
        except json.JSONDecodeError:
            print(f"Failed to parse JSON in file: {file_path}")

team_numbers_file = os.path.join(script_dir, 'team-numbers')
with open(team_numbers_file, 'w') as file:
    for team_id in sorted(team_ids):
        file.write(f"{team_id}\n")
print(f"All unique team IDs have been saved to '{team_numbers_file}'.")

team_data_dir = os.path.join(script_dir, 'team-data')
os.makedirs(team_data_dir, exist_ok=True)

with open(team_numbers_file, 'r') as file:
    team_numbers = [line.strip() for line in file.readlines()]

for team_id in team_numbers:
    print(f"Fetching data for team ID {team_id}...")
    team_url = f"https://www.milesplit.com/api/v1/teams/{team_id}"
    response = requests.get(team_url)
    if response.status_code != 200:
        print(f"Failed to fetch data for team ID {team_id}. Status code: {response.status_code}")
        continue
    try:
        data = response.json().get('data', {})
    except json.JSONDecodeError:
        print(f"Failed to parse JSON for team ID {team_id}.")
        continue
    output_file = os.path.join(team_data_dir, f"{team_id}.json")
    with open(output_file, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data for team ID {team_id} saved to '{output_file}'.")
print("All team data has been fetched and saved.")

for athlete_file_path in glob.glob(os.path.join(athlete_metadata_dir, '*.json')):
    with open(athlete_file_path, 'r') as athlete_file:
        try:
            athlete_content = json.load(athlete_file)
            athlete = athlete_content.get('athlete', {})
            team_id = athlete.get('teamId')
            if not team_id:
                print(f"No teamId found in file: {athlete_file_path}")
                continue
            team_file_path = os.path.join(team_data_dir, f"{team_id}.json")
            if not os.path.exists(team_file_path):
                print(f"No team file found for teamId {team_id} in file: {athlete_file_path}")
                continue
            with open(team_file_path, 'r') as team_file:
                team_content = json.load(team_file)
            athlete_content['team-data'] = team_content
            with open(athlete_file_path, 'w') as athlete_file_w:
                json.dump(athlete_content, athlete_file_w, indent=4)
            print(f"Updated athlete file with team-data for teamId {team_id}: {athlete_file_path}")
        except json.JSONDecodeError:
            print(f"Failed to parse JSON in file: {athlete_file_path}")
        except Exception as e:
            print(f"An error occurred while processing file {athlete_file_path}: {e}")
print("All athlete files have been updated with team-data.")
