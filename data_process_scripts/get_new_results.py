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
import pytz  # Added for timezone handling

# ========================
# Configuration and Setup
# ========================
parser = argparse.ArgumentParser(description='Athlete data processing pipeline')
parser.add_argument('--mode', choices=['collect', 'process'], required=True,
                   help='"collect" for initial data collection, "process" for sharded processing')
parser.add_argument('--shard', type=int, default=0,
                   help='Shard index for parallel execution (process mode only)')
parser.add_argument('--num_shards', type=int, default=1,
                   help='Total number of shards (process mode only)')
args = parser.parse_args()

script_dir = os.path.dirname(os.path.abspath(__file__))
current_script = os.path.basename(__file__)

# Set timezone to Eastern Time
eastern = pytz.timezone('US/Eastern')

# ========================
# Common Functions
# ========================
def clean_working_directory():
    """Clean working directory except the script itself"""
    for file_name in os.listdir(script_dir):
        file_path = os.path.join(script_dir, file_name)
        if file_name == current_script:
            continue
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to remove {file_path}: {e}")

# ========================
# Data Collection (Run once)
# ========================
def collect_initial_data():
    states = [
        "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
        "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
        "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
        "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
        "wi", "wy"
    ]

    clean_working_directory()

    meet_numbers = set()
    current_date = datetime.now(eastern).date()

    for state in states:
        url = f"https://{state}.milesplit.com/results"
        response = requests.get(url)
        if response.status_code != 200:
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='meets order-table table results')

        if table:
            for row in table.find_all('tr'):
                date_cell = row.find('td', class_='date')
                if not date_cell:
                    continue
        
                date_span = date_cell.find('span', class_='end') or date_cell.find('span', class_='start')
                if not date_span:
                    continue
        
                try:
                    date_text = date_span.text.strip()
                    parsed_date = datetime.strptime(date_text, "%m/%d").replace(year=current_date.year).date()
                    print(f"🗓 Found meet date: {parsed_date} (delta = {(parsed_date - current_date).days} days)")
        
                    #if abs((parsed_date - current_date).days) > 25:
                   if abs((parsed_date - current_date).days) <= 25 or abs((parsed_date - current_date).days) >= 45:
                        continue  # ❌ Skip meet too far from today
        
                    print(f"✔ Meet on {parsed_date} is within 30 days")
        
                    # ✅ Only collect meet numbers if the date passes
                    link_cell = row.find('td', class_='name')
                    if link_cell:
                        a_tag = link_cell.find('a', href=True)
                        if a_tag:
                            match = re.search(r'meets/(\d+)-', a_tag['href'])
                            if match:
                                meet_number = match.group(1)
                                meet_numbers.add(meet_number)
                                print(f"Collected meet number: {meet_number}")
                except ValueError:
                    continue
        
        time.sleep(2)  # ✅ inside the row loop, not after the table


    with open(os.path.join(script_dir, 'meet-numbers'), 'w') as f:
        f.writelines(f"{m}\n" for m in sorted(meet_numbers))

    meet_data_dir = os.path.join(script_dir, 'meet-data')
    os.makedirs(meet_data_dir, exist_ok=True)

    meet_numbers_path = os.path.join(script_dir, 'meet-numbers')
    with open(meet_numbers_path, 'r') as f:
        meets = [line.strip() for line in f]

    api_url_template = (
        "https://www.milesplit.com/api/v1/meets/{}/performances?"
        "ismeetpro=0&fields=id,meetId,meetName,teamId,videoId,teamName,athleteId,firstName,lastName,"
        "gender,genderName,divisionId,divisionName,meetResultsDivisionId,resultsDivisionId,ageGroupName,"
        "gradYear,eventName,eventCode,eventDistance,eventGenreOrder,round,roundName,heat,units,mark,"
        "place,windReading,profileUrl,teamProfileUrl,performanceVideoId,teamLogo,statusCode,dateStart,"
        "dateEnd,season,seasonYear,venueCity,venueState,venueCountry"
    )

    for meet in meets:
        api_url = api_url_template.format(meet)
        response = requests.get(api_url)
        print(f"Fetching meet {meet}... status code: {response.status_code}")

        if response.status_code == 200:
            try:
                data = response.json()
                num_performances = len(data.get("data", []))
                print(f"  → Found {num_performances} performances for meet {meet}")

                output_path = os.path.join(meet_data_dir, f"{meet}.json")
                with open(output_path, 'w') as f:
                  json.dump(data, f, indent=2)
                  f.flush()
                  os.fsync(f.fileno())  # ✅ Force flush to disk
                  
                print(f"📁 Successfully saved: {output_path}")
            except Exception as e:
                print(f"❌ Error parsing or saving meet {meet}: {e}")
        else:
            print(f"⚠️ Failed to fetch meet {meet}: status {response.status_code}")

        time.sleep(1)

    # ✅ Athlete ID extraction
    athlete_ids = set()
    for meet_file in sorted(glob.glob(os.path.join(meet_data_dir, '*.json'))):
        try:
            with open(meet_file, 'r') as f:
                data = json.load(f)
                performances = data.get('data', [])
                print(f"{meet_file}: {len(performances)} performances")

                for performance in performances:
                    athlete_id = performance.get('athleteId')
                    if athlete_id:
                        print(f"  → Found athlete: {athlete_id}")
                        athlete_ids.add(athlete_id)
                    else:
                        print("  → Skipped performance with missing athleteId")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"❌ Error reading {meet_file}: {e}")

    with open(os.path.join(script_dir, 'athlete-numbers'), 'w') as f:
        f.writelines(f"{id}\n" for id in sorted(athlete_ids))

    print(f"✅ Total athletes saved: {len(athlete_ids)}")

# ========================
# Sharded Processing
# ========================
def process_shard():
    athlete_dir = os.path.join(script_dir, 'athlete-metadata')
  
    # Clean the output directory before writing
    if os.path.exists(athlete_dir):
        shutil.rmtree(athlete_dir)
    os.makedirs(athlete_dir, exist_ok=True)

    # Athlete metadata processing
    with open(os.path.join(script_dir, 'athlete-numbers'), 'r') as f:
        all_athletes = [line.strip() for line in f if line.strip()]

    if args.shard >= args.num_shards:
        raise ValueError(f"Invalid shard index {args.shard} for num_shards={args.num_shards}")

    athletes = [
        athlete_id for i, athlete_id in enumerate(all_athletes)
        if i % args.num_shards == args.shard
    ]

    print(f"✅ Shard {args.shard}/{args.num_shards} assigned {len(athletes)} athletes")

    if not athletes:
        print("⚠️ No athletes assigned to this shard. Exiting.")
        return

    athlete_dir = os.path.join(script_dir, 'athlete-metadata')
    os.makedirs(athlete_dir, exist_ok=True)

    for idx, athlete_id in enumerate(athletes):
        try:
            print(f"[{idx+1}/{len(athletes)}] 🔍 Processing athlete ID: {athlete_id}")

            athlete_url = (
                f"https://www.milesplit.com/api/v1/athletes/{athlete_id}/stats?"
                "ismeetpro=0&fields=id,meetId,meetName,teamId,videoId,teamName,athleteId,firstName,"
                "lastName,gender,genderName,divisionId,divisionName,meetResultsDivisionId,"
                "resultsDivisionId,ageGroupName,gradYear,eventName,eventCode,eventDistance,"
                "eventGenreOrder,round,roundName,heat,units,mark,place,windReading,profileUrl,"
                "teamProfileUrl,performanceVideoId,teamLogo,statusCode,dateStart,dateEnd,season,"
                "seasonYear,venueCity,venueState,venueCountry,siteSubdomain,slug,nickname,birthDate,"
                "birthYear,note,honors,specialty,city,state,country,isProfilePhoto,hide,usatf,tfrrsId,"
                "lastTouch,profilePhotoUrl"
            )

            response = requests.get(athlete_url, timeout=10)
            print(f"→ Status code: {response.status_code}")

            if response.status_code != 200:
                print(f"❌ HTTP {response.status_code} for athlete ID {athlete_id}")
                continue

            try:
                response_json = response.json()
            except json.JSONDecodeError:
                print(f"❌ JSON decode error for athlete ID {athlete_id}")
                continue

            athlete = response_json.get('_embedded', {}).get('athlete', {})
            if not athlete:
                print(f"⚠️ No athlete object found in response for {athlete_id}")
                continue

            if "gradYear" not in athlete:
                print(f"⚠️ Missing gradYear for athlete {athlete_id}")

            output_content = {
                "data": response_json.get('data', []),
                "athlete": athlete
            }
            output_file = os.path.join(athlete_dir, f"{athlete_id}.json")
            with open(output_file, 'w') as f:
                json.dump(output_content, f, indent=4)

            print(f"✅ Saved athlete {athlete_id}")
            time.sleep(1)

        except Exception as e:
            print(f"❌ Exception for athlete {athlete_id}: {e}")
            continue

    # Team data processing
    team_ids = set()
    for athlete_file in glob.glob(os.path.join(athlete_dir, '*.json')):
        try:
            with open(athlete_file, 'r') as f:
                data = json.load(f)
                if team_id := data.get('athlete', {}).get('teamId'):
                    team_ids.add(team_id)
        except (json.JSONDecodeError, KeyError):
            continue

    team_dir = os.path.join(script_dir, 'team-data')
    os.makedirs(team_dir, exist_ok=True)
    
    for team_id in team_ids:
        team_url = f"https://www.milesplit.com/api/v1/teams/{team_id}"
        response = requests.get(team_url)
        if response.status_code == 200:
            try:
                team_data = response.json().get('data', {})
                output_file = os.path.join(team_dir, f"{team_id}.json")
                with open(output_file, 'w') as f:
                    json.dump(team_data, f, indent=4)
            except json.JSONDecodeError:
                pass
        time.sleep(1)

    # Data enrichment
    for athlete_file in glob.glob(os.path.join(athlete_dir, '*.json')):
        try:
            with open(athlete_file, 'r') as f:
                athlete_data = json.load(f)
                team_id = athlete_data.get('athlete', {}).get('teamId')
                
                if team_id:
                    team_file = os.path.join(team_dir, f"{team_id}.json")
                    if os.path.exists(team_file):
                        with open(team_file, 'r') as tf:
                            team_data = json.load(tf)
                            athlete_data['team-data'] = team_data
                            with open(athlete_file, 'w') as f:
                                json.dump(athlete_data, f, indent=4)
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            continue

# ========================
# Main Execution
# ========================
if __name__ == "__main__":
    if args.mode == 'collect':
        collect_initial_data()
    elif args.mode == 'process':
        process_shard()
