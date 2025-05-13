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

    # Meet number collection
    meet_numbers = set()
    current_date = datetime.now(eastern).date()  # Get current date in Eastern Time
    
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

                try:
                    date_text = re.sub(r'\s+', ' ', date_cell.text.strip())
                    if '-' in date_text or ' ' in date_text:
                        date_parts = re.split(r'[-\s]', date_text)
                        date_text = date_parts[-1]
                    
                    meet_date = datetime.strptime(date_text, "%m/%d")
                    meet_date = meet_date.replace(year=current_date.year).date()
                except ValueError:
                    continue

                if not (current_date - timedelta(days=3) <= meet_date <= current_date + timedelta(days=1)):
                    continue

                link_cell = row.find('td', class_='name')
                if link_cell:
                    a_tag = link_cell.find('a', href=True)
                    if a_tag:
                        match = re.search(r'meets/(\d+)-', a_tag['href'])
                        if match:
                             meet_number = match.group(1)
                             meet_numbers.add(meet_number)
                             print(f"Collected meet number: {meet_number}")
        
        time.sleep(2)

    with open(os.path.join(script_dir, 'meet-numbers'), 'w') as f:
        f.writelines(sorted(meet_numbers))

    # Meet data processing
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
        if response.status_code == 200:
            with open(os.path.join(meet_data_dir, f"{meet}.json"), 'w') as f:
                f.write(response.text)
        time.sleep(1)

    # Athlete ID extraction
    athlete_ids = set()
    for meet_file in glob.glob(os.path.join(meet_data_dir, '*.json')):
        try:
            with open(meet_file, 'r') as f:
                data = json.load(f)
                for performance in data.get('data', []):
                    if athlete_id := performance.get('athleteId'):
                        athlete_ids.add(athlete_id)
        except (json.JSONDecodeError, KeyError):
            continue

    with open(os.path.join(script_dir, 'athlete-numbers'), 'w') as f:
        f.writelines(sorted(str(id) for id in athlete_ids))

# ========================
# Sharded Processing
# ========================
def process_shard():
    # Athlete metadata processing
    with open(os.path.join(script_dir, 'athlete-numbers'), 'r') as f:
        all_athletes = [line.strip() for line in f]
    
    athletes = [id for i, id in enumerate(all_athletes) 
               if i % args.num_shards == args.shard]
    
    athlete_dir = os.path.join(script_dir, 'athlete-metadata')
    os.makedirs(athlete_dir, exist_ok=True)

    for athlete_id in athletes:
        athlete_url = (
            f"https://www.milesplit.com/api/v1/athletes/{athlete_id}/stats?ismeetpro=0&fields="
            "id,meetId,meetName,teamId,videoId,teamName,athleteId,firstName,lastName,gender,genderName,"
            "divisionId,divisionName,meetResultsDivisionId,resultsDivisionId,ageGroupName,gradYear,"
            "eventName,eventCode,eventDistance,eventGenreOrder,round,roundName,heat,units,mark,place,"
            "windReading,profileUrl,teamProfileUrl,performanceVideoId,teamLogo,statusCode,dateStart,"
            "dateEnd,season,seasonYear,venueCity,venueState,venueCountry,siteSubdomain,slug,nickname,"
            "birthDate,birthYear,note,honors,specialty,city,state,country,isProfilePhoto,hide,usatf,"
            "tfrrsId,lastTouch,profilePhotoUrl"
        )
        
        response = requests.get(athlete_url)
        if response.status_code == 200:
            try:
                response_json = response.json()
                athlete = response_json.get('_embedded', {}).get('athlete', {})
                grad_year = athlete.get("gradYear")
                weighted_score = athlete.get("weightedScore", 0)
                
                if (
                    grad_year and 
                    str(grad_year).isdigit() and 
                    int(grad_year) >= 2026 and
                    isinstance(weighted_score, (int, float)) and 
                    weighted_score >= 0
                ):
                    output_content = {
                        "data": response_json.get('data', []),
                        "athlete": athlete
                    }
                    output_file = os.path.join(athlete_dir, f"{athlete_id}.json")
                    with open(output_file, 'w') as f:
                        json.dump(output_content, f, indent=4)
                else:
                    print(f"Skipped athlete ID {athlete_id} due to gradYear: {grad_year}")
                
            except json.JSONDecodeError:
                print(f"Failed to parse JSON for athlete ID {athlete_id}")
        time.sleep(1)

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
