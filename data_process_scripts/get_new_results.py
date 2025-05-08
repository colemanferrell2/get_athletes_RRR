import os
import requests
from bs4 import BeautifulSoup
import re
import json  # Import to handle JSON data
from datetime import datetime, timedelta
import glob
import time  # Import time for adding delays

# Remove all files except this script from the data_process_scripts directory
script_dir = os.path.dirname(os.path.abspath(__file__))
current_script = os.path.basename(__file__)

for file_name in os.listdir(script_dir):
    file_path = os.path.join(script_dir, file_name)
    if file_name != current_script and os.path.isfile(file_path):
        try:
            os.remove(file_path)
            print(f"Removed file: {file_path}")
        except Exception as e:
            print(f"Failed to remove file: {file_path}. Error: {e}")

# List of two-letter state abbreviations
states = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me",
    "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa",
    "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"
]

# File to save all meet numbers
output_file = os.path.join(script_dir, 'meet-numbers')

# Collect all meet numbers in a set to ensure uniqueness
meet_numbers = set()

# Current date
current_date = datetime.now()

for state in states:
    # Construct the URL for the state
    url = f"https://{state}.milesplit.com/results"
    print(f"Fetching data from {url}...")

    # Send a GET request to the URL
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch the page for {state.upper()}. Status code: {response.status_code}")
        continue

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table with the class "meets order-table table results"
    table = soup.find('table', class_='meets order-table table results')

    # Extract all meet numbers from the links in the table
    if table:
        for row in table.find_all('tr'):
            # Extract the date from the 'td' element with class 'date'
            date_cell = row.find('td', class_='date')
            if not date_cell:
                continue

            # Parse the date
            try:
                date_text = date_cell.text.strip()
                # Clean the date text by replacing newlines and extra spaces
                date_text = re.sub(r'\s+', ' ', date_text).strip()
                # Handle ranges like "3/28-3/29" or "3/28 3/29"
                if '-' in date_text or ' ' in date_text:
                    # Split the range and take the second (last) date
                    date_text = re.split(r'[-\s]', date_text)[-1]
                meet_date = datetime.strptime(date_text, "%m/%d")
                # Adjust the year to the current year
                meet_date = meet_date.replace(year=current_date.year)
            except ValueError:
                print(f"Failed to parse date: {date_cell.text.strip()}")
                continue

            # Check if the date is within one week of the current date
            if not (current_date - timedelta(days=9) <= meet_date <= current_date + timedelta(days=1)):
                continue

            # Extract the meet link
            link_cell = row.find('td', class_='name')
            if not link_cell:
                continue

            a_tag = link_cell.find('a', href=True)
            if not a_tag:
                continue

            # Extract the meet number from the link
            match = re.search(r'meets/(\d+)-', a_tag['href'])
            if match:
                meet_numbers.add(match.group(1))  # Add to the set to avoid duplicates

    # Add a one-minute delay after processing each state
    print(f"Finished processing state: {state.upper()}. Waiting for 1 minute before the next state...")
    time.sleep(1)

# Write the unique meet numbers to the file
with open(output_file, 'w') as file:
    for number in sorted(meet_numbers):  # Sort the numbers before writing
        file.write(number + '\n')

print(f"All unique meet numbers saved to '{output_file}'.")

# Directory to save the meet data files
meet_data_dir = os.path.join(script_dir, 'meet-data')
os.makedirs(meet_data_dir, exist_ok=True)  # Create the folder if it doesn't exist

# Base API URL
api_url_template = (
    "https://www.milesplit.com/api/v1/meets/{}/performances?"
    "ismeetpro=0&fields=id,meetId,meetName,teamId,videoId,teamName,athleteId,firstName,lastName,"
    "gender,genderName,divisionId,divisionName,meetResultsDivisionId,resultsDivisionId,ageGroupName,"
    "gradYear,eventName,eventCode,eventDistance,eventGenreOrder,round,roundName,heat,units,mark,"
    "place,windReading,profileUrl,teamProfileUrl,performanceVideoId,teamLogo,statusCode,dateStart,"
    "dateEnd,season,seasonYear,venueCity,venueState,venueCountry"
)

# Read meet numbers from the file
with open(output_file, 'r') as file:
    meet_numbers = [line.strip() for line in file.readlines()]

# Fetch data for each meet number and save it to a file
for meet_number in meet_numbers:
    print(f"Fetching data for meet number {meet_number}...")
    api_url = api_url_template.format(meet_number)

    # Send a GET request to the API
    response = requests.get(api_url)
    if response.status_code != 200:
        print(f"Failed to fetch data for meet number {meet_number}. Status code: {response.status_code}")
        continue

    # Save the response JSON to a file
    output_file = os.path.join(meet_data_dir, f"{meet_number}.json")
    with open(output_file, 'w') as file:
        file.write(response.text)

    print(f"Data for meet number {meet_number} saved to '{output_file}'.")

print("All meet data has been fetched and saved.")

# File to save all unique athlete IDs
athlete_numbers_file = os.path.join(script_dir, 'athlete-numbers')
unique_athlete_ids = set()  # Set to store unique athlete IDs

# Extract athlete IDs from the saved meet data files
for meet_file in os.listdir(meet_data_dir):
    meet_file_path = os.path.join(meet_data_dir, meet_file)
    try:
        with open(meet_file_path, 'r') as file:
            data = json.load(file)
            for performance in data.get('data', []):
                athlete_id = performance.get('athleteId')
                if athlete_id:
                    unique_athlete_ids.add(athlete_id)  # Add athlete ID to the set
    except (json.JSONDecodeError, KeyError):
        print(f"Failed to process file: {meet_file_path}")
        continue

# Write all unique athlete IDs to the athlete-numbers file
with open(athlete_numbers_file, 'w') as file:
    for athlete_id in sorted(unique_athlete_ids):  # Sort for consistency
        file.write(str(athlete_id) + '\n')

print(f"All unique athlete IDs saved to '{athlete_numbers_file}'.")

# Directory to save the athlete metadata files
athlete_metadata_dir = os.path.join(script_dir, 'athlete-metadata')
os.makedirs(athlete_metadata_dir, exist_ok=True)  # Create the folder if it doesn't exist

# Read athlete numbers from the athlete-numbers file
with open(athlete_numbers_file, 'r') as file:
    athlete_numbers = [line.strip() for line in file.readlines()]

# Fetch metadata for each athlete and save it to a file
for athlete_id in athlete_numbers:
    print(f"Fetching metadata for athlete ID {athlete_id}...")
    athlete_url = (
        f"https://www.milesplit.com/api/v1/athletes/{athlete_id}/stats?ismeetpro=0&fields=id,meetId,meetName,teamId,videoId,teamName,athleteId,firstName,lastName,gender,genderName,divisionId,divisionName,meetResultsDivisionId,resultsDivisionId,ageGroupName,gradYear,eventName,eventCode,eventDistance,eventGenreOrder,round,roundName,heat,units,mark,place,windReading,profileUrl,teamProfileUrl,performanceVideoId,teamLogo,statusCode,dateStart,dateEnd,season,seasonYear,venueCity,venueState,venueCountry,siteSubdomain,slug,nickname,birthDate,birthYear,note,%20honors,specialty,city,state,country,isProfilePhoto,hide,usatf,tfrrsId,lastTouch,profilePhotoUrl"
    )

    # Send a GET request to the API
    response = requests.get(athlete_url)
    if response.status_code != 200:
        print(f"Failed to fetch metadata for athlete ID {athlete_id}. Status code: {response.status_code}")
        continue

    # Parse the response JSON and extract the 'data' and 'athlete' variables
    try:
        response_json = response.json()
        data = response_json.get('data', [])
        athlete = response_json.get('_embedded', {}).get('athlete', {})
    except json.JSONDecodeError:
        print(f"Failed to parse JSON for athlete ID {athlete_id}.")
        continue

    grad_year = athlete.get("gradYear")
    weighted_score = athlete.get("weightedScore", 0)  # Default to 0 if not present
    
    if (
        grad_year and str(grad_year).isdigit() and int(grad_year) >= 2026
        and isinstance(weighted_score, (int, float)) and weighted_score >= 60
    ):
        output_content = {
            "data": data,
            "athlete": athlete
        }
        output_file = os.path.join(athlete_metadata_dir, f"{athlete_id}.json")
        with open(output_file, 'w') as file:
            json.dump(output_content, file, indent=4)

        
        time.sleep(1)  # ✅ Respect API rate limits
        print(f"Metadata for athlete ID {athlete_id} saved to '{output_file}'.")
    else:
        time.sleep(1)  # ✅ Respect API rate limits
        print(f"Skipped athlete ID {athlete_id} due to gradYear: {grad_year}")

print("All athlete metadata has been fetched and saved.")

# Path to the athlete-metadata directory
athlete_metadata_dir = os.path.join(script_dir, 'athlete-metadata')

# Set to store unique team IDs
team_ids = set()

# Iterate through all JSON files in the athlete-metadata directory
for file_path in glob.glob(os.path.join(athlete_metadata_dir, '*.json')):
    with open(file_path, 'r') as file:
        try:
            # Load the JSON content
            content = json.load(file)
            # Extract the teamId from the athlete category
            athlete = content.get('athlete', {})
            team_id = athlete.get('teamId')
            if team_id:
                team_ids.add(team_id)
        except json.JSONDecodeError:
            print(f"Failed to parse JSON in file: {file_path}")

# Save all unique team IDs to the team-numbers file
team_numbers_file = os.path.join(script_dir, 'team-numbers')
with open(team_numbers_file, 'w') as file:
    for team_id in sorted(team_ids):  # Sort the IDs for consistency
        file.write(f"{team_id}\n")

print(f"All unique team IDs have been saved to '{team_numbers_file}'.")

# Directory to save the team data files
team_data_dir = os.path.join(script_dir, 'team-data')
os.makedirs(team_data_dir, exist_ok=True)  # Create the folder if it doesn't exist

# Read team numbers from the team-numbers file
with open(team_numbers_file, 'r') as file:
    team_numbers = [line.strip() for line in file.readlines()]

# Fetch data for each team number and save it to a file
for team_id in team_numbers:
    print(f"Fetching data for team ID {team_id}...")
    team_url = f"https://www.milesplit.com/api/v1/teams/{team_id}"

    # Send a GET request to the API
    response = requests.get(team_url)
    if response.status_code != 200:
        print(f"Failed to fetch data for team ID {team_id}. Status code: {response.status_code}")
        continue

    # Parse the response JSON and extract the 'data' variable
    try:
        data = response.json().get('data', {})
    except json.JSONDecodeError:
        print(f"Failed to parse JSON for team ID {team_id}.")
        continue

    # Save the 'data' variable to a file
    output_file = os.path.join(team_data_dir, f"{team_id}.json")
    with open(output_file, 'w') as file:
        json.dump(data, file, indent=4)

    print(f"Data for team ID {team_id} saved to '{output_file}'.")

print("All team data has been fetched and saved.")

# Update athlete files with team data
for athlete_file_path in glob.glob(os.path.join(athlete_metadata_dir, '*.json')):
    with open(athlete_file_path, 'r') as athlete_file:
        try:
            # Load the athlete JSON content
            athlete_content = json.load(athlete_file)
            athlete = athlete_content.get('athlete', {})
            team_id = athlete.get('teamId')

            # Skip if no teamId is found
            if not team_id:
                print(f"No teamId found in file: {athlete_file_path}")
                continue

            # Find the corresponding team file
            team_file_path = os.path.join(team_data_dir, f"{team_id}.json")
            if not os.path.exists(team_file_path):
                print(f"No team file found for teamId {team_id} in file: {athlete_file_path}")
                continue

            # Load the team JSON content
            with open(team_file_path, 'r') as team_file:
                team_content = json.load(team_file)

            # Append the team data to the athlete file under the variable 'team-data'
            athlete_content['team-data'] = team_content

            # Save the updated athlete file
            with open(athlete_file_path, 'w') as athlete_file:
                json.dump(athlete_content, athlete_file, indent=4)

            print(f"Updated athlete file with team-data for teamId {team_id}: {athlete_file_path}")

        except json.JSONDecodeError:
            print(f"Failed to parse JSON in file: {athlete_file_path}")
        except Exception as e:
            print(f"An error occurred while processing file {athlete_file_path}: {e}")

print("All athlete files have been updated with team-data.")
