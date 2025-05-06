import os
import json
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

# === File paths ===
current_athletes_folder = os.path.join(os.path.dirname(__file__), "athlete-metadata")

# === Helpers ===

def convert_time_to_seconds(time_str):
    """Convert 'MM:SS.xx' or 'SS.xx' string to seconds."""
    if isinstance(time_str, (float, int)):
        return time_str
    if isinstance(time_str, str):
        try:
            if ':' in time_str:
                minutes, seconds = map(float, time_str.split(':'))
                return minutes * 60 + seconds
            return float(time_str)
        except:
            return None
    return None

def convert_mark_to_numeric(mark):
    """
    Convert marks like '5-7.75' to inches or float equivalents for field events.
    Assumes format 'feet-inches' for jumps/throws (e.g., 5-7.75 → 67.75 inches).
    """
    if isinstance(mark, (float, int)):
        return float(mark)
    try:
        if '-' in mark:
            feet, inches = mark.split('-')
            return float(feet) * 12 + float(inches)
        return float(mark)
    except:
        return None

def load_and_prepare_scoring_tables():
    base_dir = os.path.join(os.path.dirname(__file__), "World Athletics")
    scoring_tables = {}

    file_info = {
        "M": "Men_WA_Table.csv",
        "F": "Women_WA_Table.csv"
    }

    for gender, filename in file_info.items():
        path = os.path.join(base_dir, filename)
        df = pd.read_csv(path)

        # Drop 'Gender' and 'EventType' columns if present
        df = df.drop(columns=[col for col in ['Gender', 'EventType'] if col in df.columns])
        scoring_tables[gender] = df

    return scoring_tables

def find_closest_score(scoring_tables, gender, event_code, mark):
    """Find the closest score for a given gender, event, and mark."""
    if gender not in scoring_tables:
        return None

    table = scoring_tables[gender]
    if event_code not in table.columns:
        return None

    mark_val = convert_time_to_seconds(mark)
    if mark_val is None:
        mark_val = convert_mark_to_numeric(mark)
    if mark_val is None:
        return None

    event_series = pd.to_numeric(table[event_code], errors='coerce')
    diffs = abs(event_series - mark_val)
    if diffs.isna().all():
        return None

    min_index = diffs.idxmin()
    score = table.loc[min_index, 'Points']

    # Adjust the score: subtract 100 and divide by 10
    adjusted_score = (score - 100) / 10
    return adjusted_score

def calculate_weighted_average(scores):
    """Weighted average of top 2 scores: 75% + 25%."""
    if len(scores) < 2:
        return scores[0] if scores else None
    scores = sorted(scores, reverse=True)
    return 0.75 * scores[0] + 0.25 * scores[1]

def process_athlete_file(file_path, scoring_tables):
    """Process a single JSON athlete file."""
    try:
        with open(file_path, 'r') as f:
            athlete_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Skipping invalid JSON file: {file_path} — {e}")
        return

    gender = athlete_data.get('athlete', {}).get('gender', 'M')
    scores = []

    event_table = scoring_tables.get(gender, pd.DataFrame())
    available_events = [col.lower() for col in event_table.columns if col != 'Points']

    for entry in athlete_data.get('data', []):
        event_code = entry['eventCode']
        mark = entry['mark']

        if event_code.lower() not in available_events:
            # Add "NA" for events not in the table
            entry['score'] = "NA"
            continue

        # Calculate the score for the event
        score = find_closest_score(scoring_tables, gender, event_code, mark)
        # Add the score if valid, otherwise add "NA"
        entry['score'] = score if score is not None else "NA"

        if score is not None:
            scores.append(score)

    # Calculate the weighted score for the athlete
    weighted = calculate_weighted_average(scores)
    athlete_data['athlete']['weightedScore'] = float(weighted) if weighted is not None else None

    # Write the updated data back to the JSON file
    with open(file_path, 'w') as f:
        json.dump(athlete_data, f, indent=4)

    print(f"Processed athlete file: {file_path}")

def process_all_athletes(scoring_tables):
    """Process all athlete files sequentially."""
    athlete_files = [
        os.path.join(current_athletes_folder, file_name)
        for file_name in os.listdir(current_athletes_folder)
        if file_name.endswith('.json')
    ]

    for file_path in athlete_files:
        process_athlete_file(file_path, scoring_tables)

def main():
    scoring_tables = load_and_prepare_scoring_tables()
    process_all_athletes(scoring_tables)

if __name__ == "__main__":
    main()
