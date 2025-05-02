import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from pandas import Timestamp
from prefect import flow, task
import os

# Helper function to check if it's peak hour
def is_peak_hour(dt: datetime) -> bool:
    return dt.weekday() < 5 and dt.hour >= 5 and dt.hour < 21 or (dt.hour == 21 and dt.minute <= 30)

# Fetch data from API and convert it to a DataFrame
def get_batch() -> pd.DataFrame:
    api_key = "4b20a99005d64434999fae74f7ba3f7c"
    url = "http://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
    headers = {"api_key": api_key}
    response = requests.get(url, headers=headers)
    data = response.json()
    
    call_time = datetime.now()
    peak = is_peak_hour(call_time)
    
    # Prepare a list of dictionaries for each train entry
    trains = []
    for train in data.get("Trains", []):
        if train.get("Min") == "---":
            continue
        
        # Create the full dictionary for each entry
        entry = {
            "Car": train.get("Car"),
            "Destination": train.get("Destination"),
            "DestinationCode": train.get("DestinationCode"),
            "DestinationName": train.get("DestinationName"),
            "Group": train.get("Group"),
            "Line": train.get("Line"),
            "LocationCode": train.get("LocationCode"),
            "LocationName": train.get("LocationName"),
            "Min": train.get("Min"),
            "CallTime": call_time,
            "Peak": peak,
            "LeftYet": False,
            "IsNext": False,
            "BRD_Time": None,
            "TimeToBRD": None,
            "Diff": None
        }
        
        # Append to the trains list
        trains.append(entry)
    
    # Convert to a DataFrame and return
    return pd.DataFrame(trains)

@task
def fetch_data():
    api_key = "4b20a99005d64434999fae74f7ba3f7c"
    url = "http://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
    headers = {"api_key": api_key}
    response = requests.get(url, headers=headers)
    data = response.json()
    
    call_time = datetime.now()
    peak = is_peak_hour(call_time)
    
    # Prepare a list of dictionaries for each train entry
    trains = []
    for train in data.get("Trains", []):
        if train.get("Min") == "---":
            continue
        
        # Create the full dictionary for each entry
        entry = {
            "Car": train.get("Car"),
            "Destination": train.get("Destination"),
            "DestinationCode": train.get("DestinationCode"),
            "DestinationName": train.get("DestinationName"),
            "Group": train.get("Group"),
            "Line": train.get("Line"),
            "LocationCode": train.get("LocationCode"),
            "LocationName": train.get("LocationName"),
            "Min": train.get("Min"),
            "CallTime": call_time,
            "Peak": peak,
            "LeftYet": False,
            "IsNext": False,
            "BRD_Time": None,
            "TimeToBRD": None,
            "Diff": None
        }
        
        # Append to the trains list
        trains.append(entry)
    
    # Convert to a DataFrame and return
    return pd.DataFrame(trains)

@task
def load_past_data():
    if os.path.exists("all_trains3.csv"):
        all_trains = pd.read_csv("all_trains3.csv", parse_dates=["CallTime", "BRD_Time"])
    else:
        all_trains = pd.DataFrame()

    if os.path.exists("last_batch3.csv"):
        last_batch = pd.read_csv("last_batch3.csv", parse_dates=["CallTime"])
    else:
        last_batch = pd.DataFrame()

    return all_trains, last_batch

@task
def save_data(all_trains: pd.DataFrame, current_batch: pd.DataFrame):
    all_trains.to_csv("all_trains3.csv", index=False)
    current_batch.to_csv("last_batch3.csv", index=False)

# Transform batch to update IsNext, LeftYet, etc.
def transform_batch(current_batch: pd.DataFrame, all_trains: pd.DataFrame, last_batch: pd.DataFrame) -> pd.DataFrame:
    from collections import defaultdict
    from datetime import datetime

    # Ensure 'IsNext', 'LeftYet', 'BRD_Time', 'TimeToBRD', and 'Diff' columns exist
    for col in ['IsNext', 'LeftYet', 'BRD_Time', 'TimeToBRD', 'Diff']:
        for df in [current_batch, all_trains]:
            if col not in df.columns:
                df[col] = None

    current_batch['IsNext'] = False
    current_batch['LeftYet'] = False

    # Step 1: Compute IsNext for current batch
    group_keys = ['Line', 'LocationName', 'Destination']
    
    # Safe conversion function for Min values
    def safe_to_int(value):
        try:
            return int(value)
        except ValueError:
            return None

    for key, group in current_batch.groupby(group_keys):
        not_left = group[~group['LeftYet']]

        # Filter out non-numeric Min values (only consider numeric)
        digit_mins = not_left[not_left['Min'].apply(lambda x: safe_to_int(x) is not None)]

        if not digit_mins.empty:
            min_val = digit_mins['Min'].apply(safe_to_int).min()
            idx = digit_mins[digit_mins['Min'].apply(safe_to_int) == min_val].index
            current_batch.loc[idx, 'IsNext'] = True

    # Step 2: Detect new arrivals
    arrival_keys = set(map(tuple, current_batch[current_batch['Min'].isin(['ARR', 'BRD'])][group_keys].values))
    previous_arrival_keys = set(map(tuple, last_batch[last_batch['Min'].isin(['ARR', 'BRD'])][group_keys].values))
    new_arrivals = arrival_keys - previous_arrival_keys

    # Use CallTime from batch
    arrival_time = current_batch['CallTime'].iloc[0] if not current_batch.empty else datetime.now()

    # Step 3: For each new arrival, update IsNext and LeftYet retroactively
    for key in new_arrivals:
        mask_past = (
            (all_trains['Line'] == key[0]) &
            (all_trains['LocationName'] == key[1]) &
            (all_trains['Destination'] == key[2]) &
            (~all_trains['LeftYet']) &
            (all_trains['Min'].apply(lambda x: safe_to_int(x) is not None))
        )

        for ct in all_trains[mask_past]['CallTime'].unique():
            ct_mask = mask_past & (all_trains['CallTime'] == ct)
            if not all_trains[ct_mask].empty:
                digit_ct_mask = ct_mask & all_trains['Min'].apply(lambda x: safe_to_int(x) is not None)

                if not all_trains[digit_ct_mask].empty:
                    min_val = all_trains.loc[digit_ct_mask, 'Min'].apply(safe_to_int).min()
                    is_next_mask = digit_ct_mask & (all_trains['Min'].apply(safe_to_int) == min_val)
                    all_trains.loc[is_next_mask, 'IsNext'] = True

        retro_mask = (
            (all_trains['Line'] == key[0]) &
            (all_trains['LocationName'] == key[1]) &
            (all_trains['Destination'] == key[2]) &
            (all_trains['IsNext']) &
            (~all_trains['LeftYet'])
        )

        # Update LeftYet and other columns
        all_trains.loc[retro_mask, 'LeftYet'] = True
        all_trains.loc[retro_mask, 'BRD_Time'] = arrival_time

        # Only update TimeToBRD and Diff if 'Min' is numeric
        valid_min_mask = retro_mask & all_trains['Min'].apply(lambda x: safe_to_int(x) is not None)
        all_trains.loc[valid_min_mask, 'TimeToBRD'] = (
            (arrival_time - all_trains.loc[valid_min_mask, 'CallTime']).dt.total_seconds() / 60
        )
        all_trains.loc[valid_min_mask, 'Diff'] = (
            all_trains.loc[valid_min_mask, 'TimeToBRD'] -
            all_trains.loc[valid_min_mask, 'Min'].apply(safe_to_int)
        )

    # Step 4: Final IsNext recompute
    all_trains['IsNext'] = False
    combined = pd.concat([all_trains, current_batch], ignore_index=True)

    # Filter valid Min rows before computing IsNext
    valid_min_combined = combined[combined['Min'].apply(lambda x: safe_to_int(x) is not None)]
    call_groups = valid_min_combined.groupby(['CallTime', 'Line', 'LocationName', 'Destination'])

    for _, group in call_groups:
        min_val = group['Min'].apply(safe_to_int).min()
        idx_to_update = group[group['Min'].apply(safe_to_int) == min_val].index
        combined.loc[idx_to_update, 'IsNext'] = True

    return combined

@flow
def main_pipeline():
    current_batch = fetch_data()
    all_trains, last_batch = load_past_data()
    if not last_batch.empty:
        updated_all_trains = transform_batch(current_batch, all_trains, last_batch)
    else:
        updated_all_trains = current_batch
    save_data(updated_all_trains, current_batch)

'''
# Main ETL process
def main():
    all_trains = pd.DataFrame()  # Empty dataframe to store all data
    last_batch = pd.DataFrame()  # Empty dataframe to store the last batch of data
    
    # Run for a certain number of iterations (or until you decide to stop)
    for _ in range(60):
        current_batch = get_batch()
        
        # If there is a last batch, transform the data
        if not last_batch.empty:
            transform_batch(current_batch, all_trains, last_batch)
        
        # Append current batch to all_trains
        all_trains = pd.concat([all_trains, current_batch])
        
        # Save all_trains to CSV after each batch
        all_trains.to_csv("train_data.csv", index=False)
        
        # Update last_batch for the next iteration
        last_batch = current_batch
        
        # Sleep for 30 seconds before getting the next batch
        time.sleep(30)

if __name__ == "__main__":
    main()
'''
