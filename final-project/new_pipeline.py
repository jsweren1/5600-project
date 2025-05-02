from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import requests
from collections import defaultdict
from typing import Optional, List
import pandas as pd
import time
from prefect import flow, task
import os
import json

class TrainEntry(BaseModel):
    Car: Optional[str]
    Destination: str
    DestinationCode: Optional[str]
    DestinationName: str
    Group: str
    Line: str
    LocationCode: Optional[str]
    LocationName: str
    Min: str
    CallTime: datetime
    Peak: bool
    LeftYet: bool = False
    IsNext: bool = False
    BRD_Time: Optional[datetime] = None
    TimeToBRD: Optional[float] = None
    Diff: Optional[float] = None

def is_peak_hour(dt: datetime) -> bool:
    return dt.weekday() < 5 and dt.hour >= 5 and dt.hour < 21 or (dt.hour == 21 and dt.minute <= 30)


def get_batch() -> list[TrainEntry]:
    api_key = "4b20a99005d64434999fae74f7ba3f7c"
    url = "http://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
    output_file = "wmata_data.json"
    headers = {"api_key": api_key}
    response = requests.get(url, headers=headers)
    data = response.json()
    call_time = datetime.now()
    peak = is_peak_hour(call_time)

    trains = []
    for train in data.get("Trains", []):
        if train.get("Min") == "---":
            continue
        entry = TrainEntry(
            **train,
            CallTime=call_time,
            Peak=peak
        )
        trains.append(entry)
    #print(trains)
    return trains

def transform_batch(current_batch: List[TrainEntry], all_past_batches: List[TrainEntry], last_batch: List[TrainEntry]):
    from collections import defaultdict
    from datetime import datetime

    # Group current batch by Line, Location, Destination
    groups = defaultdict(list)
    for t in current_batch:
        key = (t.Line, t.LocationName, t.Destination)
        groups[key].append(t)

    # Step 1: Compute IsNext for current batch
    for key, trains in groups.items():
        not_left = [t for t in trains if not t.LeftYet]
        digit_mins = [int(t.Min) for t in not_left if str(t.Min).isdigit()]
        if digit_mins:
            min_val = min(digit_mins)
            for t in not_left:
                if t.Min.isdigit() and int(t.Min) == min_val:
                    t.IsNext = True

    # Step 2: Detect new arrivals
    arriving_keys = {
        (t.Line, t.LocationName, t.Destination)
        for t in current_batch
        if t.Min in {"ARR", "BRD"}
    }
    previously_arriving_keys = {
        (t.Line, t.LocationName, t.Destination)
        for t in last_batch
        if t.Min in {"ARR", "BRD"}
    }
    new_arrivals = arriving_keys - previously_arriving_keys

    # Step 3: For each new arrival, update IsNext and LeftYet retroactively
    arrival_time = current_batch[0].CallTime  # assuming consistent call time in batch
    for key in new_arrivals:
        # First retroactively update IsNext on all past trains that were next
        for t in all_past_batches:
            if (
                (t.Line, t.LocationName, t.Destination) == key and
                not t.LeftYet and
                str(t.Min).isdigit()
            ):
                # Check if it was the next train in its call group
                min_val = min(
                    int(x.Min) for x in all_past_batches
                    if x.CallTime == t.CallTime and
                    (x.Line, x.LocationName, x.Destination) == key and
                    not x.LeftYet and
                    str(x.Min).isdigit()
                )
                if int(t.Min) == min_val:
                    t.IsNext = True

        # Step 4: Mark IsNext trains as LeftYet and update timings
        for t in all_past_batches:
            if (t.Line, t.LocationName, t.Destination) == key and t.IsNext and not t.LeftYet:
                t.LeftYet = True
                t.BRD_Time = arrival_time
                delta = (arrival_time - t.CallTime).total_seconds() / 60.0
                t.TimeToBRD = delta
                if str(t.Min).isdigit():
                    t.Diff = delta - int(t.Min)

    # Step 5: Recompute IsNext globally after updates
    # Reset all IsNext first
    for t in all_past_batches + current_batch:
        t.IsNext = False

    combined = all_past_batches + current_batch
    call_groups = defaultdict(list)
    for t in combined:
        if not t.LeftYet and str(t.Min).isdigit():
            key = (t.CallTime, t.Line, t.LocationName, t.Destination)
            call_groups[key].append(t)

    for key, trains in call_groups.items():
        min_val = min(int(t.Min) for t in trains)
        for t in trains:
            if int(t.Min) == min_val:
                t.IsNext = True

def save_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump([t.dict() for t in data], f, indent=4)

def load_from_json(filename) -> List[TrainEntry]:
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            data = json.load(f)
            return [TrainEntry.from_dict(d) for d in data]
    else:
        return []

@task
def load_state():
    return load_from_json("all_trains.json")

@task
def save_state(train_list):
    save_to_json(train_list, "all_trains.json")

@task
def load_last_batch():
    return load_from_json("last_batch.json")

@task
def save_last_batch(last_batch):
    save_to_json(last_batch, "last_batch.json")
