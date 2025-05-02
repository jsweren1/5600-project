import time
from datetime import datetime, timedelta
from pipeline import main_pipeline

def run_schedule_for_duration(interval_seconds=30, duration_minutes=30):
    end_time = datetime.now() + timedelta(minutes=duration_minutes)
    run_count = 0

    while datetime.now() < end_time:
        loop_start = datetime.now()
        print(f"\n[{loop_start}] Starting pipeline iteration {run_count + 1}")
        
        # Run the pipeline
        main_pipeline()
        run_count += 1

        # Calculate elapsed and sleep time
        elapsed = (datetime.now() - loop_start).total_seconds()
        sleep_time = max(0, interval_seconds - elapsed)
        if sleep_time > 0:
            print(f"[{datetime.now()}] Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

    print(f"\n[{datetime.now()}] Completed {run_count} iterations in {duration_minutes} minutes.")

if __name__ == "__main__":
    run_schedule_for_duration()
