import pandas as pd
import pyarrow.parquet as pq
import os
import argparse

def extract_trips(input_file, output_dir):
    # Read Parquet file into a Pandas DataFrame
    df = pd.read_parquet(input_file)

    # Sort the DataFrame by 'unit' and 'timestamp'
    df.sort_values(['unit', 'timestamp'], inplace=True)

    # Initialize variables
    current_trip = 0
    last_timestamp = pd.to_datetime('1900-01-01')  # Initial timestamp

    # Iterate through rows and create trip-specific CSV files
    for index, row in df.iterrows():
        current_timestamp = pd.to_datetime(row['timestamp'])

        # Check if a new trip has started
        if (current_timestamp - last_timestamp).total_seconds() > 7 * 3600:
            current_trip += 1
            trip_filename = f"{row['unit']}_{current_trip}.csv"
            trip_filepath = os.path.join(output_dir, trip_filename)

            # Write header to the CSV file if it's a new trip
            if not os.path.exists(trip_filepath):
                with open(trip_filepath, 'w') as f:
                    f.write("latitude,longitude,timestamp\n")

        # Append the row to the current trip CSV file
        with open(trip_filepath, 'a') as f:
            f.write(f"{row['latitude']},{row['longitude']},{row['timestamp']}\n")

        # Update the last timestamp
        last_timestamp = current_timestamp

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process GPS data and extract trips.")
    parser.add_argument("--to_process", required=True, help="Path to the Parquet file to be processed.")
    parser.add_argument("--output_dir", required=True, help="Folder to store resulting CSV files.")
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Call the function to extract trips
    extract_trips(args.to_process, args.output_dir)
