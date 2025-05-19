import urllib.request
import urllib.parse
import pandas as pd


class StopEventFetcher:
    def __init__(self):
        self.base_url = "https://busdata.cs.pdx.edu/api/getStopEvents"

    def fetch(self, output_file, vehicle_id):
        url = f"{self.base_url}?vehicle_num={urllib.parse.quote(vehicle_id)}"
        try:
            response = urllib.request.urlopen(url)
            html_data = response.read()

            tables = pd.read_html(html_data)
            if tables:
                df = tables[0]
                print(f"Fetched data for vehicle {vehicle_id}")
                df.to_json(output_file, orient="records", indent=2)
            else:
                print(f"No data found for vehicle {vehicle_id}")
        except Exception as e:
            print(f"Error fetching data for vehicle {vehicle_id}: {e}")


# Read IDs from the file and fetch stop events
# change path for id.txt file
with open("Jupiter/id.txt", "r") as file:
    fetcher = StopEventFetcher()
    for line in file:
        vehicle_id = line.strip()
        if vehicle_id:  # Skip empty lines
            output_filename = f"stop_events_{vehicle_id}.json"
            fetcher.fetch(output_filename, vehicle_id)
