import urllib.request
import urllib.parse
import pandas as pd


class StopEventFetcher:
    def __init__(self, vehicle_id):
        self.vehicle_id = vehicle_id
        self.base_url = "https://busdata.cs.pdx.edu/api/getStopEvents"

    def fetch(self, output_file):
        url = f"{self.base_url}?vehicle_num={urllib.parse.quote(self.vehicle_id)}"
        try:
            response = urllib.request.urlopen(url)
            html_data = response.read()

            tables = pd.read_html(html_data)
            if tables:
                df = tables[0]
                print(f"Fetched data for vehicle {self.vehicle_id}")
                df.to_json(output_file, orient="records", indent=2)
            else:
                print(f"No data found for vehicle {self.vehicle_id}")
        except Exception as e:
            print(f"Error fetching data for vehicle {self.vehicle_id}: {e}")


# Read IDs from the file and fetch stop events
# change path for id.txt file
with open("/Users/helenkhoshnaw/Desktop/DataPipeline/data-eg-chat/Jupiter/id.txt", "r") as file:
    for line in file:
        vehicle_id = line.strip()
        if vehicle_id:  # Skip empty lines
            fetcher = StopEventFetcher(vehicle_id)
            output_filename = f"stop_events_{vehicle_id}.json"
            fetcher.fetch(output_filename)
