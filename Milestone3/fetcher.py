import urllib.request
import urllib.parse
import pandas as pd

class StopEventFetcher:
    def __init__(self, vehicle_id):
        self.vehicle_id = vehicle_id
        self.base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num=<vehicle_num>"

    def fetch(self, output_file):
        url = f"{self.base_url}?vehicle_num={urllib.parse.quote(self.vehicle_id)}"
        try:
            response = urllib.request.urlopen(url)
            html_data = response.read()

            tables = pd.read_html(html_data)
            if tables:
                df = tables[0]
                print(df)
                df.to_csv(output_file, index=False)
        except Exception as e:
            print("In except block")
            print(f"Error: {e}")

# Example usage:
fetcher = StopEventFetcher("2907")
fetcher.fetch("stop_events_2907.csv")
