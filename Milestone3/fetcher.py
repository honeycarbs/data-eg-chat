import requests
import urllib.parse
from bs4 import BeautifulSoup
import re
from json import dump

import logging


class StopEventFetcher:
    def __init__(self):
        self.base_url = "https://busdata.cs.pdx.edu/api/getStopEvents"

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler("data_pipeline.log")
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        self.logger.info("initialized logger successfully")

    def fetch(self, output_file, vehicle_id):
        url = f"{self.base_url}?vehicle_num={urllib.parse.quote(vehicle_id)}"
        try:
            response = requests.get(url)            
        except Exception as e:
            self.logger.error(f"error occured while fetching vehicle {vehicle_id}: {e}")

        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        output = {"trips": []}
        filtered_cols = ["vehicle_number", "route_number", "direction", "service_key"]
        
        h2_elements = soup.find_all("h2")
        
        for h2 in h2_elements:
            h2_text = h2.get_text().strip()
            trip_id_match = re.search(r"PDX_TRIP\s*(\d+)", h2_text)
            trip_id = trip_id_match.group(1) if trip_id_match else None
            
            table = h2.find_next_sibling("table")
            table_data = []
            
            headers = []
            header_row = table.find("thead").find("tr") if table.find("thead") else table.find("tr")
            for th in header_row.find_all("th"):
                headers.append(th.get_text().strip())
            
            column_indices = {}
            for col in filtered_cols:
                column_indices[col] = headers.index(col)
            
            rows = table.find_all("tr")[1:] if table.find("thead") else table.find_all("tr")[1:]
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= len(headers):
                    row_data = {}
                    for col, idx in column_indices.items():
                        if idx < len(cells):
                            row_data[col] = cells[idx].get_text().strip()
                    table_data.append(row_data)
            
            record = {
                "trip_id": trip_id,
                "table_data": table_data
            }
            output["trips"].append(record)
        
        with open(output_file, "w", encoding="utf-8") as f:
            dump(output, f, indent=2)

fetcher = StopEventFetcher()
vehicle_id = "4219"
output_filename = f"stop_events_{vehicle_id}.json"
fetcher.fetch(output_filename, vehicle_id)

# Read IDs from the file and fetch stop events
# change path for id.txt file
# with open("Jupiter/id.txt", "r") as file:
#     fetcher = StopEventFetcher()
#     for line in file:
#         vehicle_id = line.strip()
#         if vehicle_id:  # Skip empty lines
#             output_filename = f"stop_events_{vehicle_id}.json"
#             fetcher.fetch(output_filename, vehicle_id)
