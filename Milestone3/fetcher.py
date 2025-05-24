import urllib.request
import urllib.parse
import pandas as pd

import logging


class StopEventFetcher:
    def __init__(self):
        self.base_url = "https://busdata.cs.pdx.edu/api/getStopEvents"

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler('data_pipeline.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        self.logger.info("initialized logger successfully")

    def fetch(self, output_file, vehicle_id):
        url = f"{self.base_url}?vehicle_num={urllib.parse.quote(vehicle_id)}"
        try:
            response = urllib.request.urlopen(url)
            html_data = response.read()

            tables = pd.read_html(html_data)
            if tables:
                df = tables[0]
                self.logger.info(f"fetched data for vehicle {vehicle_id}")
                df.to_json(output_file, orient="records", indent=2)
            else:
                self.logger.warning(f"no data found for vehicle {vehicle_id}")
        except Exception as e:
            self.logger.error(f"error fetching data for vehicle {vehicle_id}: {e}")
