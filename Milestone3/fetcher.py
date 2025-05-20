import urllib.parse
from bs4 import BeautifulSoup
import re, json, requests

import logging


class StopEventFetcher:
    def __init__(self, base_url, logger):
        self.base_url = base_url

        self._logger = logger
        self._logger.info("initialized logger successfully")

    def fetch(self, output_file, vehicle_id):
        try:
            html_content = self._scrape_html(vehicle_id)
            data = self._parse_html(html_content)
            if not data["trips"]:
                self._logger.info("no trips found, skipping file creation")
                return

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
                self._logger.info(f"successfully wrote {len(data['trips'])} records to {output_file}")

        except Exception as e:
            self._logger.error(f"Failed to process vehicle {vehicle_id}: {e}")

    def _scrape_html(self, vehicle_id):
        url = f"{self.base_url}?vehicle_num={urllib.parse.quote(vehicle_id)}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        
        except Exception as e:
            raise

    def _parse_html(self, html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        output = {"trips": []}
        filtered_cols = ["vehicle_number", "route_number", "direction", "service_key"]

        for h2 in soup.find_all("h2"):
            h2_text = h2.get_text().strip()
            match = re.search(r"PDX_TRIP\s*(\d+)", h2_text)
            if match:
                trip_id = match.group(1)
            else:
                raise ValueError(f"No trip_id found in H2")

            table = h2.find_next_sibling("table")
            
            if not table:
                raise LookupError(f"Missing trip table")

            header_row = table.find("tr") 
            if table.find("thead"):
                header_row = table.find("thead").find("tr") or header_row
            
            if header_row:
                headers = [th.get_text().strip() for th in header_row.find_all("th")]
            else:
                raise ValueError(f"No headers in table found")

            column_indices = {}
            for col in filtered_cols:
                try:
                    column_indices[col] = headers.index(col)
                except ValueError:
                    raise

            rows = table.find_all("tr")[1:]
            table_data = []

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < len(headers):
                    continue
                
                row_data = {}
                for col, idx in column_indices.items():
                    if idx is not None and idx < len(cells):
                        row_data[col] = cells[idx].get_text().strip()
                
                if row_data:
                    table_data.append(row_data)
            
            output["trips"].append({
                "trip_id": trip_id,
                "table_data": table_data
            })
        
        return output

