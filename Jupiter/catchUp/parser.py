import logging, json

import sys # remove later

class StopEventParser:
    def __init__(self, logger):
        self._logger = logger
        self._logger.info("initialized logger successfully")

    def _load_one_json(self, filename):
        try:
            with open(filename, 'r') as file:
                data = json.load(file)
                
            messages = []
            for trip in data.get('trips', []):
                trip_id = trip.get('trip_id')
                for table_entry in trip.get('table_data', []):
                    message = {
                        'trip_id': trip_id,
                        'vehicle_number': table_entry.get('vehicle_number'),
                        'route_number': table_entry.get('route_number'),
                        'direction': table_entry.get('direction'),
                        'service_key': table_entry.get('service_key')
                    }
                    messages.append(message)
                    
            self._logger.info(f"extracted {len(messages)} records from {filename}")
            return messages
            
        except Exception as e:
            self._logger.error(f"error processing file {filename}: {str(e)}")
            raise

    def load_json_bulk(self, filenames):
        all_messages = []
        for filename in filenames:
            file_messages = self._load_one_json(filename)
            all_messages.extend(file_messages)
            
        self._logger.info(f"{len(all_messages)} records extracted")
        return all_messages

"""
For testing purposes
"""
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logger.handlers.clear()

formatter = logging.Formatter(
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.DEBUG)
console.setFormatter(formatter)
logger.addHandler(console)

parser = StopEventParser(logger)
bulk = parser.load_json_bulk(["events/stop_events_4045.json"])
logger.info(bulk[:5])