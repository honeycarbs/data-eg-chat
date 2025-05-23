from pub import PubSubPublisher
from parser import StopEventParser
import os
import json

import logging

class DataPipeline:
    def __init__(self, LOG_LEVEL):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(LOG_LEVEL)

        fh = logging.FileHandler('data_pipeline.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        self.logger.info("initialized logger successfully")

def is_logically_empty_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return "trips" in data and isinstance(data["trips"], list) and len(data["trips"]) == 0

def list_files_in_directory(folder_path):
  try:
    files = os.listdir(folder_path)
    return [f for f in files if os.path.isfile(os.path.join(folder_path, f))]
  
  except Exception as e:
    return f"An error occurred: {e}"

if __name__ == "__main__":
    dp = DataPipeline(logging.DEBUG)
    folder = "events2"
    files = list_files_in_directory(folder)
    valid_files = []
    for file in files:
       if is_logically_empty_json(file):
          continue
       else:
          valid_files.append(file)
    test = StopEventParser.load_json_bulk(valid_files)

    future_list = []
    project_id = "data-engineering-455419"
    topic_id = "Stop-Event-Data"
    
    publisher = PubSubPublisher(project_id, topic_id)
    future_list = [publisher.publish(repr(x)) for x in test]
    dp.logger.info(f"Publishing {len(future_list)} Stop Event messages to Pub/Sub.")

    for future in future_list:
        try:
            future.result()
        except Exception as e:
            dp.logger.error(f"Error publishing message: {e}")

