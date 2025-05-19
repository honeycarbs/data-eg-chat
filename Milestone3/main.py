from fetcher import StopEventFetcher
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

        self.fetcher = StopEventFetcher()
        self.logger.info("initialized fetcher successfully")

    """
    The first step of the new pipeline: opening the file and fetching data
    """
    def PrepareIDGroup(self, id_group_file):
        ids = []
        with open(id_group_file, "r") as file:
            for line in file:
                id = line.strip()
                ids.append(id)

        self.logger.info(f"got {len(ids)} records from {id_group_file}")
        return ids
    
    def FetchBreadCrumbsBulk(self, id_list):
        for id in id_list:
            output_file = f"events/stop_events_{id}.json"
            self.fetcher.fetch(output_file, id)

if __name__ == "__main__":
    dp = DataPipeline(logging.DEBUG)
    ids = dp.PrepareIDGroup("Jupiter/id.txt")
    dp.FetchBreadCrumbsBulk(ids)