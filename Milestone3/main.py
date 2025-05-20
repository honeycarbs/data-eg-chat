import logging
import sys
from pathlib import Path

from fetcher import StopEventFetcher


class DataPipeline:
    def __init__(self, logger, fetcher):
        self._logger = logger
        self._fetcher = fetcher

    def _prepareIDGroup(self, id_group_file):
        """
        Reads IDs from file.
        Args:
            id_group_file - path of txt file with ids (one per line)
        """
        ids = []
        with open(id_group_file, "r") as file:
            for line in file:
                id = line.strip()
                ids.append(id)

        self._logger.info(f"got {len(ids)} records from {id_group_file}")
        return ids
    
    def RunPipeline(self, id_group_file):
        """
        Main method for the pipeline launch.
        """
        id_list = self._prepareIDGroup(id_group_file)

        for id in id_list:
            output_file = f"events/stop_events_{id}.json"
            self._fetcher.fetch(output_file, id)

if __name__ == "__main__":
    """
    Configuring one instance of logger with two streams
    (file for VM logs and console for developement)
    """
    log_file = "pipe_logs.log"

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

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    file = logging.FileHandler(log_file, mode="a")
    file.setLevel(logging.DEBUG)
    file.setFormatter(formatter)
    logger.addHandler(file)

    logger.info("logger set up successfully")

    """
    Configuring instances of fetcher, ... <TBD>
    """

    fetcher = StopEventFetcher(base_url="https://busdata.cs.pdx.edu/api/getStopEvents", logger=logger)

    logger.info("fetcher set up successfully")

    dp = DataPipeline(logger, fetcher)
    dp.RunPipeline("Jupiter/id.txt")