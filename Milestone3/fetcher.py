# pip install packages!!!

import urllib.request
import urllib.parse
import pandas as pd

def fetchStopEvents(vehicleID, oFile):
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={urllib.parse.quote(vehicleID)}"
    try:
        response = urllib.request.urlopen(url)
        html_data = response.read()

        tables = pd.read_html(html_data)
        if tables:
            df = tables[0]
            print(df)
            df.to_csv(oFile, index=False)

    except Exception as e:
        print("In except block")
        
# How to use it:
fetchStopEvents("2907", "stop_events_2907.csv")
