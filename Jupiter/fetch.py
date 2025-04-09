# Python program that can make an HTTP request to a web server
# and fetch the results.

import urllib.request
import urllib.parse

def fetchData(vehicleID, oFile):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={urllib.parse.quote(vehicleID)}"
    try:
        response = urllib.request.urlopen(url)
        data = response.read()

        with open(oFile, 'wb') as file:
            file.write(data)
    except urllib.error.HTTPError as err:
        pass