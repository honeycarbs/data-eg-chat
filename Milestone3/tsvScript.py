import csv
import geojson
from geojson import Feature, FeatureCollection, Point

features = []

with open("trip_data.tsv", newline="") as csvfile:
    reader = csv.reader(csvfile, delimiter="\t")

    header = next(reader, None)          # remove if the file has no header
    for lon, lat, speed in reader:       # unpack the three columns
        if not speed:                    # skip empty speed
            continue

        try:
            lon, lat = float(lon), float(lat)
            speed   = float(speed)       # or int(speed) if always whole
        except ValueError:               # bad numeric data – skip row
            continue

        features.append(
            Feature(
                geometry=Point((lon, lat)),
                properties={"speed": speed},
            )
        )

collection = FeatureCollection(features)

with open("trip_data.geojson", "w") as f:
    geojson.dump(collection, f, indent=2)