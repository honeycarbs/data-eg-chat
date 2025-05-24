import psycopg2
import json
from flask import Flask, jsonify

app = Flask(__name__)

# Database URI (replace with your own credentials if necessary)
db_uri = "postgresql://postgres:password@localhost/postgres"

@app.route('/get_breadcrumbs', methods=['GET'])
def get_breadcrumbs():
    # Fetch and transform the breadcrumb data to GeoJSON
    geojson_data = fetch_breadcrumb_data()
    return jsonify(geojson_data)

def get_db_connection():
    """Create and return a connection to the PostgreSQL database"""
    conn = psycopg2.connect(db_uri)
    return conn

def fetch_breadcrumb_data():
    """Fetch breadcrumb data from the database and return it as GeoJSON"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT 
        bc.tstamp,
        bc.latitude,
        bc.longitude,
        bc.speed,
        bc.trip_id,
        t.route_id,
        t.vehicle_id,
        t.service_key,
        t.direction
    FROM BreadCrumb bc
    JOIN Trip t ON bc.trip_id = t.trip_id
    WHERE t.service_key = 'Weekday'
    AND t.direction = 'Out'
    AND bc.tstamp BETWEEN '2025-01-01' AND '2025-12-31';
    """)

    result = cursor.fetchall()

    geojson = transform_to_geojson(result)

    cursor.close()
    conn.close()

    return geojson

def transform_to_geojson(cursor_results):
    """Transform the query result into GeoJSON format"""
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for row in cursor_results:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row[2], row[1]]  # Longitude, Latitude
            },
            "properties": {
                "timestamp": row[0].isoformat(),
                "trip_id": row[4],
                "route_id": row[5],
                "vehicle_id": row[6],
                "service_key": row[7],
                "direction": row[8],
                "speed": row[3]
            }
        }
        geojson["features"].append(feature)

    return geojson

if __name__ == '__main__':
    app.run(debug=True)
