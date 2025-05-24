from flask import Flask, jsonify
import psycopg2
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=["http://localhost:8000"])

# CHANGE DB CREDENTIALS
DB_CONFIG = {
    'host': 'localhost',
    'database': 'db_name', 
    'user': 'helenkhoshnaw',
    'password': 'password',
    'port': 5432
}

@app.route('/api/trip/<int:trip_id>')
def get_geojson(trip_id):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Corrected query to get breadcrumb data and convert it to GeoJSON format
        query = """
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(
                    jsonb_build_object(
                        'type', 'Feature',
                        'geometry', jsonb_build_object(
                            'type', 'Point',
                            'coordinates', jsonb_build_array(b.longitude, b.latitude)
                        ),
                        'properties', jsonb_build_object(
                            'timestamp', b.tstamp,
                            'speed', b.speed,
                            'trip_id', t.trip_id,
                            'route_id', t.route_id,
                            'vehicle_id', t.vehicle_id,
                            'service_key', t.service_key,
                            'direction', t.direction
                        )
                    ) ORDER BY b.tstamp
                )
            )
            FROM BreadCrumb b
            JOIN Trip t ON b.trip_id = t.trip_id
            WHERE t.trip_id = %s;
        """
        cur.execute(query, (trip_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        geojson = row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
        return jsonify(geojson)

    except Exception as e:
        print("Error:", e)
        return jsonify({'error': 'Something went wrong'}), 500

if __name__ == '__main__':
    app.run(debug=True)
