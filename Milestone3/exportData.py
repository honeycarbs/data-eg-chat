import psycopg2
import csv
import os

# Database connection settings
db_uri = os.getenv('DB_URI')

# change trip_id when creating visuals
trip_id = 1
query = f"""
    SELECT longitude, latitude, speed
    FROM breadcrumb
    WHERE trip_id = {trip_id};
"""

# Output file
output_file = "trip_data.tsv"

def export_trip_data():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(db_uri)
        cursor = conn.cursor()

        # Execute query
        cursor.execute(query)
        rows = cursor.fetchall()

        # Write results to TSV file
        with open(output_file, "w", newline='') as tsv_file:
            writer = csv.writer(tsv_file, delimiter='\t')
            writer.writerows(rows)

        print(f"Exported {len(rows)} rows to {output_file}")

    except Exception as e:
        print("Error:", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    export_trip_data()
