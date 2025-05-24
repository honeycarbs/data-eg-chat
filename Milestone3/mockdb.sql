-- Drop tables and types if they already exist
DROP TABLE IF EXISTS BreadCrumb;
DROP TABLE IF EXISTS Trip;
DROP TYPE IF EXISTS service_type;
DROP TYPE IF EXISTS tripdir_type;

-- Create custom types
CREATE TYPE service_type AS ENUM ('Weekday', 'Saturday', 'Sunday');
CREATE TYPE tripdir_type AS ENUM ('Out', 'Back');

-- Create the Trip table
CREATE TABLE Trip (
    trip_id INTEGER PRIMARY KEY,
    route_id INTEGER,
    vehicle_id INTEGER,
    service_key service_type,
    direction tripdir_type
);

-- Create the BreadCrumb table
CREATE TABLE BreadCrumb (
    tstamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    speed FLOAT,
    trip_id INTEGER,
    FOREIGN KEY (trip_id) REFERENCES Trip
);

-- Insert sample data into Trip table
INSERT INTO Trip (trip_id, route_id, vehicle_id, service_key, direction)
VALUES
    (1, 101, 1001, 'Weekday', 'Out'),
    (2, 102, 1002, 'Saturday', 'Back'),
    (3, 103, 1003, 'Sunday', 'Out'),
    (4, 104, 1004, 'Weekday', 'Back');

-- Insert sample data into BreadCrumb table
INSERT INTO BreadCrumb (tstamp, latitude, longitude, speed, trip_id)
VALUES
    ('2025-05-22 08:00:00', 40.7128, -74.0060, 30, 1),  -- Trip 1
    ('2025-05-22 08:05:00', 40.7158, -74.0100, 32, 1),
    ('2025-05-22 09:00:00', 34.0522, -118.2437, 45, 2), -- Trip 2
    ('2025-05-22 09:30:00', 34.0500, -118.2500, 40, 2),
    ('2025-05-22 10:00:00', 51.5074, -0.1278, 50, 3),  -- Trip 3
    ('2025-05-22 10:15:00', 51.5100, -0.1200, 55, 3),
    ('2025-05-22 11:00:00', 48.8566, 2.3522, 60, 4),  -- Trip 4
    ('2025-05-22 11:10:00', 48.8580, 2.3500, 58, 4);

-- Confirm the data inserted into Trip
SELECT * FROM Trip;

-- Confirm the data inserted into BreadCrumb
SELECT * FROM BreadCrumb;

