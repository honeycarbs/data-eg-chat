DROP SCHEMA IF EXISTS Trimet;

DROP TABLE IF EXISTS Trimet.BreadCrumb;
DROP TABLE IF EXISTS Trimet.Trip;
DROP TYPE IF EXISTS Trimet.service_type;
DROP TYPE IF EXISTS Trimet.tripdir_type;

CREATE SCHEMA Trimet;

CREATE TYPE Trimet.service_type as enum ('Weekday', 'Saturday', 'Sunday');
CREATE TYPE Trimet.tripdir_type as enum ('Out', 'Back');

CREATE TABLE Trimet.Trip (
        trip_id integer,
        route_id integer,
        vehicle_id integer,
        service_key service_type,
        direction tripdir_type,
        PRIMARY KEY (trip_id)
);

CREATE TABLE Trimet.BreadCrumb (
        tstamp timestamp,
        latitude float,
        longitude float,
        speed float,
        trip_id integer,
        FOREIGN KEY (trip_id) REFERENCES Trip
);

-- TODO: roles, grants