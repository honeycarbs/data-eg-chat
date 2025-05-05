DROP TABLE IF EXISTS BreadCrumb;
DROP TABLE IF EXISTS Trip;
DROP TYPE IF EXISTS service_type;
DROP TYPE IF EXISTS tripdir_type;

CREATE TYPE service_type as enum ('Weekday', 'Saturday', 'Sunday');
CREATE TYPE tripdir_type as enum ('Out', 'Back');

CREATE TABLE Trip (
        trip_id integer,
        route_id integer,
        vehicle_id integer,
        service_key service_type,
        direction tripdir_type,
        PRIMARY KEY (trip_id)
);

CREATE TABLE BreadCrumb (
        tstamp timestamp,
        latitude float,
        longitude float,
        speed float,
        trip_id integer,
        FOREIGN KEY (trip_id) REFERENCES Trip
);