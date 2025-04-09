from json import loads, load
from time import time
from collections import namedtuple

class GpsLocation:
    __coordinates: tuple 

    def __init__(self, latitude, longitude):
        self.__coordinates = (latitude, longitude)

    def latitude(self):
        return self.__coordinates[0]
    
    def longitude(self):
        return self.__coordinates[1]

class Vehicle:
    event_no_trip: int
    event_no_stop: int
    opd_date: time
    vehicle_id: int
    meters: int
    act_time: time
    gps_location: GpsLocation
    gps_satellites: float
    gps_hdop: float

    def __init__(self, event_no_trip, event_no_stop, opd_date, vehicle_id, meters, act_time, gps_longitude, gps_latitude, gps_satellites, gps_hdop):
        self.event_no_trip = event_no_trip
        self.event_no_stop = event_no_stop
        self.opd_date = opd_date
        self.vehicle_id = vehicle_id
        self.meters = meters
        self.act_time = act_time
        self.gps_location = GpsLocation(gps_latitude, gps_longitude)
        self.gps_satellites = gps_satellites
        self.gps_hdop = gps_hdop

    def __repr__(self):
        return f'EVENT_NO_TRIP: {self.event_no_trip}, EVENT_NO_STOP: {self.event_no_stop}, OPD_DATE: {self.opd_date\
            }, VEHICLE_ID: {self.vehicle_id}, METERS: {self.meters}, ACT_TIME: {self.act_time}, GPS_LONGITUDE: {self.gps_location.longitude()}, GPS_LATITUDE: { \
            self.gps_location.latitude()}, GPS_SATELLITES: {self.gps_satellites}, GPS_HDOP: {self.gps_hdop}'

    @classmethod
    def from_json(cls, json_string):
        json_string_lower = {k.lower(): v for k, v in json_string.items()}
        return cls(**json_string_lower)
    
    @classmethod
    def from_json_bulk(cls, file_path):
        vehicles_list = []

        with open(file_path, 'r') as file:
            crumbs = load(file)
            
            for crumb in crumbs:
                vehicle = cls.from_json(crumb)
                vehicles_list.append(vehicle)

        return vehicles_list
            
    