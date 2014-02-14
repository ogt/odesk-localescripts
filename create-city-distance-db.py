# coding: utf-8
from __future__ import unicode_literals
import csv
import os
from itertools import combinations
from math import radians, cos, sin, asin, sqrt



'''
Script parses custom city list, finds pairs with distance <= MAX_CITY_DISTANCE and writes matches to CSV file.
'''



CITY_DB_PATH = os.path.dirname(os.path.abspath(__file__)) + '/data/TARGET_city_list.csv'
TARGET_CITY_DISTANCES_DB_PATH = os.path.dirname(os.path.abspath(__file__)) + '/data/TARGET_city_distance.csv'
MAX_CITY_DISTANCE = 50 # miles
EARTH_RADIUS_MILES = 3963



class CustomCityListImporter(csv.DictReader):
    fieldnames = [
        'geonameid',
        'name',
        'admin1_code',
        'country_code',
        'population',
        'longitude',
        'latitude',
    ]



class CityDistanceManager(object):
    @classmethod
    def process(self, city_db, city_distance_db):
        data = self.load_custom_data(city_db)
        result = []
        for city1, city2 in combinations(data, 2):
            distance = self.get_cities_distance_miles(city1, city2)
            if distance <= MAX_CITY_DISTANCE:
                result.append([city1['geonameid'], city2['geonameid'], '%.0f' % distance])
        self.write_csv(result, city_distance_db)
        print 'Done. %d rows written' % len(result)

    @classmethod
    def get_cities_distance_miles(self, city1, city2):
        return self.get_earth_dictance_miles(city1['longitude'], city1['latitude'], city2['longitude'], city2['latitude'])

    @classmethod
    def get_earth_dictance_miles(self, lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)

        Based on http://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        miles_distance = EARTH_RADIUS_MILES * c
        return miles_distance

    @classmethod
    def write_csv(self, data, target_db):
        with open(target_db, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data)

    @classmethod
    def load_custom_data(self, path):
        with open(path , 'rb') as csv_file:
            reader = CustomCityListImporter(csv_file)
            return [self.decode_row(row) for row in reader]

    @classmethod
    def decode_row(self, row_dict, encoding='utf8'):
        row_dict['longitude'] = float(row_dict['longitude'])
        row_dict['latitude'] = float(row_dict['latitude'])
        return row_dict



if __name__ == '__main__':
    print CityDistanceManager.process(CITY_DB_PATH, TARGET_CITY_DISTANCES_DB_PATH)



