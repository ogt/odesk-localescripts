# coding: utf-8
from __future__ import unicode_literals
import csv
import os
from collections import defaultdict
import odesk
import locallib
import sys



'''
Script parses custom city and skill list and save statistics (based on oDesk API query result) to a CSV file.
'''



class CustomCityDistancesImporter(csv.DictReader):
    fieldnames = [
        'geonameid1',
        'geonameid2',
        'distance_in_miles',
    ]



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



class CustomSkills1Importer(csv.DictReader):
    fieldnames = [
        'name',
        'user_count',
    ]



class CustomSkills2Importer(csv.DictReader):
    DELIMITER = str('\t')
    fieldnames = [
        'n1',
        'n2',
        'n3',
        'name',
    ]



class CityDataManager(object):
    @classmethod
    def process(self, api_client, max_api_calls, city_db, city_distances_db, skill1_db, skill2_db, result_db):
        geonameid_to_name_map = self.load_custom_city_list_data(city_db)
        calls = 0
        with open(result_db, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for city, skill, neighbor_cities in self.get_skill_and_city_combinations(city_distances_db, skill1_db, skill2_db, geonameid_to_name_map):
                if calls >= max_api_calls:
                    break
                data = (skill, city, self.fetch_count_from_api(api_client, city, skill, neighbor_cities))
                writer.writerow(data)
                # flush every 100 rows
                if calls % 100 == 0:
                    csvfile.flush()
                    os.fsync(csvfile.fileno())
                calls += 1



    @classmethod
    def fetch_count_from_api(self, api_client, city, skill, neighbor_cities):
        sys.stdout.write('.')
        query_data = {
            'q': ' OR '.join(['location:"%s"' % l for l in [city] + neighbor_cities]),
            'skills': [skill],
        }
        return api_client.provider_v2.get('search/providers', data=query_data)['paging']['total']

    @classmethod
    def get_skill_and_city_combinations(self, city_distances_db, skill1_db, skill2_db, geonameid_to_name_map):
        for city, neighbor_cities in self.load_custom_city_distances_data(city_distances_db).iteritems():
            for skill in self.load_custom_skill_data(skill1_db, skill2_db):
                yield city, skill, [geonameid_to_name_map[i] for i in neighbor_cities]


    @classmethod
    def load_custom_city_distances_data(self, path):
        with open(path , 'rb') as csv_file:
            reader = CustomCityDistancesImporter(csv_file)
            data = [self.decode_row(row) for row in reader]
            neighbor_cities = defaultdict(list)
            for d in data:
                neighbor_cities[d['geonameid1']].append(d['geonameid2'])
                neighbor_cities[d['geonameid2']].append(d['geonameid1'])
            return neighbor_cities

    @classmethod
    def load_custom_city_list_data(self, path):
        with open(path , 'rb') as csv_file:
            reader = CustomCityListImporter(csv_file)
            return {i['geonameid']: i['name'] for i in reader}

    @classmethod
    def load_custom_skill_data(self, path1, path2):
        data = set()
        with open(path1, 'rb') as csv_file:
            reader = CustomSkills1Importer(csv_file)
            data = data.union(set([unicode(row['name'], 'utf8') for row in reader]))

        with open(path2, 'rb') as csv_file:
            reader = CustomSkills2Importer(csv_file, delimiter=CustomSkills2Importer.DELIMITER)

            data = data.union(set([unicode(row['name'], 'utf8') for row in reader]))
        return data

    @classmethod
    def decode_row(self, row_dict, encoding='utf8'):
        '''
        Covert string dict to unicode one.
        '''
        return {k.decode(encoding):v.decode(encoding) for k,v in row_dict.iteritems()}



class ApiClientFactory(object):
    @classmethod
    def get_odesk_client(self, public_key, secret_key):
        client = odesk.Client(public_key, secret_key)
        verifier = raw_input(
            'Please enter the verification code you get '
            'following this link:\n{0}\n\n> '.format(
                client.auth.get_authorize_url()))

        print 'Retrieving keys.... '
        access_token, access_token_secret = client.auth.get_access_token(verifier)
        print 'OK'

        # For further use you can store ``access_toket`` and
        # ``access_token_secret`` somewhere
        return odesk.Client(public_key, secret_key,
                              oauth_access_token=access_token,
                              oauth_access_token_secret=access_token_secret)


if __name__ == '__main__':
    config = locallib.get_config()
    print CityDataManager.process(
        ApiClientFactory.get_odesk_client(config.get('odesk', 'public_key'), config.get('odesk', 'secret_key')),
        int(config.get('odesk', 'max_api_calls')),
        locallib.get_absolute_path(config.get('output', 'target_city_db_path')),
        locallib.get_absolute_path(config.get('output','target_city_distances_db_path')),
        locallib.get_absolute_path(config.get('input','custom_skills1_db_path')),
        locallib.get_absolute_path(config.get('input','custom_skills2_db_path')),
        locallib.get_absolute_path(config.get('output','target_city_skill_contractor_count_db_path')),
    )



