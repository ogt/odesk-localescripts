# coding: utf-8
from __future__ import unicode_literals
import csv
from collections import defaultdict
import locallib


'''
Script parses custom city list, finds matches in GeoNames database and writes matches to CSV file.
'''



class CustomCityListImporter(csv.DictReader):
    fieldnames = [
        'country',
        'city',
        'region',
        'employers_count',
    ]



class GeonamesAdmin1CodesImporter(csv.DictReader):
    DELIMITER = str('\t')
    fieldnames = [
            'admin1_code',
            'name',
            'asciiname',
            'inhabitant_count',
    ]



class GeonamesCityListImporter(csv.DictReader):
    DELIMITER = str('\t')
    QUOTECHAR = str('|') # char " is used in field values
    fieldnames = [
        'geonameid',
        'name',
        'asciiname',
        'alternatenames',
        'latitude',
        'longitude',
        'feature_class',
        'feature_code',
        'country_code',
        'cc2',
        'admin1_code',
        'admin2_code',
        'admin3_code',
        'admin4_code',
        'population',
        'elevation',
        'dem',
        'timezone',
        'modification_date',
    ]



class CityManager(object):
    FORCE_MATCHIING_MAP = [
       # (original, real_match)
       ({'country': 'United States', 'region': '', 'employers_count': '401', 'city': 'newyork'}, {'country': 'United States', 'region': 'NY', 'employers_count': '401', 'city': 'new york city'} ),
       ({'country': 'United States', 'region': 'NY', 'employers_count': '142', 'city': 'newyork'}, {'country': 'United States', 'region': 'NY', 'employers_count': '142', 'city': 'new york city'} ),
    ]

    CUSTOM_COUNTRY_NAME_TO_ISO_3166_2_CODE = {
        'United Kingdom': 'GB',
        'United States': 'US',
        'Australia': 'AU',
        'Canada': 'CA',
    }
    CUSTOM_REGION_NAME_TO_ADMIN1_CODE = {
        'US.AR': 'US.AR',
        'US.DC': 'US.DC',
        'US.DE': 'US.DE',
        'US.FL': 'US.FL',
        'US.GA': 'US.GA',
        'US.KS': 'US.KS',
        'US.LA': 'US.LA',
        'US.MD': 'US.MD',
        'US.MO': 'US.MO',
        'US.MS': 'US.MS',
        'US.NC': 'US.NC',
        'US.OK': 'US.OK',
        'US.SC': 'US.SC',
        'US.TN': 'US.TN',
        'US.TX': 'US.TX',
        'US.WV': 'US.WV',
        'US.AL': 'US.AL',
        'US.CT': 'US.CT',
        'US.IA': 'US.IA',
        'US.IL': 'US.IL',
        'US.IN': 'US.IN',
        'US.ME': 'US.ME',
        'US.MI': 'US.MI',
        'US.MN': 'US.MN',
        'US.NE': 'US.NE',
        'US.NH': 'US.NH',
        'US.NJ': 'US.NJ',
        'US.NY': 'US.NY',
        'US.OH': 'US.OH',
        'US.RI': 'US.RI',
        'US.VT': 'US.VT',
        'US.WI': 'US.WI',
        'US.CA': 'US.CA',
        'US.CO': 'US.CO',
        'US.NM': 'US.NM',
        'US.NV': 'US.NV',
        'US.UT': 'US.UT',
        'US.AZ': 'US.AZ',
        'US.ID': 'US.ID',
        'US.MT': 'US.MT',
        'US.ND': 'US.ND',
        'US.OR': 'US.OR',
        'US.SD': 'US.SD',
        'US.WA': 'US.WA',
        'US.WY': 'US.WY',
        'US.HI': 'US.HI',
        'US.AK': 'US.AK',
        'US.KY': 'US.KY',
        'US.MA': 'US.MA',
        'US.PA': 'US.PA',
        'US.VA': 'US.VA',
        'CA.AB': 'CA.01',
        'CA.BC': 'CA.02',
        'CA.MB': 'CA.03',
        'CA.NB': 'CA.04',
        'CA.NT': 'CA.13',
        'CA.NS': 'CA.07',
        'CA.NU': 'CA.14',
        'CA.ON': 'CA.08',
        'CA.PE': 'CA.09',
        'CA.QC': 'CA.10',
        'CA.SK': 'CA.11',
        'CA.YT': 'CA.12',
        'CA.NL': 'CA.05',
    }

    @classmethod
    def get_region(self, official_data_row):
        return {
            'AU.08': 'Western Australia',
            'AU.05': 'South Australia',
            'AU.03': 'Northern Territory',
            'AU.07': 'Victoria',
            'AU.06': 'Tasmania',
            'AU.04': 'Queensland',
            'AU.02': 'New South Wales',
            'AU.01': 'Australian Capital Territory',
            'CA.01': 'Alberta',
            'CA.02': 'British Columbia',
            'CA.03': 'Manitoba',
            'CA.04': 'New Brunswick',
            'CA.13': 'Northwest Territories',
            'CA.07': 'Nova Scotia',
            'CA.14': 'Nunavut',
            'CA.08': 'Ontario',
            'CA.09': 'Prince Edward Island',
            'CA.10': 'Quebec',
            'CA.11': 'Saskatchewan',
            'CA.12': 'Yukon',
            'CA.05': 'Newfoundland and Labrador',
            'GB.WLS': 'Wales',
            'GB.SCT': 'Scotland',
            'GB.NIR': 'N Ireland',
            'GB.ENG': 'England',
            'US.AR': 'Arkansas',
            'US.DC': 'Washington, D.C.',
            'US.DE': 'Delaware',
            'US.FL': 'Florida',
            'US.GA': 'Georgia',
            'US.KS': 'Kansas',
            'US.LA': 'Louisiana',
            'US.MD': 'Maryland',
            'US.MO': 'Missouri',
            'US.MS': 'Mississippi',
            'US.NC': 'North Carolina',
            'US.OK': 'Oklahoma',
            'US.SC': 'South Carolina',
            'US.TN': 'Tennessee',
            'US.TX': 'Texas',
            'US.WV': 'West Virginia',
            'US.AL': 'Alabama',
            'US.CT': 'Connecticut',
            'US.IA': 'Iowa',
            'US.IL': 'Illinois',
            'US.IN': 'Indiana',
            'US.ME': 'Maine',
            'US.MI': 'Michigan',
            'US.MN': 'Minnesota',
            'US.NE': 'Nebraska',
            'US.NH': 'New Hampshire',
            'US.NJ': 'New Jersey',
            'US.NY': 'New York',
            'US.OH': 'Ohio',
            'US.RI': 'Rhode Island',
            'US.VT': 'Vermont',
            'US.WI': 'Wisconsin',
            'US.CA': 'California',
            'US.CO': 'Colorado',
            'US.NM': 'New Mexico',
            'US.NV': 'Nevada',
            'US.UT': 'Utah',
            'US.AZ': 'Arizona',
            'US.ID': 'Idaho',
            'US.MT': 'Montana',
            'US.ND': 'North Dakota',
            'US.OR': 'Oregon',
            'US.SD': 'South Dakota',
            'US.WA': 'Washington',
            'US.WY': 'Wyoming',
            'US.HI': 'Hawaii',
            'US.AK': 'Alaska',
            'US.KY': 'Kentucky',
            'US.MA': 'Massachusetts',
            'US.PA': 'Pennsylvania',
            'US.VA': 'Virginia',
        }['%s.%s' % (official_data_row['country_code'], official_data_row['admin1_code'])]

    @classmethod
    def process(self, custom_db, official_db, target_db):
        cities = self.get_cities(
            self.load_custom_data(custom_db),
            self.load_official_data(official_db)
        )
        self.write_csv(cities, target_db)

    @classmethod
    def write_csv(self, data, target_db):
        fields = [
            'geonameid',
            'name',
            'custom_region',
            'country_code',
            'population',
            'longitude',
            'latitude',
        ]
        with open(target_db, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for city in data:
                city['custom_region'] = self.get_region(city)
                row = []
                for f in fields:
                    row.append(city[f].encode('utf8'))
                writer.writerow(row)

    @classmethod
    def get_cities(self, custom_data, official_data):
        matched = defaultdict(list)
        not_matched_directly = []
        total_employeer_count = 0
        matched_employeer_count = 0
        for row in custom_data:
            total_employeer_count += int(row['employers_count'])
            city_matched = False
            key = tuple(sorted(row.items()))
            for offical_row in official_data:
                if self.row_match(row, offical_row):
                    city_matched = True
                    matched[key].append(offical_row)
                    # we don't want to count duplicates
                    if len(matched[key]) == 1:
                        matched_employeer_count += int(row['employers_count'])

            if not city_matched:
                not_matched_directly.append(row)

        singlematch_count = len([v for v in matched.values() if len(v) == 1])
        multimatch_count = len([v for v in matched.values() if len(v) > 1])
        # {custom_row => list(official_row), ...} to [offical_row, ...]
        official_cities = [v[0] for v in self.remove_duplicate_matches(matched).values()]
        # remove duplicated entries from list of cities
        official_cities = [dict(t) for t in set([tuple(sorted(d.items())) for d in official_cities])]

        print 'CITIES TO BE MATCHED (dirty data, contains duplicates and incorrect cities): %d' % len(custom_data)
        print 'DIRTY CITY MATCHED DIRECTLY (only one match found): %d' % singlematch_count
        print 'DIRTY CITY MATCHED DIRECTLY (one dirty row have multiple matches in official data; taking largest population city): %d' % multimatch_count
        print 'REMOVED DUPLICATES IN FINAL RESULT (same real city present multiple times in dirty data): %d' % (len(matched) - len(official_cities))

        print 'TOTAL EMPLOYEER COUNT: %d' % total_employeer_count
        print 'DIRECTLY MATCHED EMPLOYEER COUNT: %d' % matched_employeer_count
        print 'DIRECTLY/TOTAL RADIO: %.1f' % ((float(matched_employeer_count)/total_employeer_count) * 100)

        return official_cities

    @classmethod
    def remove_duplicate_matches(self, data):
        for key, duplicates in data.iteritems():
            if len(duplicates) == 1:
                continue
            data[key] = [reduce(lambda x,y: x if int(x['population']) > int(y['population']) else y, duplicates)]
        return data

    @classmethod
    def row_match(self, custom_data_row, official_data_row, skip_region=False, match_if_matching_prefix=False):
        for k,v in self.FORCE_MATCHIING_MAP:
            if k == custom_data_row:
                custom_data_row = v
                break

        try:
            assert self.custom_country_name_to_ISO_3166_2_code(custom_data_row['country']) == official_data_row['country_code']

            try:
                assert custom_data_row['city'].lower() == official_data_row['name'].lower()
            except AssertionError:
                try:
                    assert custom_data_row['city'].lower() == official_data_row['asciiname'].lower()
                except AssertionError:
                    pass
                    if match_if_matching_prefix:
                        try:
                            assert official_data_row['name'].lower().startswith(custom_data_row['city'].lower())
                        except AssertionError:
                            assert official_data_row['asciiname'].lower().startswith(custom_data_row['city'].lower())
                    else:
                        raise
            if custom_data_row['region'] not in ('', '-') and not skip_region:
                assert self.custom_region_name_to_admin1_code(custom_data_row['country'], custom_data_row['region'])[-2:] == official_data_row['admin1_code']
            return True
        except AssertionError:
            return False


    @classmethod
    def load_custom_data(self, path):
        with open(path , 'rb') as csv_file:
            reader = CustomCityListImporter(csv_file)
            return [self.decode_row(row) for row in reader if not self.skip_custom_db_row(row)]

    @classmethod
    def load_official_data(self, path):
        with open(path , 'rb') as csv_file:
            reader = GeonamesCityListImporter(csv_file, delimiter=GeonamesCityListImporter.DELIMITER, quotechar=GeonamesCityListImporter.QUOTECHAR)
            return [self.decode_row(row) for row in reader if not self.skip_official_db_row(row)]

    @classmethod
    def skip_custom_db_row(self, row):
        #return row['country'].lower() != 'australia'
        return False

    @classmethod
    def skip_official_db_row(self, row):
        #return row['country_code'] != 'AU'
        return row['country_code'] not in self.CUSTOM_COUNTRY_NAME_TO_ISO_3166_2_CODE.values()

    @classmethod
    def decode_row(self, row_dict, encoding='utf8'):
        '''
        Covert string dict to unicode one.
        '''
        return {k.decode(encoding):v.decode(encoding) for k,v in row_dict.iteritems()}

    @classmethod
    def custom_country_name_to_ISO_3166_2_code(self, name):
        try:
            return self.CUSTOM_COUNTRY_NAME_TO_ISO_3166_2_CODE[name]
        except KeyError:
            raise Exception('Unsupoorted country "%s".' % name)

    @classmethod
    def custom_region_name_to_admin1_code(self, custom_country, custom_region):
        key = '%s.%s' % (self.custom_country_name_to_ISO_3166_2_code(custom_country), custom_region)
        try:
            return self.CUSTOM_REGION_NAME_TO_ADMIN1_CODE[key]
        except KeyError:
            raise Exception('Unsupoorted region "%s" for country "%s".' % (custom_region, custom_country))


if __name__ == '__main__':
    config = locallib.get_config()
    print CityManager.process(
        locallib.get_absolute_path(config.get('input','custom_city_db_path')),
        locallib.get_absolute_path(config.get('input','official_city_db_path')),
        locallib.get_absolute_path(config.get('output', 'target_city_db_path')),
    )

