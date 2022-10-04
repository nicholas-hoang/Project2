import gzip
import json
from multiprocessing import Value
from operator import countOf
import os
import hashlib
import pandas as pd
import numpy as np

# Python 2
# import urllib2

# Python 3
import urllib.request as urllib

import util_5353

BASE_URL = 'https://syntheticmass.mitre.org/fhir/'
MAX_PATIENTS = 1000
CACHE_FILE = 'cache.dat'
PATH_CACHE = {}

# Returns the JSON result at the given URL.  Caches the results so we don't
# unnecessarily hit the FHIR server.  Note this ain't the best caching, as
# it's going to save a bunch of tiny files that could probably be handled more
# efficiently.


def get_url(url):
    # First check the cache
    if len(PATH_CACHE) == 0:
        for line in open(CACHE_FILE).readlines():
            split = line.strip().split('\t')
            cached_path = split[0]
            cached_url = split[1]
            PATH_CACHE[cached_url] = cached_path
    if url in PATH_CACHE:
        return json.loads(gzip.open(PATH_CACHE[url]).read().decode('utf-8'))

    print('Retrieving:', url)

    print('You are about to query the FHIR server, which probably means ' +
          'that you are doing something wrong.  But feel free to comment ' +
          'out this bit of code and proceed right ahead.')

    print('Note: the code below is not tested for Python 3, you will likely ' +
          'need to make a few changes, e.g., urllib2')

    resultstr = urllib.urlopen(url).read()
    json_result = json.loads(resultstr)

    # Remove patient photos, too much space
    if url.replace(BASE_URL, '').startswith('Patient'):
        for item in json_result['entry']:
            item['resource']['photo'] = 'REMOVED'

    m = hashlib.md5()
    m.update(url)
    md5sum = m.hexdigest()

    path_dir = 'cache/' + md5sum[0:2] + '/' + md5sum[2:4] + '/'
    if not os.path.exists('cache'):
        os.mkdir('cache')
    if not os.path.exists('cache/' + md5sum[0:2]):
        os.mkdir('cache/' + md5sum[0:2])
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
    path = path_dir + url.replace(BASE_URL, '')

    w = gzip.open(path, 'wb')
    w.write(json.dumps(json_result))
    w.close()
    w = open(CACHE_FILE, 'a')
    w.write(path + '\t' + url + '\n')
    w.close()
    PATH_CACHE[url] = path

    return json_result

# For pagination, returns the next URL


def get_next(result):
    links = result['link']
    for link in links:
        if link['relation'] == 'next':
            return link['url']


# Returns the list of patients based on the given filter
get_patients_count = 0


def get_patients(pt_filter):
    # Helpful logging to point out some programming flaws
    global get_patients_count
    get_patients_count += 1
    if get_patients_count >= 10:
        print('WARNING: get_patients called too many times')

    patients = []
    url = BASE_URL + 'Patient?_offset=0&_count=1000'
    while url is not None:
        patients_page = get_url(url)
        if 'entry' not in patients_page:
            break
        for patient_json in patients_page['entry']:
            patients.append(patient_json['resource'])
            if MAX_PATIENTS is not None and len(patients) == MAX_PATIENTS:
                return [p for p in patients if pt_filter.include(p)]
        url = get_next(patients_page)
    return [p for p in patients if pt_filter.include(p)]


# Returns the conditions for the patient with the given patient_id
get_conditions_count = 0


def get_conditions(patient_id):
    global get_conditions_count
    get_conditions_count += 1
    if get_conditions_count >= MAX_PATIENTS * 5:
        print('WARNING: get_conditions called too many times')

    url = BASE_URL + 'Condition?patient=' + patient_id + '&_offset=0&_count=1000'
    conditions = []
    while url is not None:
        conditions_page = get_url(url)
        if 'entry' in conditions_page:
            conditions.extend([c['resource']
                              for c in conditions_page['entry']])
        url = get_next(conditions_page)
    return conditions


# Returns the observations for the patient with the given patient_id
get_observations_count = 0


def get_observations(patient_id):
    # Helpful logging to point out some programming flaws
    global get_observations_count
    get_observations_count += 1
    if get_observations_count >= MAX_PATIENTS * 3:
        print('WARNING: get_observations called too many times')

    url = BASE_URL + 'Observation?patient=' + patient_id + '&_offset=0&_count=1000'
    observations = []
    while url is not None:
        observations_page = get_url(url)
        if 'entry' in observations_page:
            observations.extend([o['resource']
                                for o in observations_page['entry']])
        url = get_next(observations_page)
    return observations


# Returns the medications for the patient with the given patient_id
get_medications_count = 0


def get_medications(patient_id):
    # Helpful logging to point out some programming flaws
    global get_medications_count
    get_medications_count += 1
    if get_medications_count >= MAX_PATIENTS * 3:
        print('WARNING: get_medications called too many times')

    url = BASE_URL + 'MedicationRequest?patient=' + \
        patient_id + '&_offset=0&_count=1000'
    medications = []
    DBG = 0
    while url is not None:
        medications_page = get_url(url)
        if 'entry' in medications_page:
            medications.extend([c['resource']
                               for c in medications_page['entry']])
        url = get_next(medications_page)
    return medications

# Problem 1 [10 points]


def num_patients(pt_filter):
    tup = None
    # Begin CODE
    patients = get_patients(pt_filter)

    total = list()

    for i in range(0, len(patients) - 1):
        name = patients[i]['name'][0]['family']
        total.append(name)

    seen = set()
    uniq = [x for x in total if x not in seen and not seen.add(x)]

    tup = tuple([len(total), len(uniq)])

    # End CODE
    return tup


# Problem 2 [10 points]
def patient_stats(pt_filter):
    stats = {}
    # Begin CODE

    patients = get_patients(pt_filter)

    # Dictionaries to Hold Values
    gender = {'female': 0, 'male': 0}
    maritalStatus = {'M': 0, 'S': 0, 'UNK': 0}
    race = {'White': 0, 'Black': 0, 'Asian': 0, 'Other': 0}
    ethnicity = {'Nonhispanic': 0, 'Central_american': 0,
                 'Puerto_rican': 0, 'Mexican': 0}
    birthyr = {}
    address = {'yes_address': 0, 'no_address': 0}

    # Gender Category
    for count, value in enumerate(patients, start=0):
        value = patients[count]['gender']

        if value == 'female':
            gender['female'] += 1

        else:
            gender['male'] += 1

    # Marital Status Category
    for count, value in enumerate(patients, start=0):

        try:
            value = patients[count]['maritalStatus']['coding'][0].get('code')
            maritalStatus[value] += 1

        except KeyError:
            maritalStatus['UNK'] += 1

    # Race
    for count, value in enumerate(patients, start=0):
        value = patients[count]['extension'][0]['valueCodeableConcept']['coding'][0].get(
            'display')
        race[value] += 1

    # Ethnicity
    for count, value in enumerate(patients, start=0):
        value = patients[count]['extension'][1]['valueCodeableConcept']['coding'][0].get(
            'display')
        ethnicity[value] += 1

    # BirthYear
    birth_list = []  # Store All the Values to build dictionary
    for count, value in enumerate(patients, start=0):
        value = patients[count]['birthDate'][0:4]
        birth_list.append(int(value))

    birthcats = []

    for year in birth_list:
        new_year = year - (year % 10)
        birthcats.append(new_year)

    values, counts = np.unique(birthcats, return_counts=True)

    values = [str(x)for x in values]

    birthyr = dict(zip(values, counts))

    # Address
    for count, value in enumerate(patients, start=0):
        try:
            value = patients[count]['address'][0]
            address['yes_address'] += 1
        except KeyError:
            address['no_address'] += 1

    stats = {'gender': gender,
             'marital_status': maritalStatus,
             'race': race,
             'ethnicity': ethnicity,
             'age': birthyr,
             'address': address}

    # Generate Categorical Probability Distribution

    def categorical_probs(dictionary):
        total = sum(dictionary.values())

        for keys, values in dictionary.items():
            probability = values/total
            dictionary[keys] = probability

        return dictionary

    for categories in stats.values():
        categories = categorical_probs(categories)

    # End CODE
    return stats

# Problem 3 [15 points]


def diabetes_quality_measure(pt_filter):
    tup = None
    # Begin CODE
    patients = get_patients(pt_filter)

    id_list = []
    for count, value in enumerate(patients):
        value = patients[count]['id']
        id_list.append(value)

    # Get Total Number of Diabetes Patients
    diabetes_total = 0
    diabetes_dict = {}

    for id in id_list:
        try:
            value = get_conditions(id)
            condition = value[0]['code']['coding'][0].get('code')
        except IndexError:
            continue
        finally:
            if condition == '44054006':
                diabetes_total += 1
                diabetes_dict[id] = 0
            else:
                continue
    # Get Total of LOINC Diagnosis

    for key in diabetes_dict:
        loinc = get_observations(key)
        for i in range(0, len(loinc) - 1):
            value = loinc[i]['code']['coding'][0].get('code')
            if value == '4548-4':
                diabetes_dict[key] += 1
            else:
                continue

    print(diabetes_dict)
    # End CODE
    return tup

# Problem 4 [10 points]


def common_condition_pairs(pt_filter):
    pairs = []
    # Begin CODE

    # End CODE
    return pairs

# Problem 5 [10 points]


def common_medication_pairs(pt_filter):
    pairs = []
    # Begin CODE

    # End CODE
    return pairs

# Problem 6 [10 points]


def conditions_by_age(pt_filter):
    tup = None
    # Begin CODE

    # End CODE
    return tup

# Problem 7 [10 points]


def medications_by_gender(pt_filter):
    tup = None
    # Begin CODE

    # End CODE
    return tup

# Problem 8 [25 points]


def bp_stats(pt_filter):
    stats = []
    # Begin CODE

    # End CODE
    return stats


# Basic filter, lets everything pass
class all_pass_filter:
    def id(self):
        return 'all_pass'

    def include(self, patient):
        util_5353.assert_dict_key(patient, 'id', 'pt_filter')
        util_5353.assert_dict_key(patient, 'name', 'pt_filter')
        util_5353.assert_dict_key(patient, 'address', 'pt_filter')
        util_5353.assert_dict_key(patient, 'birthDate', 'pt_filter')
        util_5353.assert_dict_key(patient, 'gender', 'pt_filter')
        return True


# Note: don't mess with this code block!  Your code will be tested by an outside
# program that will not call this __main__ block.  So if you mess with the
# following block of code you might crash the autograder.  You're definitely
# encouraged to look at this code, however, especially if your code crashes.
if __name__ == '__main__':

    # Include all patients
    pt_filter = all_pass_filter()

    print('::: Problem 1 :::')
    one_ret = num_patients(pt_filter)
    util_5353.assert_tuple(one_ret, 2, '1')
    util_5353.assert_int_range((0, 10000000), one_ret[0], '1')
    util_5353.assert_int_range((0, 10000000), one_ret[1], '1')

    # print('::: Problem 2 :::')
    # two_ret = patient_stats(pt_filter)
    # util_5353.assert_dict(two_ret, '2')
    # for key in ['gender', 'marital_status', 'race', 'ethnicity', 'age', 'with_address']:
    #     util_5353.assert_dict_key(two_ret, key, '2')
    #     util_5353.assert_dict(two_ret[key], '2')
    #     for key2 in two_ret[key].keys():
    #         util_5353.assert_str(key2, '2')
    #     util_5353.assert_prob_dict(two_ret[key], '2')
    # for key2 in two_ret['age'].keys():
    #     if not key2.isdigit():
    #         util_5353.die('2', 'age key should be year: %s', key2)

    print('::: Problem 3 :::')
    three_ret = diabetes_quality_measure(pt_filter)
    util_5353.assert_tuple(three_ret, 3, '3')
    util_5353.assert_int_range((0, 1000000), three_ret[0], '3')
    util_5353.assert_int_range((0, 1000000), three_ret[1], '3')
    util_5353.assert_int_range((0, 1000000), three_ret[2], '3')
    if three_ret[0] < three_ret[1] or three_ret[1] < three_ret[2]:
        util_5353.die('3', 'Values should be in %d >= %d >= %d', three_ret)

    print('::: Problem 4 :::')
    four_ret = common_condition_pairs(pt_filter)
    util_5353.assert_list(four_ret, 10, '4')
    for i in range(len(four_ret)):
        util_5353.assert_tuple(four_ret[i], 2, '4')
        util_5353.assert_str(four_ret[i][0], '4')
        util_5353.assert_str(four_ret[i][1], '4')

    print('::: Problem 5 :::')
    five_ret = common_medication_pairs(pt_filter)
    util_5353.assert_list(five_ret, 10, '5')
    for i in range(len(five_ret)):
        util_5353.assert_tuple(five_ret[i], 2, '5')
        util_5353.assert_str(five_ret[i][0], '5')
        util_5353.assert_str(five_ret[i][1], '5')

    print('::: Problem 6 :::')
    six_ret = conditions_by_age(pt_filter)
    util_5353.assert_tuple(six_ret, 2, '6')
    util_5353.assert_list(six_ret[0], 10, '6')
    util_5353.assert_list(six_ret[1], 10, '6')
    for i in range(len(six_ret[0])):
        util_5353.assert_str(six_ret[0][i], '6')
        util_5353.assert_str(six_ret[1][i], '6')

    print('::: Problem 7 :::')
    seven_ret = medications_by_gender(pt_filter)
    util_5353.assert_tuple(seven_ret, 2, '6')
    util_5353.assert_list(seven_ret[0], 10, '6')
    util_5353.assert_list(seven_ret[1], 10, '6')
    for i in range(len(seven_ret[0])):
        util_5353.assert_str(seven_ret[0][i], '6')
        util_5353.assert_str(seven_ret[1][i], '6')

    print('::: Problem 8 :::')
    eight_ret = bp_stats(pt_filter)
    util_5353.assert_list(eight_ret, 3, '8')
    for i in range(len(eight_ret)):
        util_5353.assert_dict(eight_ret[i], '8')
        util_5353.assert_dict_key(eight_ret[i], 'min', '8')
        util_5353.assert_dict_key(eight_ret[i], 'max', '8')
        util_5353.assert_dict_key(eight_ret[i], 'median', '8')
        util_5353.assert_dict_key(eight_ret[i], 'mean', '8')
        util_5353.assert_dict_key(eight_ret[i], 'stddev', '8')

    print('~~~ All Tests Pass ~~~')
