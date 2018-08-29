import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import requests
import json
import logging
import re
import emission.core.get_database as edb
from __future__ import print_function
import argparse
import pprint
from datetime import datetime
from uuid import UUID
ACCESS_TOKEN = 'AIzaSyAbnpsty2SAzEX9s1VVIdh5pTHUPMjn3lQ' #GOOGLE MAPS ACCESS TOKEN
JACK_TOKEN = 'AIzaSyAXG_8bZvAAACChc26JC6SFzhuWysRqQPo'
#YELP API ACCESS KEY
YELP_API_KEY = 'jBC0box-WQr7jvQvXlI9sJuw17wfN9AYFMnu5ebxsYkgQoKTjjIRD0I_tAePUasbaIbXj28cmj4nUBDHrVxtrfHU2l6TM4E61Kk3EVeSbLZsxStLxkAVlkHK9xJ6W3Yx'
NOM_TOKEN = 
# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode

#S2: If user, found a higher reviewed restaurant closer to the trip's initial point, then suggest that new restaurant. 

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/' 

def request_yelp(host, path, api_key, url_params=None):
    url_params =  url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }
    print(u'Querying {0} ...'.format(url))
    response = requests.request('GET', url, headers=headers, params=url_params)
    return response.json()

def search(api_key, term, location):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request_yelp(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

def business_reviews(api_key, business_id):
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)

def query_api(term, location):
    response = search(API_KEY, term, location)

    businesses = response.get('businesses')

    if not businesses:
        print(u'No businesses for {0} in {1} found.'.format(term, location))
        return

    business_id = businesses[0]['id']

    print(u'{0} businesses found, querying business info ' \
        'for the top result "{1}" ...'.format(
            len(businesses), business_id))
    response = get_business(API_KEY, business_id)

    print(u'Result for business "{0}" found:'.format(business_id))
    pprint.pprint(response, indent=2)


#Obtain business name through Yelp's API
def get_business_id(api_key, lat, lon):
    url_params = {
        'location': lat + ',' + lon
    }
    #Very broad, latitudes and longitudes given, cannot exactly pinpoint the exact location
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

def review_start_loc(location = '0,0'):
    try:
        #Off at times if the latlons are of a location that takes up a small spot, especially boba shops
        business_name, city = return_address_from_location(location)
        #print(business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city))
        return business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city)['rating']
    except:
        try:
            #This EXCEPT part may error, because it grabs a list of businesses instead of matching the address to a business
            address = return_address_from_location(location)
            return match_business_address(address)
        except:
            raise ValueError("Something went wrong")

def category_of_business(location = '0,0'):

#Send Shankari code snippet
#Write script to find average amount of trips per user
#Random 5 business matches


#Calculate the distance and reviews, before outputting a suggestion. 

def calculate_yelp_suggestion(uuid):
    return_obj = { 'message': "Good job on choosing an environmentally closer location! No suggestion to show.",
    'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
    'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/inferred_section", time_query = None)
    for i in range(len(cleaned_sections) - 1, -1, -1):
        counter -= 1
    #Check if businesses are of the same category. 
        start_loc = cleaned_sections.iloc[i]["start_loc"]["coordinates"]
        start_lat = str(start_loc[0])
        start_lon = str(start_loc[1])
        end_loc = cleaned_sections.iloc[i]["end_loc"]["coordinates"]
        end_lat = str(end_loc[0])
        end_lon = str(end_loc[1])
        distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
"""
Updated return address from location FUNCTION to prevent any conflicts, from previous semester's code on their suggestion 
mode, added another RETURN parameter (returns the business name and location), makes it easier for Yelp's API to search
for businesses
"""
def updated_return_address_from_location (location='0,0'):
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
    latlng = 'latlng=' + location
    try:
        #This try block is for our first 150,000 requests. If we exceed this, use Jack's Token.
        key_string = '&key=' + ACCESS_TOKEN
        url = base_url + latlng + key_string #Builds the url
        result = requests.get(url).json() #Gets google maps json file
        cleaned = result['results'][0]['address_components']
        #Address to check against value of check_against_business_location
        chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
        business_tuple = check_against_business_location(location, chk)
        if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
            return business_tuple[1], cleaned[3]['short_name']
        else: #otherwise, we just return the address
            return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
    except:
        try:
            #Use Jack's Token in case of some invalid request problem with other API Token
            key_string = '&key=' + JACK_TOKEN
            url = base_url + latlng + key_string #Builds the url
            result = requests.get(url).json() #Gets google maps json file
            cleaned = result['results'][0]['address_components']
            #Address to check against value of check_against_business_location
            chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
            business_tuple = check_against_business_location(location, chk)
            if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
                return business_tuple[1]
            else: #otherwise, we just return the address
                return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
        except:
            raise ValueError("Something went wrong")

def match_business_address(address):
    business_path = SEARCH_PATH
    url_params = {
        'location': address.replace(' ', '+')
    }
    return request(API_HOST, business_path, API_KEY, url_params)
'''
Function to find the review of the original location of the end point of a trip
'''
def review_start_loc(location = '0,0'):
    try:
        #Off at times if the latlons are of a location that takes up a small spot, especially boba shops
        business_name, city = updated_return_address_from_location(location)
        #print(business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city))
        return business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city)['rating']
    except:
        try:
            #This EXCEPT part may error, because it grabs a list of businesses instead of matching the address to a business
            address = updated_return_address_from_location(location)
            return match_business_address(address)
        except:
            raise ValueError("Something went wrong")
    
'''
Function that RETURNS a list of categories that the business falls into
'''
def category_of_business(location = '0,0'):
    try:
        #Off at times if the latlons are of a location that takes up a small spot, especially boba shops
        business_name, city = updated_return_address_from_location(location)
        categories = []
        for c in business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city)['categories']:
            categories.append(c['alias'])
        return categories
    except:
        try:
            address = updated_return_address_from_location(location)
            return match_business_address(address)
        except:
            raise ValueError("Something went wrong")


'''
Function that RETURNS TRUE or FALSE if the categories of the two points match 
'''
def match_category(location0 = '0,0', location1 = '0,0'):
    categories0 = category_of_business(location0)
    categories1 = category_of_business(location1)
    for category in categories0:
        if category in categories1:
            return True
    return False

'''
Test Function without connecting with the server
'''
def calculate_yelp_suggestion(location = '0,0'):
    endpoint = location
    #USING A DUMMY ADDRESS AS PROOF OF CONCEPT IN TERMS OF CALCULATING THE DISTANCE
    dummy = '2700 Hearst Ave, Berkeley, CA'
    #Check for category of the location
    endpoint_categories = category_of_business(location)
    similar_businesses = {}
    business_locations = {}
    city = updated_return_address_from_location(location)[1]
    address = updated_return_address_from_location(location)[2]
    location_review = review_start_loc(location)
    for categor in endpoint_categories:
        queried_bus = search(API_KEY, categor, city)['businesses']
        for q in queried_bus:
            if q['rating'] >= location_review:
                similar_businesses[q['name']] = q['rating']
                #'Coordinates' come out as two elements, latitude and longitude
                business_locations[q['name']] = q['location']



'''
Function that calculates distances between the original starting point and potential business locations, using addresses compared to latitudes and longitudes
'''
def distance(address1, address2):
    address1 = address1.replace(' ', '+')
    address2 = address2.replace(' ', '+')

    url = 'http://www.mapquestapi.com/directions/v2/route?key=' + MAPQUEST_KEY + '&from=' + address1 + '&to=' + address2
    response = requests.get(url)
    return response.json()['route']['distance']





'''
FUNCTION THAT SHOULD BE INCLUDED IN SUGGESTION_SYS.PY
'''
def calculate_yelp_server_suggestion(uuid):
    #Given a single UUID, create a suggestion for them
    return_obj = { 'message': "Good job walking and biking! No suggestion to show.",
    'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
    'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/inferred_section", time_query = None)
    suggestion_trips = edb.get_suggestion_trips_db()
    #Go in reverse order because we check by most recent trip

    if len(cleaned_sections) == 0:
        return_obj['message'] = 'Suggestions will appear once you start taking trips!'
        return return_obj


#S1: If user could've taken a more sustainable transportation route, then suggest that sustainable
#transportation route. 

def return_address_from_location(location='0,0'):
    """
    Creates a Google Maps API call that returns the addresss given a lat, lon
    """
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
    latlng = 'latlng=' + location
    try:
        #This try block is for our first 150,000 requests. If we exceed this, use Jack's Token.
        key_string = '&key=' + ACCESS_TOKEN
        url = base_url + latlng + key_string #Builds the url
        result = requests.get(url).json() #Gets google maps json file
        cleaned = result['results'][0]['address_components']
        #Address to check against value of check_against_business_location
        chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
        business_tuple = check_against_business_location(location, chk)
        if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
            return business_tuple[1]
        else: #otherwise, we just return the address
            return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
    except:
        try:
            #Use Jack's Token in case of some invalid request problem with other API Token
            key_string = '&key=' + JACK_TOKEN
            url = base_url + latlng + key_string #Builds the url
            result = requests.get(url).json() #Gets google maps json file
            cleaned = result['results'][0]['address_components']
            #Address to check against value of check_against_business_location
            chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
            business_tuple = check_against_business_location(location, chk)
            if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
                return business_tuple[1]
            else: #otherwise, we just return the address
                return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
        except:
            raise ValueError("Something went wrong")

def check_against_business_location(location='0, 0', address = ''):
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    location = 'location=' + location
    try:
        key_string = '&key=' + ACCESS_TOKEN
        radius = '&radius=10'
        url = base_url + location + radius + key_string
        result = requests.get(url).json()
        cleaned = result['results']
        for i in cleaned:
            #If the street address matches the street address of this business, we return a tuple
            #signifying success and the business name
            if address == i['vicinity']:
                return (True, i['name'])
        else:
            return (False, '')
    except:
        try:
            key_string = '&key=' + JACK_TOKEN
            radius = '&radius=10'
            url = base_url + location + radius + key_string
            result = requests.get(url).json()
            cleaned = result['results']
            for i in cleaned:
                if address == i['vicinity']:
                    return (True, i['name'])
            else:
                return (False, '')
        except:
            raise ValueError("Something went wrong")
def insert_into_db(tripDict, tripID, collection, uuid):
    if tripDict == None:
        collection.insert_one({'uuid': uuid, 'trip_id': tripID})
    else:
        if tripDict['trip_id'] != tripID:
            collection.update_one({'uuid': uuid}, {'$set': {'trip_id' : tripID}})
def calculate_single_suggestion(uuid):
    #Given a single UUID, create a suggestion for them
    return_obj = { 'message': "Good job walking and biking! No suggestion to show.",
    'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
    'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/inferred_section", time_query = None)
    suggestion_trips = edb.get_suggestion_trips_db()
    #Go in reverse order because we check by most recent trip
    counter = 40
    if len(cleaned_sections) == 0:
        return_obj['message'] = 'Suggestions will appear once you start taking trips!'
        return return_obj
    for i in range(len(cleaned_sections) - 1, -1, -1):
        counter -= 1
        if counter < 0:
            #Iterate 20 trips back
            return return_obj
        if cleaned_sections.iloc[i]["end_ts"] - cleaned_sections.iloc[i]["start_ts"] < 5 * 60:
            continue
        distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
        mode = cleaned_sections.iloc[i]["sensed_mode"]
        start_loc = cleaned_sections.iloc[i]["start_loc"]["coordinates"]
        start_lat = str(start_loc[0])
        start_lon = str(start_loc[1])
        trip_id = cleaned_sections.iloc[i]['trip_id']
        tripDict = suggestion_trips.find_one({'uuid': uuid})
        print(tripDict)
        end_loc = cleaned_sections.iloc[i]["end_loc"]["coordinates"]
        end_lat = str(end_loc[0])
        end_lon = str(end_loc[1])
        if mode == 5 and distance_in_miles >= 5 and distance_in_miles <= 15:
            logging.debug("15 >= distance >= 5 so I'm considering distance: " + str(distance_in_miles))
            #Suggest bus if it is car and distance between 5 and 15
            default_message = return_obj['message']
            try:
                message = "Try public transportation from " + return_address_from_location(start_lon + "," + start_lat) + \
                " to " + return_address_from_location(end_lon + "," + end_lat) + " (tap me to view)"
                #savings per month, .465 kg co2/mile for car, 0.14323126 kg co2/mile for bus
                savings = str(int(distance_in_miles * 30 * .465 - 0.14323126 * distance_in_miles * 30))
                return {'message' : message, 'savings' : savings, 'start_lat' : start_lat,
                'start_lon' : start_lon, 'end_lat' : end_lat, 'end_lon' : end_lon, 'method': 'public'}
                insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
                break
            except ValueError as e:
                return_obj['message'] = default_message
                continue
        elif (mode == 5 or mode == 3 or mode == 4) and (distance_in_miles < 5 and distance_in_miles >= 1):
            logging.debug("5 > distance >= 1 so I'm considering distance: " + str(distance_in_miles))
            #Suggest bike if it is car/bus/train and distance between 5 and 1
            try:
                message = "Try biking from " + return_address_from_location(start_lon + "," + start_lat) + \
                " to " + return_address_from_location(end_lon + "," + end_lat) + " (tap me to view)"
                savings = str(int(distance_in_miles * 30 * .465))  #savings per month, .465 kg co2/mile
                insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
                return {'message' : message, 'savings' : savings, 'start_lat' : start_lat,
                'start_lon' : start_lon, 'end_lat' : end_lat, 'end_lon' : end_lon, 'method': 'bike'}
                break
            except:
                continue
        elif (mode == 5 or mode == 3 or mode == 4) and (distance_in_miles < 1):
            logging.debug("1 > distance so I'm considering distance: " + str(distance_in_miles))
            #Suggest walking if it is car/bus/train and distance less than 1
            try:
                message = "Try walking/biking from " + return_address_from_location(start_lon + "," + start_lat) + \
                " to " + return_address_from_location(end_lon + "," + end_lat) + " (tap me to view)"
                savings = str(int(distance_in_miles * 30 * .465)) #savings per month, .465 kg co2/mile
                insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
                return {'message' : message, 'savings' : savings, 'start_lat' : start_lat,
                'start_lon' : start_lon, 'end_lat' : end_lat, 'end_lon' : end_lon, 'method': 'walk'}
                break
            except:
                continue
    return return_obj

