from __future__ import print_function
from datetime import datetime
from uuid import UUID
import pandas as pd
import requests
import json
import logging
import re
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import argparse
import pprint



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


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
API_KEY= 'jBC0box-WQr7jvQvXlI9sJuw17wfN9AYFMnu5ebxsYkgQoKTjjIRD0I_tAePUasbaIbXj28cmj4nUBDHrVxtrfHU2l6TM4E61Kk3EVeSbLZsxStLxkAVlkHK9xJ6W3Yx' 
ACCESS_TOKEN = 'AIzaSyAbnpsty2SAzEX9s1VVIdh5pTHUPMjn3lQ' #GOOGLE MAPS ACCESS TOKEN
JACK_TOKEN = 'AIzaSyAXG_8bZvAAACChc26JC6SFzhuWysRqQPo'

MAPQUEST_KEY = 'AuwuGlPC5f3Ru7PGahKAtGcs4WdvARem'
# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 3
LOCATION = '37.871942'

#Helper function to query into Yelp's API
def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()

def search(api_key, term, location):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

def business_reviews(api_key, business_id):
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)

def calculate(json_file):
    return json_file["categories"][0]["title"]

#Not as accurate compared to the below functions
def get_business_id(api_key, lat, lon):
    url_params = {
        'location': lat + ',' + lon
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

#Used first semester's code to obtain the business ID and location in order to find address
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

def return_address_from_location_yelp(location='0,0'):
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
        print(url)
        result = requests.get(url).json() #Gets google maps json file
        cleaned = result['results'][0]['address_components']

        #Address to check against value of check_against_business_location
        chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
        business_tuple = check_against_business_location(location, chk)
        
        if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
            address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name']
            # print(business_tuple[1])
            # print(cleaned[3]['short_name'])
            # print(address_comp)
            return business_tuple[1], cleaned[3]['short_name'], address_comp
        else: #otherwise, we just return the address
            # print(cleaned[0]['long_name'])
            # print(cleaned[1]['short_name'])
            # print(cleaned[3]['short_name'])
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
                address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] 
                # print(address_comp)
                # print(business_tuple[1])
                # print(cleaned[3]['short_name'])
                return business_tuple[1], cleaned[3]['short_name'], address_comp
            else: #otherwise, we just return the address
                # print(cleaned[0]['long_name'])
                # print(cleaned[1]['short_name'])
                # print(cleaned[3]['short_name'])
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
        if (len(return_address_from_location_yelp(location)) == 3):
            business_name, city, address = return_address_from_location_yelp(location)
        #print(business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city))
            return business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city)['rating']
    except:
        try:
            #This EXCEPT part may error, because it grabs a list of businesses instead of matching the address to a business
            address = return_address_from_location_yelp(location)
            return match_business_address(address)
        except:
            raise ValueError("Something went wrong")
    
'''
Function that RETURNS a list of categories that the business falls into
'''
def category_of_business(location = '0,0'):
    try:
        #Off at times if the latlons are of a location that takes up a small spot, especially boba shops
        if (len(return_address_from_location_yelp(location)) == 3):
            business_name, city, address = return_address_from_location_yelp(location)
            categories = []
            for c in business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city)['categories']:
                categories.append(c['alias'])
            return categories
    except:
        try:
            address = return_address_from_location_yelp(location)
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




def distance(address1, address2):
    address1 = address1.replace(' ', '+')
    address2 = address2.replace(' ', '+')

    url = 'http://www.mapquestapi.com/directions/v2/route?key=' + MAPQUEST_KEY + '&from=' + address1 + '&to=' + address2
    response = requests.get(url)
    print(url)
    return response.json()['route']['distance']


def geojson_to_latlon(geojson):
    coordinates = geojson["coordinates"]
    lon = str(coordinates[0])
    lat = str(coordinates[1])
    lat_lon = lat + ',' + lon
    return lat_lon

#New and cleaned up version of yelp-suggestion that detects if 


def calculate_yelp_server_suggestion(uuid):
    #Given a single UUID, create a suggestion for them
    return_obj = { 'message': "Good job walking and biking! No suggestion to show.",
    'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
    'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/cleaned_trip", time_query = None)
    yelp_suggestion_trips = edb.get_yelp_db()
    # print(cleaned_sections)
    #Go in reverse order because we check by most recent trip
    counter = 40
    if len(cleaned_sections) == 0:
        return_obj['message'] = 'Suggestions will appear once you start taking trips!'
        return return_obj
    for i in range(len(cleaned_sections) - 1, -1, -1):
        counter -=1 
        if counter < 0:
            return return_obj
        #NOT QUITE SURE WHAT THIS LINE OF CODE IS SUPPOSED TO DO?
        if cleaned_sections.iloc[i]["end_ts"] - cleaned_sections.iloc[i]["start_ts"] < 5 * 60:
            continue
        #Change distance in meters to miles
        distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
        mode = cleaned_sections.iloc[i]["sensed_mode"]
        start_loc = cleaned_sections.iloc[i]["start_loc"]["coordinates"]
        start_lon = str(start_loc[0])
        start_lat = str(start_loc[1])
        start_lat_lon = start_lat + ',' + start_lon
        trip_id = cleaned_sections.iloc[i]['trip_id']
        tripDict = yelp_suggestion_trips.find_one({'uuid': uuid})
        #print(tripDict)
        end_loc = cleaned_sections.iloc[i]["end_loc"]["coordinates"]
        end_lon = str(end_loc[0])
        end_lat = str(end_loc[1])
        end_lat_lon = end_lat + ',' + end_lon
        print(end_lat_lon)
        endpoint_categories = category_of_business(end_lat_lon)
        business_locations = {}
        if len(return_address_from_location_yelp(start_lat_lon))==1:
            begin_address = return_address_from_location_yelp(start_lat_lon)
        else:
            begin_address = return_address_from_location_yelp(start_lat_lon)[2]
        if len(return_address_from_location_yelp(end_lat_lon)) == 1:
            continue
        city = return_address_from_location_yelp(end_lat_lon)[1]
        address = return_address_from_location_yelp(end_lat_lon)[2]
        #ALREADY CALCULATED BY DISTANCE_IN_MILES
        #comp_distance = distance(dummy, end_lat_lon)
        location_review = review_start_loc(end_lat_lon)
        ratings_bus = {}
        error_message = 'Sorry, unable to retrieve datapoint'
        error_message_categor = 'Sorry, unable to retrieve datapoint because datapoint is a house or datapoint does not belong in service categories'
        if (endpoint_categories):
            for categor in endpoint_categories:
                queried_bus = search(API_KEY, categor, city)['businesses']
                for q in queried_bus:
                    if q['rating'] >= location_review:
                        #'Coordinates' come out as two elements, latitude and longitude
                        ratings_bus[q['name']] = q['rating']
                        obtained = q['location']['display_address'][0] + q['location']['display_address'][1] 
                        obtained.replace(' ', '+')
                        business_locations[q['name']] = obtained
        else: 
            return {'message' : error_message_categor, 'method': 'bike'}
        for a in business_locations:
            calculate_distance = distance(start_lat_lon, business_locations[a])
            #Will check which mode the trip was taking for the integrated calculate yelp suggestion
            if calculate_distance < distance_in_miles and calculate_distance < 5 and calculate_distance >= 1:
                try:
                    message = "Why didn't you bike from " + begin_address + " to " + a + " (tap me to view) " + a + \
                    " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
                    #Not sure to include the amount of carbon saved
                    #Still looking to see what to return with this message, because currently my latitude and longitudes are stacked together in one string
                    insert_into_db(tripDict, trip_id, yelp_suggestion_trips, uuid)
                    return {'message' : message, 'method': 'bike'}

                    #insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
                    break
                except ValueError as e:
                    continue
            elif calculate_distance < distance_in_miles and calculate_distance < 1:
                try: 
                    message = "Why didn't you walk from " + begin_address + " to " + a + " (tap me to view) " + a + \
                    " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
                    insert_into_db(tripDict, trip_id, yelp_suggestion_trips, uuid)
                    return {'message' : message, 'method': 'walk'}
                    break
                except ValueError as e:
                    continue
            elif calculate_distance < distance_in_miles and calculate_distance >= 5 and calculate_distance <= 15:
                try: 
                    message = "Why didn't you check out public transportation from " + begin_address + " to " + a + " (tap me to view) " + a + \
                    " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
                    insert_into_db(tripDict, trip_id, yelp_suggestion_trips, uuid)
                    return {'message' : message, 'method': 'public'}
                    break
                except ValueError as e:
                    continue



#### WORKS WITH YELP_SUGGESTIONS.PY BY ITSELF - IF ANYTHING GOES WRONG WITH THE ABOVE CODE THAT SHOULD BE ABLE TO CONNECT WITH THE SERVER, 
#### LOOK DOWN BELOW



# #Helper function to query into Yelp's API
# def request(host, path, api_key, url_params=None):
#     """Given your API_KEY, send a GET request to the API.
#     Args:
#         host (str): The domain host of the API.
#         path (str): The path of the API after the domain.
#         API_KEY (str): Your API Key.
#         url_params (dict): An optional set of query parameters in the request.
#     Returns:
#         dict: The JSON response from the request.
#     Raises:
#         HTTPError: An error occurs from the HTTP request.
#     """
#     url_params = url_params or {}
#     url = '{0}{1}'.format(host, quote(path.encode('utf8')))
#     headers = {
#         'Authorization': 'Bearer %s' % api_key,
#     }

#     print(u'Querying {0} ...'.format(url))

#     response = requests.request('GET', url, headers=headers, params=url_params)

#     return response.json()

# def search(api_key, term, location):
#     """Query the Search API by a search term and location.
#     Args:
#         term (str): The search term passed to the API.
#         location (str): The search location passed to the API.
#     Returns:
#         dict: The JSON response from the request.
#     """

#     url_params = {
#         'term': term.replace(' ', '+'),
#         'location': location.replace(' ', '+'),
#         'limit': SEARCH_LIMIT
#     }
#     return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

# def business_reviews(api_key, business_id):
#     business_path = BUSINESS_PATH + business_id

#     return request(API_HOST, business_path, api_key)

# def calculate(json_file):
#     return json_file["categories"][0]["title"]

# #Not as accurate compared to the below functions
# def get_business_id(api_key, lat, lon):
#     url_params = {
#         'location': lat + ',' + lon
#     }
#     return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

# #Used first semester's code to obtain the business ID and location in order to find address
# def check_against_business_location(location='0, 0', address = ''):
#     if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
#         raise ValueError('Location Invalid')
#     base_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
#     location = 'location=' + location
#     try:
#         key_string = '&key=' + ACCESS_TOKEN
#         radius = '&radius=10'
#         url = base_url + location + radius + key_string
#         result = requests.get(url).json()
#         cleaned = result['results']
#         for i in cleaned:
#             #If the street address matches the street address of this business, we return a tuple
#             #signifying success and the business name
#             if address == i['vicinity']:
#                 return (True, i['name'])
#         else:
#             return (False, '')
#     except:
#         try:
#             key_string = '&key=' + JACK_TOKEN
#             radius = '&radius=10'
#             url = base_url + location + radius + key_string
#             result = requests.get(url).json()
#             cleaned = result['results']
#             for i in cleaned:
#                 if address == i['vicinity']:
#                     return (True, i['name'])
#             else:
#                 return (False, '')
#         except:
#             raise ValueError("Something went wrong")

# def return_address_from_location_yelp(location='0,0'):
#     """
#     Creates a Google Maps API call that returns the addresss given a lat, lon
#     """
#     if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
#         raise ValueError('Location Invalid')
#     base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
#     latlng = 'latlng=' + location
#     try:
#         #This try block is for our first 150,000 requests. If we exceed this, use Jack's Token.
#         key_string = '&key=' + ACCESS_TOKEN
#         url = base_url + latlng + key_string #Builds the url
#         result = requests.get(url).json() #Gets google maps json file
#         cleaned = result['results'][0]['address_components']
#         #Address to check against value of check_against_business_location
#         chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
#         business_tuple = check_against_business_location(location, chk)
#         if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
#             address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] 
#             return business_tuple[1], cleaned[3]['short_name'], address_comp
#         else: #otherwise, we just return the address
#             return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
#     except:
#         try:
#             #Use Jack's Token in case of some invalid request problem with other API Token
#             key_string = '&key=' + JACK_TOKEN
#             url = base_url + latlng + key_string #Builds the url
#             result = requests.get(url).json() #Gets google maps json file
#             cleaned = result['results'][0]['address_components']
#             #Address to check against value of check_against_business_location
#             chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
#             business_tuple = check_against_business_location(location, chk)
#             if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
#                 address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] 
#                 return business_tuple[1], cleaned[3]['short_name'], address_comp
#             else: #otherwise, we just return the address
#                 return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
#         except:
#             raise ValueError("Something went wrong")

# def match_business_address(address):
#     business_path = SEARCH_PATH
#     url_params = {
#         'location': address.replace(' ', '+')
#     }
#     return request(API_HOST, business_path, API_KEY, url_params)
# '''
# Function to find the review of the original location of the end point of a trip
# '''
# def review_start_loc(location = '0,0'):
#     try:
#         #Off at times if the latlons are of a location that takes up a small spot, especially boba shops
#         business_name, city, address = return_address_from_location_yelp(location)
#         #print(business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city))
#         return business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city)['rating']
#     except:
#         try:
#             #This EXCEPT part may error, because it grabs a list of businesses instead of matching the address to a business
#             address = return_address_from_location_yelp(location)
#             return match_business_address(address)
#         except:
#             raise ValueError("Something went wrong")
    
# '''
# Function that RETURNS a list of categories that the business falls into
# '''
# def category_of_business(location = '0,0'):
#     try:
#         #Off at times if the latlons are of a location that takes up a small spot, especially boba shops
#         business_name, city, address = return_address_from_location_yelp(location)
#         categories = []
#         for c in business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city)['categories']:
#             categories.append(c['alias'])
#         return categories
#     except:
#         try:
#             address = return_address_from_location_yelp(location)
#             return match_business_address(address)
#         except:
#             raise ValueError("Something went wrong")

# '''
# Function that RETURNS TRUE or FALSE if the categories of the two points match 
# '''
# def match_category(location0 = '0,0', location1 = '0,0'):
#     categories0 = category_of_business(location0)
#     categories1 = category_of_business(location1)
#     for category in categories0:
#         if category in categories1:
#             return True
#     return False

# '''
# Test Function without connecting with the server
# '''
# def calculate_yelp_suggestion(location = '0,0'):
#     endpoint = location
#     #USING A DUMMY ADDRESS AS PROOF OF CONCEPT IN TERMS OF CALCULATING THE DISTANCE
#     dummy = '2700 Hearst Ave, Berkeley, CA'
#     #AGAIN PROOF OF CONCEPT, on the integrated function will have the accurate latitudes and longitudes
#     dummy_lat_lon = '0,0'
#     #Check for category of the location
#     endpoint_categories = category_of_business(location)
#     business_locations = {}
#     city = return_address_from_location_yelp(location)[1]
#     address = return_address_from_location_yelp(location)[2]
#     comp_distance = distance(dummy, location)
#     location_review = review_start_loc(location)
#     ratings_bus = {}
#     error_message = 'Sorry, unable to retrieve datapoint'
#     for categor in endpoint_categories:
#         queried_bus = search(API_KEY, categor, city)['businesses']
#         for q in queried_bus:
#             if q['rating'] >= location_review:
#                 #'Coordinates' come out as two elements, latitude and longitude
#                 ratings_bus[q['name']] = q['rating']
#                 obtained = q['location']['display_address'][0] + q['location']['display_address'][1] 
#                 obtained.replace(' ', '+')
#                 business_locations[q['name']] = obtained
#     for a in business_locations:
#         calculate_distance = distance(dummy, business_locations[a])
#         #Will check which mode the trip was taking for the integrated calculate yelp suggestion
#         if calculate_distance < comp_distance and calculate_distance < 5 and calculate_distance >= 1:
#             try:
#                 message = "Why didn't you bike from " + dummy + " to " + a + " (tap me to view) " + a + \
#                 " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
#                 #Not sure to include the amount of carbon saved
#                 #Still looking to see what to return with this message, because currently my latitude and longitudes are stacked together in one string
#                 return {'message' : message, 'method': 'bike'}
#                 #insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
#                 break
#             except ValueError as e:
#                 continue
#         elif calculate_distance < comp_distance and calculate_distance < 1:
#             try: 
#                 message = "Why didn't you walk from " + dummy + " to " + a + " (tap me to view) " + a + \
#                 " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
#                 return {'message' : message, 'method': 'walk'}
#                 break
#             except ValueError as e:
#                 continue
#         elif calculate_distance < comp_distance and calculate_distance >= 5 and calculate_distance <= 15:
#             try: 
#                 message = "Why didn't you check out public transportation from " + dummy + " to " + a + " (tap me to view) " + a + \
#                 " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
#                 return {'message' : message, 'method': 'public'}
#                 break
#             except ValueError as e:
#                 continue
#     return error_message


# def distance(address1, address2):
#     address1 = address1.replace(' ', '+')
#     address2 = address2.replace(' ', '+')

#     url = 'http://www.mapquestapi.com/directions/v2/route?key=' + MAPQUEST_KEY + '&from=' + address1 + '&to=' + address2
#     response = requests.get(url)
#     print(url)
#     return response.json()['route']['distance']
# #Calculate for any businesses around location


# '''
# FUNCTION THAT SHOULD BE INCLUDED IN SUGGESTION_SYS.PY
# '''
# def calculate_yelp_server_suggestion(uuid):
#     #Given a single UUID, create a suggestion for them
#     return_obj = { 'message': "Good job walking and biking! No suggestion to show.",
#     'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
#     'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
#     all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
#     user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
#     time_series = esta.TimeSeries.get_time_series(user_id)
#     cleaned_sections = time_series.get_data_df("analysis/inferred_section", time_query = None)
#     # suggestion_trips = edb.get_suggestion_trips_db()
#     #Go in reverse order because we check by most recent trip
#     counter = 40
#     if len(cleaned_sections) == 0:
#         return_obj['message'] = 'Suggestions will appear once you start taking trips!'
#         return return_obj
#     for i in range(len(cleaned_sections) - 1, -1, -1):
#         counter -=1 
#         if counter < 0:
#             return return_obj
#         #NOT QUITE SURE WHAT THIS LINE OF CODE IS SUPPOSED TO DO?
#         if cleaned_sections.iloc[i]["end_ts"] - cleaned_sections.iloc[i]["start_ts"] < 5 * 60:
#             continue
#         #Change distance in meters to miles
#         distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
#         mode = cleaned_sections.iloc[i]["sensed_mode"]
#         start_loc = cleaned_sections.iloc[i]["start_loc"]["coordinates"]
#         start_lon = str(start_loc[0])
#         start_lat = str(start_loc[1])
#         start_lat_lon = start_lat + ',' + start_lon
#         trip_id = cleaned_sections.iloc[i]['trip_id']
#         # tripDict = suggestion_trips.find_one({'uuid': uuid})
#         # print(tripDict)
#         end_loc = cleaned_sections.iloc[i]["end_loc"]["coordinates"]
#         end_lon = str(end_loc[0])
#         end_lat = str(end_loc[1])
#         end_lat_lon = end_lat + ',' + end_lon
#         endpoint_categories = category_of_business(end_lat_lon)
#         business_locations = {}
#         begin_address = return_address_from_location_yelp(start_lat_lon)[2]
#         city = return_address_from_location_yelp(end_lat_lon)[1]
#         address = return_address_from_location_yelp(end_lat_lon)[2]
#         #ALREADY CALCULATED BY DISTANCE_IN_MILES
#         #comp_distance = distance(dummy, end_lat_lon)
#         location_review = review_start_loc(end_lat_lon)
#         ratings_bus = {}
#         error_message = 'Sorry, unable to retrieve datapoint'
#         for categor in endpoint_categories:
#             queried_bus = search(API_KEY, categor, city)['businesses']
#             for q in queried_bus:
#                 if q['rating'] >= location_review:
#                     #'Coordinates' come out as two elements, latitude and longitude
#                     ratings_bus[q['name']] = q['rating']
#                     obtained = q['location']['display_address'][0] + q['location']['display_address'][1] 
#                     obtained.replace(' ', '+')
#                     business_locations[q['name']] = obtained
#         for a in business_locations:
#             calculate_distance = distance(start_lat_lon, business_locations[a])
#             #Will check which mode the trip was taking for the integrated calculate yelp suggestion
#             if calculate_distance < distance_in_miles and calculate_distance < 5 and calculate_distance >= 1:
#                 try:
#                     message = "Why didn't you bike from " + begin_address + " to " + a + " (tap me to view) " + a + \
#                     " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
#                     #Not sure to include the amount of carbon saved
#                     #Still looking to see what to return with this message, because currently my latitude and longitudes are stacked together in one string
#                     return {'message' : message, 'method': 'bike'}
#                     #insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
#                     break
#                 except ValueError as e:
#                     continue
#             elif calculate_distance < distance_in_miles and calculate_distance < 1:
#                 try: 
#                     message = "Why didn't you walk from " + begin_address + " to " + a + " (tap me to view) " + a + \
#                     " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
#                     return {'message' : message, 'method': 'walk'}
#                     break
#                 except ValueError as e:
#                     continue
#             elif calculate_distance < distance_in_miles and calculate_distance >= 5 and calculate_distance <= 15:
#                 try: 
#                     message = "Why didn't you check out public transportation from " + begin_address + " to " + a + " (tap me to view) " + a + \
#                     " has better reviews, closer to your original starting point, and has a rating of " + str(ratings_bus[a])
#                     return {'message' : message, 'method': 'public'}
#                     break
#                 except ValueError as e:
#                     continue
# all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
# print(all_users)
# calculate_yelp_server_suggestion(UUID('e874c5be-914a-4f29-844b-5e2fe51c1c32'))

#########################################################################################################
#SEMESTER 1: If user could've taken a more sustainable transportation route, then suggest that sustainable
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

