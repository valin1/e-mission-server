from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import emission.storage.timeseries.abstract_timeseries as esta
import emission.net.ext_service.push.notify_usage as pnu
import emission.core.wrapper.user as ecwu
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import logging.config
import argparse
import pandas as pd
import requests
import json
import re
import emission.core.get_database as edb
from uuid import UUID


def handle_insert(tripDict, tripID, collection, uuid):
    if tripDict == None:
        collection.insert_one({'uuid': uuid, 'trip_id': tripID})
        return True
    else:
        if tripDict['trip_id'] != tripID:
            collection.update_one({'uuid': uuid}, {'$set': {'trip_id' : tripID}})
            return True
        else:
            return False

def calculate_single_yelp_suggestion(UUID):
	logging.debug("About to calculate single suggestion for %s" % UUID)
	yelp_suggestion_trips = edb.get_yelp_db()
    all_users = pd.DataFrame()
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_trips = time_series.get_data_df("analysis/cleaned_trip", time_query = None)
    num_cleaned_trips = len(cleaned_trips)
    for i in range(num_cleaned_trips-1, -1, -1):
        if cleaned_trips.iloc[i]["end_ts"] - cleaned_trips.iloc[i]["start_ts"] < 5*60:
            continue
        distance_in_miles = cleaned_trips.iloc[i]["distance"]*0.000621371
        trip_id = cleaned_sections.iloc[i]["trip_id"]
        #Still need to add the database to log suggestion trips

def push_to_user(uuid_list, message):
    logging.debug("About to send notifications to: %s users" % len(uuid_list))
    json_data = {
        "title": "GreenTrip Notification",
        "message": message
    }
    logging.debug(uuid_list)
    response = pnu.send_visible_notification_to_users(uuid_list,
                                                        json_data["title"],
                                                        json_data["message"],
                                                        json_data,
                                                        dev = False)
    pnu.display_response(response)