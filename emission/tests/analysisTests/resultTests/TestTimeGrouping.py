# Standard imports
import unittest
import logging
import pandas as pd
import uuid
import arrow
import dateutil.tz as tz

# Our imports
import emission.core.get_database as edb
import emission.tests.common as etc

import emission.analysis.result.metrics.time_grouping as earmt
import emission.analysis.result.metrics.simple_metrics as earmts

import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.section as ecws
import emission.core.wrapper.modestattimesummary as ecwms
import emission.core.wrapper.motionactivity as ecwm

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.local_date_queries as esdl

PST = "America/Los_Angeles"
EST = "America/New_York"
IST = "Asia/Calcutta"
BST = "Europe/London"

class TestTimeGrouping(unittest.TestCase):
    def setUp(self):
        self.testUUID = uuid.uuid4()
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)

    def tearDown(self):
        edb.get_analysis_timeseries_db().remove({'user_id': self.testUUID})

    def testLocalGroupBy(self):
        self.assertEqual(earmt._get_local_group_by(earmt.LocalFreq.DAILY),
                         ['start_local_dt_year', 'start_local_dt_month',
                          'start_local_dt_day'])
        self.assertEqual(earmt._get_local_group_by(earmt.LocalFreq.YEARLY),
                         ['start_local_dt_year'])
        with self.assertRaises(AssertionError):
            earmt._get_local_group_by("W")

    def testLocalKeyToFillFunction(self):
        self.assertEqual(earmt._get_local_key_to_fill_fn(earmt.LocalFreq.DAILY),
                         earmt.local_dt_fill_times_daily)
        self.assertEqual(earmt._get_local_key_to_fill_fn(earmt.LocalFreq.YEARLY),
                         earmt.local_dt_fill_times_yearly)
        with self.assertRaises(AssertionError):
            earmt._get_local_key_to_fill_fn("W")

    def _createTestSection(self, start_ardt, start_timezone):
        section = ecws.Section()
        self._fillDates(section, "start_", start_ardt, start_timezone)
        # Hackily fill in the end with the same values as the start
        # so that the field exists
        # in cases where the end is important (mainly for range timezone
        # calculation with local times), it can be overridden using _fillDates
        # from the test case
        self._fillDates(section, "end_", start_ardt, start_timezone)
        logging.debug("created section %s" % (section.start_fmt_time))

        entry = ecwe.Entry.create_entry(self.testUUID, esda.CLEANED_SECTION_KEY,
                                        section, create_id=True)
        self.ts.insert(entry)
        return entry

    def _fillDates(self, object, prefix, ardt, timezone):
        object["%sts" % prefix] = ardt.timestamp
        object["%slocal_dt" % prefix] = esdl.get_local_date(ardt.timestamp,
                                                     timezone)
        object["%sfmt_time" % prefix] = ardt.to(timezone).isoformat()
        logging.debug("After filling entries, keys are %s" % object.keys())
        return object

    def testLocalDtFillTimesDailyOneTz(self):
        key = (2016, 5, 3)
        test_section_list = []
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,6, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,10, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,23, tzinfo=tz.gettz(PST)),
                                    PST))

        section_group_df = pd.DataFrame(
            [self.ts._to_df_entry(s) for s in test_section_list])
        logging.debug("First row of section_group_df = %s" % section_group_df.iloc[0])
        self.assertEqual(earmt._get_tz(section_group_df), PST)

        ms = ecwms.ModeStatTimeSummary()
        earmt.local_dt_fill_times_daily(key, section_group_df, ms)
        logging.debug("before starting checks, ms = %s" % ms)
        self.assertEqual(ms.ts, 1462345199)
        self.assertEqual(ms.local_dt.day, 3)
        self.assertEqual(ms.local_dt.timezone, PST)

    def testLocalDtFillTimesDailyMultiTzGoingWest(self):
        key = (2016, 5, 3)
        test_section_list = []
        # This is perhaps an extreme use case, but it is actually a fairly
        # common one with air travel

        # Step 1: user leaves Delhi at 1am on the 3rd for JFK on the non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,1, tzinfo=tz.gettz(IST)),
                                    IST))
        # non-stop takes 15 hours, so she arrives in New York at 16:00 IST = 6:30am EDT
        # (taking into account the time difference)

        # Step 2: user leaves JFK for SFO at 7am EST on a non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,7, tzinfo=tz.gettz(EST)),
                                    EST))

        # cross-country flight takes 8 hours, so she arrives in SFO at 15:00 EDT
        # = 12:00 PDT
        test_section_list[1]['data'] = self._fillDates(test_section_list[1].data, "end_",
                        arrow.Arrow(2016,5,3,15,tzinfo=tz.gettz(EST)),
                        PST)

        # Step 2: user starts a trip out of SFO a midnight of the 4th PST
        # (earliest possible trip)
        # for our timestamp algo to be correct, this has to be after the
        # timestamp for the range
        next_day_first_trip = self._createTestSection(
            arrow.Arrow(2016,5,4,0, tzinfo=tz.gettz(PST)),
                                    PST)

        section_group_df = pd.DataFrame(
            [self.ts._to_df_entry(s) for s in test_section_list])

        # Timestamps are monotonically increasing
        self.assertEqual(section_group_df.start_ts.tolist(),
                         [1462217400, 1462273200])
        self.assertEqual(next_day_first_trip.data.start_ts, 1462345200)

        # The timezone for the end time is EST since that's where we ended
        # the last trip
        self.assertEqual(earmt._get_tz(section_group_df), PST)

        ms = ecwms.ModeStatTimeSummary()
        earmt.local_dt_fill_times_daily(key, section_group_df, ms)
        logging.debug("before starting checks, ms = %s" % ms)

        # The end of the period is the end of the day in PST. So that we can
        # capture trip home from the airport, etc.
        # The next trip must start from the same timezone
        # if a trip straddles two timezones, we need to decide how the metrics
        # are split. A similar issue occurs when the trip straddles two days.
        # We have arbitrarily decided to bucket by start_time, so we follow the
        # same logic and bucket by the timezone of the start time.
        #
        # So the bucket for this day ends at the end of the day in EDT.
        # If we included any trips after noon in SF, e.g. going home from the
        # aiport, then it would extend to midnight PDT.
        #
        # The main argument that I'm trying to articulate is that we need to
        # come up with a notion of when the bucket ended. To some extent, we can
        # set this arbitrarily between the end of the last trip on the 3rd and the
        # and the start of the first trip on the 4th.
        #
        # Picking midnight on the timezone of the last trip on the 3rd is
        # reasonable since we know that no trips have started since the last
        # trip on the 3rd to the midnight of the 3rd EST.

        # So the worry here is that the first trip on the next day may be on
        # next day in the end timezone of the trip but on the same day in the
        # start timezone of the trip
        # e.g. reverse trip
        # maybe using the end of the section is best after all

        self.assertEqual(ms.ts, 1462345199)
        self.assertEqual(ms.local_dt.day, 3)
        self.assertEqual(ms.local_dt.timezone, PST)
        self.assertGreater(next_day_first_trip.data.start_ts, ms.ts)

    def testLocalDtFillTimesDailyMultiTzGoingEast(self):
        key = (2016, 5, 3)
        test_section_list = []
        # This is perhaps an extreme use case, but it is actually a fairly
        # common one with air travel

        # Step 1: user leaves SFO at 1am on the 3rd for JFK on a cross-country flight
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,1, tzinfo=tz.gettz(PST)),
                                    PST))
        # cross-country takes 8 hours, so she arrives in New York at 9:00 IST = 12:00am EDT
        # (taking into account the time difference)
        test_section_list[0]['data'] = self._fillDates(test_section_list[0].data, "end_",
                        arrow.Arrow(2016,5,3,9,tzinfo=tz.gettz(PST)),
                        EST)

        # Step 2: user leaves JFK for LHR at 1pm EST.
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,13, tzinfo=tz.gettz(EST)),
                                    EST))

        # cross-atlantic flight takes 7 hours, so she arrives at LHR at 8:00pm EDT
        # = 2am on the 4th local time
        test_section_list[1]['data'] = self._fillDates(test_section_list[1].data, "end_",
                        arrow.Arrow(2016,5,3,21,tzinfo=tz.gettz(EST)),
                        BST)

        # Then, she catches the train from the airport to her hotel in London
        # at 3am local time = 9:00pm EST
        # So as per local time, this is a new trip
        #
        # This clearly indicates why we need to use the timezone of the end of
        # last section to generate the timestamp for the range. If we use the
        # timezone of the beginning of the trip, we will say that the range ends
        # at midnight EST. But then it should include the next_day_first_trip,
        # which starts at 9pm EST, but it does not.
        # So we should use midnight BST instead. Note that midnight BST was
        # actually during the trip, but then it is no different from a regular
        # trip (in one timezone) where the trip spans the date change
        next_day_first_trip = self._createTestSection(
            arrow.Arrow(2016,5,4,3, tzinfo=tz.gettz(BST)),
            BST)

        section_group_df = pd.DataFrame(
            [self.ts._to_df_entry(s) for s in test_section_list])

        logging.debug("first row is %s" % section_group_df.loc[0])

        # Timestamps are monotonically increasing
        self.assertEqual(section_group_df.start_ts.tolist(),
                         [1462262400, 1462294800])

        # The timezone for the end time is EST since that's where we started
        # the last trip from
        self.assertEqual(earmt._get_tz(section_group_df), BST)

        ms = ecwms.ModeStatTimeSummary()
        earmt.local_dt_fill_times_daily(key, section_group_df, ms)
        logging.debug("before starting checks, ms = %s" % ms)

        self.assertEqual(ms.ts, 1462316399)
        self.assertEqual(ms.local_dt.day, 3)
        self.assertEqual(ms.local_dt.timezone, BST)

        # This test fails if it is not BST
        self.assertGreater(next_day_first_trip.data.start_ts, ms.ts)

    # I am not testing testGroupedToSummaryTime because in order to generate the
    # time grouped df, I need to group the sections anyway, and then I might as we

    def _fillModeDistanceDuration(self, section_list):
        for i, s in enumerate(section_list):
            dw = s.data
            dw.sensed_mode = ecwm.MotionTypes.BICYCLING
            dw.duration = (i + 1) * 100
            dw.distance = (i + 1) * 1000
            s['data'] = dw
            self.ts.update(s)

    def testGroupedByTimestamp(self):
        key = (2016, 5, 3)
        test_section_list = []
        # This is perhaps an extreme use case, but it is actually a fairly
        # common one with air travel

        # Step 1: user leaves Delhi at 1am on the 3rd for JFK on the non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,1, tzinfo=tz.gettz(IST)),
                                    IST))
        # non-stop takes 15 hours, so she arrives in New York at 16:00 IST = 6:30am EDT
        # (taking into account the time difference)

        # Step 2: user leaves JFK for SFO at 7am EST on a non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,7, tzinfo=tz.gettz(EST)),
                                    EST))

        self._fillModeDistanceDuration(test_section_list)
        logging.debug("durations = %s" %
                      [s.data.duration for s in test_section_list])

        summary = earmt.group_by_timestamp(self.testUUID,
                                           arrow.Arrow(2016,5,1).timestamp,
                                           arrow.Arrow(2016,6,1).timestamp,
                                           'd', earmts.get_count)

        logging.debug(summary)

        self.assertEqual(len(summary), 2) # spans two days in UTC
        self.assertEqual(summary[0].BICYCLING, 1) # trip leaving India
        self.assertEqual(summary[0].local_dt.day, 2) # because it is the second in UTC
        self.assertEqual(summary[0].local_dt.hour, 0) # because it is the second in UTC
        self.assertEqual(summary[0].ts, 1462147200) # timestamp for






if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
