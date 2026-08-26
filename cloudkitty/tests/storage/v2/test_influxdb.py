# Copyright 2019 Objectif Libre
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
import collections
import copy
from datetime import datetime
from datetime import timedelta
import unittest
from unittest import mock

from dateutil import tz

from cloudkitty import dataframe
from cloudkitty.storage.v2 import influx
from cloudkitty.tests import TestCase
from cloudkitty.utils import tz as tzutils


class TestInfluxDBStorage(TestCase):

    def setUp(self):
        super(TestInfluxDBStorage, self).setUp()
        self.point = {
            'type': 'amazing_type',
            'unit': 'banana',
            'qty': 42,
            'price': 1.0,
            'groupby': 'one|two',
            'metadata': '1|2',
            'one': '1',
            'two': '2',
            '1': 'one',
            '2': 'two',
            'time': datetime(2019, 1, 1, tzinfo=tz.tzutc()).isoformat(),
        }

    def test_point_to_dataframe_entry_valid_point(self):
        self.assertEqual(
            influx.InfluxStorage._point_to_dataframe_entry(self.point),
            dataframe.DataPoint(
                'banana',
                42,
                1,
                {'one': '1', 'two': '2'},
                {'1': 'one', '2': 'two'},
            ),
        )

    def test_point_to_dataframe_entry_invalid_groupby_metadata(self):
        point = copy.deepcopy(self.point)
        point['groupby'] = 'a'
        point['metadata'] = None
        self.assertEqual(
            influx.InfluxStorage._point_to_dataframe_entry(point),
            dataframe.DataPoint(
                'banana',
                42,
                1,
                {'a': ''},
                {},
            ),
        )

    def test_build_dataframes_differenciates_periods(self):
        points = [copy.deepcopy(self.point) for _ in range(3)]
        for idx, point in enumerate(points):
            point[influx.PERIOD_FIELD_NAME] = 100 * (idx + 1)

        dataframes = influx.InfluxStorage()._build_dataframes(points)
        self.assertEqual(len(dataframes), 3)

        for idx, frame in enumerate(dataframes):
            self.assertEqual(
                frame.start, datetime(2019, 1, 1, tzinfo=tz.tzutc()))
            delta = timedelta(seconds=(idx + 1) * 100)
            self.assertEqual(frame.end,
                             datetime(2019, 1, 1, tzinfo=tz.tzutc()) + delta)
            typelist = list(frame.itertypes())
            self.assertEqual(len(typelist), 1)
            type_, points = typelist[0]
            self.assertEqual(len(points), 1)
            self.assertEqual(type_, 'amazing_type')


class FakeResultSet(object):
    def __init__(self, points=[], items=[]):
        self._points = points
        self._items = items

    def get_points(self):
        return self._points

    def items(self):
        return self._items


class TestInfluxClient(unittest.TestCase):
    def setUp(self):
        self.period_begin = tzutils.local_to_utc(
            tzutils.get_month_start()).isoformat()
        self.period_end = tzutils.local_to_utc(
            tzutils.get_next_month()).isoformat()
        self.client = influx.InfluxClient()
        self._storage = influx.InfluxStorage()

    def test_get_filter_query(self):
        filters = collections.OrderedDict(
            (('str_filter', 'one'), ('float_filter', 2.0)))
        self.assertEqual(
            self.client._get_filter_query(filters),
            """ AND "str_filter"='one' AND "float_filter"=2.0"""
        )

    def test_get_filter_query_no_filters(self):
        self.assertEqual(self.client._get_filter_query({}), '')

    def test_retrieve_format_with_pagination(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        m.return_value = (FakeResultSet(), FakeResultSet())

        self._storage.retrieve()
        m.assert_called_once_with(
            "SELECT COUNT(groupby) FROM \"dataframes\""
            " WHERE time >= '{0}'"
            " AND time < '{1}';"
            "SELECT * FROM \"dataframes\""
            " WHERE time >= '{0}'"
            " AND time < '{1}'"
            " LIMIT 1000 OFFSET 0;".format(
                self.period_begin, self.period_end,
            ))

    def test_retrieve_format_with_types(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        m.return_value = (FakeResultSet(), FakeResultSet())

        self._storage.retrieve(metric_types=['foo', 'bar'])
        m.assert_called_once_with(
            "SELECT COUNT(groupby) FROM \"dataframes\""
            " WHERE time >= '{0}'"
            " AND time < '{1}'"
            " AND (\"type\"='foo' OR \"type\"='bar');"
            "SELECT * FROM \"dataframes\""
            " WHERE time >= '{0}'"
            " AND time < '{1}'"
            " AND (\"type\"='foo' OR \"type\"='bar')"
            " LIMIT 1000 OFFSET 0;".format(
                self.period_begin, self.period_end,
            ))

    def test_delete_no_parameters(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        self._storage.delete()
        m.assert_called_once_with('DELETE FROM "dataframes";')

    def test_delete_begin_end(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        self._storage.delete(begin=datetime(2019, 1, 1),
                             end=datetime(2019, 1, 2))
        m.assert_called_once_with(
            """DELETE FROM "dataframes" WHERE time >= '2019-01-01T00:00:00'"""
            """ AND time < '2019-01-02T00:00:00';""")

    def test_delete_begin_end_filters(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        self._storage.delete(
            begin=datetime(2019, 1, 1), end=datetime(2019, 1, 2),
            filters={'project_id': 'foobar'})
        m.assert_called_once_with(
            """DELETE FROM "dataframes" WHERE time >= '2019-01-01T00:00:00'"""
            """ AND time < '2019-01-02T00:00:00' AND "project_id"='foobar';"""
        )

    def test_delete_end_filters(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        self._storage.delete(end=datetime(2019, 1, 2),
                             filters={'project_id': 'foobar'})
        m.assert_called_once_with(
            """DELETE FROM "dataframes" WHERE time < '2019-01-02T00:00:00' """
            """AND "project_id"='foobar';""")

    def test_delete_begin_filters(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        self._storage.delete(begin=datetime(2019, 1, 2),
                             filters={'project_id': 'foobar'})
        m.assert_called_once_with(
            """DELETE FROM "dataframes" WHERE time >= '2019-01-02T00:00:00'"""
            """ AND "project_id"='foobar';""")

    def test_delete_begin(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        self._storage.delete(begin=datetime(2019, 1, 2))
        m.assert_called_once_with("""DELETE FROM "dataframes" WHERE """
                                  """time >= '2019-01-02T00:00:00';""")

    def test_delete_end(self):
        self._storage._conn._conn.query = m = mock.MagicMock()
        self._storage.delete(end=datetime(2019, 1, 2))
        m.assert_called_once_with("""DELETE FROM "dataframes" WHERE """
                                  """time < '2019-01-02T00:00:00';""")

    def test_process_total(self):
        begin = datetime(2019, 1, 2, 10)
        end = datetime(2019, 1, 2, 11)
        groupby = ['valA', 'time']
        points_1 = [
            {
                'qty': 42,
                'price': 1.0,
                'time': begin.isoformat()
            }
        ]
        series_groupby_1 = {
            'valA': '1'
        }
        points_2 = [
            {
                'qty': 12,
                'price': 2.0,
                'time': begin.isoformat()
            }
        ]
        series_groupby_2 = {
            'valA': '2'
        }
        points_3 = [
            {
                'qty': None,
                'price': None,
                'time': None
            }
        ]
        series_groupby_3 = {
            'valA': None
        }
        series_name = 'dataframes'
        items = [((series_name, series_groupby_1), points_1),
                 ((series_name, series_groupby_2), points_2),
                 ((series_name, series_groupby_3), points_3)]
        total = FakeResultSet(items=items)
        result = self.client.process_total(total=total, begin=begin, end=end,
                                           groupby=groupby)
        expected = [{'begin': tzutils.utc_to_local(begin),
                     'end': tzutils.utc_to_local(end),
                     'qty': 42,
                     'price': 1.0,
                     'valA': '1'},
                    {'begin': tzutils.utc_to_local(begin),
                     'end': tzutils.utc_to_local(end),
                     'qty': 12,
                     'price': 2.0,
                     'valA': '2'}
                    ]
        self.assertEqual(expected, result)


class TestInfluxClientV2(unittest.TestCase):

    @mock.patch('cloudkitty.storage.v2.influx.InfluxDBClient')
    def setUp(self, client_mock):
        self.period_begin = tzutils.local_to_utc(
            tzutils.get_month_start())
        self.period_end = tzutils.local_to_utc(
            tzutils.get_next_month())
        self.client = influx.InfluxClientV2()

    @mock.patch('cloudkitty.storage.v2.influx.requests')
    def test_query(self, mock_request):
        static_vals = ['', 'result', 'table', '_start', '_value']
        custom_fields = 'last(f1) AS f1, last(f2) AS f2, last(f3) AS f3'
        groups = ['g1', 'g2', 'g3']
        data = [
            static_vals + groups,
            ['', 'f1', 0, 1, 1, 1, 2, 3],
            ['', 'f2', 0, 1, 2, 1, 2, 3],
            ['', 'f3', 0, 1, 3, 1, 2, 3],
            static_vals + groups,
            ['', 'f1', 0, 1, 3, 3, 1, 2],
            ['', 'f2', 0, 1, 1, 3, 1, 2],
            ['', 'f3', 0, 1, 2, 3, 1, 2],
            static_vals + groups,
            ['', 'f1', 0, 1, 2, 2, 3, 1],
            ['', 'f2', 0, 1, 3, 2, 3, 1],
            ['', 'f3', 0, 1, 1, 2, 3, 1]
        ]

        expected_value = [
            {'f1': 1.0, 'f2': 2.0, 'f3': 3.0, 'begin': self.period_begin,
             'end': self.period_end, 'g1': '1', 'g2': '2', 'g3': '3'},
            {'f1': 3.0, 'f2': 1.0, 'f3': 2.0, 'begin': self.period_begin,
             'end': self.period_end, 'g1': '3', 'g2': '1', 'g3': '2'},
            {'f1': 2.0, 'f2': 3.0, 'f3': 1.0, 'begin': self.period_begin,
             'end': self.period_end, 'g1': '2', 'g2': '3', 'g3': '1'}
        ]

        data_csv = '\n'.join([','.join(map(str, d)) for d in data])
        mock_request.post.return_value = mock.Mock(text=data_csv)
        response = self.client.get_total(
            None, self.period_begin, self.period_end, custom_fields,
            filters={}, groupby=groups)

        result = self.client.process_total(
            response, self.period_begin, self.period_end,
            groups, custom_fields, {})

        self.assertEqual(result, expected_value)

    def test_query_build(self):
        custom_fields = 'last(field1) AS F1, sum(field2) AS F2'
        groupby = ['group1', 'group2', 'group3']
        filters = {
            'filter1': '10',
            'filter2': 'filter2_filter'
        }
        beg = self.period_begin.isoformat()
        end = self.period_end.isoformat()
        expected = ('\n'
                    '        from(bucket:"cloudkitty")\n'
                    f'            |> range(start: {beg}, stop: {end})\n'
                    '            |> filter(fn: (r) => r["_measurement"] == '
                    '"dataframes")\n'
                    '            |> filter(fn: (r) => r["_field"] == "field1"'
                    '  and r.filter1==10 and r.filter2=="filter2_filter" )\n'
                    '            |> group(columns: ["group1","group2",'
                    '"group3"])\n'
                    '            |> last()\n'
                    '            |> keep(columns: ["group1", "group2",'
                    ' "group3", "_field", "_value", "_start", "_stop"])\n'
                    '            |> set(key: "_field", value: "F1")\n'
                    '            |> yield(name: "F1")\n'
                    '        \n'
                    '        from(bucket:"cloudkitty")\n'
                    f'            |> range(start: {beg}, stop: {end})\n'
                    '            |> filter(fn: (r) => r["_measurement"] == '
                    '"dataframes")\n'
                    '            |> filter(fn: (r) => r["_field"] == "field2"'
                    '  and r.filter1==10 and r.filter2=="filter2_filter" )\n'
                    '            |> group(columns: ["group1","group2",'
                    '"group3"])\n'
                    '            |> sum()\n'
                    '            |> keep(columns: ["group1", "group2", '
                    '"group3", "_field", "_value", "_start", "_stop"])\n'
                    '            |> set(key: "_field", value: "F2")\n'
                    '            |> yield(name: "F2")\n'
                    '        ')

        query = self.client.get_query(begin=self.period_begin,
                                      end=self.period_end,
                                      custom_fields=custom_fields,
                                      filters=filters,
                                      groupby=groupby)

        self.assertEqual(query, expected)

    def test_query_build_no_custom_fields(self):
        custom_fields = None
        groupby = ['group1', 'group2', 'group3']
        filters = {
            'filter1': '10',
            'filter2': 'filter2_filter'
        }
        beg = self.period_begin.isoformat()
        end = self.period_end.isoformat()
        self.maxDiff = None
        expected = ('\n'
                    '        from(bucket:"cloudkitty")\n'
                    f'            |> range(start: {beg}, stop: {end})\n'
                    '            |> filter(fn: (r) => r["_measurement"] == '
                    '"dataframes")\n'
                    '            |> filter(fn: (r) => r["_field"] == "price"'
                    '  and r.filter1==10 and r.filter2=="filter2_filter" )\n'
                    '            |> group(columns: ["group1","group2",'
                    '"group3"])\n'
                    '            |> sum()\n'
                    '            |> keep(columns: ["group1", "group2",'
                    ' "group3", "_field", "_value", "_start", "_stop"])\n'
                    '            |> set(key: "_field", value: "price")\n'
                    '            |> yield(name: "price")\n'
                    '        \n'
                    '        from(bucket:"cloudkitty")\n'
                    f'            |> range(start: {beg}, stop: {end})\n'
                    '            |> filter(fn: (r) => r["_measurement"] == '
                    '"dataframes")\n'
                    '            |> filter(fn: (r) => r["_field"] == "qty"'
                    '  and r.filter1==10 and r.filter2=="filter2_filter" )\n'
                    '            |> group(columns: ["group1","group2",'
                    '"group3"])\n'
                    '            |> sum()\n'
                    '            |> keep(columns: ["group1", "group2", '
                    '"group3", "_field", "_value", "_start", "_stop"])\n'
                    '            |> set(key: "_field", value: "qty")\n'
                    '            |> yield(name: "qty")\n'
                    '        ')

        query = self.client.get_query(begin=self.period_begin,
                                      end=self.period_end,
                                      custom_fields=custom_fields,
                                      filters=filters,
                                      groupby=groupby)

        self.assertEqual(query, expected)

    def test_query_build_all_custom_fields(self):
        custom_fields = '*'
        groupby = ['group1', 'group2', 'group3']
        filters = {
            'filter1': '10',
            'filter2': 'filter2_filter'
        }
        beg = self.period_begin.isoformat()
        end = self.period_end.isoformat()
        expected = (f'''
          from(bucket:"cloudkitty")
              |> range(start: {beg}, stop: {end})
              |> filter(fn: (r) => r["_measurement"] == "dataframes")
              |> filter(fn: (r) => r.filter1==10 and r.filter2=="filter
              2_filter")
              |> group(columns: ["group1","group2","group3"])
              |> drop(columns: ["_time"])
              |> yield(name: "result")'''.replace(
            ' ', '').replace('\n', '').replace('\t', ''))

        query = self.client.get_query(begin=self.period_begin,
                                      end=self.period_end,
                                      custom_fields=custom_fields,
                                      filters=filters,
                                      groupby=groupby).replace(
            ' ', '').replace('\n', '').replace('\t', '')

        self.assertEqual(query, expected)

    def test_query_build_filters_none(self):
        """A request without filters must not blow up.

        The v2 dataframes API leaves ``filters`` unset when the request
        carries no filter, and retrieve() forwards it verbatim, so
        get_query() receives None on a plain GET /v2/dataframes.
        """
        custom_fields = 'sum(price) AS price,sum(qty) AS qty'

        query_none = self.client.get_query(begin=self.period_begin,
                                           end=self.period_end,
                                           custom_fields=custom_fields,
                                           filters=None,
                                           groupby=[])
        query_empty = self.client.get_query(begin=self.period_begin,
                                            end=self.period_end,
                                            custom_fields=custom_fields,
                                            filters={},
                                            groupby=[])
        self.assertEqual(query_empty, query_none)
        self.assertIn('r["_measurement"] == "dataframes"', query_none)
        self.assertNotIn(' and r.', query_none)

    def test_wildcard_query_is_valid_flux_without_filters(self):
        """A wildcard query with no filters must still compile.

            `filter(fn: (r) => )` is a Flux syntax error, and a bare group()
            collapses the numeric and string fields of a dataframe into one
            table, which InfluxDB rejects with a schema collision. _time has
            to survive as well: _build_dataframes() keys frames on it.
        """
        query = self.client.get_query(begin=self.period_begin,
                                      end=self.period_end,
                                      custom_fields='*',
                                      filters=None,
                                      groupby=None)
        self.assertNotIn('=> )', query)
        self.assertNotIn('|> group()', query)
        self.assertNotIn('drop(columns: ["_time"])', query)
        self.assertIn('r["_measurement"] == "dataframes"', query)

    def test_wildcard_query_keeps_filters(self):
        query = self.client.get_query(begin=self.period_begin,
                                      end=self.period_end,
                                      custom_fields='*',
                                      filters={'project_id': 'abc'},
                                      groupby=None)
        self.assertIn('filter(fn: (r) => r.project_id=="abc")', query)

    @mock.patch('cloudkitty.storage.v2.influx.requests')
    def test_query_raises_on_error_response(self, mock_requests):
        """An error response must not be parsed as if it were a result set.

            Feeding CSV parsing an error body yields no rows, which makes a
            rejected query indistinguishable from an empty result.
        """
        resp = mock.MagicMock()
        resp.ok = False
        resp.status_code = 400
        resp.text = '{"code":"invalid","message":"compilation failed"}'
        resp.raise_for_status.side_effect = Exception('400 Client Error')
        mock_requests.post.return_value = resp

        self.assertRaises(Exception, self.client.query, 'from(bucket:"x")')
        resp.raise_for_status.assert_called_once_with()

    def test_process_data_wildcard_merges_rows_of_one_point(self):
        """Flux returns one row per field; they must merge back into a point.

            Rows of the same point share their tags but differ in
            _field/_value and sit in separate tables, so neither may take
            part in the merge key.
        """
        common = {'': '', 'result': 'result', '_measurement': 'dataframes',
                  '_start': '2026-08-01T00:00:00Z',
                  '_stop': '2026-09-01T00:00:00Z',
                  '_time': '2026-08-23T07:00:00Z',
                  'type': 'image.size', 'project_id': 'p1', 'id': 'r1'}
        rows = [
            dict(common, table='0', _field='qty', _value='0.5'),
            dict(common, table='1', _field='price', _value='0'),
            dict(common, table='2', _field='unit', _value='GiB'),
            dict(common, table='3', _field='groupby', _value='id|project_id'),
        ]
        handler = influx.InfluxClientV2.FluxResponseHandler(
            rows, None, ['*'], self.period_begin, self.period_end, [])
        points = list(handler.response.values())

        self.assertEqual(1, len(points))
        point = points[0]
        self.assertEqual(0.5, point['qty'])
        self.assertEqual(0.0, point['price'])
        # a string field must survive as a string, not blow up on float()
        self.assertEqual('GiB', point['unit'])
        self.assertEqual('id|project_id', point['groupby'])
        # _build_dataframes() needs 'time'
        self.assertEqual('2026-08-23T07:00:00Z', point['time'])
        self.assertEqual('image.size', point['type'])

    def test_process_data_wildcard_keeps_distinct_points_apart(self):
        common = {'': '', 'result': 'result', '_measurement': 'dataframes',
                  '_start': '2026-08-01T00:00:00Z',
                  '_stop': '2026-09-01T00:00:00Z',
                  '_time': '2026-08-23T07:00:00Z', 'type': 'image.size'}
        rows = [
            dict(common, table='0', id='r1', _field='qty', _value='1'),
            dict(common, table='1', id='r2', _field='qty', _value='2'),
        ]
        handler = influx.InfluxClientV2.FluxResponseHandler(
            rows, None, ['*'], self.period_begin, self.period_end, [])
        self.assertEqual(2, len(handler.response))

    def test_process_total_filters_none(self):
        """retrieve() also forwards None straight into process_total()."""
        out = self.client.process_total([], self.period_begin,
                                        self.period_end, None, '*', None)
        self.assertEqual([], out)

    def test_delete_end_none_is_bounded(self):
        """delete() must not hand a None bound to the InfluxDB 2 API.

        The orchestrator's scope-reset path calls delete(begin=<ts>,
        end=None), and influxdb_client rejects a None ``stop``.
        """
        delete_api = self.client.client.delete_api.return_value
        delete_api.reset_mock()

        self.client.delete(self.period_begin, None, {'project_id': 'abc'})

        args, kwargs = delete_api.delete.call_args
        self.assertEqual(self.period_begin, args[0])
        self.assertEqual(influx.INFLUX_MAX_TIME, args[1])
        self.assertIsNotNone(args[1])
        self.assertIn('_measurement="dataframes"', kwargs['predicate'])
        # get_group_filters_query() quotes the key as well as the value
        self.assertIn('"project_id"="abc"', kwargs['predicate'])

    def test_delete_begin_none_is_bounded(self):
        """Same as above for a missing lower bound."""
        delete_api = self.client.client.delete_api.return_value
        delete_api.reset_mock()

        self.client.delete(None, None, None)

        args, _ = delete_api.delete.call_args
        self.assertEqual(influx.INFLUX_MIN_TIME, args[0])
        self.assertEqual(influx.INFLUX_MAX_TIME, args[1])

    def test_query_build_no_groupby(self):
        """Test query building when groupby is empty."""
        custom_fields = 'sum(price) AS price,sum(qty) AS qty'
        groupby = []  # Empty groupby
        filters = {
            'filter1': '10',
            'filter2': 'filter2_filter'
        }
        beg = self.period_begin.isoformat()
        end = self.period_end.isoformat()
        self.maxDiff = None
        expected = ('\n'
                    '        from(bucket:"cloudkitty")\n'
                    f'            |> range(start: {beg}, stop: {end})\n'
                    '            |> filter(fn: (r) => r["_measurement"] == '
                    '"dataframes")\n'
                    '            |> filter(fn: (r) => r["_field"] == "price"'
                    '  and r.filter1==10 and r.filter2=="filter2_filter" )\n'
                    '            |> group()\n'
                    '            |> sum()\n'
                    '            |> keep(columns: ["_field", "_value", '
                    '"_start", "_stop"])\n'
                    '            |> set(key: "_field", value: "price")\n'
                    '            |> yield(name: "price")\n'
                    '        \n'
                    '        from(bucket:"cloudkitty")\n'
                    f'            |> range(start: {beg}, stop: {end})\n'
                    '            |> filter(fn: (r) => r["_measurement"] == '
                    '"dataframes")\n'
                    '            |> filter(fn: (r) => r["_field"] == "qty"'
                    '  and r.filter1==10 and r.filter2=="filter2_filter" )\n'
                    '            |> group()\n'
                    '            |> sum()\n'
                    '            |> keep(columns: ["_field", "_value", '
                    '"_start", "_stop"])\n'
                    '            |> set(key: "_field", value: "qty")\n'
                    '            |> yield(name: "qty")\n'
                    '        ')

        query = self.client.get_query(begin=self.period_begin,
                                      end=self.period_end,
                                      custom_fields=custom_fields,
                                      filters=filters,
                                      groupby=groupby)
        self.assertEqual(query, expected)
