"""Tests for AzPostgresLogDisconnectionsEvent plugin."""


import copy
import unittest

from cloudmarker.events import azpostgreslogdisconnectionsevent

base_record = {
    'com':  {
        'cloud_type': 'azure',
        'record_type': 'rdbms',
    },
    'ext': {
        'record_type': 'postgresql_server',
        'log_disconnections_enabled': False
    }
}


class AzPostgresLogDisconnectionsEventTest(unittest.TestCase):
    """Tests for AzPostgresLogDisconnectionsEvent plugin."""

    def test_com_bucket_missing(self):
        record = copy.deepcopy(base_record)
        record['com'] = None
        plugin = azpostgreslogdisconnectionsevent. \
            AzPostgresLogDisconnectionsEvent()
        events = list(plugin.eval(record))
        self.assertEqual(events, [])

    def test_cloud_non_azure(self):
        record = copy.deepcopy(base_record)
        record['com']['cloud_type'] = 'non_azure'
        plugin = azpostgreslogdisconnectionsevent. \
            AzPostgresLogDisconnectionsEvent()
        events = list(plugin.eval(record))
        self.assertEqual(events, [])

    def test_record_type_non_rdbms(self):
        record = copy.deepcopy(base_record)
        record['com']['record_type'] = 'non_rdbms'
        plugin = azpostgreslogdisconnectionsevent. \
            AzPostgresLogDisconnectionsEvent()
        events = list(plugin.eval(record))
        self.assertEqual(events, [])

    def test_ext_bucket_missing(self):
        record = copy.deepcopy(base_record)
        record['ext'] = None
        plugin = azpostgreslogdisconnectionsevent. \
            AzPostgresLogDisconnectionsEvent()
        events = list(plugin.eval(record))
        self.assertEqual(events, [])

    def test_record_type_non_postgresql_server(self):
        record = copy.deepcopy(base_record)
        record['ext']['record_type'] = 'non_postgresql_server'
        plugin = azpostgreslogdisconnectionsevent. \
            AzPostgresLogDisconnectionsEvent()
        events = list(plugin.eval(record))
        self.assertEqual(events, [])

    def test_log_disconnections_enabled(self):
        record = copy.deepcopy(base_record)
        record['ext']['log_disconnections_enabled'] = True
        plugin = azpostgreslogdisconnectionsevent. \
            AzPostgresLogDisconnectionsEvent()
        events = list(plugin.eval(record))
        self.assertEqual(events, [])

    def test_log_disconnections_disabled(self):
        record = copy.deepcopy(base_record)
        plugin = azpostgreslogdisconnectionsevent. \
            AzPostgresLogDisconnectionsEvent()
        events = list(plugin.eval(record))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['ext']['record_type'],
                         'postgres_log_disconnections_event')
        self.assertEqual(events[0]['com']['cloud_type'],
                         'azure')
        self.assertEqual(events[0]['com']['record_type'],
                         'postgres_log_disconnections_event')
        self.assertTrue('reference' in events[0]['com'])
        self.assertIsNotNone(events[0]['com']['description'])
        self.assertIsNotNone(events[0]['com']['recommendation'])
