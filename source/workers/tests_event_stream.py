"""Smoke tests for ACD workers event_stream helper."""
from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock


class AcdEventStreamTests(unittest.TestCase):

    def test_xadd_event(self):
        from event_stream import STREAM_CALLEVENTS, xadd_event

        conn = MagicMock()
        ok = xadd_event(
            conn,
            STREAM_CALLEVENTS,
            {'type': 'CAMP', 'id': 5, 'event': 'DIAL', 'call_type': '2'},
            maxlen=100,
        )
        self.assertTrue(ok)
        args = conn.execute_command.call_args[0]
        self.assertEqual(args[0], 'XADD')
        self.assertEqual(args[1], STREAM_CALLEVENTS)
        payload = json.loads(args[-1])
        self.assertEqual(payload['event'], 'DIAL')


if __name__ == '__main__':
    unittest.main()
