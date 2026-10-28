import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.pipeline import validate


class DataQualityTests(unittest.TestCase):
    def test_valid_event_passes(self):
        event = {"order_id": "1", "event_time": "2026-01-01T00:00:00Z", "customer_id": "c", "product": "p", "category": "x", "quantity": 1, "unit_price": 10}
        self.assertIsNone(validate(event))

    def test_negative_quantity_rejected(self):
        event = {"order_id": "1", "event_time": "2026-01-01T00:00:00Z", "customer_id": "c", "product": "p", "category": "x", "quantity": -1, "unit_price": 10}
        self.assertIn("quantity", validate(event))
