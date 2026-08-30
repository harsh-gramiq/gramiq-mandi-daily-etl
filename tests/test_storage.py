"""Unit tests for PostgreSQL storage layer."""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from app.storage.postgres import upsert_to_postgresql


class TestPostgresStorage(unittest.TestCase):
    def test_upsert_empty_records(self):
        inserted, quarantined = upsert_to_postgresql([])
        self.assertEqual(inserted, 0)
        self.assertEqual(quarantined, 0)

    def test_upsert_when_db_url_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            records = [{"observation_hash": "abc", "commodity": "Wheat"}]
            inserted, quarantined = upsert_to_postgresql(records)
            self.assertEqual(inserted, 1)
            self.assertEqual(quarantined, 0)

    def test_upsert_with_mocked_db(self):
        mock_psycopg2 = MagicMock()
        mock_extras = MagicMock()
        mock_psycopg2.extras = mock_extras
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        records = [
            {
                "observation_hash": "hash123",
                "source": "Agmarknet_API",
                "trade_date": "2026-08-29",
                "state": "Madhya Pradesh",
                "district": "Indore",
                "market": "Indore",
                "commodity": "Wheat",
                "variety": "Lokwan",
                "grade": "FAQ",
                "raw_min_price": "2400",
                "raw_modal_price": "2600",
                "raw_max_price": "2800",
                "raw_price_unit": "Rs/Quintal",
                "normalized_min_price_qtl": 2400.0,
                "normalized_modal_price_qtl": 2600.0,
                "normalized_max_price_qtl": 2800.0,
                "raw_arrival_quantity": "150",
                "raw_arrival_unit": "Tonnes",
                "quality_status": "PASSED",
                "created_at": "2026-08-29T12:00:00Z",
            }
        ]

        with patch.dict(sys.modules, {"psycopg2": mock_psycopg2, "psycopg2.extras": mock_extras}):
            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://test:test@localhost:5432/testdb"}):
                inserted, quarantined = upsert_to_postgresql(records)
                self.assertEqual(inserted, 1)
                self.assertEqual(quarantined, 0)
                mock_psycopg2.connect.assert_called_once()
                mock_extras.execute_batch.assert_called_once()
                mock_conn.commit.assert_called()


if __name__ == "__main__":
    unittest.main()
