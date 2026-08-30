"""Unit tests for the end-to-end pipeline orchestrator."""

import unittest
from unittest.mock import patch

from app.pipeline import run_pipeline


class TestPipelineOrchestrator(unittest.TestCase):
    @patch("app.pipeline.extract_national_agmarknet_parallel")
    @patch("app.pipeline.upsert_to_postgresql")
    @patch("app.pipeline.dispatch_card_to_teams")
    @patch("app.pipeline.write_github_step_summary")
    def test_run_pipeline_dry_run(
        self,
        mock_summary,
        mock_dispatch,
        mock_upsert,
        mock_extract,
    ):
        mock_records = [
            {
                "observation_hash": "h1",
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
        mock_telemetry = {
            "total_tasks_probed": 100,
            "tasks_with_data": 80,
            "tasks_market_closed": 20,
            "tasks_rate_limited_recovered": 5,
            "tasks_rate_limited_failed": 0,
            "tasks_network_failed": 0,
            "total_monitored_states": 31,
            "active_states_count": 28,
            "active_states": ["Madhya Pradesh"],
            "missing_states": ["Sikkim", "Lakshadweep", "Goa"],
            "total_monitored_crops": 348,
            "active_crops_count": 65,
            "active_crops": ["Wheat"],
            "missing_crops": [],
            "elapsed_seconds": 12.4,
            "task_details": [],
        }

        mock_extract.return_value = (mock_records, mock_telemetry)

        result = run_pipeline(
            target_date="2026-08-29",
            lookback_days=7,
            workers=4,
            dry_run=True,
            print_card=False,
        )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["target_date"], "2026-08-29")
        self.assertEqual(result["total_records"], 1)
        self.assertEqual(result["db_upserted"], 0)
        self.assertIn("telemetry", result)
        mock_extract.assert_called_once()
        mock_upsert.assert_not_called()  # Dry run should skip DB write
        mock_dispatch.assert_called_once()
        mock_summary.assert_called_once()


if __name__ == "__main__":
    unittest.main()
