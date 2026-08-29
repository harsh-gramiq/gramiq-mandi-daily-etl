import unittest
from unittest.mock import Mock

import main
from app.analytics import compute_live_market_analytics
from app.config import PRIORITY_COMMODITIES, PRODUCING_STATES, STATE_NAME_BY_ID


class StateAttributionTests(unittest.TestCase):
    def test_domain_configuration_is_importable_from_app_package(self):
        self.assertEqual(PRIORITY_COMMODITIES[1], "Wheat")
        self.assertIn(19, PRODUCING_STATES[1])
        self.assertEqual(STATE_NAME_BY_ID[20], "Maharashtra")

    def test_analytics_module_returns_named_states(self):
        record = {"commodity": "Wheat", "market": "Indore", "state": "Madhya Pradesh",
                  "raw_arrival_quantity": 1, "normalized_modal_price_qtl": 2500,
                  "trade_date": "2026-08-29"}
        analytics = compute_live_market_analytics([record])
        self.assertEqual(analytics["states_count"], 1)
        self.assertIn("Madhya Pradesh", analytics["state_breakdown"])

    def test_requested_state_is_used_when_response_omits_state_name(self):
        payload = {
            "markets": [{
                "marketName": "Indore APMC",
                "districtName": "Indore",
                "dates": [{
                    "arrivalDate": "29/08/2026",
                    "data": [{"modalPrice": "2500", "minimumPrice": "2400", "maximumPrice": "2600", "arrivals": "10"}],
                }],
            }],
        }
        response = Mock(status_code=200)
        response.json.return_value = payload
        session = Mock()
        session.get.return_value = response

        records = main.fetch_single_task(
            session,
            (1, "19", "Wheat", 2026, 8, {"29/08/2026": "2026-08-29"}),
            "2026-08-29T00:00:00+00:00",
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["state"], "Madhya Pradesh")

    def test_requested_partition_wins_over_stale_response_state_name(self):
        payload = {
            "markets": [{
                "marketName": "Pune APMC",
                "stateName": "Gujarat",
                "dates": [{
                    "arrivalDate": "29/08/2026",
                    "data": [{"modalPrice": "2500", "minimumPrice": "2400", "maximumPrice": "2600", "arrivals": "10"}],
                }],
            }],
        }
        response = Mock(status_code=200)
        response.json.return_value = payload
        session = Mock()
        session.get.return_value = response

        records = main.fetch_single_task(
            session,
            (1, "20", "Wheat", 2026, 8, {"29/08/2026": "2026-08-29"}),
            "2026-08-29T00:00:00+00:00",
        )

        self.assertEqual(records[0]["state"], "Maharashtra")


if __name__ == "__main__":
    unittest.main()
