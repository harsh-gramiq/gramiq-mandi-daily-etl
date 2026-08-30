"""Unit tests for active matrix loader and round-robin task interleaver."""

import unittest
from app.matrix import load_active_task_matrix, interleave_tasks_by_state
from app.config import Config


class TestMatrixLoader(unittest.TestCase):
    def test_load_existing_matrix(self):
        matrix = load_active_task_matrix()
        self.assertIsInstance(matrix, list)
        self.assertGreater(len(matrix), 500)
        # Verify structure
        sample = matrix[0]
        self.assertIn("commodity_id", sample)
        self.assertIn("state_id", sample)
        self.assertIn("commodity_name", sample)
        self.assertIn("state_name", sample)

    def test_interleave_tasks_by_state(self):
        raw_tasks = [
            {"commodity_id": 1, "state_id": 1, "state_name": "State_A"},
            {"commodity_id": 2, "state_id": 1, "state_name": "State_A"},
            {"commodity_id": 3, "state_id": 1, "state_name": "State_A"},
            {"commodity_id": 1, "state_id": 2, "state_name": "State_B"},
            {"commodity_id": 2, "state_id": 2, "state_name": "State_B"},
            {"commodity_id": 1, "state_id": 3, "state_name": "State_C"},
        ]
        interleaved = interleave_tasks_by_state(raw_tasks)
        self.assertEqual(len(interleaved), len(raw_tasks))
        # First 3 tasks should belong to 3 distinct states: State_A, State_B, State_C
        first_three_states = [t["state_name"] for t in interleaved[:3]]
        self.assertEqual(first_three_states, ["State_A", "State_B", "State_C"])

    def test_fallback_when_file_missing(self):
        matrix = load_active_task_matrix("non_existent_matrix_file_path.json")
        self.assertIsInstance(matrix, list)
        expected_len = len(Config.CORE_PRIORITY_CROPS) * len(Config.CORE_PRIORITY_STATES)
        self.assertEqual(len(matrix), expected_len)


if __name__ == "__main__":
    unittest.main()
