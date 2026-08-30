"""Unit tests for active matrix loader."""

import unittest
from app.matrix import load_active_task_matrix
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

    def test_fallback_when_file_missing(self):
        matrix = load_active_task_matrix("non_existent_matrix_file_path.json")
        self.assertIsInstance(matrix, list)
        expected_len = len(Config.CORE_PRIORITY_CROPS) * len(Config.CORE_PRIORITY_STATES)
        self.assertEqual(len(matrix), expected_len)
        self.assertEqual(matrix[0]["commodity_name"], "Wheat")


if __name__ == "__main__":
    unittest.main()
