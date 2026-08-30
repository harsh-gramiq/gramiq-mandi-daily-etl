"""Unit tests for MSP Registry and Benchmark Evaluator."""

import unittest
from app.analytics.msp_registry import get_msp_for_commodity, evaluate_msp_status


class TestMSPRegistry(unittest.TestCase):
    def test_get_msp_for_known_crops(self):
        self.assertEqual(get_msp_for_commodity("Wheat"), 2425.0)
        self.assertEqual(get_msp_for_commodity("Chana(Gram)"), 5650.0)
        self.assertEqual(get_msp_for_commodity("Mustard"), 5950.0)
        self.assertEqual(get_msp_for_commodity("Soyabean"), 4892.0)

    def test_get_msp_for_non_msp_crop(self):
        self.assertIsNone(get_msp_for_commodity("Apple"))
        self.assertIsNone(get_msp_for_commodity("Tomato"))

    def test_evaluate_msp_distress_sale(self):
        # Wheat MSP is 2425; if selling at 2000 (< 0.95 * 2425), should be BELOW_MSP and is_distress=True
        eval_res = evaluate_msp_status("Wheat", 2000.0)
        self.assertIsNotNone(eval_res)
        self.assertEqual(eval_res["status"], "BELOW_MSP")
        self.assertTrue(eval_res["is_distress"])
        self.assertEqual(eval_res["delta_rs"], -425.0)

    def test_evaluate_msp_premium_sale(self):
        # Wheat MSP is 2425; if selling at 2800 (> 1.10 * 2425), should be ABOVE_MSP
        eval_res = evaluate_msp_status("Wheat", 2800.0)
        self.assertIsNotNone(eval_res)
        self.assertEqual(eval_res["status"], "ABOVE_MSP")
        self.assertFalse(eval_res["is_distress"])
        self.assertEqual(eval_res["delta_rs"], 375.0)


if __name__ == "__main__":
    unittest.main()
