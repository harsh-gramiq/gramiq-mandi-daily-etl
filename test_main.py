"""Unified Test Runner for GramIQ MandiBhav ETL Package."""

import unittest
import sys


def suite():
    loader = unittest.TestLoader()
    return loader.discover("tests", pattern="test_*.py")


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite())
    sys.exit(0 if result.wasSuccessful() else 1)
