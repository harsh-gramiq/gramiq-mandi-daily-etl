"""
GramIQ MandiBhav — Configuration & Environment Settings
======================================================
Centralized configuration manager for the Mandi Daily ETL pipeline.
Loads database URLs, AI API keys, and notification webhooks from environment.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

# Base Directory Resolution
BASE_DIR = Path(__file__).resolve().parent.parent
ACTIVE_MATRIX_PATH = BASE_DIR / "active_national_matrix.json"

# Pipeline Tuning Defaults
DEFAULT_WORKERS = int(os.environ.get("MANDI_DEFAULT_WORKERS", "10"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("MANDI_DEFAULT_LOOKBACK_DAYS", "7"))
HTTP_TIMEOUT_SECONDS = int(os.environ.get("MANDI_HTTP_TIMEOUT_S", "15"))
HTTP_MAX_RETRIES = int(os.environ.get("MANDI_HTTP_RETRIES", "3"))
HTTP_RETRY_BACKOFF_BASE = float(os.environ.get("MANDI_RETRY_BACKOFF_BASE", "2.0"))
THREAD_DISPATCH_STAGGER_S = float(os.environ.get("MANDI_DISPATCH_STAGGER_S", "0.025"))

# AGMARKNET API Endpoints
AGMARKNET_BASE_URL = "https://api.agmarknet.gov.in/v1"
AGMARKNET_MONTHLY_ENDPOINT = f"{AGMARKNET_BASE_URL}/prices-and-arrivals/date-wise/specific-commodity"
AGMARKNET_FILTERS_ENDPOINT = f"{AGMARKNET_BASE_URL}/daily-price-arrival/filters"

# HTTP Headers for Upstream Government Gateway
AGMARKNET_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://agmarknet.gov.in",
    "Referer": "https://agmarknet.gov.in/",
    "Connection": "keep-alive",
}

# Core Priority Commodities Fallback
CORE_PRIORITY_CROPS: list[tuple[int, str]] = [
    (1, "Wheat"), (2, "Paddy(Dhan)(Common)"), (3, "Maize"), (4, "Bengal Gram(Gram)(Whole)"),
    (5, "Jowar(Sorghum)"), (6, "Bajra(Pearl Millet/Cumbu)"), (8, "Barley(Jau)"), (9, "Ragi(Finger Millet)"),
    (10, "Green Gram(Moong)(Whole)"), (11, "Black Gram(Urd Beans)(Whole)"), (12, "Mustard"),
    (13, "Soyabean"), (14, "Groundnut"), (15, "Cotton"), (23, "Onion"), (24, "Potato"),
    (28, "Tomato"), (45, "Red Gram/Arhar/Tur"), (65, "Turmeric"), (72, "Chilli Red")
]

CORE_PRIORITY_STATES: list[int] = [
    11, 12, 16, 19, 20, 28, 29, 34, 31, 32, 1, 2, 3, 4, 5, 10,
    14, 15, 18, 21, 22, 23, 24, 25, 26, 27, 30, 33, 35, 36
]


def get_database_url() -> str | None:
    return os.environ.get("DATABASE_URL") or os.environ.get("PRODUCTION_DB_URL")


def get_teams_webhook_url() -> str | None:
    return os.environ.get("TEAMS_WEBHOOK_URL")


def get_gemini_api_key() -> str | None:
    return os.environ.get("GEMINI_API_KEY_OG") or os.environ.get("GEMINI_API_KEY")


class Config:
    """Config container class providing class-level access to constants and helpers."""
    BASE_DIR = BASE_DIR
    ACTIVE_MATRIX_PATH = ACTIVE_MATRIX_PATH
    DEFAULT_WORKERS = DEFAULT_WORKERS
    DEFAULT_LOOKBACK_DAYS = DEFAULT_LOOKBACK_DAYS
    HTTP_TIMEOUT_SECONDS = HTTP_TIMEOUT_SECONDS
    HTTP_MAX_RETRIES = HTTP_MAX_RETRIES
    HTTP_RETRY_BACKOFF_BASE = HTTP_RETRY_BACKOFF_BASE
    THREAD_DISPATCH_STAGGER_S = THREAD_DISPATCH_STAGGER_S
    AGMARKNET_BASE_URL = AGMARKNET_BASE_URL
    AGMARKNET_MONTHLY_ENDPOINT = AGMARKNET_MONTHLY_ENDPOINT
    AGMARKNET_FILTERS_ENDPOINT = AGMARKNET_FILTERS_ENDPOINT
    AGMARKNET_HEADERS = AGMARKNET_HEADERS
    CORE_PRIORITY_CROPS = CORE_PRIORITY_CROPS
    CORE_PRIORITY_STATES = CORE_PRIORITY_STATES

    get_database_url = staticmethod(get_database_url)
    get_teams_webhook_url = staticmethod(get_teams_webhook_url)
    get_gemini_api_key = staticmethod(get_gemini_api_key)
