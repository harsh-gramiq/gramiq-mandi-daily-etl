"""GramIQ MandiBhav REST API & Microservice Service Contract.

Provides secure, parameterized API endpoints and OpenAPI 3.1 contracts for downstream
GramIQ services (Krishi Mitra voice RAG, Krishi Khata valuation, Krishi Clinic pathology).
"""

import json
from typing import Any


def get_openapi_schema() -> dict[str, Any]:
    """Generates canonical OpenAPI 3.1 specification for GramIQ Mandi API."""
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "GramIQ Krishi MandiBhav Intelligence API",
            "version": "2.0.0",
            "description": "National Agricultural Mandi Price Ingestion, Arbitrage, and Velocity Service.",
        },
        "paths": {
            "/api/v1/mandi/latest-rates": {
                "get": {
                    "summary": "Get latest validated mandi prices",
                    "parameters": [
                        {"name": "commodity", "in": "query", "required": False, "schema": {"type": "string"}},
                        {"name": "state", "in": "query", "required": False, "schema": {"type": "string"}},
                        {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer", "default": 50}},
                    ],
                    "responses": {
                        "200": {"description": "Paginated array of mandi price observations"}
                    },
                }
            },
            "/api/v1/mandi/arbitrage": {
                "get": {
                    "summary": "Get real-time inter-mandi price arbitrage corridors",
                    "parameters": [
                        {"name": "commodity", "in": "query", "required": False, "schema": {"type": "string"}},
                        {"name": "min_spread_rs", "in": "query", "required": False, "schema": {"type": "number", "default": 150.0}},
                    ],
                    "responses": {
                        "200": {"description": "List of high-margin trade corridors"}
                    },
                }
            },
            "/api/v1/mandi/velocity": {
                "get": {
                    "summary": "Get Day-over-Day and Week-over-Week price velocity and trends",
                    "parameters": [
                        {"name": "commodity", "in": "query", "required": False, "schema": {"type": "string"}},
                    ],
                    "responses": {
                        "200": {"description": "Price momentum, percentage shifts, and direction indicators"}
                    },
                }
            },
            "/api/v1/mandi/msp-status": {
                "get": {
                    "summary": "Get government MSP compliance and distress sale evaluation",
                    "parameters": [
                        {"name": "commodity", "in": "query", "required": True, "schema": {"type": "string"}},
                    ],
                    "responses": {
                        "200": {"description": "MSP support price comparison and discount/premium status"}
                    },
                }
            },
        },
    }


class MandiAPIService:
    """Lightweight in-memory and database-backed handler for Mandi API queries."""

    def __init__(self, cached_metrics: dict[str, Any] | None = None, records: list[dict[str, Any]] | None = None):
        self.metrics = cached_metrics or {}
        self.records = records or []

    def handle_get_rates(
        self,
        commodity: str | None = None,
        state: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Filters latest validated mandi rates with pagination."""
        filtered = self.records
        if commodity:
            filtered = [r for r in filtered if commodity.lower() in str(r.get("commodity", "")).lower()]
        if state:
            filtered = [r for r in filtered if state.lower() in str(r.get("state", "")).lower()]

        total = len(filtered)
        paginated = filtered[offset : offset + limit]

        return {
            "status": "SUCCESS",
            "total_count": total,
            "limit": limit,
            "offset": offset,
            "items": paginated,
        }

    def handle_get_arbitrage(self, commodity: str | None = None, min_spread_rs: float = 150.0) -> dict[str, Any]:
        """Returns top inter-mandi arbitrage corridors."""
        corridors = self.metrics.get("arbitrage_corridors", [])
        if commodity:
            corridors = [c for c in corridors if commodity.lower() in str(c.get("commodity", "")).lower()]
        corridors = [c for c in corridors if float(c.get("gross_spread_rs", 0)) >= min_spread_rs]

        return {
            "status": "SUCCESS",
            "count": len(corridors),
            "corridors": corridors,
        }

    def handle_get_velocity(self, commodity: str | None = None) -> dict[str, Any]:
        """Returns price velocity trends."""
        vel_dict = self.metrics.get("price_velocity", {})
        if commodity:
            match = {k: v for k, v in vel_dict.items() if commodity.lower() in k.lower()}
            return {"status": "SUCCESS", "velocity": match}
        return {"status": "SUCCESS", "velocity": vel_dict}

    def handle_get_msp_status(self, commodity: str) -> dict[str, Any]:
        """Returns MSP comparison for given commodity."""
        msp_dict = self.metrics.get("msp_evaluations", {})
        res = msp_dict.get(commodity)
        if not res:
            from app.analytics.msp_registry import evaluate_msp_status
            res = evaluate_msp_status(commodity, 0.0)

        return {
            "status": "SUCCESS",
            "evaluation": res or {"message": f"No official MSP benchmark configured for '{commodity}'"},
        }
