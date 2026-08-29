"""PostgreSQL persistence public interface."""

from app.pipeline import get_connection_config, load_records_to_postgres, open_postgres_connection

__all__ = ["get_connection_config", "load_records_to_postgres", "open_postgres_connection"]
