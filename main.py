"""Command-line entrypoint for the GramIQ MandiBhav ETL."""

from app.pipeline import fetch_single_task, main

__all__ = ["fetch_single_task", "main"]


if __name__ == "__main__":
    main()
