# Contributing

1. Create a focused branch for each change.
2. Do not commit `.env`, credentials, webhook URLs, database dumps, or generated private data.
3. Add deterministic tests for extraction, state attribution, validation, analytics, or card behavior.
4. Run `python test_main.py` and `python -m py_compile main.py app/*.py test_main.py` before pushing.
5. Keep production behavior fail-closed and avoid synthetic data in live paths.
6. Explain operational changes in the pull request, including schedule, secrets, and rollback impact.

For ingestion changes, include the affected trade-date window and expected state/commodity coverage.
