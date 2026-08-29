# Security Policy

## Reporting a vulnerability

Do not open a public issue containing credentials, webhook URLs, database details, or exploit instructions. Contact the repository owner privately through GitHub with reproduction steps and the affected commit.

## Secret handling

- Store credentials only in GitHub Actions secrets or local `.env` files.
- Never log authorization headers, API keys, database passwords, or webhook URLs.
- Rotate any credential that has been exposed.
- Use dry-run mode when testing extraction behavior against production-like data.
