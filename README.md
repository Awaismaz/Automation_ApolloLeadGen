# Apollo Lead Gen Automation

Selenium-based lead-extraction tool for [Apollo.io](https://www.apollo.io). Drives the Apollo search UI to pull contact records into a local SQLite database (`leads.db`) for downstream CRM import or outreach campaigns.

## How it works

`apollo.py` walks the Apollo search results:

1. Authenticates against Apollo
2. Applies the configured search filters (title, company, geography, etc.)
3. Paginates through results
4. Extracts contact rows and stores them in `leads.db` for deduplication and follow-up

## Quick start

```bash
pip install -r requirements.txt
python apollo.py
```

> ⚠️ Selenium needs a ChromeDriver matching your installed Chrome. The bundled `chromedriver.exe` may be outdated.

Edit `apollo.py` to set:

- Apollo credentials (via env vars — don't hard-code)
- Search query parameters
- Pagination limits

## Output

`leads.db` — SQLite database with one row per extracted lead. Inspect with any SQLite browser, or export to CSV for import into your CRM.

## Files

```
Automation_ApolloLeadGen/
├── apollo.py          # Main automation script
├── leads.db           # SQLite output (created on first run)
├── chromedriver.exe   # Selenium driver
└── requirements.txt
```

## Notes

Personal automation tool — respect Apollo's terms of service and your subscription's fair-use limits.
