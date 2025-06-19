# Azure Cost vs Sales

## Overview

**Azure Cost vs Sales** is a Python project designed to automate the retrieval and analysis of Azure Partner Center billing data. It connects to the Microsoft Partner Center API, fetches invoice data, stores it in a DuckDB database, and allows for interactive querying and analysis using SQL and pandas. This tool is ideal for organizations and partners seeking to reconcile Azure cloud costs with sales and perform deeper financial or operational analysis.

---

## Features

- **Automated Authentication:** Uses OAuth2 refresh token flow to securely obtain API access tokens.
- **Invoice Retrieval:** Fetches all invoices from the Azure Partner Center API.
- **Local Database Storage:** Uses DuckDB for efficient local analytics and SQL querying.
- **Interactive Querying:** Supports both SQL (via DuckDB) and pandas DataFrame operations.
- **Configurable Secrets:** Credentials are managed securely using a `secrets.json` file.
- **Robust Logging and Error Handling:** Tracks each stage of the workflow and handles exceptions gracefully.

---

## Directory Structure

```
src/
  └── main.py          # Main script for fetching and analyzing invoices
secrets.json           # Credentials for API and database (not included, user must provide)
duckdb.db              # Local DuckDB database file (created/used by script)
```

---

## Requirements

- Python 3.8+
- The following Python packages:
  - `requests`
  - `pandas`
  - `numpy`
  - `duckdb`
  - `zipfile` (standard library)
  - `io` (standard library)
  - `json` (standard library)
  - `datetime` (standard library)
  - `warnings` (standard library)

Install dependencies using pip:

```bash
pip install requests pandas numpy duckdb
```

---

## Setup

1. **Clone the Repository**

   ```bash
   git clone https://github.com/AbdRub/Azure-Cost-vs-Sales.git
   cd Azure-Cost-vs-Sales
   ```

2. **Create `secrets.json`**

   In the root or parent directory, provide a `secrets.json` file with your Partner Center credentials:

   ```json
   {
     "refresh_token": "YOUR_REFRESH_TOKEN",
     "app_id": "YOUR_APP_ID",
     "app_secret": "YOUR_APP_SECRET"
   }
   ```

3. **Ensure DuckDB Database**

   The script will connect to a local DuckDB database file (`duckdb.db`). If it does not exist, it will be created.

---

## Usage

From the `src` directory (or project root), run:

```bash
python src/main.py
```

**Workflow Steps:**

1. **Set Display Options:** Configures pandas and warning settings for better output.
2. **Parse Secrets:** Reads API credentials from `secrets.json`.
3. **Connect to DuckDB:** Establishes a connection to the local database.
4. **Authenticate:** Exchanges refresh token for access token with Microsoft OAuth2.
5. **Fetch Invoices:** Retrieves all invoices via Partner Center API.
6. **Display Data:** Converts invoices to a pandas DataFrame and displays the first 5 records using DuckDB SQL.

---

## Code Structure

The main logic is in `src/main.py` and includes:

- **Utility Functions:** For setting display options, connecting to DuckDB, and parsing secrets.
- **API Functions:** For obtaining OAuth2 tokens and fetching invoices.
- **Database Functions:** For executing and printing SQL queries on DuckDB.
- **Main Script:** Orchestrates the above steps, handles workflow, and prints informative logs.

---

## Example Output

```
main script started execution at 19 Jun 2025 04:30 PM
Trying to parse secrets file
Secrets file found
Trying to connect to DuckDB
Connected to DuckDB
Trying to obtain access token
Refresh Token valid, access token obtained.
Fetching invoices
Fetched 10 invoices
[Shows first 5 records as a SQL table]
```

---

## Troubleshooting

- **Secrets File Not Found:** Ensure `secrets.json` is present and correctly formatted.
- **DuckDB Connection Issues:** Check file and permissions for `duckdb.db`.
- **API Authentication Errors:** Validate refresh token, app ID, and app secret.
- **Missing Dependencies:** Reinstall required Python packages.

---

## Security

- **Do NOT share your `secrets.json` or database files.**
- Ensure that your credentials are stored securely and are not committed to source control.

---

## Customization

- Modify SQL queries in `main.py` to perform custom analysis.
- Extend the script to load invoice data into DuckDB for long-term analytics.
- Integrate additional Partner Center API endpoints as needed.

---

## License

This project is for demonstration and internal analytics purposes. Please check the repository for license details.

---

## Author

[AbdRub](https://github.com/AbdRub)

---

## References

- [Microsoft Partner Center API Documentation](https://docs.microsoft.com/en-us/partner-center/develop/)
- [DuckDB Documentation](https://duckdb.org/)
