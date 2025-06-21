# Instagram Data Manager

A Python application that manages Instagram account data with automatic synchronization between CSV files and SQLite database.

## Features

- **Automatic Sync**: Ensures `data.csv` and SQLite database are always in sync
- **Startup Validation**: Checks sync status when the app starts
- **Comprehensive Logging**: Detailed logs for debugging and monitoring
- **Error Handling**: Robust error handling with graceful fallbacks

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Database**: The application will automatically create a SQLite database (`instagram_data.db`) on first run.

## Usage

### Starting the Application

```bash
python main.py
```

The application will:
1. Initialize the database if it doesn't exist
2. Check if `data.csv` is in sync with the database
3. Automatically sync any differences
4. Start the main application

### Checking Sync Status

To check the sync status without starting the full application:

```bash
python check_sync.py
```

This will show:
- Whether CSV and database are in sync
- Count of URLs in each source
- Details of any differences
- Examples of URLs that are out of sync

## File Structure

- `main.py` - Main application with sync functionality
- `data.csv` - Source CSV file with Instagram URLs
- `instagram_data.db` - SQLite database (created automatically)
- `check_sync.py` - Utility script to check sync status
- `requirements.txt` - Python dependencies

## Database Schema

The SQLite database contains a table `instagram_accounts` with the following structure:

```sql
CREATE TABLE instagram_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    username TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Sync Process

1. **Read CSV**: Loads all URLs from `data.csv`
2. **Read Database**: Retrieves all URLs from the database
3. **Compare**: Identifies URLs that exist in CSV but not in database
4. **Sync**: Adds new URLs to the database with extracted usernames
5. **Verify**: Confirms sync was successful

## Logging

The application uses Python's logging module with INFO level by default. Logs include:
- Database initialization
- CSV reading operations
- Sync operations and results
- Error messages with details

## Error Handling

The application handles various error scenarios:
- Missing CSV file
- Database connection issues
- File reading errors
- Sync failures

All errors are logged with detailed information for debugging.

## Example Output

```
2024-01-15 10:30:00,123 - INFO - 🚀 Starting Instagram Data Manager...
2024-01-15 10:30:00,124 - INFO - Database initialized successfully
2024-01-15 10:30:00,125 - INFO - Checking CSV and database sync status...
2024-01-15 10:30:00,126 - INFO - Read 500 URLs from CSV file
2024-01-15 10:30:00,127 - INFO - Retrieved 0 URLs from database
2024-01-15 10:30:00,128 - WARNING - ⚠️  CSV and database are out of sync
2024-01-15 10:30:00,129 - INFO - 🔄 Syncing CSV to database...
2024-01-15 10:30:00,130 - INFO - Added 500 new URLs to database
2024-01-15 10:30:00,131 - INFO - ✅ Sync completed successfully
2024-01-15 10:30:00,132 - INFO - 🎉 Application started successfully with synchronized data
Instagram Data Manager is ready! 