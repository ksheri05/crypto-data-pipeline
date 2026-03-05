# 🚀 Crypto Data Pipeline

A data engineering pipeline that fetches cryptocurrency data and implements **Medallion Architecture** (Bronze-Silver-Gold layers).

## Architecture

```
CoinGecko API
     ↓
Bronze Layer (Raw Data) → S3
     ↓
Silver Layer (Cleaned Data) → S3
     ↓
Gold Layer (Curated Data) → S3 + PostgreSQL
```

## Project Structure

```
crypto-data-pipeline/
├── config/
│   └── config.py              # Configuration
├── src/
│   ├── ingestion/
│   │   └── api_client.py      # Fetch data from API
│   ├── transformation/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   └── utils/
│       ├── s3_handler.py      # S3 operations
│       └── db_handler.py      # PostgreSQL operations
├── scripts/
│   ├── setup_s3.py            # Setup AWS S3
│   └── setup_database.py      # Setup PostgreSQL
├── sql/
│   └── create_table.sql       # Database schema
├── main.py                    # Main pipeline
└── requirements.txt           # Dependencies
```

## Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your credentials.

### 3. Setup AWS S3
```bash
python scripts/setup_s3.py
```

### 4. Setup PostgreSQL
```bash
python scripts/setup_database.py
```

### 5. Run Pipeline
```bash
python main.py
```
