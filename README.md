# NL-RailTraffic-ETL-Pipeline

> Real-time data pipeline for Dutch Railways disruption analysis
> Built with Python, SQL, and Azure-ready architecture

## 🎯 Project Goals
Demonstrate production-grade data engineering practices:
- API integration with error handling
- SQL-based transformations (CTEs, window functions)
- Automated daily execution (GitHub Actions)
- Docker containerization

## 🛠️ Tech Stack
- **Python 3.11**: `requests`, `pandas`, `sqlalchemy`
- **Database**: SQLite (local) → Azure SQL (production)
- **Orchestration**: GitHub Actions
- **Deployment**: Docker + Docker Compose

## 📈 Key Features
1. **Incremental Loading**: Only fetch new disruptions (avoids duplicates)
2. **Data Quality**: Great Expectations framework validates schema
3. **Analytics**: Pre-calculated daily KPIs (7-day rolling averages)
4. **Monitoring**: Structured logging to track pipeline health

## 🚀 Quick Start
```bash
# Setup
git clone https://github.com/yourname/nl-railtraffic-etl-pipeline
cd nl-railtraffic-etl-pipeline
pip install -r requirements.txt

# Configure
cp .env.example .env
# Add your NS_API_KEY to .env

# Run pipeline
python src/pipeline.py

# View results
sqlite3 data/nl_rail.db "SELECT * FROM daily_stats LIMIT 5;"
```

## 📊 Sample Analytics Query
See `src/transformation/aggregators.py` for complex SQL with:
- Common Table Expressions (CTEs)
- Window functions (`PERCENT_RANK`, rolling sums)
- Correlated subqueries

## 🐳 Docker Deployment
```bash
docker-compose up
```

## ☁️ Azure Migration Path
- [ ] Move raw data to Azure Blob Storage
- [ ] Replace SQLite with Azure SQL Database
- [ ] Deploy via Azure Data Factory

## 📝 Lessons Learned
- **Error Handling**: NS API occasionally times out → implemented retry logic
- **Data Quality**: 3% of records have invalid timestamps → added validators
- **Performance**: Batch inserts 10x faster than row-by-row