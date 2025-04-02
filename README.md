# BMKG Weather Forecast ETL Data Pipeline using Apache Airflow
This project automates the process of collecting weather forecast data on each sub-district at Bulukumba Regency from the BMKG (Indonesian Meteorology, Climatology, and Geophysics Agency) API, transforming it into a structured format, and loading it into ClickHouse for time-series analysis and visualization. Built with Apache Airflow and Docker , this pipeline ensures reliable daily data updates with minimal manual intervention.


## Key Feature ✨

- **Data Collection Automation**: Fetch real-time Weather forecasts Data (temperature, humidity, wind direction, weather description, forecast time, etc) from BMKG API. The process run twice a day at 01.30 UTC and 13.30 UTC.
- **Data Transformation**: Cleans and standardizes raw JSON data into a tabular format
- **Scalable Storage**: Loads data into Clickhouse for high-speed analytics and large-scale data processing 
- **Monitoring & Reliability**: Uses Apache Airflow UI to schedule daily runs, track failures, and retry tasks.

## Tech Stack 🛠️

- Python: Core programming language.
- Apache Airflow: Orchestrating (scheduling and monitoring) ETL workflows.
- Pandas: Data processing to transform and validate raw data
- Docker: Containerization for easy deployment.

### Architecture
![](https://cdn-images-1.medium.com/max/1600/1*Xb4fYDkUM5fEz9Tqan7BVQ.png)

## Setup & Installation 🚀

### Prerequisites
- Python 3.8+
- Docker

### Project Setup
Clone the Github repository
```bash
git clone https://github.com/yaumianwar/bmkg-weather-forecast-etl.git
cd bmkg-weather-forecast-etl
```

Setup virtualenvironment
```bash
python3 -m venv venv
source venv/bin/activate
```
Install dependencies
```bash
pip install -r requirements.txt
```
Create .env file
```bash
touch .env
```
.env file content
```YAML
AIRFLOW_UID=50000
CLICKHOUSE_HOST=YOUR_CLICKHOUSE_HOST
CLICKHOUSE_PASSWORD=YOUR_CLICKHOUSE_PASSWORD
CLICKHOUSE_USER=YOUR_CLICKHOUSE_USER
CLICKHOUSE_DB=YOUR_CLICKHOUSE_DB
```
Run project via Docker Compose
```bash
docker compose up -d
```

### Create Master Location Data
Run scripts to get master locations data (location code and name) from `https://kodewilayah.id/` and insert it into Clickhouse
```bash
cd scripts
python create_master_locations_data.py
# Message if script run succesfully: create master location data success
```

### Access Apache Airflow UI
Airflow UI can be accessed on `http://localhost:8080/`. n the sign in page, insert airflow as user and password. Airflow UI homepage contains lists of example and our DAGs, scroll down to see etl_forecast or type it on the search form.
![](https://cdn-images-1.medium.com/max/1600/1*q1HqkD7aQBW6a1zwPwWqgg.png)

This is the view Forecast ETL DAGs detail page, the DAGs was set to run twice a day at 01.30 UTC and 13.30 UTC, but we also can manually trigger the DAGs using play button which on the top right. The DAGs is succesfully run and we can confirm it by check the data on the clickhouse forecasts table.
![](https://cdn-images-1.medium.com/max/1600/1*tZ9ZS55fJmtmep4SBzqR_g.png)
