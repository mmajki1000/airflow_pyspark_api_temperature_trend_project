Project created to explore Apache Airflow & PySpark
Idea: Checking if, and in case yes how much avg temperature increased in Wroclaw over last years

1. Set-up:
mvp:
- Open WeatherMap API
- Python venv
- Airlow
- PySpark

feature 2:
- using Azure Storage

Steps:
1. Airflow http sensor checking url - done
2. Airflow file sensor checking file with city list (in this case there is 1 city - Wrocław)
3. If all work, then Python request connecting the API, taking data and saving as a file - done
4. PySpark opening it, processing and saving as parquet - done
5. Power BI connected to make some viz - waiting
6. Airflow DAG tuning - progress
