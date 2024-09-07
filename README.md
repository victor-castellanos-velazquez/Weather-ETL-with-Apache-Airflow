# Weather ETL with Apache Airflow

This project implements a weather data extraction, transformation, and loading (ETL) pipeline using Apache Airflow. The pipeline fetches weather forecasts for municipalities in Mexico, processes the data, and stores it in a PostgreSQL database. The DAG runs daily and includes various tasks for data retrieval, transformation, and storage.

## Project Features
- **Data Extraction**: Retrieves weather forecast data for specific municipalities from an external weather API.
- **Data Transformation**: Cleans and translates weather condition descriptions from English to Spanish and formats the data for storage.
- **Data Loading**: Inserts the cleaned data into a PostgreSQL database, updating existing records when necessary.
- **Drought Indicators**: Merges drought indicator data with weather data, providing insights into current drought conditions.
- **Email Notifications**: Sends email notifications upon successful data insertion.
- **Task Automation**: Utilizes Airflow sensors to check for pre-existing data and trigger the ETL pipeline only when needed.

## Technologies Used
- **Apache Airflow**: For orchestrating the ETL pipeline with Python-based DAGs.
- **PostgreSQL**: As the database to store weather and drought data.
- **Docker**: To containerize Airflow, PostgreSQL, and other services.
- **Python**: For data processing, API requests, and email notifications.
- **Redis**: For message queuing with the CeleryExecutor in Airflow.

## How to Use
1. Clone the repository and set up the environment using Docker.
2. Add your variables in the `.json` file with the name ""variables_airflow"", in a "utils" folder.
3. Run the Airflow services using Docker Compose.
4. The DAG will execute daily, fetching weather data, processing it, and storing the results.
