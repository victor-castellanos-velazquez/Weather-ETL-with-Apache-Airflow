from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from weather_etl import (
    check_data_exists,
    get_municipios,
    fetch_weather_data,
    translate_and_clean_data,
    get_forecast_and_drought_data,
    map_sequia_indicator,
    update_sequia_indicator,
    insert_weather_data,
    send_email_notification
)

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='ETL para obtener pronósticos del clima y guardarlos en Redshift',
    schedule_interval=timedelta(days=1),
)

# Tarea 1 del sensor para verificar si ya existen datos
check_data_sensor = PythonSensor(
    task_id='check_data_exists',
    python_callable=check_data_exists,
    timeout=600,  # Espera hasta 10 minutos
    mode='reschedule',
    dag=dag,
)

# Inicia el DAG: DummyOperator
start = DummyOperator(task_id='start', dag=dag)

# Tarea 2 consultar datos de los municipios
get_municipios_task = PythonOperator(
    task_id='get_municipios',
    python_callable=get_municipios,
    provide_context=True,
    dag=dag,
)

#Tarea 3 consultar la api con datos optenidos
fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag,
)

#Tarea 4 limpiar y convertir en DF la consulta de la API
translate_and_clean_data_task = PythonOperator(
    task_id='translate_and_clean_data',
    python_callable=translate_and_clean_data,
    provide_context=True,
    dag=dag,
)

#cargar datos del clima en pronosticos_clima en redshift
get_forecast_and_drought_data_task = PythonOperator(
    task_id='get_forecast_and_drought_data',
    python_callable=get_forecast_and_drought_data,
    provide_context=True,
    dag=dag,
)

# Consultar la información de la sequia en la tabla sequia
map_sequia_indicator_task = PythonOperator(
    task_id='map_sequia_indicator',
    python_callable=map_sequia_indicator,
    provide_context=True,
    dag=dag,
)

# Usar el nombre del municipio 
update_sequia_indicator_task = PythonOperator(
    task_id='update_sequia_indicator',
    python_callable=update_sequia_indicator,
    provide_context=True,
    dag=dag,
)

# Actualizar el indicador de sequia más reciente.
insert_weather_data_task = PythonOperator(
    task_id='insert_weather_data',
    python_callable=insert_weather_data,
    provide_context=True,
    dag=dag,
)

#Notificar por correo la carga de la información
send_email_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_notification,
    provide_context=True,
    dag=dag,
)
# Definir las dependencias entre tareas
start >> check_data_sensor >> get_municipios_task >> fetch_weather_data_task >> translate_and_clean_data_task >> insert_weather_data_task >> get_forecast_and_drought_data_task >> map_sequia_indicator_task >> update_sequia_indicator_task >> send_email_task
