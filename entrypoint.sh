#!/bin/bash
echo "Debug: Starting entrypoint.sh with argument: $1"

# Crear los directorios necesarios y ajustar los permisos
mkdir -p /root/airflow/logs /root/airflow/dags /root/airflow/plugins
chmod -R 777 /root/airflow/logs /root/airflow/dags /root/airflow/plugins

# Inicializar la base de datos solo si es la primera vez
if [ "$1" = "webserver" ] && [ ! -f "/root/airflow/initialized" ]; then
    echo "Debug: Initializing Airflow DB"
    airflow db init
    echo "Debug: Creating Airflow user"
    airflow users create -u admin -p admin -f admin -l admin -r Admin -e admin@example.com
    echo "Debug: Importing Airflow variables"
    airflow variables import /root/airflow/utils/airflow_variables.json
    touch /root/airflow/initialized
fi

# Ejecutar el comando de Airflow
echo "Debug: Executing airflow command: airflow $@"
exec airflow "$@"