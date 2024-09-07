FROM python:3.10-slim

# Instala bash, jq, netcat en una sola capa y limpia los archivos temporales para reducir el tamaño de la imagen
RUN apt-get update && apt-get install -y \
    bash \
    jq \
    netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Actualiza pip e instala los paquetes de Python desde requirements.txt
# Copiamos primero el archivo para evitar que si cambian otros archivos se vuelva a ejecutar esta parte
COPY utils/requirements.txt /root/airflow/utils/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /root/airflow/utils/requirements.txt

# Crear el directorio y ajustar los permisos
RUN mkdir -p /root/airflow && chown -R root:root /root/airflow

# Copia el archivo de variables JSON a la imagen
COPY utils/airflow_variables.json /root/airflow/utils/airflow_variables.json

# Copia tu script entrypoint a la imagen
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Dale permisos de ejecución al script
RUN chmod +x /usr/local/bin/entrypoint.sh

# Copia los DAGs al directorio de Airflow
COPY dags /root/airflow/dags

# Configura el entrypoint para ejecutar el script directamente
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Establece el directorio de trabajo
WORKDIR /root/airflow

# Establece el comando por defecto
CMD ["bash"]