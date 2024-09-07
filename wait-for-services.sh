#!/bin/bash

# Esperando a que los servicios estén disponibles

echo "Esperando a que los servicios estén disponibles..."

# Aquí puedes agregar el código para esperar a que otros servicios estén disponibles

echo "Servicios disponibles. Continuando con la configuración..."

# Esperar hasta que PostgreSQL esté listo
until nc -z -v -w30 postgres 5432
do
  echo "Esperando a que PostgreSQL esté listo..."
  sleep 5
done

echo "PostgreSQL está listo"