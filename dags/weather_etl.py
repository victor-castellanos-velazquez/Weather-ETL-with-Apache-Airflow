from airflow.models import Variable
import psycopg2
import pandas as pd
import requests
import unidecode
import smtplib 
from translate import Translator
from datetime import datetime
from airflow.operators.email import EmailOperator
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Datos para sensor
def check_data_exists(**kwargs):
    r_user = Variable.get("r_user")
    r_password = Variable.get("r_password")
    r_host = Variable.get("r_host")
    r_port = Variable.get("r_port")
    r_dbname = Variable.get("r_dbname")

    conn = psycopg2.connect(
        dbname=r_dbname,
        user=r_user,
        password=r_password,
        host=r_host,
        port=r_port
    )

    cursor = conn.cursor()

    hoy = datetime.today().strftime('%Y-%m-%d')
    query = """
    SELECT 1 FROM pronosticos_clima WHERE fecha_pronostico = %s LIMIT 1
    """
    cursor.execute(query, (hoy,))
    result = cursor.fetchone()

    conn.close()

    # Si no hay datos, el sensor devuelve True para continuar
    if result is None:
        return True
    else:
        # Si hay datos, devuelve False para detener el DAG
        return False

# consulta los municipios en la tabla municipios en redshift
def get_municipios(**kwargs):
    r_user = Variable.get("r_user")
    r_password = Variable.get("r_password")
    r_host = Variable.get("r_host")
    r_port = Variable.get("r_port")
    r_dbname = Variable.get("r_dbname")
    
    conn = psycopg2.connect(
        dbname=r_dbname,
        user=r_user,
        password=r_password,
        host=r_host,
        port=r_port
    )

    query = "SELECT cve_mun, nom_mun, lat_decimal, lon_decimal FROM municipios"
    df_municipios = pd.read_sql_query(query, conn)
    conn.close()

    kwargs['ti'].xcom_push(key='df_municipios', value=df_municipios)

#guarda lat y lon para consultar la información en la API
def fetch_weather_data(**kwargs):
    df_municipios = kwargs['ti'].xcom_pull(task_ids='get_municipios', key='df_municipios')
    
    api_key = Variable.get("api_key")

    def obtener_pronostico(lat, lon):
        url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={lat},{lon}&days=1"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        return None

    pronosticos = []
    for index, row in df_municipios.iterrows():
        lat = row['lat_decimal']
        lon = row['lon_decimal']
        cve_mun = row['cve_mun']
        municipio = row['nom_mun']
        pronostico = obtener_pronostico(lat, lon)
        if pronostico and pronostico['location']['country'] == 'Mexico':
            for forecast_day in pronostico['forecast']['forecastday']:
                forecast = forecast_day['day']
                pronosticos.append({
                    'cve_mun': cve_mun,
                    'municipio': municipio,
                    'forecast_date': forecast_day['date'],
                    'maxtemp_c': forecast['maxtemp_c'],
                    'mintemp_c': forecast['mintemp_c'],
                    'avgtemp_c': forecast['avgtemp_c'],
                    'totalprecip_mm': forecast['totalprecip_mm'],
                    'condition_text': forecast['condition']['text'],
                    'wind_kph': forecast.get('maxwind_kph'),
                    'humidity': forecast.get('avghumidity')
                })
        else:
            pronosticos.append({
                'cve_mun': cve_mun,
                'municipio': municipio,
                'forecast_date': None,
                'maxtemp_c': None,
                'mintemp_c': None,
                'avgtemp_c': None,
                'totalprecip_mm': None,
                'condition_text': None,
                'wind_kph': None,
                'humidity': None
            })

    df_pronosticos = pd.DataFrame(pronosticos)
    kwargs['ti'].xcom_push(key='df_pronosticos', value=df_pronosticos)

def translate_and_clean_data(**kwargs):
    df_pronosticos = kwargs['ti'].xcom_pull(task_ids='fetch_weather_data', key='df_pronosticos')

    translator = Translator(to_lang="es")

    def traducir(texto):
        try:
            if texto is not None:
                return translator.translate(texto)
            else:
                return texto
        except Exception:
            return texto

    df_pronosticos['condition_text'] = df_pronosticos['condition_text'].apply(traducir)
    df_pronosticos.columns = ['cve_mun', 'municipio', 'fecha_pronostico', 'temp_max_c', 'temp_min_c', 'temp_prom_c', 'precipitacion_mm', 'condicion', 'viento_kph', 'humedad']
    df_pronosticos['condicion'] = df_pronosticos['condicion'].apply(lambda x: unidecode.unidecode(x) if x else x)
    df_pronosticos['municipio'] = df_pronosticos['municipio'].apply(lambda x: x.title() if x else x)

    kwargs['ti'].xcom_push(key='df_pronosticos_limpios', value=df_pronosticos)

def get_forecast_and_drought_data(**kwargs):
    r_user = Variable.get("r_user")
    r_password = Variable.get("r_password")
    r_host = Variable.get("r_host")
    r_port = Variable.get("r_port")
    r_dbname = Variable.get("r_dbname")

    with psycopg2.connect(
        dbname=r_dbname,
        user=r_user,
        password=r_password,
        host=r_host,
        port=r_port
    ) as conn:
        query = "SELECT cve_mun, municipio, fecha_pronostico, indicador_sequia FROM pronosticos_clima WHERE fecha_pronostico = (SELECT MAX(fecha_pronostico) FROM pronosticos_clima)"
        df_pronosticos_clima = pd.read_sql_query(query, conn)

        query = "SELECT cve_mun, nombre_mun, cve_ent, entidad, fecha, valor FROM sequia"
        df_sequia = pd.read_sql_query(query, conn)
        df_sequia['fecha'] = pd.to_datetime(df_sequia['fecha']).dt.date

    kwargs['ti'].xcom_push(key='df_pronosticos_clima', value=df_pronosticos_clima)
    kwargs['ti'].xcom_push(key='df_sequia', value=df_sequia)

def map_sequia_indicator(**kwargs):
    df_pronosticos_clima = kwargs['ti'].xcom_pull(task_ids='get_forecast_and_drought_data', key='df_pronosticos_clima')
    df_sequia = kwargs['ti'].xcom_pull(task_ids='get_forecast_and_drought_data', key='df_sequia')

    df_sequia_max_fecha = df_sequia.loc[df_sequia.groupby('cve_mun')['fecha'].idxmax()]
    sequia_dict = df_sequia_max_fecha.set_index('cve_mun')['valor'].to_dict()
    df_pronosticos_clima['indicador_sequia'] = df_pronosticos_clima['cve_mun'].map(sequia_dict)

    kwargs['ti'].xcom_push(key='df_pronosticos_clima_updated', value=df_pronosticos_clima)

def update_sequia_indicator(**kwargs):
    r_user = Variable.get("r_user")
    r_password = Variable.get("r_password")
    r_host = Variable.get("r_host")
    r_port = Variable.get("r_port")
    r_dbname = Variable.get("r_dbname")

    df_pronosticos_clima = kwargs['ti'].xcom_pull(task_ids='map_sequia_indicator', key='df_pronosticos_clima_updated')

    with psycopg2.connect(
        dbname=r_dbname,
        user=r_user,
        password=r_password,
        host=r_host,
        port=r_port
    ) as conn:
        cursor = conn.cursor()
        for index, row in df_pronosticos_clima.iterrows():
            update_query = """
            UPDATE pronosticos_clima
            SET indicador_sequia = %s
            WHERE cve_mun = %s AND fecha_pronostico = %s
            """
            cursor.execute(update_query, (row['indicador_sequia'], row['cve_mun'], row['fecha_pronostico']))
        conn.commit()

def insert_weather_data(**kwargs):
    r_user = Variable.get("r_user")
    r_password = Variable.get("r_password")
    r_host = Variable.get("r_host")
    r_port = Variable.get("r_port")
    r_dbname = Variable.get("r_dbname")

    df_pronosticos_limpios = kwargs['ti'].xcom_pull(task_ids='translate_and_clean_data', key='df_pronosticos_limpios')

    with psycopg2.connect(
        dbname=r_dbname,
        user=r_user,
        password=r_password,
        host=r_host,
        port=r_port
    ) as conn:
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO pronosticos_clima 
        (cve_mun, municipio, fecha_pronostico, temp_max_c, temp_min_c, temp_prom_c, precipitacion_mm, condicion, viento_kph, humedad)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for index, row in df_pronosticos_limpios.iterrows():
            cursor.execute(insert_query, (
                row['cve_mun'], row['municipio'], row['fecha_pronostico'],
                row['temp_max_c'], row['temp_min_c'], row['temp_prom_c'],
                row['precipitacion_mm'], row['condicion'], row['viento_kph'], row['humedad']
            ))

        conn.commit()
        cursor.close()

def send_email_notification(**kwargs):
    try:
        # Llamamos a las variables desde el archivo .json
        smtp_host = Variable.get("s_host")
        smtp_port = int(Variable.get("s_port"))
        smtp_user = Variable.get("s_user")
        smtp_password = Variable.get("s_password")
        mail_from = Variable.get("s_mail_from")
        mail_to = Variable.get("s_mail_to")

        # Crear el cuerpo del mensaje
        body = """
        <h3>Inserción de datos completada</h3>
        <p>Se han insertado los pronósticos del clima correctamente en la tabla <strong>pronosticos_clima</strong>.</p>
        """
        msg = MIMEText(body, 'html')  # Asegúrate de que el cuerpo esté en formato HTML
        msg['Subject'] = "Inserción de datos exitosa"
        msg['From'] = mail_from
        msg['To'] = mail_to

        # Configurar el servidor SMTP
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()  # Establece la conexión segura
            server.login(smtp_user, smtp_password)  # Autenticación
            server.sendmail(mail_from, [mail_to], msg.as_string())  # Enviar correo
            print("Correo enviado correctamente.")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")