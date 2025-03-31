import streamlit as st
import requests
import pandas  as pd
import json
import pymongo

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

# Pull data from the collection.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def get_data():
    db = client.motorcycle
    items = db.motorcycle.find()
    items = list(items)  # make hashable for st.cache_data
    return items

# Initialize connection.
conn = st.connection("postgresql", type="sql")

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
    payload = {
        "event_type": job,
        "client_payload": {
        "codeurl": codeurl,
        "dataseturl": dataseturl
    }

    }

    headers = {
    'Authorization': 'Bearer ' + token,
    'Accept': 'application/vnd.github.v3+json',
    'Content-type': 'application/json'
    }

    st.write(url)
    st.write(payload)
    st.write(headers)

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    # Display the response in the app

# Sidebar con información personal
sidebar = st.sidebar
sidebar.markdown("<h2 style='text-align: center; color: #FF4C4C;'>🌼Carlitarp🌼</h2>", unsafe_allow_html=True)
sidebar.image('carlita.jpg', width=160)
sidebar.markdown("<h4 style='text-align: center; color: #CCCCCC;'>🧡S20006731 ISW🧡</h4>", unsafe_allow_html=True)
sidebar.markdown("### 📩 Contacto")
sidebar.markdown("[✉️ zS20006731@estudiantes.uv.mx](mailto:zS20006731@estudiantes.uv.mx)")
sidebar.markdown("______")

# 🔥 Título Principal con estilo deportivo
st.markdown("<h1 style='text-align: center; color: #FF4C4C;'>🔥 Catálogo de Motos 🔥</h1>", unsafe_allow_html=True)
st.subheader("🏁 **Velocidad, potencia y rendimiento en un solo lugar**")

github_user  =  st.text_input('Github user', value='carlitaRP')
github_repo  =  st.text_input('Github repo', value='kafka')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='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')
code_url     =  st.text_input('Code URL', value='https://raw.githubusercontent.com/carlitaRP/kafka/refs/heads/main/motorbike.py')
dataset_url  =  st.text_input('Dataset URL', value='https://raw.githubusercontent.com/carlitaRP/kafka/refs/heads/main/all_bike.csv')

if st.button("🚀 **Envía tu trabajo a Spark**"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)
st.markdown("______")

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if response.status_code == 200:
        try:
            json_objects = [json.loads(line) for line in response.text.strip().split("\n")]
            st.write(json_objects)  # Muestra la lista de diccionarios
        except json.JSONDecodeError as e:
            st.error(f"Error al decodificar JSON: {e}")

st.header("🏁 **Consulta resultados de Spark**")

url_results=  st.text_input('URL results', value='https://raw.githubusercontent.com/carlitaRP/kafka/refs/heads/main/results/motorcycle_sales/part-00000-280f54d0-d2ad-4206-832b-8a212b1850eb-c000.json')

if st.button("🚀 **Consulta resultados**"):
    get_spark_results(url_results)
st.markdown("______")

st.sidebar.markdown("### 🏁 **Consulta base de datos**")
if st.sidebar.button("Obtener resultados de MongoBD"):
    st.header("🏁 **Consulta base de datos MongoDB**")
    items = get_data()

    # Mostrar la estructura real de los datos
    for item in items:
        st.write(item)  # Verifica qué datos tiene realmente cada item

        try:
            # Si "Brand" y "Model" están directamente en item, los imprimimos
            brand = item.get("Brand", "Desconocido")
            model = item.get("Model", "Fecha desconocida")
            st.write(f"{brand} : {model}")
        except Exception as e:
            st.write(f"Error: {e}")

    st.markdown("______")

if st.sidebar.button("Obtener resultados de PostgreSQL"):
    st.header("🏁 **Consulta base de datos PostgreSQL**")
    # Perform query.
    df = conn.query('SELECT * FROM motorcycle;', ttl="10m")
    # Print results.
    for row in df.itertuples():
        st.write(row)
    st.markdown("______")
st.sidebar.markdown("______")
