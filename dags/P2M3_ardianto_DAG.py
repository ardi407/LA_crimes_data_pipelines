import pandas as pd
import psycopg2 as db

from elasticsearch import Elasticsearch
import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# default arguments untuk mengatur owner, starting date, jumlah retry dan delay retry
default_args = {
    'owner': 'Ardi',
    'start_date': dt.datetime(2023, 11, 23, 22, 20, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

def import_data():
    '''
    fungsi ini berguna untuk mengambil data dari server postgres

    hasil akhir fungsi ini berupa file csv hasil pengambilan dari postgres
    '''
    # connection string
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"

    # pendefinisian connection dengan psycopg2
    conn = db.connect(conn_string)

    # proses query data
    df = pd.read_sql("select * from table_m3", conn)

    # penyimpanan data ke dalam bentuk .csv
    df.to_csv('/opt/airflow/dags/P2M3_ardianto_data_raw.csv', index=False)

def data_cleaning():
    '''
    Fungsi ini berguna untuk melakukan beberapa langkah data cleaning seperti:
    1. Mengganti nama kolom menjadi uppercase dan mengganti karakter spasi menjadi _
    2. Konversi tipe tanggal
    3. Pengambilan informasi jam
    4. Menghapus baris dengan missing value
    5. Menghapus baris yang terduplikasi
    '''
    # pembacaan file csv
    df = pd.read_csv("/opt/airflow/dags/P2M3_ardianto_data_raw.csv")

    # penamaan kolom
    df.rename(columns=lambda x: x.lower().replace(' ', '_'), inplace=True)
    
    # Mengkonversi kolom date_occ menjadi datetime
    df['date_occ'] = pd.to_datetime(df['date_occ'], format='%d/%m/%Y', errors='coerce')
    
    # mengambil informasi jam dari kolom time_occ
    df['time_occ'] = pd.to_datetime(df['time_occ'], format='%H:%M:%S', errors='coerce').dt.hour
    
    # Menghapus missing value
    df.dropna(axis=1, inplace=True)

    # menghapus duplicate
    df.drop_duplicates(keep='first', inplace=True)

    # penyimpanan file csv
    df.to_csv("/opt/airflow/dags/P2M3_ardianto_data_clean.csv", index=False)

def create_location_column(df):
    '''
    Fungsi ini digunakan untuk membuat colom baru bernama coordinates yang menggabungkan longitude dan latitude.
    '''
    # membuat kolom baru dengan mengkombinasikan kolom "lon" dan "lat"
    df['coordinates'] = df.apply(lambda row: {"lon": row['lon'], "lat": row['lat']}, axis=1)
    return df

# koneksi elasticsearch
es = Elasticsearch('http://elasticsearch:9200')

def create_index():
    '''
    Fungsi ini berfungsi untuk membuat index pada kibana dengan type data coordinates sebagai geopoint dan date_occ sebagai date
    '''
    # mapping untuk kolom coordinates sebagai geo_point yang berguna untuk keperluan membuat visualisasi maps
    # mapping kolom date_occ sebagai date
    index_mapping = {
        "mappings": {
            "properties": {
                "coordinates": {"type": "geo_point"},
                "date_occ": {"type": "date"}
            }
        }
    }

    # membuat nama index
    index_name = "table_milestone3"
    
    # jika nama index belum ada
    if not es.indices.exists(index_name):

        # membuat index dengan create dan mapping
        es.indices.create(index=index_name, body=index_mapping)

def sending():
    '''
    Fungsi ini digunakan untuk mengirim data ke kibana. 
    Pada fungsi ini dilakukan pemanggilan 2 fungsi di atas yaitu fungsi create_location_column dan create_index
    
    '''
    # load data
    df = pd.read_csv('/opt/airflow/dags/P2M3_ardianto_data_clean.csv', parse_dates=['date_occ'])

    # Buat column "Coordinates" dengan memanggil fungsi di atas
    df = create_location_column(df)

    # Membuat index dengan mapping pada fungsi create index
    create_index()

    # iterasi untuk setiap baris
    for i, r in df.iterrows():

        # Menggubah format tanggal untuk keperluan kibana
        r['date_occ'] = r['date_occ'].strftime('%Y-%m-%d')

        # setiap baris mmenjadi json
        doc = r.to_json()

        # Proses upload per baris
        res = es.index(index="table_milestone3", id=i + 1, body=doc)

        # untuk keperluan info pada log
        print(res)


# DAG dengan nama Milestone_3 dengan scheduling setiap jam 06.30
with DAG("Milestone_3",
         default_args=default_args,
         schedule_interval="30 6 * * *",
         catchup=False
        ) as dag:
    
    # job 1 - Optional
    starting_jobs = BashOperator(task_id='Starting_jobs',
                                 bash_command='echo "Starting the jobs now"')
    
    # job 2 - pemanggilan fungsi import data
    load_data = PythonOperator(task_id="Loading_data",
                               python_callable=import_data)
    
    # job 3 - pemanggilan fungsi data_cleaning
    cleaning_data = PythonOperator(task_id="Cleaning_data",
                                   python_callable=data_cleaning)
    
    # job 4 - pemanggilan fungsi sending
    sending_data = PythonOperator(task_id='Sending_to_kibana', 
                                  python_callable=sending)

# urutan job atau task pada DAG
starting_jobs >> load_data >> cleaning_data >> sending_data
    



