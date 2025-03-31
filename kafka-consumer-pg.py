from kafka import KafkaConsumer
import json
import psycopg2

print('connecting pg ...')

try:
    conn = psycopg2.connect(database = "defaultdb", 
                        user = "avnadmin", 
                        host= 'pg-359cfab6-estudiantes-bb9b.h.aivencloud.com',
                        password = "AVNS_-OKTlUSsFpRD_i04C5-",
                        port = 27234)
    cur = conn.cursor()
    print("PosgreSql Connected successfully!")
except:
    print("Could not connect to PosgreSql")

consumer = KafkaConsumer('motorcycle',bootstrap_servers=['localhost:9092'])

for msg in consumer:
    record = json.loads(msg.value.decode('utf-8')) 
    brand = record["Brand"]  
    model = record["Model"]
    
    try:
        sql = "INSERT INTO motorcycle (brand, model) VALUES (%s, %s)"  
        cur.execute(sql, (brand, model))  
        conn.commit()
        print(f"Inserted: {brand} - {model}")
    except Exception as e:
        print("Could not insert into PostgreSQL:", e)
conn.close()