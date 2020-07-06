from datetime import datetime
from json import loads

import mysql.connector
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'transformed2',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

cnx = mysql.connector.connect(user='root', password='12345678', database='ga_streaming')
cursor = cnx.cursor()

add_visitpermin = ("INSERT INTO visitpermin "
               "(begin, num) "
               "VALUES (%(begin)s, %(num)s)")

for message in consumer:
    row = message.value

    date_formatted = datetime.fromisoformat(row["begin"]).strftime("%Y-%m-%d %H:%M:%S")
    print('{} added'.format(row))

    cursor.execute("INSERT INTO visitpermin (begin, num) VALUES (%s, %s) ON DUPLICATE KEY UPDATE num=%s;",
                   (date_formatted, row["num"], row["num"]))

    cnx.commit()

cursor.close()
cnx.close()
