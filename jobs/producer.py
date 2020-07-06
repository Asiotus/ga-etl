import json
import os
from os.path import isfile, join
from time import sleep

from kafka import KafkaProducer


def main():
    # read lines from data
    startDate = "20160801"
    stopDate = "20160807"
    folder = "data/"
    directory = os.getcwd()+ "/" + folder
    fileList = os.listdir(directory)
    fileList = [f for f in fileList if 'json' in f]

    selectedDate = [f for f in fileList if (f >= 'ga_sessions_'+startDate+'.json' and f <= 'ga_sessions_'+stopDate+'.json')]
    selectedDatepaths = [directory + f for f in selectedDate]
    f = open(selectedDatepaths[0], "r")
    
    # create producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: x.encode('utf-8'))

    # send message to kafka
    while True:
        # input("press enter")
        sleep(3)
        line = f.readline()
        line_json = json.loads(line)
        time = line_json["visitStartTime"]
        producer.send("topic1", value=line, timestamp_ms=time)
        print(time)

if __name__ == '__main__': 
    main()
