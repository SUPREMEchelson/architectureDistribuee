from kafka import KafkaProducer
from datetime import datetime
from json import dumps
import pandas as pd
from time import sleep
import numpy as np
import json

import csv
# define the Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# def send_csv_to_kafka():
#     with open('./PoleEmploiDonner.csv') as file:
#         csv_reader = csv.reader(file, delimiter=';')
#         next(csv_reader, None)
#         for row in (csv_reader):

#             #col1 = row[0]
#             # col2 = row[1]
#             message = {'col1': col1}
#             producer.send('stream', value=message)
#             # print("Churn: ", row)
#             # print(message)
#             sleep(5)

# send_csv_to_kafka()
producer.send("stream",json.dumps({"year":"caps"}).encode("utf-8"))
producer.flush()