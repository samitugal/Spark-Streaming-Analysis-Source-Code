import os
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
from json import dumps
from datetime import datetime
import time

if __name__ == "__main__":

  def dataProducer(productKey):
    productList = list()


    current_date = datetime.now()
    timestamp =int(current_date.strftime("%Y%m%d"))

    customerID = random.randint(0, 25)
    branchID = random.randint(0,3)
    productList.append(branchID)
    productList.append(productKey)
    productList.append(timestamp)
    productList.append(customerID)
    time.sleep(1)
    return productList

  while True:
    data = dataProducer(random.randint(0, 19))
    producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode("utf-8"))
    producer.send("project",{'BranchID':data[0],'ProductKey':data[1],'TimeStamp':data[2],"CustomerID":data[3]})
    producer.flush()