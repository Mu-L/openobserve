import base64, json
import random

from locust import FastHttpUser, task, between

import json
import random

class ZincUser(FastHttpUser):
    # wait_time = between(1, 5)  # Adjust as necessary
    connection_timeout = 600.0
    network_timeout = 600.0
    host = "http://localhost:5080/api/default"

    @task
    def insert_json_data1(self): 
        # Open and read the JSON file line by line
        data =  open('./data/ziox_multi_600.json').read()

        user = "root@example.com"
        password = "Complexpass#123"
        bas64encoded_creds = base64.b64encode(bytes(f"{user}:{password}", "utf-8")).decode("utf-8")

        headers = {
            'Authorization': 'Basic ' + bas64encoded_creds, 
            'Content-Type': 'application/x-ndjson'
        }

        self.client.post("/locust22/_multi", data=data, headers=headers)

# locust -f locustfile.py
# localhost::8089
