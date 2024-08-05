import base64, json
import random

from locust import FastHttpUser, task, between

import json
import random

# Function to generate a random value of a specified type
def generate_random_value(value_type):
    if value_type == "int":
        return random.randint(1, 100)
    elif value_type == "float":
        return random.uniform(1.0, 100.0)
    elif value_type == "string":
        return f"random_string_{random.randint(1, 100)}"
    elif value_type == "bool":
        return random.choice([True, False])
    elif value_type == "list":
        return [random.randint(1, 10) for _ in range(3)]
    elif value_type == "dict":
        return {"key": random.randint(1, 10)}
    # Removed the function type to ensure JSON serializability
    else:
        return None

# Function to add 10 specific attributes with changing values and types
def add_specific_attributes(data_dict):
    # data_types = ["int", "float", "string", "bool", "list", "dict", "function"]
    for i in range(1, 20):
        # value_type = random.choice(data_types)
        # data_dict[f"attr{i}"] = generate_random_value(value_type)
        data_dict[f"attr{i}"] = generate_random_value("string")

# Function to add 10 random attributes to a given dictionary
def add_random_attributes(data_dict):
    for _ in range(1, random.randint(1, 1000)):
        key = f"random_attr_{random.randint(1, 20000)}"
        # value = generate_random_value(random.choice(["int", "float", "string", "bool", "list", "dict"]))
        value = generate_random_value("string")
        data_dict[key] = value

class ZincUser(FastHttpUser):
    # wait_time = between(1, 5)  # Adjust as necessary
    connection_timeout = 600.0
    network_timeout = 600.0
    host = "http://localhost:5080/api/default"

    @task
    def insert_ndjson_data(self):
        modified_data = []

        # Open and read the NDJSON file line by line
        with open('data/data33.ndjson', 'r') as file:
            for index, line in enumerate(file):
                row = json.loads(line)  # Convert each line to a dictionary
                
                # Apply modifications
                if index % 2 == 1:  # Even-numbered rows (0-indexed)
                    add_random_attributes(row)

                # Convert the dictionary back to a JSON string and add it to the list
                modified_data.append(json.dumps(row))

        # Join the list back into a single string with newline characters
        data = "\n".join(modified_data)

        user = "root@example.com"
        password = "Complexpass#123"
        bas64encoded_creds = base64.b64encode(bytes(f"{user}:{password}", "utf-8")).decode("utf-8")

        headers = {
            'Authorization': 'Basic ' + bas64encoded_creds, 
            'Content-Type': 'application/x-ndjson'
        }

        self.client.post("/_bulk", data=data, headers=headers)

# locust -f locustfile.py
# localhost::8089
