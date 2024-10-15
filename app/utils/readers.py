import csv
import json
import os
from models.pharmacies import Pharmacies
from models.claims import Claims
from models.reverts import Reverts


def read_json(directory_path):
    json_data = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            file_path = os.path.join(directory_path, filename)
            print(f"Reading file: {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
                json_data = json_data + data
    return json_data

def read_csv(directory_path):
    csv_data = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            print(f"Reading file: {file_path}")
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    csv_data.append(row)
    return csv_data

def read_pharmacies(path) -> Pharmacies:
    k = {'pharmacies': read_csv(path)}
    return Pharmacies(**k)


def read_reverts(path) -> Reverts:
    k = {'reverts': read_json(path)}
    return Reverts(**k)


def read_claims(path) -> Claims:
    k = {'claims': read_json(path)}
    return Claims(**k)
