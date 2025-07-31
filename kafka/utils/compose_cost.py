import json
from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def compute_cost(record):
    client = record["properties-client"]
    driver = record["properties-driver"]
    distance = haversine(client["latitude"], client["logitude"], driver["latitude"], driver["logitude"])
    cost = distance * record["prix_base_per_km"]

    record["distance"] = round(distance, 3)
    record["prix_travel"] = round(cost, 2)
    return record

