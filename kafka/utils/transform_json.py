from datetime import datetime

def transform(data):
    flat = {
        "nomclient": data["properties-client"]["nomclient"],
        "telephoneClient": data["properties-client"]["telephoneClient"],
        "locationClient": f"{data['properties-client']['logitude']}, {data['properties-client']['latitude']}",
        "distance": data["distance"],
        "confort": data["confort"],
        "prix_travel": data["prix_travel"],
        "nomDriver": data["properties-driver"]["nomDriver"],
        "locationDriver": f"{data['properties-driver']['logitude']}, {data['properties-driver']['latitude']}",
        "telephoneDriver": data["properties-driver"]["telephoneDriver"],
        "agent_timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return flat
