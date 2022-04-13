import paho.mqtt.client as mqtt #import the client1
from influxdb import InfluxDBClient
from ast import literal_eval
import json
import logging

logging.basicConfig(filename="app.log", level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logging.info("Program started")
def main():
    client = mqtt.Client("InfluxDBreader") #create new instance
    client.connect(config["MqttConfiguration"]["MqttUri"]) #connect to broker
    client.on_message=on_message
    client.subscribe(get_topic())
    client.loop_forever()

with open("appsettings.json", "r") as read_file:
    config = json.load(read_file)

def get_topic():
    return [
        (
            "/".join(
                [
                    config["MqttConfiguration"]["MqttHomeDeviceTopic"],
                    config["ProgramConfiguration"]["ServiceName"],
                    "HassAnalogSensor"
                    if device["DeviceDescription"]["DeviceType"] != "Plug"
                    else "HassBinarySensor",
                    device["DeviceDescription"]["Identifier"],
                ]
            ),
            0,
        )
        for device in config["ProgramConfiguration"]["Devices"]
    ]

def get_device_types():
    return [
        device["DeviceDescription"]["DeviceType"]
        for device in config["ProgramConfiguration"]["Devices"]
    ]

def get_data_type_aliaes(sensor_id):
    return type_name[data_id_type[int(sensor_id)-1]]

def convert_data_type(value, type_name):
    if type_name == "Plug":
        return "1" if value == "On" else "0"
    else:
        return value

def on_message(client, userdata, message):
    data = literal_eval(message.payload.decode('utf8'))
    logging.info(f"Receive message with topic: {message.topic}\n {' '*28}Message payload:{data}")
    influxDBinsert(data["Id"],   convert_data_type(data[get_data_type_aliaes(data["Id"])], data_id_type[int(data["Id"])-1]), )

data_id_type = get_device_types()
type_name = config["ProgramConfiguration"]["TypesAliaes"]

dbclient = InfluxDBClient(host = config["InfluxdbConfiguration"]["InfluxdbUri"], port = config["InfluxdbConfiguration"]["InfluxdbPort"])

def influxDBinsert(sensor_id, value):
    dbclient.write([f"{data_id_type[int(sensor_id)-1].lower()},id={sensor_id} value={value}"], {"db":config["InfluxdbConfiguration"]["InfluxdbName"]}, protocol="line")

if __name__ == "__main__":
    main()
