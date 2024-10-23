#!/usr/bin/python
# -*- coding: utf-8 -*-
# Usage examples for python-genieacs

import genieacs
import csv
import json
from datetime import datetime
import time
import psycopg2
import os
import glob

# Create a Connection object to interact with a GenieACS server
acs = genieacs.Connection("*", auth=True, user="*", passwd="*", port="7557")

# set a device_id for the following methods

devices = acs.device_get_all_IDs()

#brincando...
#Mudando a Rede e Senha do Wifi

def change_SSID(rede1, rede2):
    acs.task_set_parameter_values(device_id, ["Device.WiFi.SSID.1.SSID", rede1])
    acs.task_set_parameter_values(device_id, [["Device.WiFi.SSID.3.SSID", rede2]])
    print("done SSID")

def change_Password(senha):
    acs.task_set_parameter_values(device_id, [["Device.WiFi.AccessPoint.1.Security.KeyPassphrase", senha]])
    acs.task_set_parameter_values(device_id, [["Device.WiFi.AccessPoint.3.Security.KeyPassphrase", senha]])
    print("done password")

def refresh_device_parameter(device,parameter):
    print(f"Starting refresh for device {device}")
    try:
        # Refreshe o caminho da árvore completa
        acs.task_refresh_object(device, parameter)
        time.sleep(10)
        print(f"Successfully refreshed parameters for device '{device}'")
    except Exception as e:
        print(f"Failed to refresh parameters for device '{device}': {str(e)}")

def refresh_all_devices():
    for device in devices:
        refresh_device_parameter(device)
        time.sleep(60)  # Pause entre as solicitações
    print("Done refreshing all devices")

def get_wifi_stats():
    for device in devices:
        refresh_device_parameter(device, "Device.WiFi.MultiAP.APDevice.1.Radio.")
        parameters = []
        for i in range(1, 15):  # Assuming there are up to 20 associated devices, adjust the range as needed
            for j in range(1, 3):
                for k in range(1, 3): 
                    parameters.extend([
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Active",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.X_TP_HostName",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.MacAddress",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.BytesReceived",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.BytesSent",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.PacketsReceived",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.PacketsSent",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.ErrorsReceived",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.ErrorsSent",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.RetransCount"
                    ])
        output = []
        for param in parameters:
            value = acs.device_get_parameter(device, param)
            if value is not None:
                output.append((param, value))
        if output:
            print(f"Device: {device}")
            for param, value in output:
                print(f"  Parameter: {param}, Value: {value}")
    print("done wifi stats")

def download_file_json():
    for device in devices:
        refresh_device_parameter(device, "Device.")
        device_data = acs.device_get_by_id(device)
        json_file_path = fr'C:\Users\korin\OneDrive\Documentos\Códigos\Land\device_data_{device}.json'
        with open(json_file_path, 'w') as json_file:
            json.dump(device_data, json_file, indent=4)
        print(f"Device data written to {json_file_path}")

def create_table():
    # Conexão ao TimescaleDB
    conn = psycopg2.connect(
        dbname="testegenie",
        user="postgres",
        password="landufrj123",
        host="10.246.3.111",
        port="5432"
    )
    cursor = conn.cursor()

    # Criar tabela se não existir
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wifi_stats (
        time TIMESTAMPTZ NOT NULL,
        device TEXT NOT NULL,
        parameter TEXT NOT NULL,
        value FLOAT NOT NULL
    );
    """)

    # Criar uma tabela de séries temporais
    cursor.execute("SELECT create_hypertable('wifi_stats', 'time');")

    conn.commit()
    cursor.close()
    conn.close()

def insert_csv_to_timescaledb(csv_file_name):
    # Conexão ao TimescaleDB
    conn = psycopg2.connect(
        dbname="testegenie",
        user="postgres",
        password="landufrj123",
        host="10.246.3.111",
        port="5432"
    )
    cursor = conn.cursor()
    
    # Abrir o arquivo CSV e inserir os dados
    with open(csv_file_name, 'r') as csvfile:
        next(csvfile)  # Pular o cabeçalho
        cursor.copy_from(csvfile, 'wifi_stats', sep=',', columns=('time', 'device', 'parameter', 'value'))
    
    conn.commit()  # Commit as mudanças
    cursor.close()
    conn.close()
    print("Data inserted successfully")

def treat_csv_data(csv_file_name):
    """Função para ler e limpar dados de um arquivo CSV.
    
    Salva os dados limpos em um novo arquivo CSV.
    """
    # Cria o nome do arquivo de saída
    base_name, ext = os.path.splitext(csv_file_name)
    output_csv_file_name = f"{base_name}_treated{ext}"
    with open(csv_file_name, 'r') as csvfile, open(output_csv_file_name, 'w', newline='') as outfile:
        reader = csv.reader(csvfile)
        writer = csv.writer(outfile)
        
        header = next(reader)  # Pula o cabeçalho
        writer.writerow(header)  # Escreve o cabeçalho no novo arquivo
        
        for row in reader:
            try:
                # Tente converter 'value' para float
                value = float(row[3])  # Assume que 'value' está na quarta coluna
                writer.writerow(row)  # Escreve a linha no novo arquivo
            except ValueError:
                print(f"Valor inválido encontrado: {row[3]} não é um número.")

def download_wifi_stats_to_csv():
    with open(r'C:\Users\korin\OneDrive\Documentos\Códigos\Land\wifi_stats.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Time', 'Device', 'Parameter', 'Value'])  # Write the header row
        for device in devices:
            parameters = []
            for i in range(1, 11):  # Assuming there are up to 10 associated devices, adjust the range as needed
                for j in range(1, 3):
                    for k in range(1, 3): 
                        parameters.extend([
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Active",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.X_TP_HostName",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.MacAddress",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.BytesReceived",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.BytesSent",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.PacketsReceived",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.PacketsSent",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.ErrorsReceived",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.ErrorsSent",
                            f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.RetransCount"
                        ])
            output = []
            for param in parameters:
                value = acs.device_get_parameter(device, param)
                if value is not None:
                    output.append((param, value))
            if output:
                time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get current timestamp
                for param, value in output:
                    csvwriter.writerow([time, device, param, value])  # Add timestamp to the row
    print("done wifi stats to csv")



#get_wifi_stats() 
#download_wifi_stats_to_csv()
#treat_csv_data('wifi_stats.csv')
#create_table()
#insert_csv_to_timescaledb('wifi_stats_treated.csv')
print("done final")




'''
# refresh some device parameters
acs.task_refresh_object(device_id, "InternetGatewayDevice.DeviceInfo.")
# set a device parameter
acs.task_set_parameter_values(device_id, [["InternetGatewayDevice.BackupConfiguration.FileList", "backup.cfg"]])
# get a device parameter
acs.task_get_parameter_values(device_id, [["InternetGatewayDevice.BackupConfiguration.FileList"]])
# factory reset a device
acs.task_factory_reset(device_id)
# reboot a device
acs.task_reboot(device_id)
# add an object to a device
acs.task_add_object(device_id, "VPNObject", [["InternetGatewayDevice.X_TDT-DE_OpenVPN"]])
# download a file
acs.task_download(device_id, "9823de165bb983f24f782951", "Firmware.img")
# retry a faulty task
acs.task_retry("9h4769svl789kjf984ll")


# print all tasks of a given device
print(acs.task_get_all(device_id))
# print IDs of all devices
print(acs.device_get_all_IDs())
# search a device by its ID and print all corresponding data
print(acs.device_get_by_id(device_id))
# search a device by its MAC address and print all corresponding data
print(acs.device_get_by_MAC("00:01:49:ff:0f:01"))
# print the value of a given parameter of a given device
print(acs.device_get_parameter(device_id, "InternetGatewayDevice.DeviceInfo.SoftwareVersion"))
# print 2 given parameters of a given device
print(acs.device_get_parameters(device_id, "InternetGatewayDevice.DeviceInfo.SoftwareVersion,InternetGatewayDevice.X_TDT-DE_Interface.2.ProtoStatic.Ipv4.Address"))
# delete a task
acs.task_delete("9h4769svl789kjf984ll")

# create a new preset
acs.preset_create("Tagging", r'{ "weight": 0, "precondition": "{\"_tags\":{\"$ne\":\"tagged\"}}", "configurations": [ { "type": "add_tag", "tag":"tagged" }] }')
# write all existing presets to a file and store them in a json object
preset_data = acs.preset_get_all('presets.json')
# delete all presets
for preset in preset_data:
    acs.preset_delete(preset["_id"])
# create all presets from the file
acs.preset_create_all_from_file('presets.json')

# create a new object
acs.object_create("CreatedObject", r'{"Param1": "Value1", "Param2": "Value2", "_keys":["Param1"]}')
# write all existing objects to a file and store them in a json object
object_data = acs.object_get_all('objects.json')
# delete all objects
for gobject in object_data:
    acs.object_delete(gobject["_id"])
# create all objects from the file
acs.object_create_all_from_file('objects.json')

# create a new provision
acs.provision_create("Logging", '// This is a comment\nlog("Hello World!");')
# write all existing provisions to a file and store them in a json object
provision_data = acs.provision_get_all('provisions.json')
# delete all provisisions
for provision in provision_data:
    acs.provision_delete(provision["_id"])
# create all provisions from the file
acs.provision_create_all_from_file('provisions.json')

# print all tags of a given device
print(acs.tag_get_all(device_id))
# assign a tag to a device
acs.tag_assign(device_id, "tagged")
# remove a tag from a device
acs.tag_remove(device_id, "tagged")

# print all existing files in the database
print(acs.file_get_all())
# print data of a specific file
print(str(acs.file_get(fileType="12 Other File", version="0.4")))
# upload a new or modified file
acs.file_upload("Firmware.img", "1 Firmware Upgrade Image", "123456", "r4500", "2.0")
# delete a file from the database
acs.file_delete("Firmware.img")

# delete the device from the database
acs.device_delete(device_id)

# get IDs of all existing faults and delete all
faults = acs.fault_get_all_IDs()
for fault in faults:
    acs.fault_delete(fault)
'''

