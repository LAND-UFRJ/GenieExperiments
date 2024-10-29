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

# Create a Connection object to interact with a GenieACS server
acs = genieacs.Connection(ip="10.246.3.119", auth=True, user="admin", passwd="admin", port="7557")
# Conexão ao TimescaleDB
conn = psycopg2.connect(dbname="testegenie", user="postgres", password="landufrj123", host="10.246.3.111", port="5432")

# set a device_id for the following methods
devices = acs.device_get_all_IDs() # Get all devices available
device_id = "98254A-Device2-223C1S5004290"

#brincando...

#Mudando a Rede e Senha do Wifi

def change_SSID(rede1, rede2): # Change the SSID of a network
    acs.task_set_parameter_values(device_id, ["Device.WiFi.SSID.1.SSID", rede1])
    acs.task_set_parameter_values(device_id, [["Device.WiFi.SSID.3.SSID", rede2]])
    print("done SSID")

def change_Password(senha): # Change the password of a network
    acs.task_set_parameter_values(device_id, [["Device.WiFi.AccessPoint.1.Security.KeyPassphrase", senha]])
    acs.task_set_parameter_values(device_id, [["Device.WiFi.AccessPoint.3.Security.KeyPassphrase", senha]])
    print("done password")

def refresh_device_parameter(device,parameter): # Refresh some giving paramater
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
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.MacAddress",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.BytesReceived",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.BytesSent",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.PacketsReceived",
                        f"Device.WiFi.MultiAP.APDevice.1.Radio.{k}.AP.{j}.AssociatedDevice.{i}.Stats.PacketsSent",
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

def get_ethernet_stats():
    for device in devices:
        refresh_device_parameter(device, "Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice") #D0:94:66:A1:1B:58
        parameters = []
        for i in range(1, 4):  # Assuming there are up to 5 ethernet interfaces, adjust the range as needed
            parameters.extend([
                f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.IPAddress",
                f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.MACAddress",
                f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.PacketReceived",
                f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.PacketsSent",
                f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.BytesReceived",
                f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.BytesSent",
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
    print("done ethernet stats")

def download_ethernet_stats_to_csv():
    with open(r'/home/localuser/Documentos/VSCode/genieacs/ethernet_stats.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Time', 'Device', 'Parameter', 'Value'])  # Write the header row
        for device in devices:
            parameters = []
            for i in range(1, 5):  # Assuming there are up to 5 ethernet interfaces, adjust the range as needed
                parameters.extend([
                    f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.IPAddress",
                    f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.MACAddress",
                    f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.PacketReceived",
                    f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.PacketsSent",
                    f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.BytesReceived",
                    f"Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice.{i}.BytesSent",
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
    print("done ethernet stats to csv")

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
    cursor = conn.cursor()

    # Criar tabela se não existir
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ethernet_stats (
        time TIMESTAMPTZ NOT NULL,
        device TEXT NOT NULL,
        parameter TEXT NOT NULL,
        value FLOAT NOT NULL
    );
    """)

    # Criar uma tabela de séries temporais
    cursor.execute("SELECT create_hypertable('ethernet_stats', 'time');")

    conn.commit()
    cursor.close()
    conn.close()

def insert_csv_to_timescaledb(csv_file_name):
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

def create_bulkdata_profile(profile_number, name):
    """
    Create or update a bulk data profile with the given name in the specified profile slot.

    :param profile_number: The profile number (1, 2, or 3)
    :param name: The name to set for the profile
    """
    if profile_number not in [1, 2, 3]:
        print("Invalid profile number. Please choose 1, 2, or 3.")
        return

    tree = f"Device.BulkData.Profile.{profile_number}"
    parameter1 = f"{tree}.Name"
    parameter2 = f"{tree}.Enable"
    
    acs.task_set_parameter_values(device_id, [[parameter1, name]])
    acs.task_set_parameter_values(device_id, [[parameter2, "true"]])
    acs.task_refresh_object(device_id, tree)
    time.sleep(10)
    print(f"Bulk data profile '{name}' created successfully in profile slot {profile_number}.")

def see_profiles():
    acs.task_refresh_object(device_id, "Device.BulkData.Profile")
    for i in range(1, 4):    
        try:
            value_status = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{i}.Enable")
            value_name = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{i}.Name")
            print(f"Current status of profile {value_name} ({i}): {value_status}")
        except Exception as e:
            print(f"Failed to get profile {i}: {str(e)}")
    
def set_bulkdata_profile_parameter(profile_number, name, parameter):
    """
    Define a specific parameter within a given bulk data profile.

    :param profile_number: The profile number (1, 2, or 3)
    :param parameter: The parameter to set within the profile
    :param value: The value to set for the parameter
    """
    tree = f"Device.BulkData.Profile.{profile_number}.Parameter"
    try:
        refresh_device_parameter(device_id, tree)
        for i in range(1, 43):
            current_param = f"{tree}.{i}.Name"
            existing_value = acs.device_get_parameter(device_id, current_param)
            if not existing_value:
                acs.task_set_parameter_values(device_id, [[current_param, name]])
                acs.task_set_parameter_values(device_id, [[f"{tree}.{i}.Reference", parameter]])
                break
        time.sleep(10)
        # Verify if the parameter was set correctly
        saved_value = acs.device_get_parameter(device_id, f"{tree}.{i}.Reference")
        if saved_value == parameter:
            print(f"Parameter {i} set to '{parameter}' in parameter {i} in profile {profile_number} successfully.")
        else:
            print(f"Failed to set parameter '{parameter}' in profile {profile_number}.")
    except Exception as e:
        print(f"Error setting parameter '{parameter}' in profile {profile_number}: {str(e)}")

def see_bulkdata_parameters(profile_number):
    for i in range(1, 43):
        try:
            value_name = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{i}.Name")
            if value_name:
                print(f"Parameter {i} in profile {profile_number}: {value_name}")
        except Exception as e:
            print(f"Failed to get parameter {i} in profile {profile_number}: {str(e)}")
            
        except Exception as e:
            print(f"Failed to get parameter {i} in profile {profile_number}: {str(e)}")

def get_bulkdata_profile_parameter_value(profile_number, parameter_number):
    refresh_device_parameter(device_id, f"Device.BulkData.Profile.{profile_number}")
    value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{parameter_number}.Reference")
    value2 = acs.device_get_parameter(device_id, value)
    print(f'The value of parameter {value} is: {value2}')

def get_all_bulkdata_profile_parameter_values(profile_number):
    #refresh_device_parameter(device_id, f"Device.BulkData.Profile.{profile_number}")
    last_param_index = 1
    while True:
        try:
            value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{last_param_index}.Reference")
            if not value:
                break
            value2 = acs.device_get_parameter(device_id, value)
            #print(f"Parameter {last_param_index}({value}) in profile {profile_number}: {value2}")
            last_param_index += 1
        except Exception as e:
            print(f"Failed to get parameter {last_param_index} in profile {profile_number}: {str(e)}")
            break
    for i in range(1, last_param_index): # Assuming there are up to 42 parameters, adjust the range as needed
        try:
            name_value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{i}.Name")
            if name_value:  # Check if the name is not empty or None
                value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{i}.Reference")
                value2 = acs.device_get_parameter(device_id, value)
                print(f"Parameter {i}({value}) in profile {profile_number}: {value2}")
        except Exception as e:
            print(f"Failed to get parameter {i} in profile {profile_number}: {str(e)}")

def config_bulkdata_profile(profile_number, option, value):
    refresh_device_parameter(device_id, "Device.BulkData.Profile")
    if option == "URL":
        print(f"Changing URL to {value}")
        acs.task_set_parameter_values(device_id, [[f"Device.BulkData.Profile.{profile_number}.HTTP.URL", value]])
        refresh_device_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.HTTP.URL")
        check_url = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.HTTP.URL")
        if value == check_url: 
            print("URL has been changed successfully")
        else:
            print("Failed to change URL")
    if option == "Username":
        print(f"Changing Username to {value}")
        acs.task_set_parameter_values(device_id, [[f"Device.BulkData.Profile.{profile_number}.HTTP.Username", value]])
        refresh_device_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.HTTP.Username")
        check_username = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.HTTP.Username")
        if value == check_username:
            print("Username has been changed successfully")
        else:
            print("Failed to change Username")
    if option == "Password":
        print(f"Changing Password to {value}")
        acs.task_set_parameter_values(device_id, [[f"Device.BulkData.Profile.{profile_number}.HTTP.Password", value]])
        refresh_device_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.HTTP.Password")
        check_password = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.HTTP.Password")
        if value == check_password:
            print("Password has been changed successfully")
        else:
            print("Failed to change Password")
    


config_bulkdata_profile(1, "URL", "http://10.246.3.111:5432")
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
