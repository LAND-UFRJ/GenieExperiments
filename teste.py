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
import threading

# Create a Connection object to interact with a GenieACS server
acs = genieacs.Connection(ip="10.246.3.119", auth=True, user="admin", passwd="admin", port="7557")
# Conexão ao TimescaleDB
conn = psycopg2.connect(dbname="testegenie", user="postgres", password="landufrj123", host="10.246.3.111", port="5432")

# set a device_id for the following methods
devices = acs.device_get_all_IDs()  # Get all devices available
#device_id = "98254A-Device2-223C1S5004290"

# Function to execute a query and close the cursor
def execute_query(query, var=None):
    cursor = conn.cursor()
    cursor.execute(query, var)
    conn.commit()
    cursor.close()

# Function to set parameter values
def set_parameter_values(device_id, parameters, value):
    for param in parameters:
        acs.task_set_parameter_values(device_id, [[param, value]])
        refresh_device_parameter(device_id, param)
        check = acs.device_get_parameter(device_id, param)
        if check == value:
            print(f"Successfully set parameter '{param}' to '{value}' for device '{device_id}'")
        else:
            print(f"Failed to set parameter '{param}' to '{value}' for device '{device_id}'")

# Refresh some given parameters
def refresh_device_parameter(device_id, parameter):
    try:
        acs.task_refresh_object(device_id, parameter)
        time.sleep(5)
        #print(f"Successfully refreshed {parameter} for device '{device_id}'")
    except Exception as e:
        print(f"Failed to refresh {parameter} for device '{device_id}': {str(e)}")
        
# Get WiFi stats
def get_wifi_stats(device_id):
    refresh_device_parameter(device_id, "Device.WiFi.MultiAP.APDevice.1.Radio.")
    parameters = []
    for i in range(1, 21):  # Assuming there are up to 20 associated devices, adjust the range as needed
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
        value = acs.device_get_parameter(device_id, param)
        if value is not None:
            output.append((param, value))
    if output:
        print(f"Device: {device_id}")
        for param, value in output:
            print(f"  Parameter: {param}, Value: {value}")
    print("done wifi stats")

# Get Ethernet stats
def get_ethernet_stats(device_id):
    refresh_device_parameter(device_id, "Device.WiFi.MultiAP.APDevice.1.X_TP_Ethernet.AssociatedDevice")
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
        value = acs.device_get_parameter(device_id, param)
        if value is not None:
            output.append((param, value))
    if output:
        print(f"Device: {device_id}")
        for param, value in output:
            print(f"  Parameter: {param}, Value: {value}")
    print("done ethernet stats")

# Download Ethernet stats to CSV
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

# Download file JSON
def download_file_json():
    for device in devices:
        refresh_device_parameter(device, "Device.")
        device_data = acs.device_get_by_id(device)
        json_file_path = fr'C:\Users\korin\OneDrive\Documentos\Códigos\Land\device_data_{device}.json'
        with open(json_file_path, 'w') as json_file:
            json.dump(device_data, json_file, indent=4)
        print(f"Device data written to {json_file_path}")

# Create table
def create_table(name):
    query = f"""
    CREATE TABLE IF NOT EXISTS {name} (
        time TIMESTAMPTZ NOT NULL,
        device_id TEXT NOT NULL,
        mac_address TEXT NOT NULL,
        channel INTEGER NOT NULL,
        frequency_band TEXT NOT NULL,
        channel_bandwidth TEXT NOT NULL,
        ssid TEXT NOT NULL,
        signal_strength FLOAT NOT NULL
    );
    """
    execute_query(query)
    print(f"Table {name} created successfully")

# Create hypertable
def create_hypertable(name):
    query = f"""
    SELECT create_hypertable('{name}', 'time');
    """
    execute_query(query)
    print(f"Hypertable {name} created successfully")

# Insert CSV to TimescaleDB
def insert_csv_to_timescaledb(csv_file_name):
    cursor = conn.cursor()
    with open(csv_file_name, 'r') as csvfile:
        next(csvfile)  # Pular o cabeçalho
        cursor.copy_from(csvfile, 'wifi_stats', sep=',', columns=('time', 'device', 'parameter', 'value'))
    conn.commit()
    cursor.close()
    print("Data inserted successfully")

# Treat CSV data
def treat_csv_data(csv_file_name):
    base_name, ext = os.path.splitext(csv_file_name)
    output_csv_file_name = f"{base_name}_treated{ext}"
    with open(csv_file_name, 'r') as csvfile, open(output_csv_file_name, 'w', newline='') as outfile:
        reader = csv.reader(csvfile)
        writer = csv.writer(outfile)
        header = next(reader)  # Pula o cabeçalho
        writer.writerow(header)  # Escreve o cabeçalho no novo arquivo
        for row in reader:
            try:
                value = float(row[3])  # Assume que 'value' está na quarta coluna
                writer.writerow(row)  # Escreve a linha no novo arquivo
            except ValueError:
                print(f"Valor inválido encontrado: {row[3]} não é um número.")

# Create bulk data profile
def create_bulkdata_profile(device_id,profile_number, name):
    if profile_number not in [1, 2, 3]:
        print("Invalid profile number. Please choose 1, 2, or 3.")
        return
    tree = f"Device.BulkData.Profile.{profile_number}"
    set_parameter_values(device_id, [[f"{tree}.Name", name], [f"{tree}.Enable", "true"]])
    refresh_device_parameter(device_id, tree)
    time.sleep(10)
    print(f"Bulk data profile '{name}' created successfully in profile slot {profile_number}.")

# See profiles
def see_profiles(device_id):
    acs.task_refresh_object(device_id, "Device.BulkData.Profile")
    for i in range(1, 4):
        try:
            value_status = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{i}.Enable")
            value_name = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{i}.Name")
            print(f"Current status of profile {value_name} ({i}): {value_status}")
        except Exception as e:
            print(f"Failed to get profile {i}: {str(e)}")

# Set bulk data profile parameter
def set_bulkdata_profile_parameter(device_id, profile_number, name, parameter):
    tree = f"Device.BulkData.Profile.{profile_number}.Parameter"
    try:
        refresh_device_parameter(device_id, tree)
        for i in range(1, 43):
            current_param = f"{tree}.{i}.Name"
            existing_value = acs.device_get_parameter(device_id, current_param)
            if not existing_value:
                set_parameter_values(device_id, [[current_param, name], [f"{tree}.{i}.Reference", parameter]])
                break
        time.sleep(10)
        saved_value = acs.device_get_parameter(device_id, f"{tree}.{i}.Reference")
        if saved_value == parameter:
            print(f"Parameter {i} set to '{parameter}' in profile {profile_number} successfully.")
        else:
            print(f"Failed to set parameter '{parameter}' in profile {profile_number}.")
    except Exception as e:
        print(f"Error setting parameter '{parameter}' in profile {profile_number}: {str(e)}")

# See bulk data parameters
def see_bulkdata_parameters(device_id, profile_number):
    for i in range(1, 43):
        try:
            value_name = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{i}.Name")
            if value_name:
                print(f"Parameter {i} in profile {profile_number}: {value_name}")
        except Exception as e:
            print(f"Failed to get parameter {i} in profile {profile_number}: {str(e)}")

# Get bulk data profile parameter value
def get_bulkdata_profile_parameter_value(device_id, profile_number, parameter_number):
    refresh_device_parameter(device_id, f"Device.BulkData.Profile.{profile_number}")
    value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{parameter_number}.Reference")
    value2 = acs.device_get_parameter(device_id, value)
    print(f'The value of parameter {value} is: {value2}')

# Get all bulk data profile parameter values
def get_all_bulkdata_profile_parameter_values(device_id, profile_number):
    last_param_index = 1
    while True:
        try:
            value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{last_param_index}.Reference")
            if not value:
                break
            value2 = acs.device_get_parameter(device_id, value)
            last_param_index += 1
        except Exception as e:
            print(f"Failed to get parameter {last_param_index} in profile {profile_number}: {str(e)}")
            break
    for i in range(1, last_param_index):  # Assuming there are up to 42 parameters, adjust the range as needed
        try:
            name_value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{i}.Name")
            if name_value:  # Check if the name is not empty or None
                value = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile_number}.Parameter.{i}.Reference")
                value2 = acs.device_get_parameter(device_id, value)
                print(f"Parameter {i}({value}) in profile {profile_number}: {value2}")
        except Exception as e:
            print(f"Failed to get parameter {i} in profile {profile_number}: {str(e)}")

# Config bulk data profile
def config_bulkdata_profile(device_id, profile_number, option, value):
    refresh_device_parameter(device_id, "Device.BulkData.Profile")
    option_map = {
        "URL": "Device.BulkData.Profile.{profile_number}.HTTP.URL",
        "Username": "Device.BulkData.Profile.{profile_number}.HTTP.Username",
        "Password": "Device.BulkData.Profile.{profile_number}.HTTP.Password"
    }
    if option in option_map:
        param = option_map[option].format(profile_number=profile_number)
        print(f"Changing {option} to {value}")
        set_parameter_values(device_id, [[param, value]])
        refresh_device_parameter(device_id, param)
        check_value = acs.device_get_parameter(device_id, param)
        if value == check_value:
            print(f"{option} has been changed successfully")
        else:
            print(f"Failed to change {option}")

def get_data_from_timescale(name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {name}")
    records = cursor.fetchall()
    for row in records:
        print(row)
    cursor.close()

def signal_strength(device_id):
    mac_host_list = []
    hostname_list = []
    refresh_device_parameter(device_id, "Device.WiFi.AccessPoint")
    refresh_device_parameter(device_id, "Device.Hosts")
    entries_host = acs.device_get_parameter(device_id, "Device.Hosts.HostNumberOfEntries")

    for k in range(1, entries_host + 1):
        hostname = acs.device_get_parameter(device_id, f"Device.Hosts.Host.{k}.HostName")
        mac_host = acs.device_get_parameter(device_id, f"Device.Hosts.Host.{k}.PhysAddress").lower()
        mac_host_list.append(mac_host)
        hostname_list.append(hostname)

    for i in range(1, 15):  # Get signal strength for each accesspoint of this device
        entries_ap = acs.device_get_parameter(device_id, f"Device.WiFi.AccessPoint.{i}.AssociatedDeviceNumberOfEntries")
        if entries_ap != 0:
            for j in range(1, entries_ap + 1):
                signal_strength = acs.device_get_parameter(device_id, f'Device.WiFi.AccessPoint.{i}.AssociatedDevice.{j}.SignalStrength')
                mac_associateddevice = acs.device_get_parameter(device_id, f"Device.WiFi.AccessPoint.{i}.AssociatedDevice.{j}.MACAddress")
                if mac_associateddevice is not None:
                    mac_associateddevice = mac_associateddevice.lower()
                    # Check if mac_host matches mac_associateddevice and replace with hostname
                    if mac_associateddevice in mac_host_list:
                        index = mac_host_list.index(mac_associateddevice)
                        hostname = hostname_list[index]
                    else:
                        hostname = "Unknown"

                    # Insert data into TimescaleDB
                    query = """
                    INSERT INTO signal_strength (time, device_id, mac_address, hostname, signal_strength)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    current_time = datetime.now()
                    execute_query(query, (current_time, str(device_id), str(mac_associateddevice), str(hostname), int(signal_strength)))
    print(f"Signal_Strength inserted successfully for {device_id}")

def neighboring_wifi(device_id):
    acs.task_set_parameter_values(device_id, [["Device.WiFi.NeighboringWiFiDiagnostic.DiagnosticsState", "Requested"]])
    refresh_device_parameter(device_id, "Device.WiFi.NeighboringWiFiDiagnostic")
    time.sleep(5)
    entries = acs.device_get_parameter(device_id, "Device.WiFi.NeighboringWiFiDiagnostic.ResultNumberOfEntries")
    for i in range(1, entries + 1):
        mac = acs.device_get_parameter(device_id, f"Device.WiFi.NeighboringWiFiDiagnostic.Result.{i}.BSSID")
        channel = acs.device_get_parameter(device_id, f"Device.WiFi.NeighboringWiFiDiagnostic.Result.{i}.Channel")
        frequencyband = acs.device_get_parameter(device_id, f"Device.WiFi.NeighboringWiFiDiagnostic.Result.{i}.OperatingFrequencyBand")
        channel_bandwidth = acs.device_get_parameter(device_id, f"Device.WiFi.NeighboringWiFiDiagnostic.Result.{i}.OperatingChannelBandwidth")
        signal_strength = acs.device_get_parameter(device_id, f"Device.WiFi.NeighboringWiFiDiagnostic.Result.{i}.SignalStrength")
        ssid = acs.device_get_parameter(device_id, f"Device.WiFi.NeighboringWiFiDiagnostic.Result.{i}.SSID")
        #print(f"{i}. Device: {device_id} MAC: {mac}, Channel: {channel}, Frequency Band: {frequencyband}, Channel Bandwidth: {channel_bandwidth}, Signal Strength: {signal_strength}, SSID: {ssid}")
        # Insert data into TimescaleDB
        query = """
        INSERT INTO neighboring_wifi (time, device_id, mac_address, channel, frequency_band, channel_bandwidth, ssid, signal_strength)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        current_time = datetime.now()
        execute_query(query, (current_time, str(device_id), str(mac), int(channel), str(frequencyband), str(channel_bandwidth), str(ssid), int(signal_strength)))
    print(f"Neighboring_wifi inserted successfully for {device_id}")
        
def loop(device):
    while True:
        x = 0.5
        y = 0.1
        time.sleep(x)
        signal_strength(device)
        time.sleep(x)
        neighboring_wifi(device)
        time.sleep(y)

#Main
print(devices)
threads = []
for device in devices[:4]:  # Assuming you want to parallelize for the first four devices
    t = threading.Thread(target=loop, args=(device,))
    threads.append(t)
    t.start()


for t in threads:
    t.join()
