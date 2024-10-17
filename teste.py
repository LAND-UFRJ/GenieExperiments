#!/usr/bin/python
# -*- coding: utf-8 -*-
# Usage examples for python-genieacs

import genieacs
import csv
import json

# Create a Connection object to interact with a GenieACS server
acs = genieacs.Connection("10.246.3.119", auth=True, user="admin", passwd="admin", port="7557")

# set a device_id for the following methods

devices = acs.device_get_all_IDs()
device_id = "5091E3-EX141-2237011003026"
obj = "Device"

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

def download_file_json():
    for device in devices:
        device_data = acs.device_get_by_id(device)
        json_file_path = f'/home/localuser/Documentos/VSCode/genieacs/device_data_{device}.json'
        with open(json_file_path, 'w') as json_file:
            json.dump(device_data, json_file, indent=4)
        print(f"Device data written to {json_file_path}")

def get_wifi_stats():
    for device in devices:
        parameters = []
        for i in range(1, 21):  # Assuming there are up to 10 associated devices, adjust the range as needed
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

    def download_wifi_stats_to_csv():
        with open('/home/localuser/Documentos/VSCode/genieacs/wifi_stats.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Device', 'Parameter', 'Value'])  # Write the header row
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
                    for param, value in output:
                        csvwriter.writerow([device, param, value])
        print("done wifi stats to csv")

    download_wifi_stats_to_csv()
#change_SSID("vasco", "flamengo")
#change_Password("12345678")
#download_file_json()
get_wifi_stats()
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

