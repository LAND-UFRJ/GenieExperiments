import genieacs
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='')
load_dotenv(dotenv_path='')

acs = genieacs.Connection(
    ip=os.getenv("GENIE_IP"),
    auth=os.getenv("GENIE_AUTH") == "True",
    user=os.getenv("GENIE_USER"),
    passwd=os.getenv("GENIE_PASSWORD"),
    port=os.getenv("GENIE_PORT")
)

nginx_port=os.getenv("NGINX_PORT")
nginx_ip=os.getenv("NGINX_IP")

def config_profile(device, profile, alias, name, username, password, interval, ip, port):
    bulkdata_config = [
        {
            'name_path': "Device.BulkData.Enable",
            'name_value': "true"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.EncodingType",
            'name_value': "JSON"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.HTTP.Password",
            'name_value': f"{password}"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.HTTP.URL",
            'name_value': f"http://{ip}:{port}/bulkdata"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.HTTP.Method",
            'name_value': "POST"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.HTTP.UseDateHeader",
            'name_value': "true"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.HTTP.Username",
            'name_value': f"{username}"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.JSONEncoding.ReportFormat",
            'name_value': "ObjectHierarchy"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.JSONEncoding.ReportTimestamp",
            'name_value': "Unix-Epoch"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Name",
            'name_value': f"{name}"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Protocol",
            'name_value': "HTTP"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.ReportingInterval",
            'name_value': f"{interval}"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.X_TP_CollectInterval",
            'name_value': f"{interval}"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.TimeReference",
            'name_value': "0"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Alias",
            'name_value': f"{alias}"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Enable",
            'name_value': "true"
        }
    ]

    print(f"Configurando BulkData no dispositivo {device}")
    #print(bulkdata_config)
    
    # Iterate directly over bulkdata_config
    for parameter in bulkdata_config:
        # Pass the parameter as a list containing one list
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")
    
    print(f"Sucesso! BulkData configurado no profile {profile} do dispositivo {device}.")

def interfaces_wan_lan(device, profile, i):

    parameter_set = [
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Name",
            'name_value': "UpTime",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Reference",
            'reference_value': "Device.DeviceInfo.UpTime"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Name",
            'name_value': "Device_ID",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Reference",
            'reference_value': "Device.ManagementServer.ConnectionRequestUsername"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Name",
            'name_value': "Bytes Received WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.BytesReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Name",
            'name_value': "Bytes Sent WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.BytesSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Name",
            'name_value': "Packets Received WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.PacketsReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Name",
            'name_value': "Packets Sent WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.PacketsSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Name",
            'name_value': "Errors Received WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.ErrorsReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Name",
            'name_value': "Errors Sent WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.ErrorsSent"
        }
    ]
    unique_parameters = avoid_duplicate_parameters(device, profile, parameter_set)
    #print(f'Unique parameters: {unique_parameters}')
    for parameter in unique_parameters:
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")

def wifi_stats(device, profile, i):
    parameter_set = [
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Name",
            'name_value': "Signal Strength WiFi",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Reference",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.SignalStrength"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Name",
            'name_value': "Mac_WiFi",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Reference",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.MACAddress"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Name",
            'name_value': "HostName",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Reference",
            'reference_value': "Device.Hosts.Host.*.HostName"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Name",
            'name_value': "Mac_Host",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Reference",
            'reference_value': "Device.Hosts.Host.*.PhysAddress"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Name",
            'name_value': "Device_ID",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Reference",
            'reference_value': "Device.ManagementServer.ConnectionRequestUsername"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Name",
            'name_value': "Packets_Sent",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Reference",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.PacketsSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Name",
            'name_value': "Packets_Received",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Reference",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.PacketsReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Name",
            'name_value': "Bytes_Sent",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Reference",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.BytesSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 8}.Name",
            'name_value': "Bytes_Received",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 8}.Reference",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.BytesReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 9}.Name",
            'name_value': "Mac_Router",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 9}.Reference",
            'reference_value': "Device.WiFi.DataElements.Network.Device.1.Radio.*.BSS.2.BSSID"
        }
    ]

    unique_parameters = avoid_duplicate_parameters(device, profile, parameter_set)
    #print(f'Unique parameters: {unique_parameters}')
    for parameter in unique_parameters:
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")

def neighboring_wifi_config(device, profile, i):
    parameter_set = [
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Name",
            'name_value': "FrequencyBand_NBW",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Reference",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.OperatingFrequencyBand"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Name",
            'name_value': "ChannelBandwidth_NBW",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Reference",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.OperatingChannelBandwidth"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Name",
            'name_value': "SSID_NBW",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Reference",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.SSID"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Name",
            'name_value': "signal_strength_NBW",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Reference",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.SignalStrength"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Name",
            'name_value': "Mac_NBW",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Reference",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.BSSID"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Name",
            'name_value': "Channel_NBW",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Reference",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.Channel"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Name",
            'name_value': "Device_ID",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Reference",
            'reference_value': "Device.ManagementServer.ConnectionRequestUsername"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Name",
            'name_value': "Mac_Router",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Reference",
            'reference_value': "Device.WiFi.DataElements.Network.Device.1.Radio.*.BSS.2.BSSID"
        }
    ]

    unique_parameters = avoid_duplicate_parameters(device, profile, parameter_set)
    #print(f'Unique parameters: {unique_parameters}')
    for parameter in unique_parameters:
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")

def management_and_stats_parameters(device, profile, i):

    parameter_set = [
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Name",
            'name_value': "Device_ID",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i}.Reference",
            'reference_value': "Device.ManagementServer.ConnectionRequestUsername"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Name",
            'name_value': "UpTime",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Reference",
            'reference_value': "Device.DeviceInfo.UpTime"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Name",
            'name_value': "Bytes Sent WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.BytesSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Name",
            'name_value': "Bytes Received WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.BytesReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Name",
            'name_value': "Packets Sent WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.PacketsSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Name",
            'name_value': "Packets Received WAN/LAN",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Reference",
            'reference_value': "Device.IP.Interface.*.Stats.PacketsReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Name",
            'name_value': "Bytes Sent WiFi 2.4GHz/5GHz",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Reference",
            'reference_value': "Device.WiFi.Radio.*.Stats.BytesSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Name",
            'name_value': "Bytes Received WiFi 2.4GHz/5GHz",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Reference",
            'reference_value': "Device.WiFi.Radio.*.Stats.BytesReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 8}.Name",
            'name_value': "Packets Sent WiFi 2.4GH/5GHz",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 8}.Reference",
            'reference_value': "Device.WiFi.Radio.*.Stats.PacketsSent"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 9}.Name",
            'name_value': "Packets Received WiFi 2.4GHz/5GHz",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 9}.Reference",
            'reference_value': "Device.WiFi.Radio.*.Stats.PacketsReceived"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 10}.Name",
            'name_value': "WiFi Channel 2.4GHz/5GHz",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 10}.Reference",
            'reference_value': "Device.WiFi.Radio.*.Channel"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 11}.Name",
            'name_value': "Current Channel Bandwidth 2.4GHz/5GHz",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 11}.Reference",
            'reference_value': "Device.WiFi.Radio.*.CurrentOperatingChannelBandwidth"
        },
        {
            'name_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 12}.Name",
            'name_value': "WiFi SSID 2.4GHz/5GHz",
            'reference_path': f"Device.BulkData.Profile.{profile}.Parameter.{i + 12}.Reference",
            'reference_value': "Device.WiFi.SSID.*.SSID"
        }
    ]
    
    unique_parameters = avoid_duplicate_parameters(device, profile, parameter_set)
    #print(f'Unique parameters: {unique_parameters}')
    for parameter in unique_parameters:
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")

def compare_parameter_sets(parameter_set1, parameter_set2):
    reference_to_name1 = {param['reference_value']: param['name_value'] for param in parameter_set1}
    reference_to_name2 = {param['reference_value']: param['name_value'] for param in parameter_set2}

    differences = []
    for reference, name in reference_to_name1.items():
        if reference in reference_to_name2 and reference_to_name2[reference] != name:
            differences.append((reference, name, reference_to_name2[reference]))
    print(f"Differences: {differences}")
    return differences
#Selecionando um profile
def select_profile(device_id):
    profile_choices = []

    for idx in range(1, 9):
        profile_name = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{idx}.Name")
        profile_choices.append((idx, profile_name))

    print("Profiles:")
    for jdx, (profile_idx, profile_name) in enumerate(profile_choices, start=1):
        print(f"{jdx}. Profile {profile_name} - {profile_idx}")

    profile_choice = int(input("Select the profile by its number: ")) - 1
    selected_profile = profile_choices[profile_choice]
    print(f"Selected profile: {selected_profile}")

    return selected_profile

#Selecionando o primeiro parametro
def first_empty_parameter(device_id, profile):
    empty_parameters = []

    for idx in range(1, 108):
        param_name = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Name")
        if not param_name:
            empty_parameters.append(idx)
    """
    print("Empty parameters:")
    for param in empty_parameters:
            print(f"Parameter {param}")

    if not empty_parameters:
        print("No empty parameters available.")
        return None
    """
    #param_choice = int(input("Select the empty parameter by its number: "))
    #selected_parameter = empty_parameters[param_choice - 1]
    first_parameter = empty_parameters[0]
    print(f"Selected empty parameter: {first_parameter}")

    return first_parameter
    
#Vendo os Parametros
def see_parameters(device_id, profile):
    for idx in range(1, 108):
        param_name = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Name")
        if param_name:
            print(f"Profile {profile}: Parameter {idx}: {param_name}")

def avoid_duplicate_parameters(device, profile, parameter_set):
    existing_parameters = set()
    for idx in range(1, 108):  # Adjust the range if necessary
        param_name = acs.device_get_parameter(device, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Name")
        param_reference = acs.device_get_parameter(device, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Reference")
        if param_name and param_reference:
            existing_parameters.add((param_name, param_reference))
    
    unique_parameters = []
    for param in parameter_set:
        if (param['name_value'], param['reference_value']) not in existing_parameters:
            unique_parameters.append([param['name_path'], param['name_value']])
            unique_parameters.append([param['reference_path'], param['reference_value']])
        else:
            print(f"Duplicate parameter {param['name_value']} ({param['reference_value']}) detected and avoided.")
    
    return unique_parameters

#Inicio do código

devices = acs.device_get_all_IDs()

#Selecionando o dispositivo
for idx, device in enumerate(devices, start=1):
    print(f"{idx}. {device}")

choice = int(input("Select the device by its number: ")) - 1
selected_device = devices[choice]
print(f"Selected device: {selected_device}")

selected_profile = select_profile(selected_device)


