import genieacs
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='')

acs = genieacs.Connection(
    ip=os.getenv("GENIE_IP"),
    auth=os.getenv("GENIE_AUTH") == "True",
    user=os.getenv("GENIE_USER"),
    passwd=os.getenv("GENIE_PASSWORD"),
    port=os.getenv("GENIE_PORT")
)

nginx_port=os.getenv("NGINX_PORT")

def config_profile(device, profile, alias, name, password, username, interval, ip, port):
    bulkdata_config = [
        ["Device.BulkData.Enable", "true"],
        [f"Device.BulkData.Profile.{profile}.EncodingType", "JSON"],
        [f"Device.BulkData.Profile.{profile}.HTTP.Password", f"{password}"],
        [f"Device.BulkData.Profile.{profile}.HTTP.URL", f"http://{ip}:{port}/bulkdata"],
        [f"Device.BulkData.Profile.{profile}.HTTP.Method", "POST"],
        [f"Device.BulkData.Profile.{profile}.HTTP.UseDateHeader", "true"],
        [f"Device.BulkData.Profile.{profile}.HTTP.Username", f"{username}"],
        [f"Device.BulkData.Profile.{profile}.JSONEncoding.ReportFormat", "ObjectHierarchy"],
        [f"Device.BulkData.Profile.{profile}.JSONEncoding.ReportTimestamp", "Unix-Epoch"],
        [f"Device.BulkData.Profile.{profile}.Name", f"{name}"],
        [f"Device.BulkData.Profile.{profile}.Protocol", "HTTP"],
        [f"Device.BulkData.Profile.{profile}.ReportingInterval", f"{interval}"],
        [f"Device.BulkData.Profile.{profile}.X_TP_CollectInterval", f"{interval}"],
        [f"Device.BulkData.Profile.{profile}.TimeReference", "0"],
        [f"Device.BulkData.Profile.{profile}.Alias", f"{alias}"],
        [f"Device.BulkData.Profile.{profile}.Enable", "true"]
    ]

    print(f"Configurando BulkData no dispositivo {device}")
    print(bulkdata_config)
    
    # Iterate directly over bulkdata_config
    for parameter in bulkdata_config:
        # Pass the parameter as a list containing one list
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")
    
    print(f"Sucesso! BulkData configurado no profile {profile} do dispositivo {device}.")

def interfaces_wan_lan(device, profile, i):

    interface_set_parameters = [
        [f"Device.BulkData.Profile.{profile}.Parameter.{i}.Name", "Tempo Real para Teste"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i}.Reference", "Device.DeviceInfo.UpTime"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Name", "DeviceID2"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Reference", "Device.ManagementServer.ConnectionRequestUsername"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Name", "BytesReceived WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Reference", "Device.IP.Interface.*.Stats.BytesReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Name", "Bytes Sent WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Reference", "Device.IP.Interface.*.Stats.BytesSent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Name", "Packets Received WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Reference", "Device.IP.Interface.*.Stats.PacketsReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Name", "Packets Sent WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Reference", "Device.IP.Interface.*.Stats.PacketsSent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Name", "Errors Received WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Reference", "Device.IP.Interface.*.Stats.ErrorsReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Name", "Errors Sent WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Reference", "Device.IP.Interface.*.Stats.ErrorsSent"]
    ]

    for parameter in interface_set_parameters:
        # Pass the parameter as a list containing one list
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")

def wifi_stats(device, profile, i):
    wifi_set_parameters = [
        [f"Device.BulkData.Profile.{profile}.Parameter.{i}.Name", "Signal Strength WiFi"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i}.Reference", "Device.WiFi.AccessPoint.*.AssociatedDevice.*.SignalStrength"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Name", "MacAddress WiFi"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Reference", "Device.WiFi.AccessPoint.*.AssociatedDevice.*.MACAddress"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Name", "HostName"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Reference", "Device.Hosts.Host.*.HostName"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Name", "MacAddress Host"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Reference", "Device.Hosts.Host.*.PhysAddress"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Name", "DeviceID"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Reference", "Device.ManagementServer.ConnectionRequestUsername"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Name", "Packets_Sent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Reference", "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.PacketsSent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Name", "Packets_Received"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Reference", "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.PacketsReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Name", "Bytes_Sent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Reference", "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.BytesSent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 8}.Name", "Bytes_Received"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 8}.Reference", "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.BytesReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 9}.Name", "mac"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 9}.Reference", "Device.WiFi.DataElements.Network.Device.1.Radio.*.BSS.2.BSSID"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 10}.Name", "BytesReceived WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 10}.Reference", "Device.IP.Interface.*.Stats.BytesReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 11}.Name", "Bytes Sent WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 11}.Reference", "Device.IP.Interface.*.Stats.BytesSent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 12}.Name", "Packets Received WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 12}.Reference", "Device.IP.Interface.*.Stats.PacketsReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 13}.Name", "Packets Sent WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 13}.Reference", "Device.IP.Interface.*.Stats.PacketsSent"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 14}.Name", "Errors Received WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 14}.Reference", "Device.IP.Interface.*.Stats.ErrorsReceived"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 15}.Name", "Erros Sent WAN/LAN"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 15}.Reference", "Device.IP.Interface.*.Stats.ErrorsSent"],
    ]

    for parameter in wifi_set_parameters:
        # Pass the parameter as a list containing one list
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")

def neighboring_wifi_config(device, profile, i):
    nbw_set_parameters = [
        [f"Device.BulkData.Profile.{profile}.Parameter.{i}.Name", "FrequencyBand_nbw"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i}.Reference", "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.OperatingFrequencyBand"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Name", "ChannelBandwidth_nbw"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 1}.Reference", "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.OperatingChannelBandwidth"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Name", "SSID_nbw"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 2}.Reference", "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.SSID"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Name", "signal_strength_nbw"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 3}.Reference", "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.SignalStrength"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Name", "Mac_Address_nbw"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 4}.Reference", "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.BSSID"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Name", "Channel_nbw"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 5}.Reference", "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.Channel"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Name", "Device_ID"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 6}.Reference", "Device.ManagementServer.ConnectionRequestUsername"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Name", "MacAddress router"],
        [f"Device.BulkData.Profile.{profile}.Parameter.{i + 7}.Reference", "Device.WiFi.DataElements.Network.Device.1.Radio.*.BSS.2.BSSID"]
    ]

    for parameter in nbw_set_parameters:
        # Pass the parameter as a list containing one list
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")

#Selecionando um profile
def select_profile(device_id):
    profile_choices = []

    for idx in range(1, 9):
        profiles = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{idx}.Enable")
        names = acs.device_get_parameter(device_id, f"Device.BulkData.Profile.{idx}.Name")
        if profiles:
            profile_choices.append((idx, names))
        print(f'Profile {idx} - {names}: {profiles}')

    print("Available profiles:")
    for jdx, (profile_idx, profile_name) in enumerate(profile_choices, start=1):
        print(f"{jdx}. Profile {profile_name} - {profile_idx}")

    profile_choice = int(input("Select the profile by its number: ")) - 1
    selected_profile = profile_choices[profile_choice]
    print(f"Selected profile: {selected_profile}")

    return selected_profile

#Vendo os Parametros
def see_parameters(device_id, profile):
    for idx in range(1, 108):
        parameters = [acs.device_get_parameters(device_id, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Name")]
        #print(parameters)
        for param_idx, param_name in enumerate(parameters, start=1):
            if param_name:
                param_value = param_name.get('Device', {}).get('BulkData', {}).get('Profile', {}).get(str(profile), {}).get('Parameter', {}).get(str(idx), {}).get('Name')
                if param_value:
                    print(f"Profile {profile}: Parameter {idx}: {param_value}")

#Inicio do código

devices = acs.device_get_all_IDs()
#Selecionando o dispositivo

for idx, device in enumerate(devices, start=1):
    print(f"{idx}. {device}")

choice = int(input("Select the device by its number: ")) - 1
selected_device = devices[choice]
print(f"Selected device: {selected_device}")
selected_profile = select_profile(selected_device)
see_parameters(selected_device, selected_profile[0])


