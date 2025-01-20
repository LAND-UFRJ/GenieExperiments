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
    #print(type(bulkdata_config))
    formatted_list = [[item['name_path'], item['name_value']] for item in bulkdata_config]

    # Iterate directly over bulkdata_config
    for parameter in formatted_list:
        # Pass the parameter as a list containing one list
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")
    
    print(f"Sucesso! BulkData configurado no profile {profile} do dispositivo {device}.")
  
def dispositivos_conectados(device, profile, i):
    parameter_set = [
        {
            'name_value': "Signal Strength WiFi",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.SignalStrength"
        },
        {
            'name_value': "Mac_AD_WiFi",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.MACAddress"
        },
        {
            'name_value': "HostName",
            'reference_value': "Device.Hosts.Host.*.HostName"
        },
        {
            'name_value': "Mac_Host",
            'reference_value': "Device.Hosts.Host.*.PhysAddress"
        },
        {
            'name_value': "Device_ID",
            'reference_value': "Device.ManagementServer.ConnectionRequestUsername"
        },
        {
            'name_value': "Packets_Sent",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.PacketsSent"
        },
        {
            'name_value': "Packets_Received",
            'reference_value': "Device.WiFi.AccessPoint.*.AssociatedDevice.*.Stats.PacketsReceived"
        },
        {
            'name_value': "Mac_Router",
            'reference_value': "Device.WiFi.DataElements.Network.Device.1.Radio.*.BSS.2.BSSID"
        }
    ]

    existing_parameters = avoid_duplicate_parameters(device, profile, parameter_set)
    
    current_index = i  # Usar para ajustar os índices dinamicamente
    for param in parameter_set:
        if (param['name_value'], param['reference_value']) not in existing_parameters:
            # Configura o parâmetro com índice atualizado
            parameter_name_path = f"Device.BulkData.Profile.{profile}.Parameter.{current_index}.Name"
            parameter_reference_path = f"Device.BulkData.Profile.{profile}.Parameter.{current_index}.Reference"
            
            acs.task_set_parameter_values(device, [
                [parameter_name_path, param['name_value']],
                [parameter_reference_path, param['reference_value']]
            ])
            print(f"Sucesso! {param['name_value']} configurado no profile {profile} do dispositivo {device}.")
            
            current_index += 1  # Atualiza o índice apenas para parâmetros configurados
        else:
            print(f"Parâmetro duplicado {param['name_value']} ({param['reference_value']}) detectado e ignorado.")

def neighboring_wifi_config(device, profile, i):
    parameter_set = [
        {
            'name_value': "FrequencyBand_NBW",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.OperatingFrequencyBand"
        },
        {
            'name_value': "ChannelBandwidth_NBW",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.OperatingChannelBandwidth"
        },
        {
            'name_value': "SSID_NBW",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.SSID"
        },
        {
            'name_value': "signal_strength_NBW",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.SignalStrength"
        },
        {
            'name_value': "Mac_NBW",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.BSSID"
        },
        {
            'name_value': "Channel_NBW",
            'reference_value': "Device.WiFi.NeighboringWiFiDiagnostic.Result.*.Channel"
        },
        {
            'name_value': "Device_ID",
            'reference_value': "Device.ManagementServer.ConnectionRequestUsername"
        },
        {
            'name_value': "Mac_Router",
            'reference_value': "Device.WiFi.DataElements.Network.Device.1.Radio.*.BSS.2.BSSID"
        }
    ]

    existing_parameters = avoid_duplicate_parameters(device, profile, parameter_set)
    
    current_index = i  # Usar para ajustar os índices dinamicamente
    for param in parameter_set:
        if (param['name_value'], param['reference_value']) not in existing_parameters:
            # Configura o parâmetro com índice atualizado
            parameter_name_path = f"Device.BulkData.Profile.{profile}.Parameter.{current_index}.Name"
            parameter_reference_path = f"Device.BulkData.Profile.{profile}.Parameter.{current_index}.Reference"
            
            acs.task_set_parameter_values(device, [
                [parameter_name_path, param['name_value']],
                [parameter_reference_path, param['reference_value']]
            ])
            print(f"Sucesso! {param['name_value']} configurado no profile {profile} do dispositivo {device}.")
            
            current_index += 1  # Atualiza o índice apenas para parâmetros configurados
        else:
            print(f"Parâmetro duplicado {param['name_value']} ({param['reference_value']}) detectado e ignorado.")

def dados(device, profile, i):
    parameter_set = [
        {'name_value': "Device_ID", 'reference_value': "Device.ManagementServer.ConnectionRequestUsername"},
        {'name_value': "UpTime", 'reference_value': "Device.DeviceInfo.UpTime"},
        {'name_value': "Bytes Sent WAN/LAN", 'reference_value': "Device.IP.Interface.*.Stats.BytesSent"}
    ]
    
    # Filtra parâmetros únicos
    unique_parameters = avoid_duplicate_parameters(device, profile, parameter_set)
    
    # Escreve os parâmetros únicos
    current_index = i  # Índice inicial
    for param in unique_parameters:
        # Gera os paths dinamicamente
        parameter_name_path = f"Device.BulkData.Profile.{profile}.Parameter.{current_index}.Name"
        parameter_reference_path = f"Device.BulkData.Profile.{profile}.Parameter.{current_index}.Reference"
        
        acs.task_set_parameter_values(device, [
            [parameter_name_path, param['name_value']],
            [parameter_reference_path, param['reference_value']]
        ])
        print(f"Sucesso! {param['name_value']} configurado no profile {profile} do dispositivo {device}.")
        
        # Incrementa o índice apenas para parâmetros escritos
        current_index += 1

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

    # Descobre os índices já configurados no dispositivo
    idx = 1
    while True:
        param_name = acs.device_get_parameter(device, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Name")
        param_reference = acs.device_get_parameter(device, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Reference")
        
        if not param_name and not param_reference:
            break  # Para quando não houver mais parâmetros configurados
        
        if param_name and param_reference:
            existing_parameters.add((param_name, param_reference))
        
        idx += 1

    # Filtra os parâmetros únicos
    unique_parameters = []
    for param in parameter_set:
        if (param['name_value'], param['reference_value']) not in existing_parameters:
            unique_parameters.append(param)
        else:
            print(f"Parâmetro duplicado {param['name_value']} ({param['reference_value']}) detectado e ignorado.")
    
    return unique_parameters

def clear_bulkdata(device, profile):
    config_profile(device, profile, "", "", "", "", 60, "", "" )
    acs.task_set_parameter_values(device, [[f"Device.BulkData.Profile.{profile}.Enable", "false"]])

    print(f"BulkData disabled for profile {profile}.")
    for idx in range(1, 108):
        #if not acs.device_get_parameter(device, f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Name"):
            acs.task_set_parameter_values(device, [[f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Name", ""]])
            acs.task_set_parameter_values(device, [[f"Device.BulkData.Profile.{profile}.Parameter.{idx}.Reference", ""]])
            print(f"Parameter {idx} deleted.")
    print(f"Profile {profile} cleared.")

#Inicio do código22

devices = acs.device_get_all_IDs()

#Selecionando o dispositivo
for idx, device in enumerate(devices, start=1):
    print(f"{idx}. {device}")

choice = int(input("Select the device by its number: ")) - 1
selected_device = devices[choice]
print(f"Selected device: {selected_device}")

selected_profile = select_profile(selected_device)

#config_profile(selected_device, selected_profile[0], "Collection of Dados", "Dados", "land", "landufrj123", 60, nginx_ip, nginx_port)

#dados(selected_device, selected_profile[0], first_empty_parameter(selected_device, selected_profile[0]))

#neighboring_wifi_config(selected_device, selected_profile[0], first_empty_parameter(selected_device, selected_profile[0]))

#dispositivos_conectados(selected_device, selected_profile[0], first_empty_parameter(selected_device, selected_profile[0]))

#clear_bulkdata(selected_device, selected_profile[0])

#see_parameters(selected_device, selected_profile[0])

#dispositivos_conectados(selected_device, selected_profile[0], first_empty_parameter(selected_device, selected_profile[0]))

