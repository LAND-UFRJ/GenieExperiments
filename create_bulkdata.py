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

def config_profile(device, profile, alias, name, password, username, interval):
    bulkdata_config = [
        ["Device.BulkData.Enable", "true"],
    ]
    
    bulkdata_set_profile = [
        [f"Device.BulkData.Profile.{profile}.EncodingType", "JSON"],
        [f"Device.BulkData.Profile.{profile}.HTTP.Password", f"{password}"],
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
    
    # Extend instead of append to flatten the list
    bulkdata_config.extend(bulkdata_set_profile)
    
    print(f"Configurando BulkData no dispositivo {device}")
    print(bulkdata_config)
    
    # Iterate directly over bulkdata_config
    for parameter in bulkdata_config:
        # Pass the parameter as a list containing one list
        acs.task_set_parameter_values(device, [parameter])
        print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")
    
    print(f"Sucesso! BulkData configurado no profile {profile} do dispositivo {device}.")


devices = acs.device_get_all_IDs()

for idx, device in enumerate(devices, start=1):
    print(f"{idx}. {device}")

choice = int(input("Select the device by its number: ")) - 1
selected_device = devices[choice]
print(f"Selected device: {selected_device}")

config_profile(selected_device, "2", "teste do código", "teste1", "passwordteste1", "usertest1", "1000")

