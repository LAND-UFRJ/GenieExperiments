import genieacs
import time
from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv

# Loading .env file
load_dotenv(dotenv_path='')


# Create a Connection object to interact with a GenieACS server
acs = genieacs.Connection(
    ip=os.getenv("GENIE_IP"),
    auth=os.getenv("GENIE_AUTH") == "True",
    user=os.getenv("GENIE_USER"),
    passwd=os.getenv("GENIE_PASSWORD"),
    port=os.getenv("GENIE_PORT")
)

devices = acs.device_get_all_IDs()  # Get all devices available
print(devices)

def process_device(device):
    z = 0
    while True:
        acs.task_set_parameter_values(device, [["Device.WiFi.NeighboringWiFiDiagnostic.DiagnosticsState", "Requested"]])
        acs.task_refresh_object(device, "Device.WiFi.NeighboringWiFiDiagnostic")
        z += 1
        print(f"Device: {device}, Iteration: {z}, Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(300)

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(process_device, device) for device in devices[:2]]
    for future in futures:
        future.result()
 
