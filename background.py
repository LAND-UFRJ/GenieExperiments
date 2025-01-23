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

def fetch_devices():
    global devices
    devices = acs.device_get_all_IDs()  # Get all devices available
    print(devices)

# Schedule the fetch_devices function to run every 20 minutes
schedule.every(20).minutes.do(fetch_devices)

# Initial fetch
fetch_devices()

# Start the scheduler in a separate thread
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

scheduler_thread = ThreadPoolExecutor(max_workers=1)
scheduler_thread.submit(run_scheduler)

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
 
