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

devices = acs.device_get_all_IDs()

for idx, device in enumerate(devices, start=1):
    print(f"{idx}. {device}")

choice = int(input("Select the device by its number: ")) - 1
selected_device = devices[choice]
print(f"Selected device: {selected_device}")

bulkdata = acs.device_get_parameter_values(selected_device, [""])
