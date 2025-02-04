import genieacs
import time
from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv
import schedule

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

# Variável global para armazenar os dispositivos
devices = []

def fetch_devices():
    global devices
    new_devices = acs.device_get_all_IDs()  # Obtém todos os dispositivos disponíveis
    if new_devices != devices:
        print("Dispositivos atualizados:", new_devices)
        devices = new_devices  # Atualiza a lista de dispositivos
    else:
        print("Nenhuma mudança nos dispositivos.")

# Agenda a função fetch_devices para ser executada a cada 20 minutos
schedule.every(20).minutes.do(fetch_devices)

# Execução inicial
fetch_devices()

# Inicia o agendador em uma thread separada
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

scheduler_thread = Thread(target=run_scheduler)
scheduler_thread.daemon = True
scheduler_thread.start()

# Função para processar um dispositivo
def set_neighboring(device):
    z = 0
    while True:
        acs.task_set_parameter_values(device, [["Device.WiFi.NeighboringWiFiDiagnostic.DiagnosticsState", "Requested"]])
        acs.task_refresh_object(device, "Device.WiFi.NeighboringWiFiDiagnostic")
        z += 1
        print(f"Set NeighBoring to Requested | Device: {device} | Iteração: {z} | Horário: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(360)

# Função para gerenciar o ThreadPoolExecutor com base na lista atualizada de dispositivos
def manage_device_processing():
    global devices
    executor = ThreadPoolExecutor(max_workers=4)
    futures = []

    while True:
        current_devices = devices[:4]  # Limita ao máximo de 4 dispositivos
        # Note: Each device creates 2 tasks so the expected length of futures is 2 * len(current_devices)
        if len(futures) != 2 * len(current_devices) or any(future.done() for future in futures):
            # Cancela as tarefas antigas se a lista de dispositivos mudar
            for future in futures:
                future.cancel()
            futures = (
                [executor.submit(set_neighboring, device) for device in current_devices] +
                [executor.submit(check_variable, device) for device in current_devices]
            )
        time.sleep(300)  # Verifica a cada minuto se a lista de dispositivos mudou

# Função para verificar a variável a cada hora
def check_variable(device):
    acs.task_refresh_object(device, "Device.DeviceInfo.UpTime")
    while True:
        uptime = acs.device_get_parameter(device, "Device.DeviceInfo.UpTime")
        if uptime < 3600:
            print(f"Reboot | Device: {device} | Uptime: {uptime} | Horário: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            for idx in range(1, 9):
                profile_name = acs.device_get_parameter(device, f"Device.BulkData.Profile.{idx}.Name")
                if profile_name == 'NeighboringWiFi':
                    profile = idx
                    interval = 1800
                elif profile_name == 'Dispositivos Conectados & Dados':
                    profile = idx
                    interval = 60
                else:
                    print(f"Nenhum profile atualizado para {device}.")
                    continue
                config = [
                    {
                        'name_path': f"Device.BulkData.Profile.{profile}.Enable",
                        'name_value': "true"
                    },
                    {
                        'name_path': f"Device.BulkData.Profile.{profile}.ReportingInterval",
                        'name_value': f"{interval}"
                    },
                    {
                        'name_path': f"Device.BulkData.Profile.{profile}.X_TP_CollectInterval",
                        'name_value': f"{interval}"
                    }
                ]
                formatted_list = [[item['name_path'], item['name_value']] for item in config]
                for parameter in formatted_list:
                    acs.task_set_parameter_values(device, [parameter])
                    print(f"Sucesso! {parameter[0]} configurado no profile {profile} do dispositivo {device}.")
        else:
            print(f"No Reboot | Device: {device} | Uptime: {uptime} | Horário: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(3600)

# Inicia o gerenciador de processamento de dispositivos em uma thread separada
device_manager_thread = Thread(target=manage_device_processing)
device_manager_thread.daemon = True
device_manager_thread.start()


# Mantém o programa principal em execução
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Programa interrompido.")
 
