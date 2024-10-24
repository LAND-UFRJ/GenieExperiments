import genieacs
import psycopg2
import json
import time

# Conectar ao GenieACS
print("Conectando ao GenieACS...")
acs = genieacs.Connection("10.246.3.119", auth=True, user="admin", passwd="admin", port="7557")

# Conectar ao TimescaleDB
print("Conectando ao TimescaleDB...")
conn = psycopg2.connect(
    host="10.246.3.111",
    database="otherparameters",
    user="postgres",
    password="landufrj123"
)

cur = conn.cursor()

# Criar tabela TimescaleDB (sem restrições de unicidade)
print("Criando tabela TimescaleDB...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS device_data (
        id SERIAL PRIMARY KEY,
        device_id TEXT,
        data JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")
conn.commit()

print("Tabela criada com sucesso ou já existia.")

# Definir o ID do dispositivo específico
specific_device_id = "5091E3-EX141-2237011003026"  # Substitua pelo ID real do dispositivo desejado
interval = 1  # Intervalo em segundos (30 segundos)

def refresh_parameter_in_genieacs():
    print(f"Atualizando parâmetro para o dispositivo {specific_device_id}...")
    task_id = acs.task_refresh_object(specific_device_id, "Device.WiFi.MultiAP.APDevice.1.Radio.1.AP.2.AssociatedDevice.4.SignalStrength")
    print(f"Parâmetro atualizado com sucesso. Task ID: {task_id}")

def download_signal_strength_to_timescale():
    print(f"Coletando dados de intensidade de sinal para dispositivo {specific_device_id}...")
    signal_strength = acs.device_get_parameter(specific_device_id, "Device.WiFi.MultiAP.APDevice.1.Radio.1.AP.2.AssociatedDevice.4.SignalStrength")
    
    # Verifique a estrutura dos dados coletados
    print("Dados coletados (json):", signal_strength)

    if not signal_strength:
        print(f"Nenhum dado de intensidade de sinal coletado para o dispositivo {specific_device_id}")
        return

    # Certifique-se de que o dado não é nulo ou vazio
    if signal_strength is None or signal_strength == "":
        print("Dado de intensidade de sinal está vazio.")
        return

    try:
        # Inserir dados no TimescaleDB acumulando-os
        formatted_data = json.dumps({"Device.WiFi.MultiAP.APDevice.1.Radio.1.AP.2.AssociatedDevice.4.SignalStrength": signal_strength})
        cur.execute("""
            INSERT INTO device_data (device_id, data, created_at)
            VALUES (%s, %s, NOW());
        """, (specific_device_id, formatted_data))
        conn.commit()
        print(f"Signal strength data inserted for {specific_device_id}")
    except Exception as e:
        print(f"Erro ao inserir dados de intensidade de sinal para o dispositivo {specific_device_id}: {e}")

# Loop para atualização contínua
try:
    while True:
        refresh_parameter_in_genieacs()  # Atualizar parâmetro no GenieACS
        download_signal_strength_to_timescale()  # Coletar e inserir dados no TimescaleDB
        time.sleep(interval)
except KeyboardInterrupt:
    print("Interrompido pelo usuário. Fechando conexões.")

# Fechar a conexão com TimescaleDB
cur.close()
conn.close()









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

