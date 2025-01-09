from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, ValidationError
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
from redis import ConnectionPool
import redis
import uvicorn
import logging
import os
from dotenv import load_dotenv

# Configurações iniciais
load_dotenv(dotenv_path='')

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_SOCKET_TIMEOUT = 10
REDIS_DECODE_RESPONSES = True
REDIS_MAX_CONNECTIONS = 10

pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    socket_timeout=REDIS_SOCKET_TIMEOUT,
    decode_responses=REDIS_DECODE_RESPONSES,
    max_connections=REDIS_MAX_CONNECTIONS
)

redis_client = redis.Redis(connection_pool=pool)

try:
    redis_client.ping()
    logger.info("Conexão com Redis bem-sucedida.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Erro de conexão com Redis: {e}")
    raise HTTPException(status_code=500, detail="Conexão com Redis falhou.")


# Modelo Pydantic para validar os dados recebidos
class DeviceData(BaseModel):
    CollectionTime: int
    Device: Dict[str, Any]

class BulkData(BaseModel):
    Report: List[DeviceData]

# Helper function to safely get nested dictionary values
def safe_get(dictionary: Dict, path: List[str], default=None):
    for key in path:
        if not isinstance(dictionary, dict):
            return default
        dictionary = dictionary.get(key, default)
    return dictionary

# Processamento dos dados
@app.post("/bulkdata")
async def receive_bulkdata(request: Request):
    try:
        print("Recebendo dados...")
        body = await request.json()
        print(f"Dados recebidos: {body}")
        data = BulkData(**body)  # Validação automática via Pydantic
        
        nbw_records, testtable_records, wifistats_records, interface_wan_records, interface_lan_records = process_data(data)
        
        print(f"Registros WiFi processados: {nbw_records}")
        print(f"Registros TestTable processados: {testtable_records}")
        print(f"Registros WiFi Stats processados: {wifistats_records}")
        print(f"Registros Interface WAN processados: {interface_wan_records}")
        print(f"Registros Interface LAN processados: {interface_lan_records}")
        
        store_data_in_redis(nbw_records, "redes_proximas", "redes_proximas_stream", ["detected_at", "device_id", "bssid_router", "bssid_rede", "signal_strength", "ssid_rede", "channel", "channel_bandwidth"])
        store_data_in_redis(testtable_records, "testtable", "testtable_stream", ["device_id", "uptime"])
        store_data_in_redis(wifistats_records, "wifidata", "wifidata_stream", ["detected_at", "device_id", "mac_address_ap", "hostname", "signal_strength", "packets_sent", "packets_received"])
        store_data_in_redis(interface_wan_records, "interface_wan", "interface_wan_stream", ["time", "device_id", "wan_packets_received", "wan_bytes_received", "wan_bytes_per_packets_received", "wan_packets_sent", "wan_bytes_sent", "wan_bytes_per_packets_sent", "wan_errors_sent", "wan_errors_received"])
        store_data_in_redis(interface_lan_records, "interface_lan", "interface_lan_stream", ["time", "device_id", "lan_packets_received", "lan_bytes_received", "lan_bytes_per_packets_received", "lan_packets_sent", "lan_bytes_sent", "lan_bytes_per_packets_sent", "lan_errors_sent", "lan_errors_received"])
        return {"message": "Dados processados e enviados ao Redis com sucesso."}

    except ValidationError as e:
        print(f"Erro de validação: {e}")
        raise HTTPException(status_code=400, detail=f"Erro de validação: {e}")
    except Exception as e:
        print(f"Erro no processamento: {e}")
        raise HTTPException(status_code=500, detail=f"Erro no processamento: {e}")

def process_data(data: BulkData) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Processa os dados recebidos e separa em duas listas:
    - registros de redes próximas (nbw_records)
    - registros de tabela testtable (testtable_records)

    Retorna:
    Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]
    """
    nbw_records = []
    testtable_records = []
    wifistats_records = []
    interface_wan_records = []
    interface_lan_records = []

    for item in data.Report:
        if not isinstance(item.Device, dict):
            print(f"Erro: item.Device não é um dicionário: {item.Device}")
            continue
        
        collection_time = datetime.fromtimestamp(item.CollectionTime, timezone.utc)
        device_id = safe_get(item.Device, ["ManagementServer", "ConnectionRequestUsername"], "unknown")
        
        # Processa dados para tabela testtable
        test_record = process_test_data(device_id, item)
        if test_record:
            testtable_records.append(test_record)
        
        # Processa dados do WiFi Neighboring
        wifi_results = process_neighboring_wifi(item, collection_time, device_id, [])
        nbw_records.extend(wifi_results)
        
        # Processa dados do WiFi Stats
        wifi_stats_results = process_wifi_stats(item, collection_time, device_id, [])
        wifistats_records.extend(wifi_stats_results)

        # Processa dados das interfaces WAN
        interface_wan_results = interface_wan(item, collection_time, device_id, [])
        interface_wan_records.extend(interface_wan_results)

        # Processa dados das interfaces LAN
        interface_lan_results = interface_lan(item, collection_time, device_id, [])
        interface_lan_records.extend(interface_lan_results)

    return nbw_records, testtable_records, wifistats_records, interface_wan_records, interface_lan_records

def process_test_data(device_id: str, item: DeviceData):
    try:
        uptime = item.Device.get("DeviceInfo", {}).get("UpTime")
        if not uptime:
            raise ValueError("Uptime is missing or invalid.")
        
        record = {
            "device_id": device_id,
            "uptime": uptime
        }
        print(f"Dados para tabela testtable: {record}")
        return record
      
    except ValueError as e:
        print(f"Erro ao processar uptime: {e}")
        return None
    except Exception as e:
        print(f"Erro ao processar dados para tabela horas: {e}")
        return None
      
def process_neighboring_wifi(item: DeviceData, collection_time: datetime, device_id, records: List[Dict[str, Any]]):
    neighboring_wifi = item.Device.get("WiFi", {}).get("NeighboringWiFiDiagnostic", {}).get("Result", {})
    bssid_router2 = safe_get(item.Device, ["WiFi", "DataElements", "Network", "Device", "1", "Radio", "1", "BSS", "2", "BSSID"], "Unknown2").upper()
    bssid_router5 = safe_get(item.Device, ["WiFi", "DataElements", "Network", "Device", "1", "Radio", "2", "BSS", "2", "BSSID"], "Unknown5").upper()
    
    if not isinstance(neighboring_wifi, dict):
        print(f"neighboring_wifi is not a dictionary: {neighboring_wifi}")
        return records

    for bssid, details in neighboring_wifi.items():
        try:
            bssid_rede = details.get('BSSID', 'Unknown').upper()
            signal_strength = int(details.get('SignalStrength', 1))
            channel = int(details.get('Channel', 'Unknown'))
            channel_bandwidth = details.get('OperatingChannelBandwidth', 'Unknown')
            ssid_rede = details.get('SSID', '0').strip() or '0'
            record = {
                "detected_at": collection_time.astimezone(timezone(timedelta(hours=-3))).isoformat(),  # Convertendo para o fuso horário brasileiro
                "device_id": str(device_id),
                "bssid_router": bssid_router2 if 0 < channel < 14 else bssid_router5,
                "bssid_rede": bssid_rede,
                "signal_strength": signal_strength,
                "ssid_rede": ssid_rede,
                "channel": channel,
                "channel_bandwidth": channel_bandwidth
            }
            records.append(record)
            print(f"WiFi Neighboring Processado: {record}")
        except (ValueError, TypeError) as e:
            print(f"Erro ao processar Neighboring WiFi para {bssid}: {e}")
    return records

def process_wifi_stats(item, collection_time, device_id, records):
    host_data = item.Device.get('Device', {}).get('Hosts', {}).get('Host', {})
    if not isinstance(host_data, dict):  # Verifica se host_data é um dicionário
        host_data = {}

    accesspoint_data = item.Device.get('Device', {}).get('WiFi', {}).get('AccessPoint', {})
    if not isinstance(accesspoint_data, dict):  # Verifica se accesspoint_data é um dicionário
        accesspoint_data = {}

    for host in host_data.values():
        hostname = host.get('HostName', '0')
        mac_address_host = str(host.get('PhysAddress', '0').upper())
        for ap in accesspoint_data.values():
            associateddevice = ap.get('AssociatedDevice', {})
            if isinstance(associateddevice, dict):  # Verifica se AssociatedDevice é um dicionário
                for ad in associateddevice.values():
                    mac_address_ap = str(ad.get('MACAddress', '0').upper())
                    signal_strength = int(ad.get('SignalStrength', 0))
                    packets_sent = int(ad.get('Stats', {}).get('PacketsSent', 0))
                    packets_received = int(ad.get('Stats', {}).get('PacketsReceived', 0))
                    if mac_address_host == mac_address_ap:
                        record = {
                            "time": collection_time.astimezone(timezone(timedelta(hours=-3))).isoformat(),  # Convertendo para o fuso horário brasileiro
                            "device_id": device_id,
                            "mac_address": mac_address_ap,
                            "hostname": hostname,
                            "signal_strength": signal_strength,
                            "packets_sent": packets_sent,
                            "packets_received": packets_received
                        }
                        records.append(record)
                        print(f"WiFi Stats Processado: {record}")
            else:
                print("AssociatedDevice não é um dicionário:", associateddevice)
    return records

def interface_wan(item: DeviceData, collection_time: datetime, device_id, records: List[Dict[str, Any]]):
    interface_wan = item.Device.get('IP', {}).get('Interface', {}).get('1', {})
    print(f"interface_wan: {interface_wan}")
    print(f"interface_wan type: {type(interface_wan)}")
    print(f"items: {interface_wan.items()}")
    print(f"values: {interface_wan.values()}")

    if not isinstance(interface_wan, dict):  # Verifica se interface_wan é um dicionário
        print(f"Interface WAN is not a dictionary: {interface_wan}")
        return records

    for details in interface_wan.values():  # Itera sobre os valores do dicionário
        try:
            bytes_received_wan = int(details.get('BytesReceived', -1))
            bytes_sent_wan = int(details.get('BytesSent', -1))
            packets_received_wan = int(details.get('PacketsReceived', -1))
            packets_sent_wan = int(details.get('PacketsSent', -1))
            errors_received_wan = int(details.get('ErrorsReceived', -1))
            errors_sent_wan = int(details.get('ErrorsSent', -1))
            record = {
                "time": collection_time.astimezone(timezone(timedelta(hours=-3))).isoformat(),  # Convertendo para o fuso horário brasileiro
                "device_id": device_id,
                "wan_packets_received": packets_received_wan,
                "wan_bytes_received": bytes_received_wan,
                "wan_bytes_per_packets_received": bytes_received_wan / packets_received_wan if packets_received_wan > 0 else None,
                "wan_packets_sent": packets_sent_wan,
                "wan_bytes_sent": bytes_sent_wan,
                "wan_bytes_per_packets_sent": bytes_sent_wan / packets_sent_wan if packets_sent_wan > 0 else None,
                "wan_errors_sent": errors_sent_wan,
                "wan_errors_received": errors_received_wan
            }
            records.append(record)
            print(f"Interface WAN Processada: {record}")
        except (ValueError, TypeError) as e:
            print(f"Erro ao processar Interface WAN do dispositivo {device_id}: {e}")
    return records

def interface_lan(item: DeviceData, collection_time: datetime, device_id, records: List[Dict[str, Any]]):
    interface_lan = item.Device.get('IP', {}).get('Interface', {}).get('4', {})
    print(f"interface_lan: {interface_lan}")
    print(f"interface_lan type: {type(interface_lan)}")

    if not isinstance(interface_lan, dict):  # Verifica se interface_lan é um dicionário
        print(f"Interface LAN is not a dictionary: {interface_lan}")
        return records

    for details in interface_lan.values():  # Itera sobre as tuplas (chave, valor)
        try:
            bytes_received_lan = int(details.get('BytesReceived', -1))
            bytes_sent_lan = int(details.get('BytesSent', -1))
            packets_received_lan = int(details.get('PacketsReceived', -1))
            packets_sent_lan = int(details.get('PacketsSent', -1))
            errors_received_lan = int(details.get('ErrorsReceived', -1))
            errors_sent_lan = int(details.get('ErrorsSent', -1))
            record = {
                "time": collection_time.astimezone(timezone(timedelta(hours=-3))).isoformat(),  # Convertendo para o fuso horário brasileiro
                "device_id": device_id,
                "lan_packets_received": packets_received_lan,
                "lan_bytes_received": bytes_received_lan,
                "lan_bytes_per_packets_received": bytes_received_lan / packets_received_lan if packets_received_lan > 0 else None,
                "lan_packets_sent": packets_sent_lan,
                "lan_bytes_sent": bytes_sent_lan,
                "lan_bytes_per_packets_sent": bytes_sent_lan / packets_sent_lan if packets_sent_lan > 0 else None,
                "lan_errors_sent": errors_sent_lan,
                "lan_errors_received": errors_received_lan
            }
            records.append(record)
            print(f"Interface LAN Processada: {record}")
        except (ValueError, TypeError) as e:
            print(f"Erro ao processar Interface LAN do dispositivo {device_id}: {e}")
    return records

def store_data_in_redis(records: List[Dict[str, Any]], redis_key_prefix: str, redis_stream: str, key_fields: List[str]):
    for record in records:
        try:
            # Converte valores None para string 'null' antes de armazenar
            record = {key: (str(value) if value is not None else '-1') for key, value in record.items()}
            redis_key = f"{redis_key_prefix};{';'.join(record.get(field, 'missing') for field in key_fields)}"
            print(f"Armazenando no Redis com chave: {redis_key} e dados: {record}")
            redis_client.hset(redis_key, mapping=record)
            redis_client.xadd(redis_stream, record)
        except Exception as e:
            print(f"Erro ao armazenar registro no Redis: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv('UVICORN_HOST'), port=int(os.getenv('UVICORN_PORT')))
