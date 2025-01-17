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
        
        nbw_records, wifistats_records, dados_records, routers_records = process_data(data)
        
        print(f"Registros WiFi_NBW processados: {nbw_records}")
        print(f"Registros WiFi Stats processados: {wifistats_records}")
        print(f"Registros de Dados processados: {dados_records}")
        print(f"Registro de Routers processados: {routers_records}")
        
        store_data_in_redis(nbw_records, "redes_proximas", "redes_proximas_stream", ["detected_at", "device_id", "bssid_router", "bssid_rede", "signal_strength", "ssid_rede", "channel", "channel_bandwidth"])
        store_data_in_redis(wifistats_records, "wifistats", "wifistats_stream", ["time", "device_id", "mac_address", "hostname", "signal_strength", "packets_sent", "packets_received"])
        store_data_in_redis(dados_records, "dados", "dados_stream", ["time", "device_id", "wan_bytes_sent", "wan_bytes_received", "wan_packets_sent", "wan_packets_received", "lan_bytes_sent", "lan_bytes_received", "lan_packets_sent", "lan_packets_received", "wifi_bytes_sent", "wifi_bytes_received", "wifi_packets_sent", "wifi_packets_received", "signal_pon", "wifi2_4_channel", "wifi2_4bandwith", "wifi2_4ssid", "wifi_5_channel", "wifi_5_bandwith", "wifi_5_ssid", "uptime"])
        #store_data_in_redis(routers_records, "routers", "routers_stream", ["device_id", "latitude", "longitude", "ssid", "mac_address"])
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
    wifistats_records = []
    dados_records = []
    routers_records = []

    for item in data.Report:
        if not isinstance(item.Device, dict):
            print(f"Erro: item.Device não é um dicionário: {item.Device}")
            continue
        
        collection_time = datetime.fromtimestamp(item.CollectionTime, timezone.utc)
        device_id = safe_get(item.Device, ["ManagementServer", "ConnectionRequestUsername"], "unknown")
        
        # Processa dados do WiFi Neighboring
        wifi_results = process_neighboring_wifi(item, collection_time, device_id, [])
        nbw_records.extend(wifi_results)
        
        # Processa dados do WiFi Stats
        wifi_stats_results = process_wifi_stats(item, collection_time, device_id, [])
        wifistats_records.extend(wifi_stats_results)
        
        # Processa dados de Dados
        dados_results = dados(item, collection_time, device_id, [])
        dados_records.extend(dados_results)
        
        '''
        # Processa dados de Routers
        routers_records_results = routers(item, collection_time, device_id, [])
        routers_records.extend(routers_records_results)'''

    return nbw_records, wifistats_records, dados_records, routers_records

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
    host_data = item.Device.get('Hosts', {}).get('Host', {})
    #print(f"host_data: {host_data}")
    if not isinstance(host_data, dict):  # Verifica se host_data é um dicionário
        host_data = {}

    accesspoint_data = item.Device.get('WiFi', {}).get('AccessPoint', {})
    #print(f"accesspoint_data: {accesspoint_data}")
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

def dados(item: DeviceData, collection_time: datetime, device_id, records: List[Dict[str, Any]]):
    ip_lan = item.Device.get('IP', {}).get('Interface', {}).get('1', {}).get('Stats', {})
    ip_wan = item.Device.get('IP', {}).get('Interface', {}).get('4', {}).get('Stats', {})

    if not isinstance(ip_lan, dict):
        ip_lan = {}
    if not isinstance(ip_wan, dict):
        ip_wan = {}

    wifi = item.Device.get('WiFi', {}).get('Radio', {})
    wifi2_4 = wifi.get('1', {})
    wifi2_4status = wifi2_4.get('Stats', {})
    wifi5 = wifi.get('2', {})
    wifi5status = wifi5.get('Stats', {})

    # Create the record dictionary
    record = {
        "time": collection_time.astimezone(timezone(timedelta(hours=-3))).isoformat(),
        "device_id": device_id,
        "wan_bytes_sent": int(ip_wan.get('BytesSent', -1)),
        "wan_bytes_received": int(ip_wan.get('BytesReceived', -1)),
        "wan_packets_sent": int(ip_wan.get('PacketsSent', -1)),
        "wan_packets_received": int(ip_wan.get('PacketsReceived', -1)),
        "lan_bytes_sent": int(ip_lan.get('BytesSent', -1)),
        "lan_bytes_received": int(ip_lan.get('BytesReceived', -1)),
        "lan_packets_sent": int(ip_lan.get('PacketsSent', -1)),
        "lan_packets_received": int(ip_lan.get('PacketsReceived', -1)),
        "wifi_bytes_sent": int(wifi2_4status.get('BytesSent', -1)) + int(wifi5status.get('BytesSent', -1)),
        "wifi_bytes_received": int(wifi2_4status.get('BytesReceived', -1)) + int(wifi5status.get('BytesReceived', -1)),
        "wifi_packets_sent": int(wifi2_4status.get('PacketsSent', -1)) + int(wifi5status.get('PacketsSent', -1)),
        "wifi_packets_received": int(wifi2_4status.get('PacketsReceived', -1)) + int(wifi5status.get('PacketsReceived', -1)),
        "signal_pon": int(-1),
        "wifi2_4_channel": int(wifi2_4.get('Channel', -1)),
        "wifi2_4bandwith": wifi2_4.get('CurrentOperatingChannelBandwidth', 'Unknown'),
        "wifi2_4ssid": item.Device.get('WiFi',{}).get('SSID', {}).get('1', {}).get('SSID', 'Unknown'),
        "wifi_5_channel": int(wifi5.get('Channel', -1)),
        "wifi_5_bandwith": wifi5.get('CurrentOperatingChannelBandwidth', 'Unknown'),
        "wifi_5_ssid": item.Device.get('WiFi',{}).get('SSID', {}).get('3', {}).get('SSID', 'Unknown'),
        "uptime": int(item.Device.get('DeviceInfo', {}).get('UpTime', -1))
    }

    # Define critical fields that must have values other than -1
    critical_fields = [
        "wan_bytes_sent",
        "lan_bytes_sent"
    ]

    # Check if all critical fields have valid values
    if all(record[field] != -1 for field in critical_fields):
        records.append(record)
        print(f"Dados Processados: {record}")
    else:
        print("Skipping record due to missing critical values.")

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

