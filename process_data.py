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

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log/process_data.log"),
        logging.StreamHandler()
    ]
)

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
    logging.info("Conexão com Redis bem-sucedida.")
except redis.exceptions.ConnectionError as e:
    logging.error(f"Erro de conexão com Redis: {e}")
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
        #print("Recebendo dados...")
        body = await request.json()
        logging.info(f"Dados recebidos: {body}")
        data = BulkData(**body)  # Validação automática via Pydantic
        
        nbw_records, wifistats_records, dados_records, routers_records = process_data(data)
        
        #print(f"Registros WiFi_NBW processados: {nbw_records}")
        #print(f"Registros WiFi Stats processados: {wifistats_records}")
        #print(f"Registros de Dados processados: {dados_records}")
        #print(f"Registro de Routers processados: {routers_records}")
        
        store_data_in_redis(nbw_records, "redes_proximas", "redes_proximas_stream", ["detected_at", "device_id", "bssid_router", "bssid_rede", "signal_strength", "ssid_rede", "channel", "channel_bandwidth"])
        store_data_in_redis(wifistats_records, "wifistats", "wifistats_stream", ["time", "device_id", "mac_address", "hostname", "signal_strength", "packets_sent", "packets_received", "bytes_sent", "bytes_received", "errors_sent", "errors_received", "radio_connected", "time_since_connected"])
        store_data_in_redis(dados_records, "dados", "dados_stream", ["time", "device_id", "wan_bytes_sent", "wan_bytes_received", "wan_packets_sent", "wan_packets_received", "lan_bytes_sent", "lan_bytes_received", "lan_packets_sent", "lan_packets_received", "wifi_bytes_sent", "wifi_bytes_received", "wifi_packets_sent", "wifi_packets_received", "signal_pon", "wifi2_4_channel", "wifi2_4bandwith", "wifi2_4ssid", "wifi_5_channel", "wifi_5_bandwith", "wifi_5_ssid", "uptime"])
        #store_data_in_redis(routers_records, "routers", "routers_stream", ["device_id", "latitude", "longitude", "ssid", "mac_address"])
        return {"message": "Dados processados e enviados ao Redis com sucesso."}

    except ValidationError as e:
        logging.error(f"Erro de validação: {e}")
        raise HTTPException(status_code=400, detail=f"Erro de validação: {e}")
    except Exception as e:
        logging.error(f"Erro no processamento: {e}")
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
            logging.error(f"Erro: item.Device não é um dicionário: {item.Device}")
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
        logging.error(f"neighboring_wifi is not a dictionary: {neighboring_wifi}")
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
            #print(f"WiFi Neighboring Processado: {record}")
        except (ValueError, TypeError) as e:
            logging.error(f"Erro ao processar Neighboring WiFi para {bssid}: {e}")
    return records

def process_wifi_stats(item, collection_time, device_id, records):
    host_data = item.Device.get('Hosts', {}).get('Host', {})
    radio_network_data = item.Device.get('WiFi', {}).get('DataElements', {}).get('Network', {}).get('Device', {}).get('1', {}).get('Radio', {})
    AD2_4GHz_data = radio_network_data.get('1', {}).get('BSS', {}).get('2', {}).get('STA', {})
    AD5GHz_data = radio_network_data.get('2', {}).get('BSS', {}).get('2', {}).get('STA', {})

    if not isinstance(host_data, dict):  # Verifica se host_data é um dicionário
        host_data = {}

    if not isinstance(radio_network_data, dict):  # Verifica se radio_network_data é um dicionário
        radio_network_data = {}

    for host in host_data.values():
        hostname = host.get('HostName', '0')
        mac_address_host = str(host.get('PhysAddress', '0').upper())
        for ad2_4 in AD2_4GHz_data.values():
            mac_address2_4 = ad2_4.get('MACAddress', '0')
            if mac_address_host == mac_address2_4:
                record = {
                    "time": collection_time.astimezone(timezone(timedelta(hours=-3))).isoformat(),  # Convertendo para o fuso horário brasileiro
                    "device_id": device_id,
                    "mac_address": mac_address2_4,
                    "hostname": hostname,
                    "signal_strength": int((int(ad2_4.get('SignalStrength', '0')) / 2) - 110), #(resultado/2) -110
                    "packets_sent": ad2_4.get('PacketsSent', '0'),
                    "packets_received": ad2_4.get('PacketsReceived', '0'),
                    "bytes_sent": ad2_4.get('BytesSent', '0'),
                    "bytes_received": ad2_4.get('BytesReceived', '0'),
                    "errors_sent": ad2_4.get('ErrorsSent', '0'),
                    "errors_received": ad2_4.get('ErrorsReceived', '0'),
                    "radio_connected": "2.4GHz",
                    "time_since_connected": ad2_4.get('LastConnectTime', '0')
                }
                records.append(record)
                #print(f"WiFi Stats Processado: {record}")

        for ad5 in AD5GHz_data.values():
            mac_address5 = ad5.get('MACAddress', '0')
            if mac_address_host == mac_address5:
                record = {
                    "time": collection_time.astimezone(timezone(timedelta(hours=-3))).isoformat(),  # Convertendo para o fuso horário brasileiro
                    "device_id": device_id,
                    "mac_address": mac_address5,
                    "hostname": hostname,
                    "signal_strength": int((int(ad5.get('SignalStrength', '0')) / 2) - 110),
                    "packets_sent": ad5.get('PacketsSent', '0'),
                    "packets_received": ad5.get('PacketsReceived', '0'),
                    "bytes_sent": ad5.get('BytesSent', '0'),
                    "bytes_received": ad5.get('BytesReceived', '0'),
                    "errors_sent": ad5.get('ErrorsSent', '0'),
                    "errors_received": ad5.get('ErrorsReceived', '0'),
                    "radio_connected": "5GHz",
                    "time_since_connected": ad5.get('LastConnectTime', '0')
                }
                records.append(record)
                #print(f"WiFi Stats Processado: {record}")

    return records

def dados(item: DeviceData, collection_time: datetime, device_id, records: List[Dict[str, Any]]):
    # Inicialização correta das variáveis
    total_packets_sent2_4 = total_packets_sent5 = 0
    total_packets_received2_4 = total_packets_received5 = 0
    total_bytes_sent2_4 = total_bytes_sent5 = 0
    total_bytes_received2_4 = total_bytes_received5 = 0

    # Acessando dados com fallback seguro
    interfaces = item.Device.get('IP', {}).get('Interface', {})
    ip_lan = interfaces.get('1', {}).get('Stats', {}) if isinstance(interfaces, dict) else {}
    ip_wan = interfaces.get('4', {}).get('Stats', {}) if isinstance(interfaces, dict) else {}

    radio_network_data = item.Device.get('WiFi', {}).get('DataElements', {}).get('Network', {}).get('Device', {}).get('1', {}).get('Radio', {})
    
    # Coleta de dados WiFi 2.4GHz
    AD2_4GHz_data = {}
    if '1' in radio_network_data:
        bss = radio_network_data['1'].get('BSS', {})
        AD2_4GHz_data = bss.get('2', {}).get('STA', {}) if isinstance(bss, dict) else {}

    # Coleta de dados WiFi 5GHz
    AD5GHz_data = {}
    if '2' in radio_network_data:
        bss = radio_network_data['2'].get('BSS', {})
        AD5GHz_data = bss.get('2', {}).get('STA', {}) if isinstance(bss, dict) else {}

    # Processamento WiFi 2.4GHz
    for ad2_4 in AD2_4GHz_data.values():
        if isinstance(ad2_4, dict):
            total_packets_sent2_4 += int(ad2_4.get('PacketsSent', 0))
            total_packets_received2_4 += int(ad2_4.get('PacketsReceived', 0))
            total_bytes_sent2_4 += int(ad2_4.get('BytesSent', 0))
            total_bytes_received2_4 += int(ad2_4.get('BytesReceived', 0))

    # Processamento WiFi 5GHz
    for ad5 in AD5GHz_data.values():
        if isinstance(ad5, dict):
            total_packets_sent5 += int(ad5.get('PacketsSent', 0))
            total_packets_received5 += int(ad5.get('PacketsReceived', 0))
            total_bytes_sent5 += int(ad5.get('BytesSent', 0))
            total_bytes_received5 += int(ad5.get('BytesReceived', 0))

    # Manter o tráfego dos dispositivos desconectados
    previous_data = redis_client.hgetall(f"previous_data:{device_id}")
    if previous_data:
        total_packets_sent2_4 += int(previous_data.get('total_packets_sent2_4', 0))
        total_packets_received2_4 += int(previous_data.get('total_packets_received2_4', 0))
        total_bytes_sent2_4 += int(previous_data.get('total_bytes_sent2_4', 0))
        total_bytes_received2_4 += int(previous_data.get('total_bytes_received2_4', 0))
        total_packets_sent5 += int(previous_data.get('total_packets_sent5', 0))
        total_packets_received5 += int(previous_data.get('total_packets_received5', 0))
        total_bytes_sent5 += int(previous_data.get('total_bytes_sent5', 0))
        total_bytes_received5 += int(previous_data.get('total_bytes_received5', 0))

    # Armazenar os dados atuais para uso futuro
    redis_client.hmset(f"previous_data:{device_id}", {
        'total_packets_sent2_4': total_packets_sent2_4,
        'total_packets_received2_4': total_packets_received2_4,
        'total_bytes_sent2_4': total_bytes_sent2_4,
        'total_bytes_received2_4': total_bytes_received2_4,
        'total_packets_sent5': total_packets_sent5,
        'total_packets_received5': total_packets_received5,
        'total_bytes_sent5': total_bytes_sent5,
        'total_bytes_received5': total_bytes_received5
    })

    # Dados WiFi
    wifi = item.Device.get('WiFi', {}).get('Radio', {})
    wifi2_4 = wifi.get('1', {}) if isinstance(wifi, dict) else {}
    wifi5 = wifi.get('2', {}) if isinstance(wifi, dict) else {}

    # Montagem do registro
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
        "wifi_bytes_sent": total_bytes_sent2_4 + total_bytes_sent5,
        "wifi_bytes_received": total_bytes_received2_4 + total_bytes_received5,
        "wifi_packets_sent": total_packets_sent2_4 + total_packets_sent5,
        "wifi_packets_received": total_packets_received2_4 + total_packets_received5,
        "signal_pon": -1,
        "wifi2_4_channel": int(wifi2_4.get('Channel', -1)),
        "wifi2_4bandwith": wifi2_4.get('CurrentOperatingChannelBandwidth', 'Unknown'),
        "wifi2_4ssid": radio_network_data.get('1', {}).get('BSS', {}).get('2', {}).get('SSID', 'Unknown'),
        "wifi_5_channel": int(wifi5.get('Channel', -1)),
        "wifi_5_bandwith": wifi5.get('CurrentOperatingChannelBandwidth', 'Unknown'),
        "wifi_5_ssid": radio_network_data.get('2', {}).get('BSS', {}).get('2', {}).get('SSID', 'Unknown'),
        "uptime": int(item.Device.get('DeviceInfo', {}).get('UpTime', -1))
    }

    # Verificação de campos críticos
    critical_fields = ["wan_bytes_sent", "lan_bytes_sent"]
    if all(record[field] != -1 for field in critical_fields):
        records.append(record)
        #print(f"Dados Processados: {record}")
    else:
        print("Registro ignorado devido a valores críticos ausentes.")

    return records

def store_data_in_redis(records: List[Dict[str, Any]], redis_key_prefix: str, redis_stream: str, key_fields: List[str]):
    for record in records:
        try:
            # Converte valores None para string 'null' antes de armazenar
            record = {key: (str(value) if value is not None else '-1') for key, value in record.items()}
            redis_key = f"{redis_key_prefix};{';'.join(record.get(field, 'missing') for field in key_fields)}"
            logging.info(f"Armazenando no Redis com chave: {redis_key} e dados: {record}")
            redis_client.hset(redis_key, mapping=record)
            redis_client.xadd(redis_stream, record)
        except Exception as e:
            logging.error(f"Erro ao armazenar registro no Redis: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv('UVICORN_HOST'), port=int(os.getenv('UVICORN_PORT')))
