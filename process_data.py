from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, ValidationError
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone, timedelta
from redis import ConnectionPool
from dotenv import load_dotenv
import redis
import uvicorn
import logging
import os
import json

# Configurações iniciais
load_dotenv(dotenv_path='')
load_dotenv(dotenv_path='')

# Initialize FastAPI
app = FastAPI()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("log/process_data.log"),
        logging.StreamHandler()
    ]
)

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_SOCKET_TIMEOUT = 10
REDIS_DECODE_RESPONSES = True
REDIS_MAX_CONNECTIONS = 10

pool = ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    socket_timeout=REDIS_SOCKET_TIMEOUT,
    decode_responses=REDIS_DECODE_RESPONSES,
    max_connections=REDIS_MAX_CONNECTIONS
)
redis_client = redis.Redis(connection_pool=pool)

# Validate Redis connection
try:
    redis_client.ping()
    logging.info("Connected to Redis successfully.")
except redis.ConnectionError as e:
    logging.error(f"Redis connection error: {e}")
    raise RuntimeError("Failed to connect to Redis.")

# Pydantic models
class DeviceData(BaseModel):
    CollectionTime: int
    Device: Dict[str, Any]

class BulkData(BaseModel):
    Report: List[DeviceData]

# Helper functions
def safe_get(dictionary: Dict, path: List[str], default=None):
    current = dictionary
    for key in path:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current

def get_brazilian_time(dt: datetime) -> datetime:
    return dt.astimezone(timezone(timedelta(hours=-3)))

# Data processing
@app.post("/bulkdata")
async def receive_bulkdata(request: Request):
    try:
        # Get raw body first for logging
        raw_body = await request.body()
        try:
            # Try to parse JSON after logging
            body = await request.json()
            logging.info(f"Received data: {(body)}...")  # Safe truncation
        except json.JSONDecodeError:
            logging.error("Received invalid JSON payload")
            raise HTTPException(status_code=400, detail="Invalid JSON format")
        data = BulkData(**body)
        
        nbw, wifistats, dados_records, routers = process_data(data)
        
        # Store data with pipelines
        with redis_client.pipeline() as pipe:
            store_data_in_redis(nbw, "redes_proximas", "redes_proximas_stream", pipe, ["detected_at", "device_id", "bssid_router", "bssid_rede", "signal_strength", "ssid_rede", "channel", "channel_bandwidth"])
            store_data_in_redis(wifistats, "wifistats", "wifistats_stream", pipe, ["time", "device_id", "mac_address", "hostname", "signal_strength", "packets_sent", "packets_received", "bytes_sent", "bytes_received", "errors_sent", "errors_received", "radio_connected", "time_since_connected"])
            store_data_in_redis(dados_records, "dados", "dados_stream", pipe, ["time", "device_id", "wan_bytes_sent", "wan_bytes_received", "wan_packets_sent", "wan_packets_received", "lan_bytes_sent", "lan_bytes_received", "lan_packets_sent", "lan_packets_received", "wifi_bytes_sent", "wifi_bytes_received", "wifi_packets_sent", "wifi_packets_received", "signal_pon", "wifi2_4_channel", "wifi2_4bandwith", "wifi2_4ssid", "wifi_5_channel", "wifi_5_bandwith", "wifi_5_ssid", "uptime", "memory_free", "memory_total", "cpu_usage"])
            pipe.execute()
        
        return {"message": "Data processed successfully."}
    except ValidationError as e:
        logging.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        logging.debug(f"Raw request body: {raw_body}")  # Safe truncation for debugging
        raise HTTPException(status_code=500, detail="Internal server error")

def process_data(data: BulkData) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    nbw_records = []
    wifistats_records = []
    dados_records = []
    routers_records = []

    for item in data.Report:
        if not isinstance(item.Device, dict):
            logging.warning(f"Skipping invalid device data: {item.Device}")
            continue
        
        collection_time = get_brazilian_time(datetime.fromtimestamp(item.CollectionTime, timezone.utc))
        device_id = safe_get(item.Device, ["ManagementServer", "ConnectionRequestUsername"], "unknown")
        
        # Process each section
        process_neighboring_wifi(item, collection_time, device_id, nbw_records)
        process_wifi_stats(item, collection_time, device_id, wifistats_records)
        process_dados(item, collection_time, device_id, dados_records)
    
    return nbw_records, wifistats_records, dados_records, routers_records

def process_neighboring_wifi(item: DeviceData, time: datetime, device_id: str, records: list):
    wifi_data = safe_get(item.Device, ["WiFi", "NeighboringWiFiDiagnostic", "Result"], {})
    bssid_router2 = safe_get(item.Device, ["WiFi", "DataElements", "Network", "Device", "1", "Radio", "1", "BSS", "2", "BSSID"], "Unknown2").upper()
    bssid_router5 = safe_get(item.Device, ["WiFi", "DataElements", "Network", "Device", "1", "Radio", "2", "BSS", "2", "BSSID"], "Unknown5").upper()

    for bssid, details in wifi_data.items():
        try:
            records.append({
                "detected_at": time.isoformat(),
                "device_id": device_id,
                "bssid_router": bssid_router2 if 0 < int(details.get('Channel', 0)) < 14 else bssid_router5,
                "bssid_rede": details.get('BSSID', 'Unknown').upper(),
                "signal_strength": int(details.get('SignalStrength', 0)),
                "ssid_rede": details.get('SSID', '0').strip() or '0',
                "channel": int(details.get('Channel', 0)),
                "channel_bandwidth": details.get('OperatingChannelBandwidth', 'Unknown')
            })
        except (ValueError, KeyError) as e:
            logging.warning(f"Invalid WiFi data: {e}")

def process_wifi_stats(item: DeviceData, time: datetime, device_id: str, records: list):
    hosts = safe_get(item.Device, ["Hosts", "Host"], {})
    radio_2ghz = safe_get(item.Device, ["WiFi", "DataElements", "Network", "Device", "1", "Radio", "1", "BSS", "2", "STA"], {})
    radio_5ghz = safe_get(item.Device, ["WiFi", "DataElements", "Network", "Device", "1", "Radio", "2", "BSS", "2", "STA"], {})

    for host in hosts.values():
        hostname = host.get('HostName', 'Unknown')
        mac = host.get('PhysAddress', '').upper()
        
        # Check 2.4GHz connections
        for sta in radio_2ghz.values():
            if sta.get('MACAddress', '').upper() == mac:
                records.append(create_wifi_stat_record(time, device_id, hostname, mac, sta, "2.4GHz"))
        
        # Check 5GHz connections
        for sta in radio_5ghz.values():
            if sta.get('MACAddress', '').upper() == mac:
                records.append(create_wifi_stat_record(time, device_id, hostname, mac, sta, "5GHz"))

def create_wifi_stat_record(time: datetime, device_id: str, hostname: str, mac: str, data: dict, band: str) -> dict:
    return {
        "time": time.isoformat(),
        "device_id": device_id,
        "mac_address": mac,
        "hostname": hostname,
        "signal_strength": (int(data.get('SignalStrength', 0)) // 2) - 110,
        "packets_sent": int(data.get('PacketsSent', 0)),
        "packets_received": int(data.get('PacketsReceived', 0)),
        "bytes_sent": int(data.get('BytesSent', 0)),
        "bytes_received": int(data.get('BytesReceived', 0)),
        "errors_sent": int(data.get('ErrorsSent', 0)),
        "errors_received": int(data.get('ErrorsReceived', 0)),
        "radio_connected": band,
        "time_since_connected": int(data.get('LastConnectTime', 0))
    }

def process_dados(item: DeviceData, time: datetime, device_id: str, records: list):
    try:
        # Extração de dados das interfaces de rede
        interfaces = safe_get(item.Device, ["IP", "Interface"], {})
        ip_wan = safe_get(interfaces, ["4", "Stats"], {})
        ip_lan = safe_get(interfaces, ["1", "Stats"], {})

        # Extração de dados WiFi
        radio_network_data = safe_get(item.Device, ["WiFi", "DataElements", "Network", "Device", "1", "Radio"], {})
        AD2_4GHz_data = safe_get(radio_network_data, ["1", "BSS", "2", "STA"], {})
        AD5GHz_data = safe_get(radio_network_data, ["2", "BSS", "2", "STA"], {})

        # Cálculo de estatísticas WiFi
        wifi_stats = {
            "total_packets_sent2_4": sum(int(sta.get('PacketsSent', 0)) for sta in AD2_4GHz_data.values()),
            "total_packets_received2_4": sum(int(sta.get('PacketsReceived', 0)) for sta in AD2_4GHz_data.values()),
            "total_bytes_sent2_4": sum(int(sta.get('BytesSent', 0)) for sta in AD2_4GHz_data.values()),
            "total_bytes_received2_4": sum(int(sta.get('BytesReceived', 0)) for sta in AD2_4GHz_data.values()),
            "total_packets_sent5": sum(int(sta.get('PacketsSent', 0)) for sta in AD5GHz_data.values()),
            "total_packets_received5": sum(int(sta.get('PacketsReceived', 0)) for sta in AD5GHz_data.values()),
            "total_bytes_sent5": sum(int(sta.get('BytesSent', 0)) for sta in AD5GHz_data.values()),
            "total_bytes_received5": sum(int(sta.get('BytesReceived', 0)) for sta in AD5GHz_data.values()),
        }

        # Recuperar dados anteriores e acumular
        previous_data = redis_client.hgetall(f"previous_data:{device_id}")
        if previous_data:
            for key in wifi_stats:
                wifi_stats[key] += int(previous_data.get(key, 0))

        # Armazenar dados atuais para o próximo ciclo
        redis_client.hset(f"previous_data:{device_id}", mapping={
            k: str(v) for k, v in wifi_stats.items()  # Convert all values to strings
        })

        # Configurações WiFi
        wifi_radio = safe_get(item.Device, ["WiFi", "Radio"], {})
        wifi2_4 = safe_get(wifi_radio, ["1"], {})
        wifi5 = safe_get(wifi_radio, ["2"], {})

        # Dados do dispositivo
        device_info = safe_get(item.Device, ["DeviceInfo"], {})
        memory = safe_get(device_info, ["MemoryStatus"], {"Free": 0, "Total": 0})
        process_status = safe_get(device_info, ["ProcessStatus"], {"CPUUsage": 0})

        record = {
            "time": time.isoformat(),
            "device_id": device_id,
            # Dados WAN
            "wan_bytes_sent": int(ip_wan.get('BytesSent', 0)),
            "wan_bytes_received": int(ip_wan.get('BytesReceived', 0)),
            "wan_packets_sent": int(ip_wan.get('PacketsSent', 0)),
            "wan_packets_received": int(ip_wan.get('PacketsReceived', 0)),
            # Dados LAN
            "lan_bytes_sent": int(ip_lan.get('BytesSent', 0)),
            "lan_bytes_received": int(ip_lan.get('BytesReceived', 0)),
            "lan_packets_sent": int(ip_lan.get('PacketsSent', 0)),
            "lan_packets_received": int(ip_lan.get('PacketsReceived', 0)),
            # Estatísticas WiFi consolidadas
            "wifi_bytes_sent": wifi_stats["total_bytes_sent2_4"] + wifi_stats["total_bytes_sent5"],
            "wifi_bytes_received": wifi_stats["total_bytes_received2_4"] + wifi_stats["total_bytes_received5"],
            "wifi_packets_sent": wifi_stats["total_packets_sent2_4"] + wifi_stats["total_packets_sent5"],
            "wifi_packets_received": wifi_stats["total_packets_received2_4"] + wifi_stats["total_packets_received5"],
            # Configurações WiFi
            "wifi2_4_channel": int(wifi2_4.get('Channel', 0)),
            "wifi2_4bandwith": wifi2_4.get('CurrentOperatingChannelBandwidth', 'Unknown'),
            "wifi2_4ssid": safe_get(radio_network_data, ["1", "BSS", "2", "SSID"], 'Unknown'),
            "wifi_5_channel": int(wifi5.get('Channel', 0)),
            "wifi_5_bandwith": wifi5.get('CurrentOperatingChannelBandwidth', 'Unknown'),
            "wifi_5_ssid": safe_get(radio_network_data, ["2", "BSS", "2", "SSID"], 'Unknown'),
            # Status do dispositivo
            "uptime": int(device_info.get("UpTime", 0)),
            "memory_free": int(memory.get("Free", 0)),
            "memory_total": int(memory.get("Total", 0)),
            "cpu_usage": int(process_status.get("CPUUsage", 0)),
            "signal_pon": -1  # Placeholder para implementação futura
        }

        # Validação de campos críticos
        if all(record[k] != 0 for k in ["wan_bytes_sent", "lan_bytes_sent"]):
            records.append(record)
        else:
            logging.warning("Registro ignorado devido a valores críticos ausentes")

    except Exception as e:
        logging.error(f"Erro no processamento de dados: {str(e)}", exc_info=True)
    except Exception as e:
        logging.error(f"Error processing device data: {e}")

def store_data_in_redis(records: List[Dict], key_prefix: str, stream_name: str, pipeline, key_fields: List[str]):
    for record in records:
        try:
            # Convert all values to Redis-safe types
            safe_record = {}
            for k, v in record.items():
                if isinstance(v, (dict, list)):
                    safe_record[k] = json.dumps(v)
                elif v is None:
                    safe_record[k] = ''
                else:
                    safe_record[k] = str(v)
            
            # Create Redis key
            key_values = [safe_record.get(field, 'missing') for field in key_fields]
            redis_key = f"{key_prefix};{';'.join(key_values)}"
            
            # Add to pipeline
            pipeline.hset(redis_key, mapping=safe_record)
            pipeline.xadd(stream_name, safe_record)
            
            # Print the data being added for debugging
            logging.info(f"Adding to Redis key: {redis_key}{safe_record}")
        except Exception as e:
            logging.error(f"Redis storage error: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv('UVICORN_HOST'), port=int(os.getenv('UVICORN_PORT')))
