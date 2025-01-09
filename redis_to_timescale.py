import time
import redis
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv(dotenv_path='')

# Configurações do Redis
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_PREFIXES = ['testtable;', 'redes_proximas;', 'wifistats;', 'interface_lan;', 'interface_wan;']  # Prefixos das chaves do Redis

# Configurações do TimescaleDB
PG_HOST = os.getenv('PG_HOST')
PG_PORT = int(os.getenv('PG_PORT'))
PG_DB_geo = os.getenv('PG_DB_geo')
PG_DB_bulk = os.getenv('PG_DB_bulk')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# Conectar ao Redis
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)
# Limpar todos os dados do Redis
redis_client.flushall()
print("Memória do Redis limpa.")

# Conectar ao TimescaleDB
def connect_to_timescale():
    try:
        # Conexão ao primeiro banco de dados
        connection_geo = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB_geo, 
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("Conexão com TimescaleDB (geo) bem-sucedida.")

        # Conexão ao segundo banco de dados
        connection_bulkdata = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB_bulk, 
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("Conexão com TimescaleDB (bulkdata) bem-sucedida.")

        # Retornar ambas as conexões
        return connection_geo, connection_bulkdata
    except Exception as e:
        print(f"Erro ao conectar ao TimescaleDB: {e}")
        raise

# Processar e inserir dados no TimescaleDB
def insert_data_into_timescale(connection, query, data):
    try:
        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        print(f"Dados inseridos: {data}")
        cursor.close()
    except Exception as e:
        print(f"Erro ao inserir dados no TimescaleDB: {e}")
        connection.rollback()

def insert_device_data(connection, device_id, uptime):
    query = sql.SQL("""
        INSERT INTO testtable (device_id, uptime)
        VALUES (%s, %s)
    """)
    data = (device_id, uptime)
    insert_data_into_timescale(connection, query, data)
    
def insert_redes_proximas_data(connection, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth):
    query = sql.SQL("""
        INSERT INTO redes_proximas (detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
    insert_data_into_timescale(connection, query, data)

def insert_wifi_data(connection, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received, bytes_sent, bytes_received):
    query = sql.SQL("""
        INSERT INTO wifi_stats (time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)
    data = (time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received, bytes_sent, bytes_received)
    insert_data_into_timescale(connection, query, data)

def insert_interface_lan(connection, time, device_id, lan_packets_received, lan_bytes_received, lan_bytes_per_packets_received, lan_packets_sent, lan_bytes_sent, lan_bytes_per_packets_sent, lan_errors_sent, lan_errors_received):
    query = sql.SQL("""
        INSERT INTO interface_lan (time, device_id, lan_packets_received, lan_bytes_received, lan_bytes_per_packets_received, lan_packets_sent, lan_bytes_sent, lan_bytes_per_packets_sent, lan_errors_sent, lan_errors_received)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (time, device_id, lan_packets_received, lan_bytes_received, lan_bytes_per_packets_received, lan_packets_sent, lan_bytes_sent, lan_bytes_per_packets_sent, lan_errors_sent, lan_errors_received)
    insert_data_into_timescale(connection, query, data)

def insert_interface_wan(connection, time, device_id, wan_packets_received, wan_bytes_received, wan_bytes_per_packets_received, wan_packets_sent, wan_bytes_sent, wan_bytes_per_packets_sent, wan_errors_sent, wan_errors_received):
    query = sql.SQL("""
        INSERT INTO interface_wan (time, device_id, wan_packets_received, wan_bytes_received, wan_bytes_per_packets_received, wan_packets_sent, wan_bytes_sent, wan_bytes_per_packets_sent, wan_errors_sent, wan_errors_received)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (time, device_id, wan_packets_received, wan_bytes_received, wan_bytes_per_packets_received, wan_packets_sent, wan_bytes_sent, wan_bytes_per_packets_sent, wan_errors_sent, wan_errors_received)
    insert_data_into_timescale(connection, query, data)

# Transferir dados do Redis para o TimescaleDB
def process_redis_keys(connection_geo, connection_bulkdata, processed_keys):
    for prefix in REDIS_PREFIXES:
        keys = redis_client.keys(f"{prefix}*")
        print(f"Chaves encontradas no Redis com prefixo {prefix}: {keys}")
        
        for key in keys:
            if key in processed_keys:
                continue  # Ignorar chaves já processadas

            try:
                if prefix == 'testtable;':
                    _, device_id, uptime = key.split(";")
                    insert_device_data(connection_geo, device_id, uptime)
                elif prefix == 'redes_proximas;':
                    _, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth = key.split(";")
                    insert_redes_proximas_data(connection_geo, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
                elif prefix == 'wifistats;':
                    _, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received = key.split(";")
                    insert_wifi_data(connection_bulkdata, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received)
                elif prefix == 'interface_wan;':
                    _, time, device_id, wan_packets_received, wan_bytes_received, wan_bytes_per_packets_received, wan_packets_sent, wan_bytes_sent, wan_bytes_per_packets_sent, wan_errors_sent, wan_errors_received = key.split(";")
                    insert_interface_wan(connection_bulkdata, time, device_id, wan_packets_received, wan_bytes_received, wan_bytes_per_packets_received, wan_packets_sent, wan_bytes_sent, wan_bytes_per_packets_sent, wan_errors_sent, wan_errors_received)
                elif prefix == 'interface_lan;':
                    _, time, device_id, lan_packets_received, lan_bytes_received, lan_bytes_per_packets_received, lan_packets_sent, lan_bytes_sent, lan_bytes_per_packets_sent, lan_errors_sent, lan_errors_received = key.split(";")
                    insert_interface_lan(connection_bulkdata, time, device_id, lan_packets_received, lan_bytes_received, lan_bytes_per_packets_received, lan_packets_sent, lan_bytes_sent, lan_bytes_per_packets_sent, lan_errors_sent, lan_errors_received)
                             
                processed_keys.add(key)  # Marcar a chave como processada
            except ValueError as e:
                print(f"Erro ao processar a chave {key}: {e}")
            except Exception as e:
                print(f"Erro inesperado ao processar a chave {key}: {e}")

# Função principal
def main():
    try:
        connection_geo, connection_bulkdata = connect_to_timescale()
        processed_keys = set()  # Rastrear chaves já processadas
        
        while True:
            process_redis_keys(connection_geo, connection_bulkdata, processed_keys)
            time.sleep(5)  # Aguardar 5 segundos antes de verificar novamente
    except KeyboardInterrupt:
        print("Encerrando o programa.")
    except Exception as e:
        print(f"Erro no programa principal: {e}")
    finally:
        if 'connection_geo' in locals() and connection_geo:
            connection_geo.close()
        if 'connection_bulkdata' in locals() and connection_bulkdata:
            connection_bulkdata.close()

if __name__ == "__main__":
    main()
