import time
import redis
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv(dotenv_path='')
load_dotenv(dotenv_path='')

# Configurações do Redis
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_PREFIXES = ['redes_proximas;', 'wifistats;', 'dados;', 'routers;']  # Prefixos das chaves do Redis

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
# Salvar todos os dados do Redis em um arquivo antes de limpar
def save_redis_data_to_file():
    start_time = time.strftime("%Y%m%d-%H%M%S")
    filename = f"redis_backup_{start_time}.txt"
    backup_dir = os.getenv('BACKUP_DIR')
    
    with open(filename, 'w') as file:
        for key in redis_client.keys('*'):
            value_type = redis_client.type(key)
            if value_type == 'string':
                value = redis_client.get(key)
            elif value_type == 'hash':
                value = redis_client.hgetall(key)
            elif value_type == 'list':
                value = redis_client.lrange(key, 0, -1)
            elif value_type == 'set':
                value = redis_client.smembers(key)
            elif value_type == 'zset':
                value = redis_client.zrange(key, 0, -1)
            else:
                value = None
            file.write(f"{key}: {value}\n")
    
    if os.path.getsize(filename) > 0:
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        backup_path = os.path.join(backup_dir, filename)
        os.rename(filename, backup_path)
        print(f"Dados do Redis salvos em {backup_path}")
    else:
        os.remove(filename)
        print("Nenhum dado encontrado no Redis para salvar.")

# Salvar e limpar todos os dados do Redis
save_redis_data_to_file()
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
def insert_data_into_timescale(connection, query, data, prefix):
    try:
        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        print(f"Dados inseridos: {prefix}:{data}")
        cursor.close()
    except Exception as e:
        print(f"Erro ao inserir dados no TimescaleDB: {e}")
        connection.rollback()
 
def insert_redes_proximas_data(connection, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth):
    query = sql.SQL("""
        INSERT INTO redes_proximas (detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
    insert_data_into_timescale(connection, query, data, 'redes_proximas')

def insert_wifi_data(connection, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received):
    query = sql.SQL("""
        INSERT INTO wifi_stats (time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)
    data = (time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received)
    insert_data_into_timescale(connection, query, data, 'wifi_stats')

def insert_dados(connection, time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime):
    query = sql.SQL("""
        INSERT INTO dados (time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime)
    insert_data_into_timescale(connection, query, data, 'dados')

def insert_routers(connection, device_id, latitude, longitude, ssid, mac_address):
    query = sql.SQL("""
        INSERT INTO routers (device_id, latitude, longitude, ssid, mac_address
        VALUES (%s, %s, %s, %s, %s)
    """)
    data = (device_id, latitude, longitude, ssid, mac_address)
    insert_data_into_timescale(connection, query, data, 'routers')

# Transferir dados do Redis para o TimescaleDB
def process_redis_keys(connection_geo, connection_bulkdata, processed_keys):
    for prefix in REDIS_PREFIXES:
        keys = redis_client.keys(f"{prefix}*")
        if keys:
            recent_keys = [key for key in keys if redis_client.ttl(key) > 0]
            if recent_keys:
                print(f"Chaves recentes encontradas no Redis com prefixo {prefix}: {recent_keys}")
        for key in keys:
            if key in processed_keys:
                continue  # Ignorar chaves já processadas

            try:
                if prefix == 'redes_proximas;':
                    _, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth = key.split(";")
                    insert_redes_proximas_data(connection_geo, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
                elif prefix == 'wifistats;':
                    _, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received = key.split(";")
                    insert_wifi_data(connection_bulkdata, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received)
                elif prefix == 'dados;' :
                    _, time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4_channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime = key.split(";")
                    insert_dados(connection_bulkdata, time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4_channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime)
                elif prefix == 'routers;':
                    _, device_id, latitude, longitude, ssid, mac_address = key.split(";")
                    insert_routers(connection_bulkdata, device_id, latitude, longitude, ssid, mac_address)

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
