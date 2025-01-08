import time
import redis
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv(dotenv_path='Land/redis_to_timescale.env')

# Configurações do Redis
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_PREFIXES = ['testtable;', 'redes_proximas;']  # Prefixos das chaves do Redis

# Configurações do TimescaleDB
PG_HOST = os.getenv('PG_HOST')
PG_PORT = int(os.getenv('PG_PORT'))
PG_DATABASE = os.getenv('PG_DATABASE')
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
        connection = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("Conexão com TimescaleDB bem-sucedida.")
        return connection
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

# Transferir dados do Redis para o TimescaleDB
def process_redis_keys(connection, processed_keys):
    for prefix in REDIS_PREFIXES:
        keys = redis_client.keys(f"{prefix}*")
        print(f"Chaves encontradas no Redis com prefixo {prefix}: {keys}")
        
        for key in keys:
            if key in processed_keys:
                continue  # Ignorar chaves já processadas

            try:
                if prefix == 'testtable;':
                    _, device_id, uptime = key.split(";")
                    insert_device_data(connection, device_id, uptime)
                elif prefix == 'redes_proximas;':
                    _, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth = key.split(";")
                    insert_redes_proximas_data(connection, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
                elif prefix == 'wifistats;':
                    _, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received = key.split(";")
                    insert_wifi_data(connection, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received)
                             
                processed_keys.add(key)  # Marcar a chave como processada
            except ValueError as e:
                print(f"Erro ao processar a chave {key}: {e}")
            except Exception as e:
                print(f"Erro inesperado ao processar a chave {key}: {e}")

# Função principal
def main():
    connection = connect_to_timescale()
    processed_keys = set()  # Rastrear chaves já processadas
    
    try:
        while True:
            process_redis_keys(connection, processed_keys)
            time.sleep(5)  # Aguardar 5 segundos antes de verificar novamente
    except KeyboardInterrupt:
        print("Encerrando o programa.")
    finally:
        connection.close()

if __name__ == "__main__":
    main()
