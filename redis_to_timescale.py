import time
import redis
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
import logging

# Carregar variáveis de ambiente do arquivo .env
load_dotenv(dotenv_path='')
load_dotenv(dotenv_path='')

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("log/redis_to_timescaled.log"),
        logging.StreamHandler()
    ]
)

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

# Conectar ao TimescaleDB com retentativas
def connect_to_timescale():
    max_retries = 1000 # Número máximo de tentativas
    retry_delay = 30  # Segundos entre tentativas

    for attempt in range(max_retries):
        try:
            # Conexão ao primeiro banco de dados
            connection_geo = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB_geo,
                user=PG_USER,
                password=PG_PASSWORD
            )
            # Conexão ao segundo banco de dados
            connection_bulkdata = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB_bulk,
                user=PG_USER,
                password=PG_PASSWORD
            )
            logging.info("Conexões com TimescaleDB bem-sucedidas.")
            return connection_geo, connection_bulkdata
        except Exception as e:
            logging.error(f"Erro ao conectar ao TimescaleDB (tentativa {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise  # Se todas as tentativas falharem, propaga o erro

# Processar e inserir dados no TimescaleDB (com verificação de conexão)
def insert_data_into_timescale(connection, query, data, prefix):
    try:
        # Verifica se a conexão está fechada
        if connection.closed:
            logging.error("Conexão fechada. Uma reconexão será feita no próximo ciclo.")
            raise OperationalError("Conexão fechada.")

        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        logging.info(f"Dados inseridos: {prefix}:{data}")
        cursor.close()
    except OperationalError as e:
        if "SSL connection has been closed unexpectedly" in str(e):
            logging.error("Erro de SSL. A conexão será reiniciada.")
            connection.close()  # Fecha a conexão corrompida
        else:
            logging.error(f"Erro operacional ao inserir dados: {e}")
        connection.rollback()
    except Exception as e:
        logging.error(f"Erro ao inserir dados no TimescaleDB: {e}")
        connection.rollback()
 
def insert_redes_proximas_data(connection, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth):
    query = sql.SQL("""
        INSERT INTO redes_proximas (detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
    insert_data_into_timescale(connection, query, data, 'redes_proximas')

def insert_wifi_data(connection, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received, bytes_sent, bytes_received, errors_sent, errors_received, radio_connected, time_since_connected):
    query = sql.SQL("""
        INSERT INTO wifi_stats (time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received, bytes_sent, bytes_received, errors_sent, errors_received, radio_connected, time_since_connected)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received, bytes_sent, bytes_received, errors_sent, errors_received, radio_connected, time_since_connected)
    insert_data_into_timescale(connection, query, data, 'wifi_stats')

def insert_dados(connection, time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime, memory_free, memory_total, cpu_usage):
    query = sql.SQL("""
        INSERT INTO dados (time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime, memory_free, memory_total, cpu_usage)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    data = (time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime, memory_free, memory_total, cpu_usage)
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
                logging.info(f"Chaves recentes encontradas no Redis com prefixo {prefix}: {recent_keys}")
        for key in keys:
            if key in processed_keys:
                continue  # Ignorar chaves já processadas

            try:
                if prefix == 'redes_proximas;':
                    _, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth = key.split(";")
                    insert_redes_proximas_data(connection_geo, detected_at, device_id, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
                elif prefix == 'wifistats;':
                    _, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received, bytes_sent, bytes_received, errors_sent, errors_received, radio_connected, time_since_connected = key.split(";")
                    insert_wifi_data(connection_bulkdata, time, device_id, mac_address, hostname, signal_strength, packets_sent, packets_received, bytes_sent, bytes_received, errors_sent, errors_received, radio_connected, time_since_connected)
                elif prefix == 'dados;':
                    _, time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4_channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime, memory_free, memory_total, cpu_usage = key.split(";")
                    insert_dados(connection_bulkdata, time, device_id, wan_bytes_sent, wan_bytes_received, wan_packets_sent, wan_packets_received, lan_bytes_sent, lan_bytes_received, lan_packets_sent, lan_packets_received, wifi_bytes_sent, wifi_bytes_received, wifi_packets_sent, wifi_packets_received, signal_pon, wifi2_4_channel, wifi2_4bandwith, wifi2_4ssid, wifi_5_channel, wifi_5_bandwith, wifi_5_ssid, uptime, memory_free, memory_total, cpu_usage)
                elif prefix == 'routers;':
                    _, device_id, latitude, longitude, ssid, mac_address = key.split(";")
                    insert_routers(connection_bulkdata, device_id, latitude, longitude, ssid, mac_address)

                processed_keys.add(key)  # Marcar a chave como processada
            except ValueError as e:
                logging.info(f"Erro ao processar a chave {key}: {e}")
            except Exception as e:
                logging.error(f"Erro inesperado ao processar a chave {key}: {e}")

# Função principal com lógica de reconexão
def main():
    processed_keys = set()
    connection_geo = None
    connection_bulkdata = None

    while True:  # Loop externo para reconexão
        try:
            # Conectar ao TimescaleDB
            connection_geo, connection_bulkdata = connect_to_timescale()

            # Loop interno para processamento contínuo
            while True:
                # Verifica se as conexões estão ativas
                if connection_geo.closed or connection_bulkdata.closed:
                    logging.error("Conexões perdidas. Reconectando...")
                    break  # Sai do loop interno para reconectar

                # Processa as chaves do Redis
                process_redis_keys(connection_geo, connection_bulkdata, processed_keys)
                time.sleep(5)

        except OperationalError as e:
            logging.error(f"Erro de conexão: {e}. Tentando reconectar em 5 segundos...")
            time.sleep(5)
        except KeyboardInterrupt:
            logging.info("Encerrando o programa.")
            break
        except Exception as e:
            logging.error(f"Erro inesperado: {e}. Tentando reconectar em 5 segundos...")
            time.sleep(5)
        finally:
            # Fecha as conexões ao sair ou em caso de erro
            if connection_geo and not connection_geo.closed:
                connection_geo.close()
            if connection_bulkdata and not connection_bulkdata.closed:
                connection_bulkdata.close()

if __name__ == "__main__":
    main()

