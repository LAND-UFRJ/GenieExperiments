import json
import redis
import psycopg2
from flask import Flask, request, jsonify
from psycopg2.extras import execute_values

# Inicializa o aplicativo Flask
app = Flask(__name__)

# Configura o cliente Redis
redis_client = redis.Redis(host='10.246.3.128', port=10003, db=0)

class DataProcessor:
    def __init__(self):
        # Inicializa o cliente Redis na criação de uma instância de DataProcessor
        print("Inicializando o cliente Redis...")
        self.redis_client = redis.Redis(host='10.246.3.128', port=10003, db=0)
        print("Cliente Redis inicializado.")

    def send_to_redis(self, channel, data):
        try:
            print(f"Enviando dados para o canal Redis {channel}...")
            self.redis_client.publish(channel, json.dumps(data))
            print(f"Dados enviados para o canal Redis {channel}: {data}")
        except Exception as e:
            print(f"Erro ao enviar dados para o Redis: {e}")


    def insert_into_timescale(self, table_name, records, cursor):
        if not records:
            # Verifica se há registros para inserir
            print(f"Nenhum dado para inserir na tabela {table_name}")
            return

        # Define as queries de inserção para diferentes tabelas
        queries = {
            'routers_conhecidos': """
                INSERT INTO routers_conhecidos (bssid_router, device_id, ssid, latitude, longitude)
                VALUES %s
            """,
            'redes_proximas': """
                INSERT INTO redes_proximas (detected_at, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
                VALUES %s
            """
        }

        # Obtém a query correspondente à tabela especificada
        query = queries.get(table_name)
        if query:
            # Executa a inserção dos registros na tabela TimescaleDB
            print(f"Inserindo dados na tabela {table_name}...")
            execute_values(cursor, query, records)
            print(f"Dados inseridos na tabela {table_name}: {records}")
        else:
            print(f"Tabela desconhecida: {table_name}")

    def process_and_insert(self, data, connection):
        with connection.cursor() as cursor:
            try:
                self.send_to_redis('data_channel', data)
                print("Processando e inserindo dados...")
                self.process_data(data, cursor)
                connection.commit()
                print("Dados inseridos com sucesso no banco de dados")
            except Exception as e:
                connection.rollback()
                print(f"Erro ao processar e inserir dados: {e}")

    def process_data(self, data, cursor):
        # Sua lógica para processar os dados (não implementada no exemplo)
        print("Processando dados...")
        pass

# Define a rota no Flask para receber dados via POST
@app.route('/bulkdata', methods=['POST'])
def receive_data():
    # Recebe os dados em formato JSON da solicitação
    data = request.get_json()
    print(f"Dados recebidos: {json.dumps(data, indent=4)}")  # Print formatado dos dados recebidos
    processor = DataProcessor()
    
    # Envia os dados para o Redis
    processor.send_to_redis('data_channel', data)
    
    # Conectando e processando dados no TimescaleDB
    print("Conectando ao banco de dados TimescaleDB...")
    connection = psycopg2.connect(
        host='postgres',
        dbname='geolocation',
        user='localuser',
        password='landufrj123'
    )
    print("Conexão com o banco de dados estabelecida.")
    
    # Processa e insere os dados recebidos no TimescaleDB
    processor.process_and_insert(data, connection)
    return jsonify({"status": "success", "received_data": data}), 200
# Inicializa o servidor Flask no IP 0.0.0.0 e porta 12000 para escutar todas as interfaces
if __name__ == '__main__':
    print("Iniciando o servidor Flask na porta 12000...")
    app.run(host='0.0.0.0', port=12000)
    print("Servidor Flask iniciado.")
