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
        self.redis_client = redis.Redis(host='10.246.3.128', port=10003, db=0)
    
    def send_to_redis(self, channel, data):
        # Publica os dados no canal Redis especificado
        self.redis_client.publish(channel, json.dumps(data))
        print(f"Dados enviados para o canal Redis {channel}: {data}")

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
            execute_values(cursor, query, records)
            print(f"Dados inseridos na tabela {table_name}: {records}")
        else:
            print(f"Tabela desconhecida: {table_name}")

    def process_and_insert(self, data, connection):
        with connection.cursor() as cursor:
            try:
                # Envia os dados para o Redis
                self.send_to_redis('data_channel', data)
                
                # Processa e insere os dados no TimescaleDB
                self.process_data(data, cursor)
                connection.commit()
                print("Dados inseridos com sucesso no banco de dados")
            except Exception as e:
                # Desfaz a transação em caso de erro
                connection.rollback()
                print(f"Erro ao processar e inserir dados: {e}")

    def process_data(self, data, cursor):
        # Sua lógica para processar os dados (não implementada no exemplo)
        pass

# Define a rota no Flask para receber dados via POST
@app.route('/bulkdata', methods=['POST'])
def receive_data():
    # Recebe os dados em formato JSON da solicitação
    data = request.get_json()
    processor = DataProcessor()
    
    # Estabelece a conexão com o banco de dados TimescaleDB
    connection = psycopg2.connect(
        host='postgres',
        dbname='geolocation',
        user='localuser',
        password='landufrj123'
    )
    
    # Processa e insere os dados recebidos
    processor.process_and_insert(data, connection)
    return jsonify({"status": "success"}), 200

# Inicializa o servidor Flask na porta 12000
if __name__ == '__main__':
    app.run(port=12000)
