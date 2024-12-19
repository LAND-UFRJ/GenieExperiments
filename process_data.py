from flask import Flask, request, jsonify
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import socket
import json

app = Flask(__name__)


# Variável para armazenar os dados recebidos
received_data = {}

# Função para enviar dados para a porta 10002
def send_data_to_redis(data):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('10.246.3.128', 10002))  # Conecte-se ao Redis
            # Formatar comando SET no protocolo RESP
            redis_command = f"*3\r\n$3\r\nSET\r\n${len('key')}\r\nkey\r\n${len(json.dumps(data))}\r\n{json.dumps(data)}\r\n"
            s.sendall(redis_command.encode('utf-8'))
            response = s.recv(1024)
        print(f"Resposta do Redis: {response.decode('utf-8')}")
    except Exception as e:
        print(f"Erro ao enviar dados para o Redis: {e}")


# Rota para receber dados POST
@app.route('/bulkdata', methods=['POST'])
def receive_json():
    global received_data
    received_data = request.json  # Armazena o JSON recebido
    print("JSON recebido:", received_data)

    # Enviar os dados para a porta 10002
    try:
        response = send_data_to_redis(received_data)  # Chama a função de envio
        if response:
            print(f"Resposta do servidor na porta 10002: {response}")
        else:
            print("Falha ao receber resposta do servidor na porta 10002.")
    except Exception as e:
        print(f"Erro ao enviar dados para a porta 10002: {e}")

    return jsonify({"message": "Dados recebidos e enviados com sucesso"}), 200

# Rota para enviar dados via GET
@app.route('/get_json', methods=['GET'])
def get_json():
    if received_data:
        return jsonify(received_data), 200
    else:
        return jsonify({"message": "Nenhum dado disponível"}), 404

'''
def process_data(self, received_data, cursor):
    #print("Processando dados...")
    records = {
        'redes_proximas': []
    }
    report = received_data.get('Report', [])
    for item in report:
        #print("Processando item...")
        collection_time = datetime.fromtimestamp(int(item['CollectionTime']), timezone.utc)
        device_id = item.get('Device', {}).get('ManagementServer', {}).get('ConnectionRequestUsername', 'unknown')  # Captura o identificador do dispositivo
        self.process_neighboring_wifi(item, collection_time, records['redes_proximas'])
    for table_name, table_records in records.items():
        #print(f"Inserindo dados na tabela {table_name}...")
        self.insert_into_timescale(table_name, table_records, cursor)

def process_neighboring_wifi(self, item, collection_time, records):
    #print("Processando Neighboring WiFi...")
    neighboring_wifi = item.get('Device', {}).get('WiFi', {}).get('NeighboringWiFiDiagnostic', {}).get('Result', {})
    bssid_router2 = item.get('Device', {}).get('WiFi', {}).get('SSID', {}).get('1', {}).get('BSSID', 'Unknown').upper()
    bssid_router5 = item.get('Device', {}).get('WiFi', {}).get('SSID', {}).get('3', {}).get('BSSID', 'Unknown').upper()
    if isinstance(neighboring_wifi, dict):
        for bssid, details in neighboring_wifi.items():
            try:
                bssid_rede = details.get('BSSID', 'Unknown').upper()
                signal_strength = int(details.get('SignalStrength', 1))
                channel = int(details.get('Channel', 'Unknown'))
                channel_bandwidth = details.get('OperatingChannelBandwidth', 'Unknown')
                ssid_rede = details.get('SSID', '0').strip() or '0'
                if  0 < channel < 14: 
                    records.append((
                        collection_time, bssid_router2, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth
                    ))
                    print(f"WiFi Neighboring Processado: {records[-1]}")
                elif 35 < channel < 166:
                    records.append((
                        collection_time, bssid_router5, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth
                    ))
                    print(f"WiFi Neighboring Processado: {records[-1]}")
            except (ValueError, TypeError) as e:
                print(f"Erro ao processar Neighboring WiFi para {bssid}: {e}")
    elif isinstance(neighboring_wifi, list):
        for details in neighboring_wifi:
            try:
                bssid_rede = details.get('BSSID', 'Unknown').upper()
                signal_strength = int(details.get('SignalStrength', 1))
                channel = details.get('Channel', 'Unknown')
                channel_bandwidth = details.get('OperatingChannelBandwidth', 'Unknown')
                ssid_rede = details.get('SSID', '0').strip() or '0'
                if 0 < channel < 14:
                    records.append((
                        collection_time, bssid_router2, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth
                    ))
                    print(f"WiFi Neighboring Processado: {records[-1]}")
                elif 35 < channel < 166:
                    records.append((
                        collection_time, bssid_router5, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth
                    ))
                    print(f"WiFi Neighboring Processado: {records[-1]}")
            except (ValueError, TypeError) as e:
                print(f"Erro ao processar Neighboring WiFi para {bssid}: {e}")
    if records:
        print(f"Neighboring WiFi: {records}")    

#Inscrição dos dados no timescale

def insert_into_timescale(self, table_name, records, cursor):
    if not records:
        print(f"Nenhum dado para inserir na tabela {table_name}")
        return

    queries = {
        'localizacao_redes': """
            INSERT INTO localizacao_redes (bssid, device_id ,ssid, latitude, longitude, radius)
            VALUES %s
        """,
        'redes_proximas': """
            INSERT INTO redes_proximas (detected_at, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth)
            VALUES %s
        """
    }

    query = queries.get(table_name)
    if query:
        execute_values(cursor, query, records)
        print(f"Dados inseridos na tabela {table_name}: {records}")
    else:
        print(f"Tabela desconhecida: {table_name}")

def process_and_insert(self, data, connection):
    #print("Processando e inserindo dados...")
    with connection.cursor() as cursor:
        try:
            self.process_data(data, cursor)
            connection.commit()
            #print("Dados inseridos com sucesso no banco de dados")
        except Exception as e:
            connection.rollback()
            print(f"Erro ao processar e inserir dados: {e}")
'''
if __name__ == '__main__':
    app.run(host='', port=10001, debug=True)
