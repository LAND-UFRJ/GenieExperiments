from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import base64
import psycopg2
import argparse
from psycopg2.extras import execute_values
from datetime import datetime, timezone

# Configurações do banco de dados
DB_CONFIG = {
    "dbname": "geolocation",
    "user": "postgres",
    "password": "landufrj123",
    "host": "10.246.3.111",
    "port": 5432
}

# Configurações do servidor HTTP/GenieACS
USERNAME = 'land'
PASSWORD = 'landufrj123'

# Argument parser para obter o valor da porta
parser = argparse.ArgumentParser(description='Iniciar servidor HTTP.')
parser.add_argument('--port', type=int, default=10000, help='Porta para o servidor HTTP')
args = parser.parse_args()
PORT = args.port

def get_db_connection():
    print("Estabelecendo conexão com o banco de dados...")
    return psycopg2.connect(**DB_CONFIG)

class RequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        print("Recebida solicitação POST")
        if not self.is_authenticated():
            self.send_authentication_error()
            return

        try:
            content_length = int(self.headers['Content-Length'])
            print(f"Tamanho do conteúdo: {content_length}")
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            print(f"Dados recebidos: {data}")

            with get_db_connection() as connection:
                self.process_and_insert(data, connection)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'status': 'success', 'message': 'Data inserted into TimescaleDB'}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            print("Dados inseridos com sucesso")
        except Exception as e:
            print(f"Erro durante a solicitação POST: {e}")
            self.send_error_response(500, str(e))

    def do_GET(self):
        print("Recebida solicitação GET")
        if not self.is_authenticated():
            self.send_authentication_error()
            return

        response = {'message': 'GET request received'}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))
        print("Resposta GET enviada")

    def do_OPTIONS(self):
        print("Recebida solicitação OPTIONS")
        self.send_response(200)
        self.send_header('Allow', 'GET, POST, OPTIONS')
        self.send_header('Content-Length', '0')
        self.end_headers()
        print("Resposta OPTIONS enviada")

    def is_authenticated(self):
        auth_header = self.headers.get('Authorization')
        if auth_header is None:
            print("Cabeçalho de autenticação ausente")
            return False

        auth_type, encoded_credentials = auth_header.split(' ', 1)
        if auth_type.lower() != 'basic':
            print("Tipo de autenticação inválido")
            return False

        decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
        username, password = decoded_credentials.split(':', 1)
        authenticated = username == USERNAME and password == PASSWORD
        print(f"Autenticação {'bem-sucedida' if authenticated else 'falhou'}")
        return authenticated

    def send_authentication_error(self):
        print("Erro de autenticação")
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="Login Required"')
        self.end_headers()

    def send_error_response(self, code, message):
        print(f"Enviando resposta de erro: {code}, {message}")
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {'status': 'error', 'message': message}
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def process_data(self, data, cursor):
        print("Processando dados...")
        records = {
            'localizacao_redes': [],
            'redes_proximas': []
        }
        report = data.get('Report', [])
        for item in report:
            print("Processando item...")
            collection_time = datetime.fromtimestamp(int(item['CollectionTime']), timezone.utc)
            device_id = item.get('DeviceId', 'Unknown')  # Captura o identificador do dispositivo
            self.process_neighboring_wifi(item, collection_time, device_id, records['redes_proximas'])
            self.process_localizacao_redes(item, collection_time, device_id, records['localizacao_redes'])
        for table_name, table_records in records.items():
            print(f"Inserindo dados na tabela {table_name}...")
            self.insert_into_timescale(table_name, table_records, cursor)

    """ WORKING ON THIS FUNCTION
    def process_neighboring_wifi(self, item, collection_time, device_id, records):
        print("Processando Neighboring WiFi...")
        neighboring_wifi = item.get('Device', {}).get('WiFi', {}).get('NeighboringWiFiDiagnostic', {}).get('Result', {})
        macaddress_router = item.get('Device', {}).get('WiFi', {}).get('DataElements', {}).get('Network', {}).get('Device', {}).get('1', {}).get('Radio', [])
        print(macaddress_router)
        '''for macrouter in macaddress_router.items():
            if macaddress_router == '1':
            mac_router = macrouter.get('MACAddress', 'Unknown').upper()
        '''    
        if isinstance(neighboring_wifi, dict):
            for bssid, details in neighboring_wifi.items():
                try:
                    bssid = details.get('BSSID', 'Unknown').upper()
                    signal_strength = int(details.get('SignalStrength', 0))
                    channel = details.get('Channel', 'Unknown')
                    frequency_band = details.get('OperatingFrequencyBand', 'Unknown')
                    channel_bandwidth = details.get('OperatingChannelBandwidth', 'Unknown')
                    ssid = details.get('SSID', '0').strip() or '0'

                    records.append((
                        collection_time, device_id, bssid, channel, frequency_band, 
                        channel_bandwidth, signal_strength, ssid
                    ))
                    print(f"WiFi Neighboring Processado: {records[-1]}")
                except (ValueError, TypeError) as e:
                    print(f"Erro ao processar Neighboring WiFi para {bssid}: {e}")
        elif isinstance(neighboring_wifi, list):
            for details in neighboring_wifi:
                try:
                    bssid = details.get('BSSID', 'Unknown').upper()
                    signal_strength = int(details.get('SignalStrength', 0))
                    channel = details.get('Channel', 'Unknown')
                    frequency_band = details.get('OperatingFrequencyBand', 'Unknown')
                    channel_bandwidth = details.get('OperatingChannelBandwidth', 'Unknown')
                    ssid = details.get('SSID', '0').strip() or '0'

                    records.append((
                        collection_time, device_id, bssid, channel, frequency_band, 
                        channel_bandwidth, signal_strength, ssid
                    ))
                    print(f"WiFi Neighboring Processado: {records[-1]}")
                except (ValueError, TypeError) as e:
                    print(f"Erro ao processar Neighboring WiFi: {e}")

        if records:
            print(f"Neighboring WiFi: {records}")
""" 
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
                INSERT INTO redes_proximas (detected_at, bssid_router, bssid_rede, signal_strength, ssid_rede, channel, channel_bandwidth))
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
        print("Processando e inserindo dados...")
        with connection.cursor() as cursor:
            try:
                self.process_data(data, cursor)
                connection.commit()
                print("Dados inseridos com sucesso no banco de dados")
            except Exception as e:
                connection.rollback()
                print(f"Erro ao processar e inserir dados: {e}")

def run(server_class=HTTPServer, handler_class=RequestHandler, port=PORT):
    print(f'Iniciando o servidor HTTP na porta {port}')
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Servidor HTTP iniciado na porta {port}')
    httpd.serve_forever()

if __name__ == "__main__":
    run(port=PORT)
