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
    return psycopg2.connect(**DB_CONFIG)

class RequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        if not self.is_authenticated():
            self.send_authentication_error()
            return

        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)

            with get_db_connection() as connection:
                self.process_and_insert(data, connection)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'status': 'success', 'message': 'Data inserted into TimescaleDB'}
            self.wfile.write(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.send_error_response(500, str(e))

    def do_GET(self):
        if not self.is_authenticated():
            self.send_authentication_error()
            return

        response = {'message': 'GET request received'}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Allow', 'GET, POST, OPTIONS')
        self.send_header('Content-Length', '0')
        self.end_headers()

    def is_authenticated(self):
        auth_header = self.headers.get('Authorization')
        if auth_header is None:
            return False

        auth_type, encoded_credentials = auth_header.split(' ', 1)
        if auth_type.lower() != 'basic':
            return False

        decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
        username, password = decoded_credentials.split(':', 1)
        return username == USERNAME and password == PASSWORD

    def send_authentication_error(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="Login Required"')
        self.end_headers()

    def send_error_response(self, code, message):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {'status': 'error', 'message': message}
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def process_data(self, data, cursor):
        records = {
            'redes_localizadas': [],
            'neighboring_wifi': []
        }
        report = data.get('Report', [])
        for item in report:
            collection_time = datetime.fromtimestamp(int(item['CollectionTime']), timezone.utc)
            device_id = item.get('DeviceId', 'Unknown')  # Captura o identificador do dispositivo
            self.process_neighboring_wifi(item, collection_time, device_id, records['neighboring_wifi'])

        for table_name, table_records in records.items():
            self.insert_into_timescale(table_name, table_records, cursor)
            
    def process_neighboring_wifi(self, item, collection_time, device_id, records):
        neighboring_wifi = item.get('Device', {}).get('WiFi', {}).get('NeighboringWiFiDiagnostic', {}).get('Result', {})
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
                except (ValueError, TypeError) as e:
                    print(f"Erro ao processar Neighboring WiFi: {e}")

        if records:
            print(f"Neighboring WiFi:{records}")

    def insert_into_timescale(self, table_name, records, cursor):
        if not records:
            print(f"Nenhum dado para inserir na tabela {table_name}")
            return

        queries = {
            'roteadores_conhecidos': """
                INSERT INTO roteadores_conhecidos (bssid, ssid, latitude, longitude, channel, channel_bandwidth)
                VALUES %s
            """,
            'redes_localizadas': """
                INSERT INTO redes_localizadas (detected_at, bssid_routers, bssid_nbw, signal_strength_nbw)
                VALUES %s
            """             
        }

        query = queries.get(table_name)
        if query:
            execute_values(cursor, query, records)
        else:
            raise ValueError(f"Tabela desconhecida: {table_name}")

    def process_and_insert(self, data, connection):
        with connection.cursor() as cursor:
            try:
                self.process_data(data, cursor)
                connection.commit()
            except Exception as e:
                connection.rollback()
                print(f"Erro ao processar e inserir dados: {e}")

def run(server_class=HTTPServer, handler_class=RequestHandler, port=PORT):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd server on port {port}')
    httpd.serve_forever()

if __name__ == "__main__":
    run(port=PORT)
