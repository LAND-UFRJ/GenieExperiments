# GenieExperiments

Repositório do laboratório de computação LAND da COPPE/UFRJ para o projeto de coleta, monitoramento e análise de dados de redes, utilizando dados coletados pelo GenieACS, processados via Nginx, Redis e TimescaleDB.

## Descrição

Este projeto tem como objetivo coletar, processar e armazenar dados de redes de CPEs monitoradas via GenieACS usando o protocolo TR-069. O fluxo de dados é o seguinte:

1. **GenieACS**: Coleta dados de dispositivos via `bulkdata`.
2. **Nginx**: Recebe os dados do GenieACS e os encaminha para um servidor HTTP.
3. **Redis**: Armazena temporariamente os dados recebidos do servidor HTTP.
4. **TimescaleDB**: Armazena os dados processados para análise e consulta.

Os scripts principais do repositório são responsáveis por gerenciar diferentes partes desse fluxo.

## Estrutura do Repositório

- **README.md**: Este arquivo, contendo informações sobre o repositório e o projeto.
- **background.py**: Atualiza o GenieACS na árvore `neighbouring_wifi` para coletar dados de dispositivos vizinhos.
- **genieacs.py**: Ativa o serviço da API do GenieACS para coleta de dados.
- **process_data.py**: Recebe os dados do servidor HTTP e os envia para o Redis.
- **redis_to_timescale.py**: Transfere os dados do Redis para o TimescaleDB para armazenamento e análise.
- **example.py**: Exemplo de código para referência e testes.
- **create_bulkdata.py**: Automatiza a configuração do `bulkdata` para novas CPEs conectadas ao GenieACS.

## Fluxo de Dados

1. **Coleta de Dados**:
   - O `genieacs.py` ativa a API do GenieACS para coletar dados de dispositivos via `bulkdata`.
   - O `background.py` atualiza a árvore `neighbouring_wifi` no GenieACS para garantir que os dados de dispositivos vizinhos sejam coletados.
   - O `create_bulkdata.py` configura automaticamente o `bulkdata` para novas CPEs conectadas ao GenieACS.

2. **Recebimento e Processamento**:
   - Os dados coletados são enviados para um servidor HTTP via Nginx.
   - O `process_data.py` recebe os dados do servidor HTTP e os envia para o Redis.

3. **Armazenamento**:
   - O `redis_to_timescale.py` transfere os dados do Redis para o TimescaleDB, onde são armazenados para análise e consulta.

## Bibliotecas Utilizadas

O projeto utiliza as seguintes bibliotecas Python:

- **genieacs**: Para interagir com a API do GenieACS.
- **time**: Para manipulação de tempo e delays.
- **concurrent.futures**: Para execução concorrente usando `ThreadPoolExecutor`.
- **os**: Para interagir com o sistema operacional e variáveis de ambiente.
- **dotenv**: Para carregar variáveis de ambiente a partir de arquivos `.env`.
- **requests**: Para fazer requisições HTTP.
- **json**: Para manipulação de dados no formato JSON.
- **fastapi**: Para criar o servidor HTTP que recebe os dados.
- **pydantic**: Para validação de dados e criação de modelos.
- **datetime**: Para manipulação de datas e horários.
- **redis**: Para interagir com o Redis.
- **uvicorn**: Para rodar o servidor HTTP baseado no FastAPI.
- **logging**: Para registro de logs.
- **psycopg2**: Para interagir com o TimescaleDB (baseado em PostgreSQL).

## Como Usar

1. Clone o repositório:

   ```bash
   git clone https://github.com/LAND-UFR/GenieExperiments.git
