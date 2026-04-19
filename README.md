# SD-2025-2026-G30

## Enquadramento

Este trabalho prático insere-se no tema Serviços de Monitorização Urbana para One Health, com foco na recolha e encaminhamento de dados ambientais em contexto distribuído.

A arquitetura implementada segue três entidades:
1. SENSOR
2. GATEWAY
3. SERVIDOR

## Estado atual do sistema

O projeto encontra-se estruturado como um sistema distribuído com 3 processos:
1. SensorNode: simula um sensor urbano, recolhe medições e envia dados.
2. GatewayNode: recebe dados do sensor e encaminha para o servidor.
3. ServerNode: recebe os dados finais e apresenta o registo em consola.

No estado atual, o registo final ainda é feito em consola no servidor, sem persistência em base de dados ou ficheiro.

## Comunicação atual

- Protocolo de transporte: TCP
- Fluxo: SensorNode -> GatewayNode -> ServerNode
- Portas:
1. Gateway em 5000
2. Servidor em 6000
- Formato de mensagem usado no sensor:
DATA|ID|TIPO|VALOR|TIMESTAMP
- Respostas de controlo:
ACK|ORIGEM|TIPO_MENSAGEM
NACK|ORIGEM|MOTIVO

## Configuracao do Gateway

O ficheiro GatewayNode/config.csv esta em formato CSV com delimitador ';' e cabecalho:

sensor_id;estado;zona;tipos_dados;last_sync

## Protocolo formal

O protocolo formal encontra-se documentado em PROTOCOLO.md.

Conjunto de mensagens obrigatorias definido:

- START|sensor_id|timestamp
- DATA|sensor_id|tipo|valor|timestamp
- HB|sensor_id|timestamp
- END|sensor_id|motivo|timestamp

## Ficheiros principais do projeto

### Core

- SensorNode/Program.cs
Interface de texto simples, construção de mensagem e envio para o gateway.

- GatewayNode/Program.cs
Escuta de sensores, receção de mensagens e encaminhamento para o servidor.

- ServerNode/Program.cs
Escuta do gateway, receção de dados e apresentação do registo.

- SensorNode.sln
Solução com os três projetos.

- SensorNode/SensorNode.csproj
- GatewayNode/GatewayNode.csproj
- ServerNode/ServerNode.csproj
Definição dos projetos .NET (net8.0, consola).

### Secundarios

- README.md
Documentação do projeto.

- Pastas bin de cada projeto
Artefactos compilados.

- Pastas obj de cada projeto
Artefactos intermédios de compilação.

## Execução do sistema (estado atual)

1. Iniciar ServerNode.
2. Iniciar GatewayNode.
3. Iniciar SensorNode e enviar medições.

Com os três processos ativos, observa-se:
- no gateway: receção das mensagens do sensor e encaminhamento;
- no servidor: receção final para registo em consola.

## Tarefas já realizadas (com base no protocolo)

- Implementação usando sockets em C#.
- Implementação de SENSOR, GATEWAY e SERVIDOR simples.
- Interface de texto simples no SENSOR.
- Envio de medições ambientais do SENSOR para o GATEWAY.
- Encaminhamento de dados do GATEWAY para o SERVIDOR.
- Receção de dados no SERVIDOR.

