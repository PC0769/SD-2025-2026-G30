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

## Tarefas futuras (com base no protocolo)

- Definir e documentar formalmente o protocolo SENSOR/GATEWAY/SERVIDOR com mensagens e estados.
- Incluir mensagens explícitas de início e finalização de comunicação.
- Implementar identificação inicial do SENSOR por ID durante handshake.
- Implementar registo explícito dos tipos de dados ativos por SENSOR.
- Implementar heartbeat periódico do SENSOR.
- Implementar deteção de indisponibilidade por ausência de heartbeat.
- Implementar pedido de criação de stream de vídeo no protocolo.
- Criar e usar ficheiros CSV no GATEWAY para sensor_id, estado, zona, tipos_dados e last_sync.
- Validar no GATEWAY se o sensor está registado e com estado válido.
- Validar se o tipo de dado enviado é suportado.
- Atualizar estado e campo last_sync no GATEWAY.
- Registar dados recebidos no GATEWAY.
- Garantir atendimento concorrente no GATEWAY com threads e mutexes.
- Garantir atendimento concorrente no SERVIDOR com threads e mutexes.
- Armazenar dados por tipo em ficheiros distintos no SERVIDOR.
- Considerar persistência em base de dados relacional como funcionalidade extra.
- Preparar relatório final (até 3 páginas, excluindo anexos) com decisões de implementação e protocolo.
