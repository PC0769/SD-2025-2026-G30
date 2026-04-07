# SD-2025-2026-G30

## Estado atual (simplificado)

O projeto já está estruturado como um sistema distribuído com **3 agentes/processos**:

1. **SensorNode**: simula uma máquina/sensor que regista medições (temperatura ou ruído) e envia dados.
2. **GatewayNode**: recebe dados do sensor e encaminha para o servidor.
3. **ServerNode**: recebe os dados finais e mostra no terminal (simulando armazenamento).

### Comunicação atual

- Protocolo: **TCP**
- Fluxo: **SensorNode -> GatewayNode -> ServerNode**
- Portas:
	- Gateway escuta em `5000`
	- Servidor escuta em `6000`
- Formato de mensagem no sensor:
	- `DATA|ID|TIPO|VALOR|TIMESTAMP`

### Nota de correção de enquadramento

A leitura que fizeste está correta no essencial: há uma máquina que regista/envia, um gateway e um servidor.
Neste estado, o "registo" final ainda é em **consola** no servidor (não há base de dados nem ficheiro persistente).

## Ficheiros core

### Lógica distribuída

- `SensorNode/Program.cs`
	- Interface de consola para escolher tipo de medição.
	- Constrói a mensagem e envia para `127.0.0.1:5000`.

- `GatewayNode/Program.cs`
	- Escuta sensores na porta `5000`.
	- Lê mensagem recebida e reencaminha para `127.0.0.1:6000`.

- `ServerNode/Program.cs`
	- Escuta gateway na porta `6000`.
	- Recebe a mensagem e apresenta como armazenamento.

### Estrutura e arranque

- `SensorNode.sln`
	- Solução com os 3 projetos (`SensorNode`, `GatewayNode`, `ServerNode`).

- `SensorNode.slnLaunch.user`
	- Perfil para iniciar os 3 projetos em conjunto no Visual Studio.

- `SensorNode/SensorNode.csproj`
- `GatewayNode/GatewayNode.csproj`
- `ServerNode/ServerNode.csproj`
	- Definição dos projetos .NET (todos em `net8.0`, executáveis de consola).

## Ficheiros secundários

- `README.md`
	- Documentação do projeto.

- Pastas `bin/` de cada projeto
	- Artefactos compilados (saída de build/debug).

- Pastas `obj/` de cada projeto
	- Artefactos intermédios de compilação/restore.

> Estes ficheiros/pastas secundários não contêm a lógica funcional principal do sistema distribuído.

## Como executar (estado atual)

1. Iniciar `ServerNode`.
2. Iniciar `GatewayNode`.
3. Iniciar `SensorNode` e enviar medições.

Se os três estiverem ativos, deves ver:
- no gateway: receção do sensor e encaminhamento;
- no servidor: receção final para "armazenamento" em consola.
