# TP2 — Plano de Implementação (SD 2025/2026 G30)

Evolução do sistema TP1 (sockets TCP) para arquitetura distribuída com RPC (Python), Pub/Sub (RabbitMQ), base de dados (SQLite) e interface web (Python/Flask).

---

## Arquitetura Final

```
[SensorNode C#] ──publish──► [RabbitMQ] ◄──subscribe── [GatewayNode C#]
                                                               │
                                                         RPC call (HTTP/JSON)
                                                               ▼
                                                   [PreProcessingService Python]
                                                               │
                                                      (dados normalizados)
                                                               │
                                                    TCP (protocolo existente)
                                                               ▼
                                                        [ServerNode C#]
                                                               │
                                                         RPC call (HTTP/JSON)
                                                               ▼
                                                    [AnalysisService Python]
                                                               │
                                                           SQLite DB
                                                               │
                                                    [WebInterface Python/Flask]
```

---

## Fase 0 — Pré-requisitos e Setup (antes de codificar)

- [ ] **0.1** Instalar Python 3.11+ (se não tiver): https://www.python.org/downloads/
- [ ] **0.2** Instalar RabbitMQ:
  - Download: https://www.rabbitmq.com/install-windows.html
  - Requer Erlang: https://www.erlang.org/downloads
  - Após instalar, iniciar o serviço: `rabbitmq-service start`
  - Ativar management plugin: `rabbitmq-plugins enable rabbitmq_management`
  - Dashboard disponível em: http://localhost:15672 (user: guest / guest)
- [ ] **0.3** Instalar pacotes Python necessários:
  ```
  pip install flask requests pika
  ```
- [ ] **0.4** Adicionar pacotes NuGet ao GatewayNode e ServerNode:
  - `RabbitMQ.Client` → GatewayNode
  - `Microsoft.Data.Sqlite` → ServerNode

---

## Fase 1 — RPC: Serviços em Python

### Etapa 1.1 — PreProcessingService (Python)

**Novo ficheiro:** `PreProcessingService/app.py`

Serviço HTTP/JSON que expõe um endpoint RPC:
- `POST /preprocess` — recebe um DATA packet em JSON, normaliza o valor (escala, formato), devolve JSON normalizado

Campos de entrada/saída:
```json
// Input
{ "sensor_id": "S101", "tipo": "TEMP", "valor": "22,5", "timestamp": "..." }
// Output
{ "sensor_id": "S101", "tipo": "TEMP", "valor": 22.50, "timestamp": "...", "normalizado": true }
```

Lógica de normalização:
- Substituição de vírgula por ponto em valores decimais
- Arredondamento a 2 casas decimais
- Validação de gama por tipo (TEMP, HUM, RUIDO, PM2.5, PM10, AR)

**Porta:** 7000

---

### Etapa 1.2 — Integração RPC no GatewayNode (C#)

**Ficheiro a alterar:** `GatewayNode/Program.cs`

- Adicionar função `ChamarPreProcessamento(dados)` que faz `HttpClient POST` para `http://localhost:7000/preprocess`
- Substituir a normalização local atual pela chamada RPC
- Se o serviço não responder → fallback para normalização local (resiliência)

---

### Etapa 1.3 — AnalysisService (Python)

**Novo ficheiro:** `AnalysisService/app.py`

Serviço HTTP/JSON que expõe endpoints RPC:
- `POST /analyze` — recebe lista de AGGDATA records, devolve análise estatística
- `GET /health` — health check

Análises implementadas:
- Média, desvio padrão, mínimo, máximo por tipo de sensor
- Deteção de valores acima de threshold (ex: TEMP > 35 → risco)
- Tendência (subida/descida) nas últimas N amostras

**Porta:** 7001

---

### Etapa 1.4 — Integração RPC no ServerNode (C#)

**Ficheiro a alterar:** `ServerNode/Program.cs`

- Adicionar função `ChamarAnalise(aggData)` que faz `HttpClient POST` para `http://localhost:7001/analyze`
- Invocar após receber AGGDATA do Gateway
- Guardar resultado da análise na base de dados (preparação para Fase 3)

---

## Fase 2 — Pub/Sub com RabbitMQ

### Etapa 2.1 — Adaptar SensorNode (C#)

**Ficheiro a alterar:** `SensorNode/Program.cs`

- Adicionar pacote NuGet `RabbitMQ.Client`
- Substituir `EnviarMensagem()` (TCP direto) por publicação em RabbitMQ
- Exchange: `sensor_data` (type: `topic`)
- Routing key: `sensor.<zona>.<tipo>` (ex: `sensor.ZONA_CENTRO.TEMP`)
- Mensagem publicada: formato JSON com os campos do protocolo existente
- Manter menu de consola igual; só muda o transporte

---

### Etapa 2.2 — Adaptar GatewayNode (C#)

**Ficheiro a alterar:** `GatewayNode/Program.cs`

- Substituir listener TCP `:5000` por subscrição RabbitMQ
- Subscrever routing keys baseadas na zona do `config.csv` (ex: `sensor.ZONA_CENTRO.*`)
- Ao receber mensagem: desserializar JSON → invocar RPC de pré-processamento (Fase 1) → lógica de agregação existente → encaminhar para ServerNode via TCP (mantém protocolo TP1 com Servidor)
- Manter toda a lógica de agregação, sessões e heartbeat timeout

---

## Fase 3 — Base de Dados e Interface Web

### Etapa 3.1 — SQLite no ServerNode (C#)

**Ficheiro a alterar:** `ServerNode/Program.cs`

- Adicionar pacote `Microsoft.Data.Sqlite`
- Criar BD `onehealth.db` com tabelas:
  - `sensor_sessions` (sensor_id, fase, timestamp_inicio, timestamp_fim)
  - `sensor_data` (id, sensor_id, tipo, valor, timestamp)
  - `aggregated_data` (id, sensor_id, tipo, media, minimo, maximo, contagem, janela_inicio, janela_fim)
  - `analysis_results` (id, sensor_id, tipo, resultado_json, timestamp)
- Substituir `AppendServidorFicheiro()` por inserções SQL
- Manter ficheiros `.txt` como log secundário (opcional)

---

### Etapa 3.2 — Interface Web Python/Flask

**Novo ficheiro:** `WebInterface/app.py`

Lê diretamente da BD SQLite `onehealth.db` e expõe:

- `GET /` — dashboard com últimas leituras por sensor
- `GET /sensors` — lista de sensores com último estado de sessão
- `GET /data?sensor_id=X&tipo=TEMP&from=...&to=...` — consulta parametrizada
- `GET /analysis` — últimos resultados de análise do AnalysisService
- `POST /analyze` — disparar nova análise com parâmetros

**Porta:** 8080

---

## Resumo de Ficheiros Novos e Alterados

| Ação | Ficheiro |
|------|----------|
| **NOVO** | `PreProcessingService/app.py` |
| **NOVO** | `AnalysisService/app.py` |
| **NOVO** | `WebInterface/app.py` |
| **ALTERADO** | `GatewayNode/Program.cs` + `.csproj` |
| **ALTERADO** | `ServerNode/Program.cs` + `.csproj` |
| **ALTERADO** | `SensorNode/Program.cs` + `.csproj` |

---

## Ordem de Arranque (sistema completo)

```
1. RabbitMQ (serviço Windows)
2. python PreProcessingService/app.py     (porta 7000)
3. python AnalysisService/app.py          (porta 7001)
4. dotnet run --project ServerNode        (porta 6000)
5. dotnet run --project GatewayNode       (subscreve RabbitMQ + liga :6000)
6. dotnet run --project SensorNode        (publica em RabbitMQ)
7. python WebInterface/app.py             (porta 8080)
```

---

## Progresso

- [ ] Fase 0 — Pré-requisitos
- [ ] Fase 1.1 — PreProcessingService Python
- [ ] Fase 1.2 — Integração RPC no GatewayNode
- [ ] Fase 1.3 — AnalysisService Python
- [ ] Fase 1.4 — Integração RPC no ServerNode
- [ ] Fase 2.1 — SensorNode publica no RabbitMQ
- [ ] Fase 2.2 — GatewayNode subscreve RabbitMQ
- [ ] Fase 3.1 — SQLite no ServerNode
- [ ] Fase 3.2 — Interface Web Flask
