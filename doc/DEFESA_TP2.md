# DEFESA TP2 — Documentação Técnica e Teoria de Sistemas Distribuídos

**Sistemas Distribuídos 2025/2026 — Grupo G30 — UTAD/ECT**
**Trabalho:** Serviços de Análise e Monitorização Urbana para One Health

> Este documento acompanha o código do TP2, fase a fase.
> Contém teoria de SD, decisões de design, explicações funcionais, excertos anotados e possíveis perguntas da defesa.

---

## Índice

- [Fase 1 — RPC: Serviços de Pré-processamento e Análise](#fase-1--rpc-serviços-de-pré-processamento-e-análise)
- [Fase 2 — Pub/Sub com RabbitMQ](#fase-2--pubsub-com-rabbitmq) *(a preencher)*
- [Fase 3 — Base de Dados e Interface Web](#fase-3--base-de-dados-e-interface-web) *(a preencher)*
- [Glossário de Conceitos SD](#glossário-de-conceitos-sd)

---

---

# Fase 1 — RPC: Serviços de Pré-processamento e Análise

**Referência ao enunciado:** Protocolo TP2 §2.1 — Comunicação RPC

---

## 1.1 Teoria: O que é RPC (Remote Procedure Call)

### Definição

**RPC (Remote Procedure Call)** é um paradigma de comunicação em Sistemas Distribuídos onde um processo chama uma função/procedimento que corre **noutro processo, potencialmente noutro nó da rede**, como se essa função fosse local.

O objetivo central é a **transparência de localização**: o chamador não precisa de saber onde o serviço está fisicamente nem de gerir os detalhes de transporte (ligação, serialização, envio, receção, desserialização).

### Como funciona (modelo geral)

```
[Processo A — chamador]          [Processo B — servidor RPC]
        │                                    │
        │  1. Chama função local (stub)       │
        │──────────────────────────────────► │
        │  2. Stub serializa argumentos       │
        │     e envia pela rede               │
        │                                    │  3. Recebe e desserializa
        │                                    │  4. Executa função real
        │                                    │  5. Serializa resultado
        │◄────────────────────────────────── │
        │  6. Stub recebe e desserializa      │
        │  7. Retorna resultado ao chamador   │
```

### Variantes de RPC

| Variante | Descrição | Exemplo |
|----------|-----------|---------|
| **XML-RPC** | RPC com serialização XML sobre HTTP | Legado |
| **JSON-RPC** | RPC com serialização JSON sobre HTTP/TCP | Simples e leve |
| **gRPC** | RPC moderno com Protocol Buffers sobre HTTP/2 | Alta performance |
| **REST-RPC** | HTTP + JSON, estilo REST, sem framework RPC específico | **Este projeto** |

**Neste projeto** optámos por HTTP/JSON (REST-RPC) porque:
- Sem dependências externas além do Flask (Python) e HttpClient (.NET 8)
- Fácil de testar manualmente com curl ou browser
- Suficiente para os requisitos do TP2

### Conceitos-chave para a defesa

- **Transparência de localização**: o Gateway chama `TryPreprocessarViaRpc()` sem saber que o serviço é Python a correr numa porta diferente.
- **Serialização**: conversão de estruturas de dados em bytes transmissíveis pela rede. Aqui usamos **JSON**.
- **Serviço stateless**: cada chamada RPC é independente — o PreProcessingService não guarda estado entre chamadas. Isto facilita escalabilidade e tolerância a falhas.
- **Idempotência**: chamar `/preprocess` com os mesmos dados produz sempre o mesmo resultado. Importante para retries seguros.

---

## 1.2 PreProcessingService — O que faz e porquê

### Ficheiro: `PreProcessingService/app.py`

### O que faz

Serviço Python/Flask que expõe um endpoint RPC (`POST /preprocess`) para **normalizar valores brutos de sensores** antes de serem agregados no Gateway.

**Passos de normalização:**
1. Substitui vírgula por ponto no valor decimal (invariância de locale)
2. Converte para `float`
3. Valida a gama por tipo de dado (ex: TEMP entre -50°C e 80°C)
4. Arredonda a 2 casas decimais

### Porquê um serviço externo?

O enunciado (§2.1) exige que o Gateway utilize **RPC para invocar um serviço externo** de pré-processamento, em vez de fazer a normalização localmente. Isto introduz:
- **Separação de responsabilidades**: o Gateway não precisa de conter a lógica de normalização
- **Substituibilidade**: o serviço pode ser substituído/melhorado sem tocar no Gateway
- **Multi-linguagem**: o serviço é Python, o Gateway é C# — fator de valorização do enunciado

### Gamas de validação implementadas

| Tipo   | Mínimo | Máximo | Unidade |
|--------|--------|--------|---------|
| TEMP   | -50    | 80     | °C      |
| HUM    | 0      | 100    | %       |
| RUIDO  | 0      | 200    | dB      |
| PM2.5  | 0      | 1000   | µg/m³   |
| PM10   | 0      | 1000   | µg/m³   |
| AR     | 0      | 1000   | índice  |

### Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET`  | `/health` | Health check — responde `{"status": "ok"}` |
| `POST` | `/preprocess` | Normaliza um registo DATA |

### Excerto de código anotado

```python
def normalizar_valor(tipo: str, valor_raw: str) -> dict:
    # Passo 1: normalizar separador decimal (vírgula → ponto)
    # Sensores podem enviar "22,5" em vez de "22.5" dependendo da locale
    valor_str = str(valor_raw).strip().replace(",", ".")

    # Passo 2: tentar converter para float
    # Se falhar → valor não é numérico → rejeitar com erro descritivo
    try:
        valor_float = float(valor_str)
    except ValueError:
        return {"valor": None, "normalizado": False, "erro": f"Valor '{valor_raw}' não é numérico"}

    # Passo 3: validar gama por tipo (só se o tipo for conhecido)
    tipo_upper = tipo.strip().upper()
    if tipo_upper in GAMAS_POR_TIPO:
        minimo, maximo = GAMAS_POR_TIPO[tipo_upper]
        if not (minimo <= valor_float <= maximo):
            return {"valor": None, "normalizado": False,
                    "erro": f"Valor {valor_float} fora de gama para {tipo_upper}"}

    # Passo 4: arredondar a 2 casas decimais (precisão suficiente para dados ambientais)
    valor_normalizado = round(valor_float, 2)
    return {"valor": valor_normalizado, "normalizado": True, "erro": None}
```

---

## 1.3 Integração RPC no GatewayNode — Como e porquê

### Ficheiro: `GatewayNode/Program.cs`

### O que mudou

Adicionámos a função `TryPreprocessarViaRpc()` que substitui a chamada à normalização local `TryPreprocessarValor()` no fluxo de processamento de mensagens DATA.

### Padrão de Fallback / Degradação Graciosa

Este é um conceito fundamental em Sistemas Distribuídos:

> **Degradação graciosa** (*graceful degradation*): quando um componente externo falha, o sistema não colapsa — degrada para um comportamento menos capaz mas ainda funcional.

No nosso caso:
- **Normal**: Gateway chama RPC → PreProcessingService normaliza → Gateway agrega
- **Fallback**: PreProcessingService não responde → Gateway normaliza localmente → fluxo continua

```
TryPreprocessarViaRpc()
       │
       ├─── Tenta HTTP POST /preprocess (timeout: 2s)
       │         │
       │         ├─── Sucesso (HTTP 200) → usa valor normalizado pelo serviço
       │         ├─── HTTP 4xx/5xx → rejeita DATA (valor inválido)
       │         └─── Timeout / exceção → FALLBACK LOCAL
       │
       └─── TryPreprocessarValor() [fallback]
```

### Porquê timeout de 2 segundos?

Em SD, **nunca se deve esperar indefinidamente** por um serviço remoto porque:
1. O sensor está a aguardar resposta do Gateway
2. O Gateway não pode bloquear por ligações lentas
3. 2 segundos é suficiente para uma chamada local na mesma máquina (latência < 1ms)

### Excerto de código anotado

```csharp
bool TryPreprocessarViaRpc(string tipo, string sensorId, string valorOriginal, string timestamp,
                            out double valorNormalizado, out string erro)
{
    try
    {
        // Serializar o pedido em JSON para enviar ao serviço Python
        var payload = new { sensor_id = sensorId, tipo, valor = valorOriginal, timestamp };
        string json = JsonSerializer.Serialize(payload);
        using StringContent conteudo = new StringContent(json, Encoding.UTF8, "application/json");

        // Chamada síncrona com .GetAwaiter().GetResult() porque esta função não é async
        // (está no caminho crítico do processamento TCP)
        HttpResponseMessage resposta = httpClient.PostAsync(PreProcessingServiceUrl, conteudo)
                                                 .GetAwaiter().GetResult();

        // HTTP 422 = valor fora de gama → rejeitar (não é falha de serviço, é dado inválido)
        if (!resposta.IsSuccessStatusCode)
        {
            erro = /* mensagem de erro do JSON */;
            return false;
        }

        // Extrair valor normalizado da resposta JSON
        valorNormalizado = Math.Round(valorEl.GetDouble(), 2);
        return true;
    }
    catch (Exception ex)
    {
        // Timeout, conexão recusada, etc. → degradação graciosa
        Console.WriteLine("[RPC][FALLBACK]: " + ex.Message);
        return TryPreprocessarValor(tipo, valorOriginal, out valorNormalizado, out erro);
    }
}
```

---

## 1.4 AnalysisService — O que faz e porquê

### Ficheiro: `AnalysisService/app.py`

### O que faz

Serviço Python/Flask que expõe um endpoint RPC (`POST /analyze`) para **analisar registos AGGDATA** recebidos do Gateway, calculando:

1. **Estatísticas descritivas**: média, desvio padrão, mínimo, máximo
2. **Alertas de threshold**: deteta valores que excedem limites de risco
3. **Tendência temporal**: compara primeira metade vs segunda metade da série

### Thresholds de alerta implementados

| Tipo   | Threshold | Significado |
|--------|-----------|-------------|
| TEMP   | > 35°C    | Risco de calor extremo |
| RUIDO  | > 80 dB   | Risco auditivo (OMS) |
| PM2.5  | > 25 µg/m³ | Limite diário OMS |
| PM10   | > 50 µg/m³ | Limite diário OMS |
| HUM    | > 90%     | Humidade excessiva |

### Porquê serviço separado para análise?

O enunciado (§2.1) distingue explicitamente dois serviços RPC:
- **Pré-processamento**: normalização de dados (sem estado, aplicado por leitura)
- **Análise e Previsão**: análise estatística, padrões, riscos (aplicado a conjuntos de dados)

Separar estes serviços respeita o **princípio de responsabilidade única** e permite evoluir a análise (ex: machine learning) sem tocar no pré-processamento.

### Exemplo de resposta do AnalysisService

```json
{
  "sensor_id": "S101",
  "tipo": "TEMP",
  "estatisticas": {
    "media": 28.4,
    "desvio_padrao": 3.2,
    "minimo": 22.1,
    "maximo": 36.7,
    "contagem": 5
  },
  "alertas": [
    {
      "valor": 36.7,
      "threshold": 35.0,
      "mensagem": "TEMP = 36.70 excede o limite de 35.0"
    }
  ],
  "tendencia": "subida"
}
```

---

## 1.5 Integração RPC no ServerNode — Como e porquê

### Ficheiro: `ServerNode/Program.cs`

### O que mudou

Após persistir um registo AGGDATA em ficheiro, o Servidor dispara uma chamada ao AnalysisService em **background** (fire-and-forget assíncrono).

### Padrão Fire-and-Forget

> **Fire-and-forget**: enviar uma mensagem/chamada sem aguardar resposta. O chamador não bloqueia e não verifica o resultado.

Usamos este padrão porque:
- A análise é **enriquecimento opcional** — o Gateway não precisa de esperar por ela
- Se o AnalysisService estiver em baixo, o fluxo principal não é afetado
- A análise pode demorar mais que o esperado sem comprometer latência do protocolo

### Excerto de código anotado

```csharp
// Após persistir o AGGDATA em ficheiro:

// Fire-and-forget: Task.Run inicia a chamada RPC num thread separado
// O "_" descarta a Task (não aguardamos o resultado)
_ = Task.Run(() => ChamarAnaliseAsync(pacote));

return "ACK|SERVER|AGGDATA";  // Responde imediatamente ao Gateway
```

```csharp
async Task ChamarAnaliseAsync(ProtocoloMensagem pacote)
{
    // Constrói o payload com os dados AGGDATA
    var payload = new {
        sensor_id = pacote.SensorId,
        tipo      = pacote.TipoDado,
        registos  = new[] { new { media = pacote.Media, minimo = pacote.Minimo, ... } }
    };

    // Chamada HTTP assíncrona (await = não bloqueia o thread)
    HttpResponseMessage resposta = await httpClientAnalise.PostAsync(AnalysisServiceUrl, conteudo);
    // Resultado impresso na consola — em Fase 3 será guardado na BD
}
```

---

## 1.6 Arquitetura da Fase 1 — Diagrama

```
[SensorNode C#]
      │  TCP porta 5000
      ▼
[GatewayNode C#] ─── HTTP POST /preprocess ──► [PreProcessingService Python :7000]
      │                                                │
      │  valor normalizado (ou fallback local)         │
      │                                                │
      │  TCP porta 6000                                │
      ▼                                                │
[ServerNode C#] ──── HTTP POST /analyze ─────► [AnalysisService Python :7001]
                     (fire-and-forget)
```

---

## 1.7 Possíveis Perguntas da Defesa — Fase 1

**P: O que é RPC e como o implementaram neste projeto?**
> RPC é um mecanismo de comunicação onde um processo chama uma função que corre noutro processo como se fosse local. Implementámos via HTTP/JSON: o Gateway faz um HTTP POST para o PreProcessingService (Python/Flask, porta 7000), serializa os dados em JSON, e desserializa a resposta. O mesmo para o Servidor com o AnalysisService (porta 7001).

**P: Porquê HTTP/JSON e não gRPC?**
> gRPC seria mais eficiente em termos de performance (Protocol Buffers + HTTP/2), mas para este projeto HTTP/JSON é suficiente, mais simples de implementar e de demonstrar. O objetivo é mostrar o conceito de RPC, não otimizar throughput.

**P: O que acontece se o PreProcessingService cair?**
> O Gateway tem um mecanismo de fallback: se a chamada RPC falhar (timeout de 2s ou exceção), aplica a normalização local que existia desde o TP1. O fluxo continua sem interrupção — é degradação graciosa.

**P: Porquê o timeout de 2 segundos no Gateway e 5 segundos no Servidor?**
> O Gateway está no caminho crítico (o sensor aguarda resposta), por isso o timeout é curto (2s). O Servidor chama a análise em background (fire-and-forget), por isso pode-se dar ao luxo de esperar mais (5s).

**P: O que é um serviço stateless? O PreProcessingService é stateless?**
> Um serviço stateless não guarda estado entre chamadas — cada pedido é completamente independente. Sim, o PreProcessingService é stateless: recebe dados, normaliza, devolve resultado. Não há sessões, caches ou estado persistente. Isto facilita escalabilidade (pode correr múltiplas instâncias).

**P: Porquê usar fire-and-forget no Servidor para a análise?**
> A análise é enriquecimento opcional — o Gateway precisa de receber o ACK rapidamente para poder responder ao sensor. Se o Servidor bloqueasse à espera da análise, a latência de todo o sistema aumentaria. Fire-and-forget desacopla o fluxo principal da análise estatística.

**P: O que é idempotência e aplica-se aqui?**
> Uma operação é idempotente se executá-la múltiplas vezes com os mesmos dados produz sempre o mesmo resultado. A normalização no PreProcessingService é idempotente: normalizar "22,5" de TEMP sempre devolve 22.5. Isto é importante porque permite retries seguros.

---

---

# Fase 2 — Pub/Sub com RabbitMQ

*(A preencher após implementação da Fase 2)*

---

# Fase 3 — Base de Dados e Interface Web

*(A preencher após implementação da Fase 3)*

---

---

# Glossário de Conceitos SD

| Conceito | Definição |
|----------|-----------|
| **RPC** | Remote Procedure Call — chamada de função em processo remoto como se fosse local |
| **Serialização** | Conversão de estruturas de dados em bytes transmissíveis pela rede |
| **JSON** | JavaScript Object Notation — formato de serialização legível e leve |
| **Stateless** | Serviço sem estado entre chamadas — cada pedido é independente |
| **Idempotência** | Propriedade de uma operação cujo resultado é o mesmo independentemente do nº de execuções |
| **Transparência de localização** | O chamador não sabe onde o serviço está fisicamente |
| **Degradação graciosa** | Quando um componente falha, o sistema mantém-se funcional com capacidade reduzida |
| **Fire-and-forget** | Enviar pedido sem aguardar resposta — não bloqueia o chamador |
| **Timeout** | Tempo máximo de espera por resposta remota antes de desistir |
| **Pub/Sub** | Publish/Subscribe — padrão de mensagens desacoplado *(Fase 2)* |
| **Message Broker** | Intermediário de mensagens (ex: RabbitMQ) *(Fase 2)* |
| **SQLite** | Base de dados relacional em ficheiro, sem servidor *(Fase 3)* |
