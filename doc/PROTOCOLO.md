# Protocolo SENSOR/GATEWAY/SERVIDOR

## 1. Objetivo

Definir um protocolo de aplicacao para comunicacao entre SENSOR, GATEWAY e SERVIDOR em cima de TCP.

## 2. Transporte e Enderecamento

- Transporte: TCP
- Fluxo: SENSOR -> GATEWAY -> SERVIDOR
- Porta do GATEWAY: 5000
- Porta do SERVIDOR: 6000
- Codificacao: ASCII

## 3. Tipos de Mensagem Obrigatorios

Todas as mensagens usam o separador | e terminam no fim do stream TCP da ligacao.

### 3.1 START

Indica inicio de sessao de comunicacao de um sensor.

Formato:
START|sensor_id|timestamp

Exemplo:
START|S101|2026-04-19T14:20:00

### 3.2 DATA

Envia uma medicao produzida pelo sensor.

Formato:
DATA|sensor_id|tipo|valor|timestamp

Exemplo:
DATA|S101|TEMP|22.6|2026-04-19T14:21:10

### 3.3 HB

Heartbeat para indicar disponibilidade do sensor.

Formato:
HB|sensor_id|timestamp

Exemplo:
HB|S101|2026-04-19T14:21:30

### 3.4 END

Indica finalizacao de sessao de comunicacao do sensor.

Formato:
END|sensor_id|motivo|timestamp

Exemplo:
END|S101|USER_EXIT|2026-04-19T14:25:00

### 3.5 ACK e NACK

Resposta de controlo devolvida pelo recetor na mesma ligacao TCP.

Formato ACK:
ACK|origem|tipo_mensagem

Formato NACK:
NACK|origem|motivo

Exemplos:
ACK|GATEWAY|DATA
ACK|SERVER|START
NACK|PROTO|DATA mal formado

### 3.6 AGGDATA

Mensagem agregada gerada pelo GATEWAY para encaminhamento ao SERVIDOR.

Formato:
AGGDATA|sensor_id|tipo|media|minimo|maximo|contagem|janela_inicio|janela_fim

Exemplo:
AGGDATA|S101|TEMP|22.10|21.90|22.40|3|2026-04-19T16:20:00|2026-04-19T16:20:50

## 4. Estados da Sessao

Estados logicos do sensor:

1. IDLE: sessao nao iniciada.
2. ACTIVE: apos START valido, pode enviar DATA e HB.
3. CLOSING: envio de END.
4. CLOSED: sessao finalizada.

Transicoes esperadas:

1. IDLE -> ACTIVE com START
2. ACTIVE -> ACTIVE com DATA/HB
3. ACTIVE -> CLOSED com END

Regras de rejeicao de sessao (enforcement operacional):

- START em ACTIVE deve ser rejeitado com NACK|SESSION.
- DATA, HB ou END sem START previo ativo devem ser rejeitados com NACK|SESSION.
- Depois de END (CLOSED), uma nova sessao so recomeca com novo START.

## 5. Regras Minimas de Validacao

- Mensagens mal formadas devem ser rejeitadas.
- sensor_id deve existir e nao ser vazio.
- timestamp deve estar em formato ISO 8601.
- DATA deve conter tipo e valor.
- valor de DATA deve ser numerico e dentro da gama esperada por tipo.
- HB e START/END devem conter os campos obrigatorios.
- AGGDATA deve conter campos numericos validos e contagem maior que zero.

## 6. Pre-processamento e Agregacao no GATEWAY

- Pre-processamento de DATA:
normalizacao de decimal para formato invariante e arredondamento para 2 casas.

- Agregacao:
por chave sensor_id+tipo.

- Parametros de agregacao implementados:
janela temporal de 60 segundos e emissao imediata quando existem pelo menos 3 amostras na janela.

- A janela tambem e fechada por expiracao temporal se nao houver novas amostras.

## 7. Configuracao do GATEWAY (CSV)

Ficheiro de configuracao: GatewayNode/config.csv

Formato CSV com delimitador ';' e cabecalho:

sensor_id;estado;zona;tipos_dados;last_sync

Exemplo:
S101;ativo;ZONA CENTRO;TEMP,HUM,RUIDO;2026-04-19T10:00:00

- tipos_dados e uma lista separada por virgulas.
- estado deve permitir no minimo "ativo" e "manutencao".
- last_sync e atualizado pelo GATEWAY quando recebe mensagens validas.

## 8. Ficheiros Operacionais do GATEWAY

- gateway_raw_data.txt
registo de dados DATA apos pre-processamento.

- gateway_aggregated_data.txt
registo das agregacoes enviadas ao SERVIDOR.

- gateway_sessions.txt
registo de eventos START, END e HB_TIMEOUT.

## 9. Heartbeat e Deteccao de Indisponibilidade

- O GATEWAY monitoriza sensores com sessao ativa.
- Timeout configurado: 90 segundos sem HB.
- Ao expirar timeout:
regista evento HB_TIMEOUT e envia END com motivo HB_TIMEOUT para o SERVIDOR.

## 10. Sequencia Base de Comunicacao

1. SENSOR envia START.
2. SENSOR envia uma ou mais DATA.
3. GATEWAY pre-processa, valida, agrega e encaminha (DATA e AGGDATA).
4. SENSOR envia HB periodico enquanto ACTIVE.
5. SENSOR envia END ao terminar.

## 11. Concorrencia

- GATEWAY e SERVIDOR atendem multiplas ligacoes em paralelo.
- Escritas em ficheiros partilhados sao protegidas por locks para evitar corrupcao.

## 12. Nota de Evolucao

No estado atual, o GATEWAY e o SERVIDOR ja tratam explicitamente START, DATA, HB, END e AGGDATA com validacao e resposta ACK/NACK.

## 13. Guia de Estudo e Defesa (Fluxo do Projeto com Blocos de Codigo)

Esta secao e um guiao pratico para explicar o sistema ponta-a-ponta em apresentacao.

### 13.1 Arranque dos 3 processos

```powershell
dotnet run --project ServerNode
dotnet run --project GatewayNode
dotnet run --project SensorNode
```

Ordem recomendada para demo:

1. Servidor (porta 6000)
2. Gateway (porta 5000)
3. Sensor (cliente)

### 13.2 Fluxo nominal na rede (wire protocol)

```text
SENSOR -> GATEWAY: START|S101|2026-04-19T17:10:00
GATEWAY -> SENSOR: ACK|GATEWAY|START

SENSOR -> GATEWAY: DATA|S101|TEMP|22.30|2026-04-19T17:10:05
GATEWAY -> SENSOR: ACK|GATEWAY|DATA

SENSOR -> GATEWAY: HB|S101|2026-04-19T17:10:10
GATEWAY -> SENSOR: ACK|GATEWAY|HB

SENSOR -> GATEWAY: END|S101|USER_REQUEST|2026-04-19T17:10:20
GATEWAY -> SENSOR: ACK|GATEWAY|END
```

Pontos para defender oralmente:

1. O sensor nunca escreve diretamente no servidor.
2. O gateway valida autenticacao, ordem da sessao e formato.
3. O servidor aplica validacao final e persiste os dados.

### 13.3 Fluxo invalido (enforcement do ponto 4)

Casos tipicos que devem falhar com NACK|SESSION:

```text
SENSOR -> GATEWAY: DATA|S101|TEMP|22.10|2026-04-19T17:05:39
GATEWAY -> SENSOR: NACK|SESSION|DATA invalido (sessao nao iniciada)

SENSOR -> GATEWAY: START|S101|2026-04-19T17:05:40
GATEWAY -> SENSOR: ACK|GATEWAY|START

SENSOR -> GATEWAY: START|S101|2026-04-19T17:05:41
GATEWAY -> SENSOR: NACK|SESSION|START invalido (sessao ja ativa)
```

Mensagem chave para defesa: o protocolo nao e apenas documentado, esta efetivamente aplicado em runtime.

### 13.4 Exemplo do cliente (SensorNode)

```csharp
msg = $"START|{id}|{DateTime.Now:s}";
bool startAceite = EnviarMensagem(msg);

msg = $"DATA|{id}|TEMP|{valor}|{DateTime.Now:s}";
EnviarMensagem(msg);

msg = $"END|{id}|USER_REQUEST|{DateTime.Now:s}";
EnviarMensagem(msg);
```

### 13.5 Exemplo de validacao de transicao no Gateway

```csharp
if (pacote.TipoMensagem == "START")
{
	if (sessaoAtiva)
	{
		erro = "START invalido (sessao ja ativa)";
		return false;
	}
	return true;
}

if (pacote.TipoMensagem == "DATA" || pacote.TipoMensagem == "HB" || pacote.TipoMensagem == "END")
{
	if (!sessaoAtiva)
	{
		erro = pacote.TipoMensagem + " invalido (sessao nao iniciada)";
		return false;
	}
	return true;
}
```

### 13.6 Exemplo de defesa adicional no Servidor

```csharp
if (ExigeSessaoAtiva(pacote.TipoMensagem) && !TryValidarTransicaoSessao(pacote, out string erroSessao))
{
	EnviarResposta(stream, "NACK|SESSION|" + erroSessao);
	return;
}

string resposta = ProcessarMensagemValida(pacote);
EnviarResposta(stream, resposta);
```

### 13.7 Script curto de teste para demonstracao

```powershell
$c=[System.Net.Sockets.TcpClient]::new('127.0.0.1',5000)
$s=$c.GetStream()
$m='DATA|S101|TEMP|22.10|2026-04-19T17:20:00'
$b=[System.Text.Encoding]::ASCII.GetBytes($m)
$s.Write($b,0,$b.Length)
$rb=New-Object byte[] 1024
$n=$s.Read($rb,0,$rb.Length)
[System.Text.Encoding]::ASCII.GetString($rb,0,$n)
```

Saida esperada para este caso (sem START previo):

```text
NACK|SESSION|DATA invalido (sessao nao iniciada)
```

## 14. Enquadramento do Enunciado (TP1)

Segundo o enunciado oficial, o objetivo do trabalho e simular um servico distribuido de monitorizacao urbana no contexto One Health, com 3 entidades:

1. SENSOR: recolhe dados ambientais e comunica com o gateway.
2. GATEWAY: valida sensores, agrega e encaminha dados.
3. SERVIDOR: armazena e processa os dados recebidos.

Tipos de dados referidos no enunciado:

- TEMP
- HUM
- RUIDO
- AR
- PM2.5
- PM10
- LUMINOSIDADE
- IMAGEM/VIDEO (stream)

## 15. Justificacao da Escolha do Protocolo

Escolha adotada: protocolo de aplicacao textual sobre TCP com mensagens delimitadas por `|`.

Razoes tecnicas para defesa:

1. Fiabilidade: TCP garante entrega ordenada e retransmissao em caso de perda.
2. Simplicidade de debug: mensagens textuais permitem teste manual com scripts e logs legiveis.
3. Evolucao incremental: novos tipos de mensagem podem ser acrescentados sem quebrar o desenho base.
4. Adequado ao enunciado: requisito explicito de implementacao com sockets em C#.

Porque nao UDP neste TP:

1. O protocolo inclui controlo de sessao (START/HB/END) e ACK/NACK, o que beneficia de um transporte fiavel.
2. O custo adicional de gerir perdas/reordenacao em UDP aumentaria a complexidade sem vantagem clara para o objetivo didatico.

## 16. Mapeamento Enunciado -> Implementacao Atual

### 16.1 SENSOR

- Interface de texto simples: implementado.
- Identificacao por sensor_id: implementado.
- Envio de DATA: implementado.
- Heartbeat: implementado (menu + validacao no gateway).
- Finalizacao correta: implementado (END).
- Parametro IP do gateway na inicializacao: parcial (atualmente endereco fixo `127.0.0.1`).
- Pedido de criacao de stream de video: nao implementado nesta versao.

### 16.2 GATEWAY

- Validacao de sensor registado e estado: implementado.
- Validacao do tipo de dado suportado: implementado.
- Atualizacao de last_sync: implementado.
- Deteccao de indisponibilidade por heartbeat timeout: implementado.
- Pre-processamento e agregacao: implementado (janela + contagem minima).
- Encaminhamento para servidor: implementado com ACK/NACK.
- Atendimento concorrente: implementado (tasks + locks).

Nota sobre configuracao CSV: o enunciado mostra exemplo com `:`, enquanto a implementacao atual padroniza em `;` com cabecalho, mantendo compatibilidade de leitura com ambos os formatos.

### 16.3 SERVIDOR

- Rececao concorrente de pedidos de gateways: implementado.
- Validacao protocolar e de sessao: implementado.
- Armazenamento por tipo de dado em ficheiros: implementado.
- Respostas de controlo para o gateway: implementado (ACK/NACK).

## 17. Faseamento do TP e Evidencia no Projeto

1. Fase 1 (desenho do protocolo): formalizado neste `PROTOCOLO.md`.
2. Fase 2 (SENSOR/GATEWAY/SERVIDOR simples): fluxo START/DATA/END funcional.
3. Fase 3 (operacao do gateway): validacao, pre-processamento, agregacao e encaminhamento ativos.
4. Fase 4 (concorrencia): gateway e servidor com atendimento paralelo e sincronizacao de escrita.

## 18. O que Ainda Pode Ser Evoluido

Itens do enunciado que podem ser desenvolvidos como extensao:

1. Mensagem especifica para negociacao de stream de video/imagem e respetivo pipeline edge no gateway.
2. Parametrizacao do IP/porta do gateway no arranque do sensor (argumentos CLI ou menu de configuracao).
3. Persistencia em base de dados relacional (opcao extra referida no enunciado).
4. Testes automatizados de conformidade do protocolo (sequencias validas e invalidas).

## 19. Resumo Curto para Defesa Oral

Frase de abertura recomendada:

"Escolhemos um protocolo textual sobre TCP porque o trabalho exige sockets em C#, precisavamos de fiabilidade na ordem das mensagens e de uma implementacao clara para validar sessao, concorrencia e integridade dos dados em todo o fluxo SENSOR -> GATEWAY -> SERVIDOR." 

Pontos-chave para fechar a defesa:

1. O protocolo foi desenhado, implementado e validado com casos de sucesso e erro.
2. O ponto critico de sessao (START/DATA/HB/END) esta enforced em gateway e servidor.
3. O sistema cumpre o faseamento do TP e deixa caminho preparado para extensoes (video e base de dados).