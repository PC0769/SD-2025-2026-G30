// ============================================================================
// Ficheiro : GatewayNode/Program.cs
// Modulo   : Gateway (Servidor TCP para sensores + Cliente TCP para servidor)
// Portas   : Escuta sensores na porta 5000; liga ao servidor na porta 6000
// Descricao: Entidade intermedia que recebe mensagens dos sensores, valida
//            o protocolo e a configuracao (config.csv), normaliza valores,
//            persiste dados brutos e agregados localmente, e encaminha tudo
//            para o Servidor Central. Gere sessoes por sensor com timeout
//            de heartbeat (90s) e produz registos agregados (AGGDATA) por
//            janela temporal de 60s ou minimo de 3 amostras.
// Ficheiros locais:
//   config.csv                   - configuracao dos sensores autorizados
//   gateway_raw_data.txt         - dados brutos recebidos (normalizados)
//   gateway_aggregated_data.txt  - registos de agregacao produzidos
//   gateway_sessions.txt         - eventos de sessao (START/HB/END/TIMEOUT)
// ============================================================================
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Net.Http;
using System.Text.Json;

// --- Ponto de entrada (top-level statements -> Main implicito) ---
Console.WriteLine("--- GATEWAY ONE HEALTH ---");

// Cliente HTTP partilhado para chamadas RPC ao PreProcessingService.
// HttpClient deve ser instanciado uma vez e reutilizado (evita socket exhaustion).
// Protocolo TP2 §2.1 — Gateway -> Serviço de Pré-processamento (porta 7000)
HttpClient httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
const string PreProcessingServiceUrl = "http://localhost:7000/preprocess";

// Portas TCP e regras operacionais do gateway.
const int GatewayPort = 5000;
const int ServerPort = 6000;
const int AggregationMinSamples = 3;

// Parametros de tempo para agregacao e timeout de heartbeat.
TimeSpan aggregationWindow = TimeSpan.FromSeconds(60);
TimeSpan heartbeatTimeout = TimeSpan.FromSeconds(90);
string[] timestampFormats = { "yyyy-MM-ddTHH:mm:ss", "o" };

// Caminhos de persistencia local do gateway.
string pastaGatewayDados = ObterPastaGatewayDados();
string ficheiroRawGateway = Path.Combine(pastaGatewayDados, "gateway_raw_data.txt");
string ficheiroAgregadoGateway = Path.Combine(pastaGatewayDados, "gateway_aggregated_data.txt");
string ficheiroSessoesGateway = Path.Combine(pastaGatewayDados, "gateway_sessions.txt");

// Locks para proteger acesso concorrente a ficheiros e estruturas partilhadas.
object configLock = new object();
object gatewayFileLock = new object();
object aggregationLock = new object();
object sessionLock = new object();

// Estado em memoria das sessoes e buckets de agregacao.
var estadosSessao = new Dictionary<string, SensorSessionState>(StringComparer.OrdinalIgnoreCase);
var bucketsAgregacao = new Dictionary<string, AggregateBucket>(StringComparer.OrdinalIgnoreCase);

// Inicializa listener TCP para receber mensagens dos sensores.
TcpListener listener = new TcpListener(IPAddress.Any, GatewayPort);
listener.Start();
Console.WriteLine("A escuta no Porto 5000...");

// Monitor de timeout de heartbeat e expiracao de janelas de agregacao.
_ = Task.Run(MonitorizarEstadoGatewayAsync);

// Loop principal para aceitar ligacoes dos sensores.
while (true)
{
    try
    {
        TcpClient sensorClient = listener.AcceptTcpClient();
        _ = Task.Run(() => ProcessarLigacaoSensor(sensorClient));
    }
    catch (Exception ex)
    {
        Console.WriteLine("Erro: " + ex.Message);
    }
}

/// <summary>
/// Processa uma ligacao TCP individual de um sensor.
/// Le a mensagem, valida o protocolo, despacha para o handler adequado
/// (sessao ou dados) e devolve ACK ou NACK ao sensor no mesmo stream.
/// Executada numa Task dedicada por ligacao.
/// </summary>
/// <param name="sensorClient">Ligacao TCP aceite do sensor.</param>
void ProcessarLigacaoSensor(TcpClient sensorClient)
{
    using (sensorClient)
    {
        try
        {
            NetworkStream stream = sensorClient.GetStream();
            byte[] buffer = new byte[1024];
            int bytesRead = stream.Read(buffer, 0, buffer.Length);

            if (bytesRead <= 0)
            {
                EnviarResposta(stream, "NACK|PROTO|Mensagem vazia");
                return;
            }

            string mensagem = Encoding.ASCII.GetString(buffer, 0, bytesRead);
            if (!TryParseMensagem(mensagem, out ProtocoloMensagem pacote, out string erroValidacao))
            {
                Console.WriteLine("[RECUSADO]: " + erroValidacao + " -> " + mensagem);
                EnviarResposta(stream, "NACK|PROTO|" + erroValidacao);
                return;
            }

            string respostaSensor = pacote.TipoMensagem == "DATA"
                ? ProcessarMensagemData(pacote)
                : ProcessarMensagemSessao(pacote, mensagem);

            EnviarResposta(stream, respostaSensor);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro ao processar ligacao do sensor: " + ex.Message);
        }
    }
}

/// <summary>
/// Trata mensagens de controlo de sessao (START, HB, END).
/// Valida autorizacao no config.csv, verifica transicao de sessao,
/// atualiza estado em memoria, persiste evento localmente e
/// encaminha a mensagem original ao Servidor Central.
/// </summary>
/// <param name="pacote">Mensagem ja parseada.</param>
/// <param name="mensagemOriginal">String bruta para reencaminhamento fiel.</param>
/// <returns>Resposta de protocolo para o sensor (ACK ou NACK).</returns>
string ProcessarMensagemSessao(ProtocoloMensagem pacote, string mensagemOriginal)
{
    if (!ValidarSensorAtivo(pacote.SensorId))
    {
        Console.WriteLine("[RECUSADO]: Sensor " + pacote.SensorId + " nao autorizado ou inativo.");
        return "NACK|AUTH|Sensor nao autorizado ou inativo";
    }

    if (!TryValidarTransicaoSessao(pacote, out string erroSessao))
    {
        Console.WriteLine("[RECUSADO]: " + erroSessao + " -> " + pacote.TipoMensagem + "|" + pacote.SensorId);
        return "NACK|SESSION|" + erroSessao;
    }

    AtualizarLastSync(pacote.SensorId);
    AtualizarEstadoSessao(pacote, DateTime.UtcNow);
    RegistarSessaoGateway(pacote.TipoMensagem, pacote.SensorId, pacote.Timestamp, pacote.Motivo);

    if (TryEncaminharComAck(mensagemOriginal, out string respostaServidor))
    {
        Console.WriteLine("[SESSAO]: Encaminhado para o Servidor -> " + mensagemOriginal);
        return "ACK|GATEWAY|" + pacote.TipoMensagem;
    }

    return "NACK|SERVER|" + respostaServidor;
}

/// <summary>
/// Trata mensagens de dados (DATA). Valida sensor e tipo no config.csv,
/// verifica sessao ativa, normaliza o valor numerico, persiste raw local,
/// encaminha dados normalizados ao Servidor e acumula leitura no bucket
/// de agregacao. Quando o bucket atinge o minimo de amostras ou a janela
/// temporal expira, gera e encaminha um registo AGGDATA.
/// </summary>
/// <param name="pacote">Mensagem DATA ja parseada.</param>
/// <returns>Resposta de protocolo para o sensor (ACK ou NACK).</returns>
string ProcessarMensagemData(ProtocoloMensagem pacote)
{
    if (!ValidarSensorAtivoETipo(pacote.SensorId, pacote.TipoDado!))
    {
        Console.WriteLine("[RECUSADO]: Sensor " + pacote.SensorId + " ou tipo " + pacote.TipoDado + " invalido.");
        return "NACK|AUTH|Sensor ou tipo invalido";
    }

    if (!TryValidarTransicaoSessao(pacote, out string erroSessao))
    {
        Console.WriteLine("[RECUSADO]: " + erroSessao + " -> DATA|" + pacote.SensorId);
        return "NACK|SESSION|" + erroSessao;
    }

    if (!TryPreprocessarViaRpc(pacote.TipoDado!, pacote.SensorId, pacote.Valor!, pacote.Timestamp, out double valorNormalizado, out string erroPreProcessamento))
    {
        return "NACK|DATA|" + erroPreProcessamento;
    }

    AtualizarLastSync(pacote.SensorId);
    AtualizarEstadoSessao(pacote, DateTime.UtcNow);
    RegistarRawGateway(pacote, valorNormalizado);

    string mensagemNormalizada = "DATA|"
        + pacote.SensorId + "|"
        + pacote.TipoDado + "|"
        + valorNormalizado.ToString("0.00", CultureInfo.InvariantCulture) + "|"
        + pacote.Timestamp;

    if (!TryEncaminharComAck(mensagemNormalizada, out string respostaServidorRaw))
    {
        return "NACK|SERVER|" + respostaServidorRaw;
    }

    Console.WriteLine("[DADOS]: Encaminhado para o Servidor -> " + mensagemNormalizada);

    DateTime timestampUtc = ParseTimestampUtc(pacote.Timestamp);
    if (AdicionarLeituraParaAgregacao(pacote.SensorId, pacote.TipoDado!, valorNormalizado, timestampUtc, out AggregateRecord? agregadoGerado)
        && agregadoGerado is not null)
    {
        AggregateRecord agregado = agregadoGerado;
        RegistarAgregadoGateway(agregado);

        string msgAgregado = MontarMensagemAgregada(agregado);
        if (!TryEncaminharComAck(msgAgregado, out string respostaServidorAgregado))
        {
            Console.WriteLine("[AGREGACAO][ERRO]: Falha ao encaminhar agregado -> " + respostaServidorAgregado);
        }
        else
        {
            Console.WriteLine("[AGREGACAO]: Encaminhado para o Servidor -> " + msgAgregado);
        }
    }

    return "ACK|GATEWAY|DATA";
}

/// <summary>
/// Verifica se a transicao de sessao e valida para o sensor.
/// Impede START quando ja ha sessao ativa e DATA/HB/END sem sessao aberta.
/// Acesso protegido por <c>sessionLock</c>.
/// </summary>
/// <param name="pacote">Mensagem com SensorId e TipoMensagem.</param>
/// <param name="erro">Descricao do erro se a transicao for invalida.</param>
/// <returns><c>true</c> se a transicao e permitida.</returns>
bool TryValidarTransicaoSessao(ProtocoloMensagem pacote, out string erro)
{
    erro = string.Empty;

    lock (sessionLock)
    {
        bool sessaoAtiva = estadosSessao.TryGetValue(pacote.SensorId, out SensorSessionState? estado)
            && estado.Fase == SessionPhase.Active;

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
    }

    return true;
}

/// <summary>
/// Atualiza fase, heartbeat e carimbos de atividade da sessao do sensor.
/// START abre sessao, HB renova heartbeat, END fecha sessao.
/// Acesso protegido por <c>sessionLock</c>.
/// </summary>
/// <param name="pacote">Mensagem que origina a atualizacao.</param>
/// <param name="agoraUtc">Carimbo temporal UTC atual.</param>
void AtualizarEstadoSessao(ProtocoloMensagem pacote, DateTime agoraUtc)
{
    lock (sessionLock)
    {
        SensorSessionState estado = ObterOuCriarEstadoSessao(pacote.SensorId);
        estado.LastActivityUtc = agoraUtc;

        if (pacote.TipoMensagem == "START")
        {
            estado.Fase = SessionPhase.Active;
            estado.LastHeartbeatUtc = agoraUtc;
            estado.LastStartTimestamp = pacote.Timestamp;
        }
        else if (pacote.TipoMensagem == "HB")
        {
            estado.LastHeartbeatUtc = agoraUtc;
        }
        else if (pacote.TipoMensagem == "END")
        {
            estado.Fase = SessionPhase.Closed;
            estado.LastEndTimestamp = pacote.Timestamp;
        }
    }
}

/// <summary>
/// Devolve o estado de sessao existente para o sensor ou cria um novo
/// com fase Idle. Deve ser chamado dentro de <c>sessionLock</c>.
/// </summary>
/// <param name="sensorId">Identificador do sensor.</param>
/// <returns>Estado de sessao do sensor.</returns>
SensorSessionState ObterOuCriarEstadoSessao(string sensorId)
{
    if (!estadosSessao.TryGetValue(sensorId, out SensorSessionState? estado))
    {
        estado = new SensorSessionState();
        estadosSessao[sensorId] = estado;
    }

    return estado;
}

/// <summary>
/// Tarefa de fundo executada em loop infinito a cada 10 segundos.
/// 1) Deteta sensores cuja sessao expirou por falta de heartbeat (>90s)
///    e encaminha END com motivo HB_TIMEOUT ao Servidor.
/// 2) Descarrega buckets de agregacao cuja janela de 60s expirou
///    sem novas leituras, encaminhando AGGDATA ao Servidor.
/// </summary>
async Task MonitorizarEstadoGatewayAsync()
{
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(10));

        DateTime agoraUtc = DateTime.UtcNow;
        List<(string SensorId, string Timestamp)> sensoresExpirados = new List<(string SensorId, string Timestamp)>();

        lock (sessionLock)
        {
            foreach ((string sensorId, SensorSessionState estado) in estadosSessao)
            {
                if (estado.Fase == SessionPhase.Active && agoraUtc - estado.LastHeartbeatUtc > heartbeatTimeout)
                {
                    string timeoutTimestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                    estado.Fase = SessionPhase.Closed;
                    estado.LastActivityUtc = agoraUtc;
                    estado.LastEndTimestamp = timeoutTimestamp;
                    sensoresExpirados.Add((sensorId, timeoutTimestamp));
                }
            }
        }

        foreach ((string sensorId, string timeoutTimestamp) in sensoresExpirados)
        {
            RegistarSessaoGateway("HB_TIMEOUT", sensorId, timeoutTimestamp, "Sem heartbeat dentro do timeout");

            string msgTimeout = "END|" + sensorId + "|HB_TIMEOUT|" + timeoutTimestamp;
            if (!TryEncaminharComAck(msgTimeout, out string respostaServidor))
            {
                Console.WriteLine("[HB_TIMEOUT][ERRO]: " + sensorId + " -> " + respostaServidor);
            }
            else
            {
                Console.WriteLine("[HB_TIMEOUT]: Sensor " + sensorId + " marcado como inativo.");
            }
        }

        List<AggregateRecord> agregadosExpirados = ExtrairAgregadosExpirados(agoraUtc);
        foreach (AggregateRecord agregado in agregadosExpirados)
        {
            RegistarAgregadoGateway(agregado);
            string msgAgregado = MontarMensagemAgregada(agregado);
            if (!TryEncaminharComAck(msgAgregado, out string respostaServidor))
            {
                Console.WriteLine("[AGREGACAO][ERRO]: " + respostaServidor);
            }
            else
            {
                Console.WriteLine("[AGREGACAO]: Janela expirada encaminhada -> " + msgAgregado);
            }
        }
    }
}

/// <summary>
/// Acumula uma leitura no bucket de agregacao identificado por sensorId+tipo.
/// Emite um <see cref="AggregateRecord"/> quando:
///   - A janela temporal (60s) expira com a nova leitura, ou
///   - O bucket atinge o minimo de amostras (3).
/// Acesso protegido por <c>aggregationLock</c>.
/// </summary>
/// <param name="sensorId">Identificador do sensor.</param>
/// <param name="tipo">Tipo de dado (e.g. "TEMP").</param>
/// <param name="valor">Valor numerico ja normalizado.</param>
/// <param name="timestampUtc">Carimbo UTC da leitura.</param>
/// <param name="agregadoGerado">Agregado produzido, ou null se ainda nao ha dados suficientes.</param>
/// <returns><c>true</c> se um agregado foi gerado.</returns>
bool AdicionarLeituraParaAgregacao(string sensorId, string tipo, double valor, DateTime timestampUtc, out AggregateRecord? agregadoGerado)
{
    agregadoGerado = null;
    string chave = sensorId + "|" + tipo.ToUpperInvariant();

    lock (aggregationLock)
    {
        if (!bucketsAgregacao.TryGetValue(chave, out AggregateBucket? bucketExistente))
        {
            bucketsAgregacao[chave] = new AggregateBucket
            {
                SensorId = sensorId,
                Tipo = tipo.ToUpperInvariant(),
                JanelaInicioUtc = timestampUtc,
                UltimaAtualizacaoUtc = timestampUtc,
                Soma = valor,
                Minimo = valor,
                Maximo = valor,
                Contagem = 1
            };
            return false;
        }

        AggregateBucket bucket = bucketExistente;
        if (timestampUtc - bucket.JanelaInicioUtc >= aggregationWindow)
        {
            agregadoGerado = CriarAgregado(bucket, timestampUtc);
            bucketsAgregacao[chave] = new AggregateBucket
            {
                SensorId = sensorId,
                Tipo = tipo.ToUpperInvariant(),
                JanelaInicioUtc = timestampUtc,
                UltimaAtualizacaoUtc = timestampUtc,
                Soma = valor,
                Minimo = valor,
                Maximo = valor,
                Contagem = 1
            };
            return true;
        }

        bucket.Contagem += 1;
        bucket.Soma += valor;
        bucket.Minimo = Math.Min(bucket.Minimo, valor);
        bucket.Maximo = Math.Max(bucket.Maximo, valor);
        bucket.UltimaAtualizacaoUtc = timestampUtc;

        if (bucket.Contagem >= AggregationMinSamples)
        {
            agregadoGerado = CriarAgregado(bucket, timestampUtc);
            bucketsAgregacao.Remove(chave);
            return true;
        }

        bucketsAgregacao[chave] = bucket;
        return false;
    }
}

/// <summary>
/// Percorre todos os buckets de agregacao e extrai os que expiraram
/// (sem novas leituras durante a janela de 60s). Os buckets extraidos
/// sao removidos do dicionario. Protegido por <c>aggregationLock</c>.
/// </summary>
/// <param name="agoraUtc">Carimbo UTC atual para comparacao.</param>
/// <returns>Lista de agregados prontos para persistencia e envio.</returns>
List<AggregateRecord> ExtrairAgregadosExpirados(DateTime agoraUtc)
{
    var agregados = new List<AggregateRecord>();

    lock (aggregationLock)
    {
        var chavesParaRemover = new List<string>();
        foreach ((string chave, AggregateBucket bucket) in bucketsAgregacao)
        {
            if (bucket.Contagem > 0 && agoraUtc - bucket.UltimaAtualizacaoUtc >= aggregationWindow)
            {
                agregados.Add(CriarAgregado(bucket, agoraUtc));
                chavesParaRemover.Add(chave);
            }
        }

        foreach (string chave in chavesParaRemover)
        {
            bucketsAgregacao.Remove(chave);
        }
    }

    return agregados;
}

/// <summary>
/// Cria um registo agregado final a partir dos dados acumulados no bucket.
/// Calcula a media (soma/contagem) e preserva minimo, maximo e contagem.
/// </summary>
/// <param name="bucket">Bucket com os dados acumulados.</param>
/// <param name="janelaFimUtc">Carimbo de fim da janela de agregacao.</param>
/// <returns>Registo agregado pronto para persistencia e envio.</returns>
AggregateRecord CriarAgregado(AggregateBucket bucket, DateTime janelaFimUtc)
{
    return new AggregateRecord
    {
        SensorId = bucket.SensorId,
        Tipo = bucket.Tipo,
        Media = bucket.Soma / bucket.Contagem,
        Minimo = bucket.Minimo,
        Maximo = bucket.Maximo,
        Contagem = bucket.Contagem,
        JanelaInicioUtc = bucket.JanelaInicioUtc,
        JanelaFimUtc = janelaFimUtc
    };
}

/// <summary>
/// Serializa um registo agregado no formato de protocolo AGGDATA:
/// AGGDATA|sensorId|tipo|media|min|max|contagem|janelaInicio|janelaFim
/// </summary>
/// <param name="agregado">Registo agregado a serializar.</param>
/// <returns>String pronta para envio TCP ao Servidor.</returns>
string MontarMensagemAgregada(AggregateRecord agregado)
{
    return "AGGDATA|"
        + agregado.SensorId + "|"
        + agregado.Tipo + "|"
        + agregado.Media.ToString("0.00", CultureInfo.InvariantCulture) + "|"
        + agregado.Minimo.ToString("0.00", CultureInfo.InvariantCulture) + "|"
        + agregado.Maximo.ToString("0.00", CultureInfo.InvariantCulture) + "|"
        + agregado.Contagem + "|"
        + agregado.JanelaInicioUtc.ToString("yyyy-MM-ddTHH:mm:ss") + "|"
        + agregado.JanelaFimUtc.ToString("yyyy-MM-ddTHH:mm:ss");
}

/// <summary>
/// Persiste uma linha de dados brutos normalizados no ficheiro gateway_raw_data.txt.
/// Formato: timestamp;sensorId;tipoDado;valorNormalizado
/// Acesso serializado por <c>gatewayFileLock</c>.
/// </summary>
/// <param name="pacote">Mensagem DATA parseada.</param>
/// <param name="valorNormalizado">Valor apos normalizacao numerica.</param>
void RegistarRawGateway(ProtocoloMensagem pacote, double valorNormalizado)
{
    string linha = pacote.Timestamp + ";" + pacote.SensorId + ";" + pacote.TipoDado + ";" + valorNormalizado.ToString("0.00", CultureInfo.InvariantCulture);
    lock (gatewayFileLock)
    {
        File.AppendAllText(ficheiroRawGateway, linha + Environment.NewLine);
    }
}

/// <summary>
/// Persiste um registo agregado no ficheiro gateway_aggregated_data.txt.
/// Formato: janelaFim;sensorId;tipo;media;min;max;contagem;janelaInicio;janelaFim
/// Acesso serializado por <c>gatewayFileLock</c>.
/// </summary>
/// <param name="agregado">Registo agregado a persistir.</param>
void RegistarAgregadoGateway(AggregateRecord agregado)
{
    string linha = agregado.JanelaFimUtc.ToString("yyyy-MM-ddTHH:mm:ss")
        + ";" + agregado.SensorId
        + ";" + agregado.Tipo
        + ";" + agregado.Media.ToString("0.00", CultureInfo.InvariantCulture)
        + ";" + agregado.Minimo.ToString("0.00", CultureInfo.InvariantCulture)
        + ";" + agregado.Maximo.ToString("0.00", CultureInfo.InvariantCulture)
        + ";" + agregado.Contagem
        + ";" + agregado.JanelaInicioUtc.ToString("yyyy-MM-ddTHH:mm:ss")
        + ";" + agregado.JanelaFimUtc.ToString("yyyy-MM-ddTHH:mm:ss");

    lock (gatewayFileLock)
    {
        File.AppendAllText(ficheiroAgregadoGateway, linha + Environment.NewLine);
    }
}

/// <summary>
/// Persiste um evento de sessao no ficheiro gateway_sessions.txt para auditoria.
/// Formato: timestamp;sensorId;evento;detalhe
/// Acesso serializado por <c>gatewayFileLock</c>.
/// </summary>
/// <param name="evento">Tipo de evento (START, HB, END, HB_TIMEOUT).</param>
/// <param name="sensorId">Identificador do sensor.</param>
/// <param name="timestamp">Carimbo temporal do evento.</param>
/// <param name="detalhe">Informacao adicional (motivo, etc.), pode ser null.</param>
void RegistarSessaoGateway(string evento, string sensorId, string timestamp, string? detalhe)
{
    string linha = timestamp + ";" + sensorId + ";" + evento + ";" + (detalhe ?? string.Empty);
    lock (gatewayFileLock)
    {
        File.AppendAllText(ficheiroSessoesGateway, linha + Environment.NewLine);
    }
}

/// <summary>
/// Faz parse e validacao estrutural da mensagem recebida do sensor.
/// Identifica o tipo (START, HB, DATA, END), valida numero de campos e
/// formatos (sensorId, timestamp, valor numerico, tipo de dado).
/// </summary>
/// <param name="mensagem">String bruta recebida via TCP do sensor.</param>
/// <param name="pacote">Estrutura preenchida se o parse for bem-sucedido.</param>
/// <param name="erro">Descricao do erro se a validacao falhar.</param>
/// <returns><c>true</c> se a mensagem foi convertida com sucesso.</returns>
bool TryParseMensagem(string mensagem, out ProtocoloMensagem pacote, out string erro)
{
    pacote = default;
    erro = string.Empty;

    string[] partes = mensagem.Split('|');
    if (partes.Length == 0)
    {
        erro = "Mensagem vazia";
        return false;
    }

    string tipoMensagem = partes[0];
    if (tipoMensagem == "START" || tipoMensagem == "HB")
    {
        if (partes.Length != 3)
        {
            erro = tipoMensagem + " mal formado";
            return false;
        }

        if (!IsSensorIdValido(partes[1]) || !IsTimestampValido(partes[2]))
        {
            erro = tipoMensagem + " invalido (sensor/timestamp)";
            return false;
        }

        pacote = new ProtocoloMensagem(tipoMensagem, partes[1], null, null, null, partes[2]);
        return true;
    }

    if (tipoMensagem == "DATA")
    {
        if (partes.Length != 5)
        {
            erro = "DATA mal formado";
            return false;
        }

        if (!IsSensorIdValido(partes[1]) || string.IsNullOrWhiteSpace(partes[2]) || !IsTimestampValido(partes[4]))
        {
            erro = "DATA invalido (sensor/tipo/timestamp)";
            return false;
        }

        if (!IsValorValidoPorTipo(partes[2], partes[3], out string erroValor))
        {
            erro = erroValor;
            return false;
        }

        pacote = new ProtocoloMensagem(tipoMensagem, partes[1], partes[2], partes[3], null, partes[4]);
        return true;
    }

    if (tipoMensagem == "END")
    {
        if (partes.Length != 4)
        {
            erro = "END mal formado";
            return false;
        }

        if (!IsSensorIdValido(partes[1]) || string.IsNullOrWhiteSpace(partes[2]) || !IsTimestampValido(partes[3]))
        {
            erro = "END invalido (sensor/motivo/timestamp)";
            return false;
        }

        pacote = new ProtocoloMensagem(tipoMensagem, partes[1], null, null, partes[2], partes[3]);
        return true;
    }

    erro = "Tipo de mensagem desconhecido";
    return false;
}

/// <summary>
/// Verifica se o sensor existe no config.csv e tem estado "ativo".
/// Sensores em "manutencao" ou ausentes sao rejeitados.
/// </summary>
/// <param name="id">Identificador do sensor a validar.</param>
/// <returns><c>true</c> se o sensor existe e esta ativo.</returns>
bool ValidarSensorAtivo(string id)
{
    if (!TryObterSensorConfig(id, out SensorConfig? sensor) || sensor is null)
    {
        return false;
    }

    return string.Equals(sensor.Estado, "ativo", StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// Verifica se o sensor esta ativo E se tem permissao para o tipo de dado.
/// A lista de tipos permitidos e definida no campo tipos_dados do config.csv.
/// </summary>
/// <param name="id">Identificador do sensor.</param>
/// <param name="tipo">Tipo de dado recebido (e.g. "TEMP").</param>
/// <returns><c>true</c> se o sensor esta ativo e autorizado para o tipo.</returns>
bool ValidarSensorAtivoETipo(string id, string tipo)
{
    if (!TryObterSensorConfig(id, out SensorConfig? sensor) || sensor is null)
    {
        return false;
    }

    if (!string.Equals(sensor.Estado, "ativo", StringComparison.OrdinalIgnoreCase))
    {
        return false;
    }

    return sensor.TiposDados.Any(t => string.Equals(t, tipo, StringComparison.OrdinalIgnoreCase));
}

/// <summary>
/// Procura a configuracao de um sensor por ID no config.csv.
/// Carrega o ficheiro a cada chamada para refletir alteracoes externas.
/// Acesso protegido por <c>configLock</c>.
/// </summary>
/// <param name="id">Identificador do sensor a procurar.</param>
/// <param name="sensor">Configuracao encontrada, ou null se nao existir.</param>
/// <returns><c>true</c> se o sensor foi encontrado.</returns>
bool TryObterSensorConfig(string id, out SensorConfig? sensor)
{
    lock (configLock)
    {
        sensor = CarregarSensoresConfig()
            .FirstOrDefault(s => string.Equals(s.SensorId, id, StringComparison.OrdinalIgnoreCase));
    }

    return sensor is not null;
}

/// <summary>
/// Atualiza o campo last_sync do sensor no config.csv com a data/hora atual.
/// Carrega o ficheiro, modifica o registo e reescreve o CSV completo.
/// Acesso protegido por <c>configLock</c>.
/// </summary>
/// <param name="id">Identificador do sensor a atualizar.</param>
void AtualizarLastSync(string id)
{
    lock (configLock)
    {
        List<SensorConfig> sensores = CarregarSensoresConfig();
        SensorConfig? sensor = sensores.FirstOrDefault(s => string.Equals(s.SensorId, id, StringComparison.OrdinalIgnoreCase));
        if (sensor is null)
        {
            return;
        }

        sensor.LastSync = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
        GuardarSensoresConfig(sensores);
    }
}

/// <summary>
/// Le e interpreta o ficheiro config.csv, devolvendo a lista de sensores
/// configurados. Suporta delimitadores ';' e ':'. Ignora o cabecalho
/// e linhas vazias. Deve ser chamado dentro de <c>configLock</c>.
/// </summary>
/// <returns>Lista de objetos <see cref="SensorConfig"/> carregados.</returns>
List<SensorConfig> CarregarSensoresConfig()
{
    string path = ObterCaminhoConfig();
    var sensores = new List<SensorConfig>();

    if (!File.Exists(path))
    {
        return sensores;
    }

    foreach (string linhaOriginal in File.ReadAllLines(path))
    {
        string linha = linhaOriginal.Trim();
        if (string.IsNullOrWhiteSpace(linha))
        {
            continue;
        }

        if (linha.StartsWith("sensor_id;", StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        string[] campos;
        if (linha.Contains(';'))
        {
            campos = linha.Split(';');
        }
        else if (linha.Contains(':'))
        {
            campos = linha.Split(':');
        }
        else
        {
            continue;
        }

        if (campos.Length < 5)
        {
            continue;
        }

        sensores.Add(new SensorConfig
        {
            SensorId = campos[0].Trim(),
            Estado = campos[1].Trim(),
            Zona = campos[2].Trim(),
            TiposDados = ParseListaTipos(campos[3]).ToList(),
            LastSync = campos[4].Trim()
        });
    }

    return sensores;
}

/// <summary>
/// Reescreve o ficheiro config.csv com a lista completa de sensores,
/// incluindo o cabecalho. Usado apos atualizacao de last_sync.
/// </summary>
/// <param name="sensores">Lista atualizada de configuracoes.</param>
void GuardarSensoresConfig(List<SensorConfig> sensores)
{
    string path = ObterCaminhoConfig();
    var linhas = new List<string> { "sensor_id;estado;zona;tipos_dados;last_sync" };

    foreach (SensorConfig sensor in sensores)
    {
        string tipos = string.Join(",", sensor.TiposDados);
        linhas.Add(sensor.SensorId + ";" + sensor.Estado + ";" + sensor.Zona + ";" + tipos + ";" + sensor.LastSync);
    }

    File.WriteAllLines(path, linhas);
}

/// <summary>
/// Resolve o caminho absoluto do config.csv, procurando por esta ordem:
/// 1) diretorio de trabalho atual, 2) subpasta GatewayNode/, 3) diretorio do executavel.
/// Devolve o caminho da subpasta GatewayNode como fallback.
/// </summary>
/// <returns>Caminho absoluto para o ficheiro config.csv.</returns>
string ObterCaminhoConfig()
{
    string cwdDireto = Path.Combine(Directory.GetCurrentDirectory(), "config.csv");
    if (File.Exists(cwdDireto))
    {
        return cwdDireto;
    }

    string cwdGateway = Path.Combine(Directory.GetCurrentDirectory(), "GatewayNode", "config.csv");
    if (File.Exists(cwdGateway))
    {
        return cwdGateway;
    }

    string baseDir = Path.Combine(AppContext.BaseDirectory, "config.csv");
    if (File.Exists(baseDir))
    {
        return baseDir;
    }

    return cwdGateway;
}

/// <summary>
/// Resolve a pasta onde o gateway grava os ficheiros de dados locais.
/// Prefere a subpasta GatewayNode/ se existir; caso contrario usa o diretorio atual.
/// </summary>
/// <returns>Caminho absoluto para a pasta de dados do gateway.</returns>
string ObterPastaGatewayDados()
{
    string pastaGatewayNoCwd = Path.Combine(Directory.GetCurrentDirectory(), "GatewayNode");
    if (Directory.Exists(pastaGatewayNoCwd))
    {
        return pastaGatewayNoCwd;
    }

    return Directory.GetCurrentDirectory();
}

/// <summary>
/// Interpreta o campo tipos_dados do CSV, que pode estar no formato
/// "TEMP,HUM,RUIDO" ou "[TEMP,HUM,RUIDO]" (com parentes retos).
/// </summary>
/// <param name="campoTipos">Valor bruto do campo tipos_dados.</param>
/// <returns>Sequencia de tipos individuais, sem espacos.</returns>
IEnumerable<string> ParseListaTipos(string? campoTipos)
{
    if (string.IsNullOrWhiteSpace(campoTipos))
    {
        return Enumerable.Empty<string>();
    }

    string texto = campoTipos.Trim();
    if (texto.StartsWith("[") && texto.EndsWith("]"))
    {
        texto = texto[1..^1];
    }

    return texto
        .Split(',', StringSplitOptions.RemoveEmptyEntries)
        .Select(t => t.Trim());
}

/// <summary>
/// Validacao minima do identificador de sensor (nao pode ser vazio ou whitespace).
/// </summary>
/// <param name="sensorId">Identificador recebido na mensagem.</param>
/// <returns><c>true</c> se o ID e nao-vazio.</returns>
bool IsSensorIdValido(string sensorId)
{
    return !string.IsNullOrWhiteSpace(sensorId);
}

/// <summary>
/// Valida se o timestamp segue um dos formatos aceites: "yyyy-MM-ddTHH:mm:ss" ou ISO 8601 "o".
/// </summary>
/// <param name="timestamp">Timestamp textual recebido na mensagem.</param>
/// <returns><c>true</c> se o formato e reconhecido.</returns>
bool IsTimestampValido(string timestamp)
{
    return DateTime.TryParseExact(
        timestamp,
        timestampFormats,
        CultureInfo.InvariantCulture,
        DateTimeStyles.None,
        out _);
}

/// <summary>
/// Converte um timestamp textual para <see cref="DateTime"/> em UTC.
/// Tenta os formatos aceites (local -> UTC). Se falhar, devolve <c>DateTime.UtcNow</c>
/// como fallback seguro para nao bloquear o fluxo.
/// </summary>
/// <param name="timestamp">Timestamp textual a converter.</param>
/// <returns>Data/hora em UTC.</returns>
DateTime ParseTimestampUtc(string timestamp)
{
    if (DateTime.TryParseExact(
        timestamp,
        timestampFormats,
        CultureInfo.InvariantCulture,
        DateTimeStyles.AssumeLocal,
        out DateTime parsed))
    {
        return parsed.ToUniversalTime();
    }

    return DateTime.UtcNow;
}

/// <summary>
/// Valida se o valor e numerico e se cai dentro dos limites esperados para o tipo.
/// Limites: TEMP [-50,80], RUIDO [0,200], HUM [0,100], PM2.5/PM10/AR [0,1000].
/// Tipos desconhecidos sao aceites sem restricao de gama.
/// </summary>
/// <param name="tipo">Tipo de medicao (e.g. "TEMP").</param>
/// <param name="valor">Valor textual a validar.</param>
/// <param name="erro">Descricao do erro se a validacao falhar.</param>
/// <returns><c>true</c> se o valor e valido.</returns>
bool IsValorValidoPorTipo(string tipo, string valor, out string erro)
{
    erro = string.Empty;
    string valorNormalizado = valor.Replace(',', '.');
    if (!double.TryParse(valorNormalizado, NumberStyles.Float, CultureInfo.InvariantCulture, out double numero))
    {
        erro = "DATA invalido (valor nao numerico)";
        return false;
    }

    string tipoUpper = tipo.Trim().ToUpperInvariant();
    (double Min, double Max)? intervalo = tipoUpper switch
    {
        "TEMP" => (-50, 80),
        "RUIDO" => (0, 200),
        "HUM" => (0, 100),
        "PM2.5" => (0, 1000),
        "PM10" => (0, 1000),
        "AR" => (0, 1000),
        _ => null
    };

    if (intervalo is null)
    {
        return true;
    }

    if (numero < intervalo.Value.Min || numero > intervalo.Value.Max)
    {
        erro = "DATA invalido (valor fora de gama para " + tipo + ")";
        return false;
    }

    return true;
}

/// <summary>
/// [Protocolo TP2 §2.1 — Fase 1.2] Invoca o PreProcessingService via RPC (HTTP POST).
/// Se o serviço estiver indisponível ou responder com erro, aplica a normalização
/// local como fallback para garantir continuidade (degradação graciosa).
/// </summary>
/// <param name="tipo">Tipo de dado (e.g. "TEMP").</param>
/// <param name="sensorId">Identificador do sensor.</param>
/// <param name="valorOriginal">Valor textual bruto do sensor.</param>
/// <param name="timestamp">Timestamp da leitura.</param>
/// <param name="valorNormalizado">Valor numerico normalizado (saida).</param>
/// <param name="erro">Descricao do erro se a normalizacao falhar.</param>
/// <returns><c>true</c> se o valor foi normalizado com sucesso.</returns>
bool TryPreprocessarViaRpc(string tipo, string sensorId, string valorOriginal, string timestamp, out double valorNormalizado, out string erro)
{
    valorNormalizado = 0;
    erro = string.Empty;

    try
    {
        var payload = new
        {
            sensor_id = sensorId,
            tipo      = tipo,
            valor     = valorOriginal,
            timestamp = timestamp
        };

        string json = JsonSerializer.Serialize(payload);
        using StringContent conteudo = new StringContent(json, Encoding.UTF8, "application/json");

        // Chamada síncrona ao serviço RPC (timeout de 2s definido no HttpClient).
        HttpResponseMessage resposta = httpClient.PostAsync(PreProcessingServiceUrl, conteudo).GetAwaiter().GetResult();
        string corpoResposta = resposta.Content.ReadAsStringAsync().GetAwaiter().GetResult();

        using JsonDocument doc = JsonDocument.Parse(corpoResposta);
        JsonElement root = doc.RootElement;

        if (!resposta.IsSuccessStatusCode)
        {
            // Serviço devolveu erro de validação (ex: valor fora de gama) — não usar fallback, rejeitar.
            erro = root.TryGetProperty("erro", out JsonElement erroEl) ? erroEl.GetString() ?? "Erro RPC" : "Erro RPC";
            return false;
        }

        if (root.TryGetProperty("valor", out JsonElement valorEl) && valorEl.ValueKind == JsonValueKind.Number)
        {
            valorNormalizado = Math.Round(valorEl.GetDouble(), 2);
            Console.WriteLine("[RPC]: Pré-processamento via serviço remoto -> " + tipo + "=" + valorNormalizado);
            return true;
        }

        erro = "Resposta RPC sem campo 'valor' válido";
        return false;
    }
    catch (Exception ex)
    {
        // Fallback local: serviço RPC indisponível — normaliza localmente.
        Console.WriteLine("[RPC][FALLBACK]: PreProcessingService inacessível (" + ex.Message + "). A usar normalização local.");
        return TryPreprocessarValor(tipo, valorOriginal, out valorNormalizado, out erro);
    }
}

/// <summary>
/// Normalização local de fallback. Substitui ',' por '.', verifica limites
/// por tipo e arredonda a 2 casas decimais.
/// Usada quando o PreProcessingService não está disponível.
/// </summary>
/// <param name="tipo">Tipo de dado (e.g. "TEMP").</param>
/// <param name="valorOriginal">Valor textual bruto do sensor.</param>
/// <param name="valorNormalizado">Valor numerico normalizado (saida).</param>
/// <param name="erro">Descricao do erro se a validacao falhar.</param>
/// <returns><c>true</c> se o valor foi normalizado com sucesso.</returns>
bool TryPreprocessarValor(string tipo, string valorOriginal, out double valorNormalizado, out string erro)
{
    valorNormalizado = 0;
    erro = string.Empty;

    if (!IsValorValidoPorTipo(tipo, valorOriginal, out erro))
    {
        return false;
    }

    string valorTratado = valorOriginal.Replace(',', '.');
    if (!double.TryParse(valorTratado, NumberStyles.Float, CultureInfo.InvariantCulture, out double valor))
    {
        erro = "Falha no pre-processamento do valor";
        return false;
    }

    valorNormalizado = Math.Round(valor, 2);
    return true;
}

/// <summary>
/// Encaminha uma mensagem ao Servidor Central e verifica se a resposta
/// comeca por "ACK|". Combina transporte + validacao de resposta.
/// </summary>
/// <param name="mensagem">Mensagem a encaminhar.</param>
/// <param name="respostaServidor">Resposta bruta do servidor (saida).</param>
/// <returns><c>true</c> se o servidor respondeu com ACK.</returns>
bool TryEncaminharComAck(string mensagem, out string respostaServidor)
{
    (bool sucesso, string resposta) = EncaminharParaServidor(mensagem);
    respostaServidor = resposta;

    return sucesso && resposta.StartsWith("ACK|", StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// Abre uma ligacao TCP curta ao Servidor Central (127.0.0.1:6000),
/// envia a mensagem e le a resposta com timeout de 3 segundos.
/// Se o servidor estiver offline, captura a excepcao e devolve erro.
/// </summary>
/// <param name="msg">Mensagem a enviar ao servidor.</param>
/// <returns>
/// Tuplo (Sucesso, RespostaServidor): Sucesso indica se houve comunicacao;
/// RespostaServidor contem a resposta textual ou codigo de erro.
/// </returns>
(bool Sucesso, string RespostaServidor) EncaminharParaServidor(string msg)
{
    try
    {
        using TcpClient serverClient = new TcpClient("127.0.0.1", ServerPort);
        serverClient.ReceiveTimeout = 3000;

        NetworkStream stream = serverClient.GetStream();
        byte[] dados = Encoding.ASCII.GetBytes(msg);
        stream.Write(dados, 0, dados.Length);

        byte[] respostaBuffer = new byte[1024];
        int respostaBytes = stream.Read(respostaBuffer, 0, respostaBuffer.Length);
        if (respostaBytes <= 0)
        {
            return (false, "SEM_RESPOSTA_SERVIDOR");
        }

        string resposta = Encoding.ASCII.GetString(respostaBuffer, 0, respostaBytes);
        return (true, resposta);
    }
    catch (Exception ex)
    {
        Console.WriteLine("[ERRO]: Servidor offline! " + ex.Message);
        return (false, "ERRO_SERVIDOR_OFFLINE");
    }
}

/// <summary>
/// Codifica e envia uma resposta textual ASCII no stream TCP de volta ao sensor.
/// </summary>
/// <param name="stream">Stream da ligacao TCP ativa com o sensor.</param>
/// <param name="resposta">Texto da resposta (e.g. "ACK|GATEWAY|DATA").</param>
void EnviarResposta(NetworkStream stream, string resposta)
{
    byte[] dados = Encoding.ASCII.GetBytes(resposta);
    stream.Write(dados, 0, dados.Length);
}

/// <summary>
/// Estrutura imutavel (record struct) que representa uma mensagem parseada.
/// Contem campos para os tipos START, HB, DATA e END; campos nao aplicaveis
/// ficam a null. Usada como contrato interno entre parser e handlers.
/// </summary>
readonly record struct ProtocoloMensagem(
    string TipoMensagem,
    string SensorId,
    string? TipoDado,
    string? Valor,
    string? Motivo,
    string Timestamp);

/// <summary>
/// Modelo de configuracao de sensor carregado do config.csv.
/// Campos: SensorId, Estado (ativo/manutencao), Zona, TiposDados e LastSync.
/// </summary>
sealed class SensorConfig
{
    public string SensorId { get; set; } = string.Empty;
    public string Estado { get; set; } = string.Empty;
    public string Zona { get; set; } = string.Empty;
    public List<string> TiposDados { get; set; } = new List<string>();
    public string LastSync { get; set; } = string.Empty;
}

/// <summary>
/// Estado de sessao mantido em memoria para cada sensor ligado ao Gateway.
/// Controla fase (Idle/Active/Closed), timestamps de heartbeat e atividade.
/// </summary>
sealed class SensorSessionState
{
    public SessionPhase Fase { get; set; } = SessionPhase.Idle;
    public DateTime LastHeartbeatUtc { get; set; } = DateTime.UtcNow;
    public DateTime LastActivityUtc { get; set; } = DateTime.UtcNow;
    public string LastStartTimestamp { get; set; } = string.Empty;
    public string LastEndTimestamp { get; set; } = string.Empty;
}

/// <summary>
/// Acumulador temporario para agregacao de dados por sensor e tipo.
/// Acumula soma, minimo, maximo e contagem dentro de uma janela temporal.
/// Quando a janela expira ou a contagem atinge o minimo, gera um <see cref="AggregateRecord"/>.
/// </summary>
sealed class AggregateBucket
{
    public string SensorId { get; set; } = string.Empty;
    public string Tipo { get; set; } = string.Empty;
    public DateTime JanelaInicioUtc { get; set; }
    public DateTime UltimaAtualizacaoUtc { get; set; }
    public double Soma { get; set; }
    public double Minimo { get; set; }
    public double Maximo { get; set; }
    public int Contagem { get; set; }
}

/// <summary>
/// Resultado final de uma agregacao: media, minimo, maximo e contagem
/// de leituras dentro de uma janela temporal. Pronto para persistencia
/// local (gateway_aggregated_data.txt) e envio ao Servidor como AGGDATA.
/// </summary>
sealed class AggregateRecord
{
    public string SensorId { get; set; } = string.Empty;
    public string Tipo { get; set; } = string.Empty;
    public double Media { get; set; }
    public double Minimo { get; set; }
    public double Maximo { get; set; }
    public int Contagem { get; set; }
    public DateTime JanelaInicioUtc { get; set; }
    public DateTime JanelaFimUtc { get; set; }
}

/// <summary>
/// Fases possiveis de uma sessao: Idle (nao iniciada),
/// Active (sessao aberta, aceita DATA e HB) e Closed (sessao encerrada).
/// </summary>
enum SessionPhase
{
    Idle,
    Active,
    Closed
}
