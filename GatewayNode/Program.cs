using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("--- GATEWAY ONE HEALTH ---");

// Portas e regras operacionais do gateway.
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

// Processa uma ligacao TCP individual do sensor e devolve ACK/NACK.
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

// Valida e trata mensagens de sessao (START/HB/END), com encaminhamento para o servidor.
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

// Valida, normaliza e encaminha dados brutos; gera agregados quando aplicavel.
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

    if (!TryPreprocessarValor(pacote.TipoDado!, pacote.Valor!, out double valorNormalizado, out string erroPreProcessamento))
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

// Garante que a sequencia de mensagens respeita o estado atual da sessao.
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

// Atualiza o estado da sessao e metadados de atividade/heartbeat.
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

// Devolve estado de sessao existente ou cria um novo para o sensor.
SensorSessionState ObterOuCriarEstadoSessao(string sensorId)
{
    if (!estadosSessao.TryGetValue(sensorId, out SensorSessionState? estado))
    {
        estado = new SensorSessionState();
        estadosSessao[sensorId] = estado;
    }

    return estado;
}

// Tarefa periodica para fechar sessoes por timeout e descarregar agregados expirados.
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

// Acumula leituras por sensor/tipo e emite agregado por contagem minima ou janela temporal.
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

// Extrai e remove agregados cuja janela expirou sem novas leituras.
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

// Materializa um registo agregado a partir do bucket acumulado.
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

// Converte o agregado para o formato de protocolo AGGDATA.
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

// Persiste dados brutos normalizados recebidos no gateway.
void RegistarRawGateway(ProtocoloMensagem pacote, double valorNormalizado)
{
    string linha = pacote.Timestamp + ";" + pacote.SensorId + ";" + pacote.TipoDado + ";" + valorNormalizado.ToString("0.00", CultureInfo.InvariantCulture);
    lock (gatewayFileLock)
    {
        File.AppendAllText(ficheiroRawGateway, linha + Environment.NewLine);
    }
}

// Persiste os dados agregados produzidos localmente.
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

// Persiste eventos de sessao (START/HB/END/timeout) para auditoria.
void RegistarSessaoGateway(string evento, string sensorId, string timestamp, string? detalhe)
{
    string linha = timestamp + ";" + sensorId + ";" + evento + ";" + (detalhe ?? string.Empty);
    lock (gatewayFileLock)
    {
        File.AppendAllText(ficheiroSessoesGateway, linha + Environment.NewLine);
    }
}

// Faz parse e validacao estrutural das mensagens recebidas do sensor.
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

// Valida se o sensor existe no ficheiro de configuracao e esta ativo.
bool ValidarSensorAtivo(string id)
{
    if (!TryObterSensorConfig(id, out SensorConfig? sensor) || sensor is null)
    {
        return false;
    }

    return string.Equals(sensor.Estado, "ativo", StringComparison.OrdinalIgnoreCase);
}

// Valida sensor ativo e permissao para o tipo de dado recebido.
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

// Procura a configuracao de um sensor por ID.
bool TryObterSensorConfig(string id, out SensorConfig? sensor)
{
    lock (configLock)
    {
        sensor = CarregarSensoresConfig()
            .FirstOrDefault(s => string.Equals(s.SensorId, id, StringComparison.OrdinalIgnoreCase));
    }

    return sensor is not null;
}

// Atualiza no CSV o timestamp de ultima sincronizacao do sensor.
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

// Carrega e interpreta o ficheiro config.csv para objetos de configuracao.
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

// Reescreve o config.csv com o estado mais recente dos sensores.
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

// Resolve o caminho de config.csv em diferentes contextos de execucao.
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

// Resolve pasta destino para os ficheiros de dados do gateway.
string ObterPastaGatewayDados()
{
    string pastaGatewayNoCwd = Path.Combine(Directory.GetCurrentDirectory(), "GatewayNode");
    if (Directory.Exists(pastaGatewayNoCwd))
    {
        return pastaGatewayNoCwd;
    }

    return Directory.GetCurrentDirectory();
}

// Interpreta a lista de tipos de dados permitidos no campo do CSV.
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

// Validacao minima de identificador de sensor.
bool IsSensorIdValido(string sensorId)
{
    return !string.IsNullOrWhiteSpace(sensorId);
}

// Valida timestamp segundo os formatos aceites no protocolo.
bool IsTimestampValido(string timestamp)
{
    return DateTime.TryParseExact(
        timestamp,
        timestampFormats,
        CultureInfo.InvariantCulture,
        DateTimeStyles.None,
        out _);
}

// Converte timestamp textual para UTC; usa agora UTC como fallback seguro.
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

// Valida valor numerico e intervalo esperado por tipo de medicao.
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

// Aplica validacao e normalizacao numerica antes de persistir/encaminhar.
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

// Encaminha para o servidor e exige ACK de protocolo para sucesso.
bool TryEncaminharComAck(string mensagem, out string respostaServidor)
{
    (bool sucesso, string resposta) = EncaminharParaServidor(mensagem);
    respostaServidor = resposta;

    return sucesso && resposta.StartsWith("ACK|", StringComparison.OrdinalIgnoreCase);
}

// Envia mensagem ao servidor central e devolve resultado de transporte + resposta.
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

// Escreve uma resposta textual no stream TCP do sensor.
void EnviarResposta(NetworkStream stream, string resposta)
{
    byte[] dados = Encoding.ASCII.GetBytes(resposta);
    stream.Write(dados, 0, dados.Length);
}

// Contrato interno para dados de mensagem ja validados e parseados.
readonly record struct ProtocoloMensagem(
    string TipoMensagem,
    string SensorId,
    string? TipoDado,
    string? Valor,
    string? Motivo,
    string Timestamp);

// Modelo de configuracao de sensor carregado do config.csv.
sealed class SensorConfig
{
    public string SensorId { get; set; } = string.Empty;
    public string Estado { get; set; } = string.Empty;
    public string Zona { get; set; } = string.Empty;
    public List<string> TiposDados { get; set; } = new List<string>();
    public string LastSync { get; set; } = string.Empty;
}

// Estado de sessao mantido em memoria para cada sensor.
sealed class SensorSessionState
{
    public SessionPhase Fase { get; set; } = SessionPhase.Idle;
    public DateTime LastHeartbeatUtc { get; set; } = DateTime.UtcNow;
    public DateTime LastActivityUtc { get; set; } = DateTime.UtcNow;
    public string LastStartTimestamp { get; set; } = string.Empty;
    public string LastEndTimestamp { get; set; } = string.Empty;
}

// Acumulador temporario para agregacao por janela de tempo/contagem.
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

// Resultado final de uma agregacao pronto para persistencia e envio.
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

// Fases possiveis de uma sessao de comunicacao do sensor.
enum SessionPhase
{
    Idle,
    Active,
    Closed
}
