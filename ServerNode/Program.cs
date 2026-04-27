// ============================================================================
// Ficheiro : ServerNode/Program.cs
// Modulo   : Servidor Central (Servidor TCP)
// Porta    : Escuta na porta 6000
// Descricao: Recebe mensagens encaminhadas pelo Gateway e persiste-as em
//            ficheiros de texto organizados por tipo (sessao, dados, agregados).
//            Valida o protocolo, controla transicoes de sessao por sensor e
//            responde com ACK ou NACK a cada mensagem recebida.
// Ficheiros de saida:
//   session_start.txt      - registos de inicio de sessao
//   session_end.txt        - registos de fim de sessao (inclui motivo)
//   heartbeats.txt         - heartbeats recebidos
//   {TIPO}.txt             - dados brutos por tipo (TEMP.txt, RUIDO.txt, ...)
//   aggregated_{TIPO}.txt  - dados agregados por tipo
//   protocol_errors.txt    - mensagens rejeitadas por erro de protocolo
// ============================================================================
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

// --- Ponto de entrada (top-level statements -> Main implicito) ---
Console.WriteLine("--- SERVIDOR CENTRAL ---");

// Porta TCP onde o servidor escuta e formatos aceites para timestamps.
const int ServerPort = 6000;
string[] timestampFormats = { "yyyy-MM-ddTHH:mm:ss", "o" };

// Locks para escrita em ficheiros e gestao de estado de sessao concorrente.
object serverFileLock = new object();
object sessionLock = new object();
var estadosSessao = new Dictionary<string, SessionState>(StringComparer.OrdinalIgnoreCase);

// Inicializa listener TCP para receber dados do gateway.
TcpListener serverListener = new TcpListener(IPAddress.Any, ServerPort);
serverListener.Start();
Console.WriteLine("Aguardando dados na porta 6000...");

// Loop principal de aceites e despacho de ligacoes para tarefas dedicadas.
while (true)
{
    try
    {
        TcpClient client = serverListener.AcceptTcpClient();
        _ = Task.Run(() => ProcessarLigacaoCliente(client));
    }
    catch (Exception ex)
    {
        Console.WriteLine("Erro no Servidor: " + ex.Message);
    }
}

/// <summary>
/// Processa uma ligacao TCP individual recebida do Gateway.
/// Le a mensagem, valida o protocolo, verifica transicoes de sessao
/// e devolve a resposta adequada (ACK ou NACK) no mesmo stream.
/// Executada numa Task dedicada por ligacao.
/// </summary>
/// <param name="client">Ligacao TCP aceite pelo listener.</param>
void ProcessarLigacaoCliente(TcpClient client)
{
    using (client)
    {
        try
        {
            NetworkStream stream = client.GetStream();
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
                Console.WriteLine("[ERRO_PROTOCOLO]: " + erroValidacao + " -> " + mensagem);
                AppendServidorFicheiro("protocol_errors.txt", DateTime.Now.ToString("s") + ";" + erroValidacao + ";" + mensagem);
                EnviarResposta(stream, "NACK|PROTO|" + erroValidacao);
                return;
            }

            if (ExigeSessaoAtiva(pacote.TipoMensagem) && !TryValidarTransicaoSessao(pacote, out string erroSessao))
            {
                Console.WriteLine("[ERRO_SESSAO]: " + erroSessao + " -> " + mensagem);
                EnviarResposta(stream, "NACK|SESSION|" + erroSessao);
                return;
            }

            string resposta = ProcessarMensagemValida(pacote);
            EnviarResposta(stream, resposta);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro no processamento do cliente: " + ex.Message);
        }
    }
}

/// <summary>
/// Aplica a logica de negocio para cada tipo de mensagem valida e persiste
/// o registo no ficheiro adequado. Atualiza o estado da sessao em memoria.
/// Tipos tratados: START, HB, DATA, AGGDATA, END.
/// </summary>
/// <param name="pacote">Mensagem ja parseada e validada.</param>
/// <returns>Resposta de protocolo, e.g. "ACK|SERVER|DATA".</returns>
string ProcessarMensagemValida(ProtocoloMensagem pacote)
{
    if (pacote.TipoMensagem == "START")
    {
        AtualizarEstadoSessao(pacote, SessionPhase.Active);
        string linha = pacote.Timestamp + ";" + pacote.SensorId + ";START";
        AppendServidorFicheiro("session_start.txt", linha);
        Console.WriteLine("[SESSAO]: Inicio registado -> " + linha);
        return "ACK|SERVER|START";
    }

    if (pacote.TipoMensagem == "HB")
    {
        AtualizarEstadoSessao(pacote, SessionPhase.Active);
        string linha = pacote.Timestamp + ";" + pacote.SensorId + ";HB";
        AppendServidorFicheiro("heartbeats.txt", linha);
        Console.WriteLine("[HEARTBEAT]: Registado -> " + linha);
        return "ACK|SERVER|HB";
    }

    if (pacote.TipoMensagem == "DATA")
    {
        AtualizarEstadoSessao(pacote, SessionPhase.Active);
        string linha = pacote.Timestamp + ";" + pacote.SensorId + ";" + pacote.TipoDado + ";" + pacote.Valor;
        AppendServidorFicheiro((pacote.TipoDado ?? "DADOS") + ".txt", linha);
        Console.WriteLine("[DADOS]: Gravado em " + pacote.TipoDado + ".txt -> " + linha);
        return "ACK|SERVER|DATA";
    }

    if (pacote.TipoMensagem == "AGGDATA")
    {
        string media = pacote.Media?.ToString("0.00", CultureInfo.InvariantCulture) ?? string.Empty;
        string minimo = pacote.Minimo?.ToString("0.00", CultureInfo.InvariantCulture) ?? string.Empty;
        string maximo = pacote.Maximo?.ToString("0.00", CultureInfo.InvariantCulture) ?? string.Empty;
        string linha = pacote.JanelaFim + ";" + pacote.SensorId + ";" + pacote.TipoDado + ";"
            + media + ";" + minimo + ";" + maximo + ";"
            + pacote.Contagem + ";" + pacote.JanelaInicio + ";" + pacote.JanelaFim;
        AppendServidorFicheiro("aggregated_" + pacote.TipoDado + ".txt", linha);
        Console.WriteLine("[AGREGACAO]: Gravado agregado de " + pacote.TipoDado + " -> " + linha);
        return "ACK|SERVER|AGGDATA";
    }

    AtualizarEstadoSessao(pacote, SessionPhase.Closed);
    string linhaFim = pacote.Timestamp + ";" + pacote.SensorId + ";" + pacote.Motivo;
    AppendServidorFicheiro("session_end.txt", linhaFim);
    Console.WriteLine("[SESSAO]: Fim registado -> " + linhaFim);
    return "ACK|SERVER|END";
}

/// <summary>
/// Indica se o tipo de mensagem exige verificacao do estado da sessao.
/// AGGDATA nao exige sessao ativa porque e gerado autonomamente pelo Gateway.
/// </summary>
/// <param name="tipoMensagem">Tipo da mensagem (START, HB, DATA, END, AGGDATA).</param>
/// <returns><c>true</c> se a mensagem requer sessao; <c>false</c> caso contrario.</returns>
bool ExigeSessaoAtiva(string tipoMensagem)
{
    return tipoMensagem == "START"
        || tipoMensagem == "HB"
        || tipoMensagem == "DATA"
        || tipoMensagem == "END";
}

/// <summary>
/// Verifica se a transicao de sessao e valida para o estado atual do sensor.
/// Impede START duplicado e DATA/HB/END sem sessao aberta.
/// Acesso protegido por <c>sessionLock</c>.
/// </summary>
/// <param name="pacote">Mensagem parseada com SensorId e TipoMensagem.</param>
/// <param name="erro">Descricao do erro se a transicao for invalida.</param>
/// <returns><c>true</c> se a transicao e permitida.</returns>
bool TryValidarTransicaoSessao(ProtocoloMensagem pacote, out string erro)
{
    erro = string.Empty;

    lock (sessionLock)
    {
        bool sessaoAtiva = estadosSessao.TryGetValue(pacote.SensorId, out SessionState? estado)
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

        if (pacote.TipoMensagem == "HB" || pacote.TipoMensagem == "DATA" || pacote.TipoMensagem == "END")
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
/// Atualiza a fase da sessao e os carimbos temporais (atividade e heartbeat)
/// para o sensor identificado na mensagem. Acesso protegido por <c>sessionLock</c>.
/// </summary>
/// <param name="pacote">Mensagem que origina a atualizacao.</param>
/// <param name="faseDestino">Nova fase pretendida (Active ou Closed).</param>
void AtualizarEstadoSessao(ProtocoloMensagem pacote, SessionPhase faseDestino)
{
    lock (sessionLock)
    {
        if (!estadosSessao.TryGetValue(pacote.SensorId, out SessionState? estado))
        {
            estado = new SessionState();
            estadosSessao[pacote.SensorId] = estado;
        }

        estado.Fase = faseDestino;
        estado.LastActivityUtc = DateTime.UtcNow;

        if (pacote.TipoMensagem == "START" || pacote.TipoMensagem == "HB")
        {
            estado.LastHeartbeatUtc = DateTime.UtcNow;
        }
    }
}

/// <summary>
/// Acrescenta uma linha a um ficheiro de saida do servidor.
/// O acesso ao sistema de ficheiros e serializado por <c>serverFileLock</c>
/// para evitar corrupcao em acessos concorrentes de multiplas tasks.
/// </summary>
/// <param name="nomeFicheiro">Nome do ficheiro (e.g. "TEMP.txt").</param>
/// <param name="linha">Conteudo da linha a adicionar, sem newline.</param>
void AppendServidorFicheiro(string nomeFicheiro, string linha)
{
    lock (serverFileLock)
    {
        File.AppendAllText(nomeFicheiro, linha + Environment.NewLine);
    }
}

/// <summary>
/// Faz parse e validacao sintatica/semantica da mensagem recebida.
/// Identifica o tipo (START, HB, DATA, AGGDATA, END), valida o numero de campos
/// e os formatos de cada campo (sensorId, timestamp, valor numerico, etc.).
/// </summary>
/// <param name="mensagem">String bruta recebida via TCP.</param>
/// <param name="pacote">Estrutura preenchida se o parse for bem-sucedido.</param>
/// <param name="erro">Descricao do erro se a validacao falhar.</param>
/// <returns><c>true</c> se a mensagem e valida e foi convertida em <paramref name="pacote"/>.</returns>
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

        pacote = new ProtocoloMensagem(tipoMensagem, partes[1], null, null, null, partes[2], null, null, null, null, null, null);
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

        pacote = new ProtocoloMensagem(tipoMensagem, partes[1], partes[2], partes[3], null, partes[4], null, null, null, null, null, null);
        return true;
    }

    if (tipoMensagem == "AGGDATA")
    {
        if (partes.Length != 9)
        {
            erro = "AGGDATA mal formado";
            return false;
        }

        if (!IsSensorIdValido(partes[1]) || string.IsNullOrWhiteSpace(partes[2]))
        {
            erro = "AGGDATA invalido (sensor/tipo)";
            return false;
        }

        if (!double.TryParse(partes[3], NumberStyles.Float, CultureInfo.InvariantCulture, out double media)
            || !double.TryParse(partes[4], NumberStyles.Float, CultureInfo.InvariantCulture, out double minimo)
            || !double.TryParse(partes[5], NumberStyles.Float, CultureInfo.InvariantCulture, out double maximo)
            || !int.TryParse(partes[6], out int contagem)
            || contagem <= 0
            || !IsTimestampValido(partes[7])
            || !IsTimestampValido(partes[8]))
        {
            erro = "AGGDATA invalido (campos numericos/timestamps)";
            return false;
        }

        pacote = new ProtocoloMensagem(
            tipoMensagem,
            partes[1],
            partes[2],
            null,
            null,
            partes[8],
            media,
            minimo,
            maximo,
            contagem,
            partes[7],
            partes[8]);
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

        pacote = new ProtocoloMensagem(tipoMensagem, partes[1], null, null, partes[2], partes[3], null, null, null, null, null, null);
        return true;
    }

    erro = "Tipo de mensagem desconhecido";
    return false;
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
/// Valida se o valor e numerico e se cai dentro dos limites esperados para o tipo.
/// Limites: TEMP [-50,80], RUIDO [0,200], HUM [0,100], PM2.5/PM10/AR [0,1000].
/// Tipos desconhecidos sao aceites sem restricao de gama.
/// </summary>
/// <param name="tipo">Tipo de medicao (e.g. "TEMP", "RUIDO").</param>
/// <param name="valor">Valor textual a validar.</param>
/// <param name="erro">Descricao do erro se a validacao falhar.</param>
/// <returns><c>true</c> se o valor e numerico e esta dentro dos limites.</returns>
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
/// Codifica e envia uma resposta textual ASCII no stream TCP de volta ao Gateway.
/// </summary>
/// <param name="stream">Stream da ligacao TCP ativa.</param>
/// <param name="resposta">Texto da resposta (e.g. "ACK|SERVER|DATA").</param>
void EnviarResposta(NetworkStream stream, string resposta)
{
    byte[] dados = Encoding.ASCII.GetBytes(resposta);
    stream.Write(dados, 0, dados.Length);
}

/// <summary>
/// Estrutura imutavel que representa uma mensagem ja parseada e validada.
/// Contem campos para todos os tipos de mensagem (START, HB, DATA, AGGDATA, END);
/// campos nao aplicaveis ficam a null.
/// </summary>
readonly record struct ProtocoloMensagem(
    string TipoMensagem,
    string SensorId,
    string? TipoDado,
    string? Valor,
    string? Motivo,
    string? Timestamp,
    double? Media,
    double? Minimo,
    double? Maximo,
    int? Contagem,
    string? JanelaInicio,
    string? JanelaFim);

/// <summary>
/// Estado de sessao mantido em memoria para cada sensor ligado ao servidor.
/// Controla a fase atual (Idle/Active/Closed) e os carimbos de atividade.
/// </summary>
sealed class SessionState
{
    public SessionPhase Fase { get; set; } = SessionPhase.Idle;
    public DateTime LastActivityUtc { get; set; } = DateTime.UtcNow;
    public DateTime LastHeartbeatUtc { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Fases possiveis de uma sessao: Idle (ainda nao iniciada),
/// Active (sessao aberta) e Closed (sessao encerrada).
/// </summary>
enum SessionPhase
{
    Idle,
    Active,
    Closed
}
