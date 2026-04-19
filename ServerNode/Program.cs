using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("--- SERVIDOR CENTRAL ---");

const int ServerPort = 6000;
string[] timestampFormats = { "yyyy-MM-ddTHH:mm:ss", "o" };

object serverFileLock = new object();
object sessionLock = new object();
var estadosSessao = new Dictionary<string, SessionState>(StringComparer.OrdinalIgnoreCase);

TcpListener serverListener = new TcpListener(IPAddress.Any, ServerPort);
serverListener.Start();
Console.WriteLine("Aguardando dados na porta 6000...");

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

bool ExigeSessaoAtiva(string tipoMensagem)
{
    return tipoMensagem == "START"
        || tipoMensagem == "HB"
        || tipoMensagem == "DATA"
        || tipoMensagem == "END";
}

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

void AppendServidorFicheiro(string nomeFicheiro, string linha)
{
    lock (serverFileLock)
    {
        File.AppendAllText(nomeFicheiro, linha + Environment.NewLine);
    }
}

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

bool IsSensorIdValido(string sensorId)
{
    return !string.IsNullOrWhiteSpace(sensorId);
}

bool IsTimestampValido(string timestamp)
{
    return DateTime.TryParseExact(
        timestamp,
        timestampFormats,
        CultureInfo.InvariantCulture,
        DateTimeStyles.None,
        out _);
}

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

void EnviarResposta(NetworkStream stream, string resposta)
{
    byte[] dados = Encoding.ASCII.GetBytes(resposta);
    stream.Write(dados, 0, dados.Length);
}

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

sealed class SessionState
{
    public SessionPhase Fase { get; set; } = SessionPhase.Idle;
    public DateTime LastActivityUtc { get; set; } = DateTime.UtcNow;
    public DateTime LastHeartbeatUtc { get; set; } = DateTime.UtcNow;
}

enum SessionPhase
{
    Idle,
    Active,
    Closed
}
