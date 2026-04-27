// ============================================================================
// Ficheiro : SensorNode/Program.cs
// Modulo   : Sensor (Cliente TCP)
// Porta    : Liga-se ao Gateway em 127.0.0.1:5000
// Descricao: Aplicacao de consola que simula um sensor ambiental. Apresenta
//            um menu interativo que permite ao utilizador abrir/fechar sessoes,
//            enviar medicoes de temperatura e ruido, e emitir heartbeats.
//            Cada operacao abre uma ligacao TCP curta ao Gateway, envia a
//            mensagem no formato do protocolo e le a resposta (ACK/NACK).
// Protocolo: Mensagens delimitadas por '|'.
//            START|<id>|<timestamp>
//            DATA|<id>|<tipo>|<valor>|<timestamp>
//            HB|<id>|<timestamp>
//            END|<id>|<motivo>|<timestamp>
// ============================================================================

using System.Net.Sockets;
using System.Text;

// --- Ponto de entrada (top-level statements -> Main implicito) ---
Console.WriteLine("--- SENSOR ONE HEALTH ---");

try
{
    // Le ID do sensor a usar na sessao atual.
    Console.Write("ID do Sensor: ");
    string id = Console.ReadLine() ?? "S101";
    bool sessaoAtiva = false;  // Controla se existe sessao aberta com o Gateway.

    // Menu principal de operacoes do sensor.
    while (true)
    {
        Console.WriteLine("\n1. Iniciar Comunicacao\n2. Enviar Temperatura\n3. Enviar Ruido\n4. Enviar Heartbeat\n5. Finalizar Comunicacao\n6. Sair");
        string opcao = Console.ReadLine() ?? "";

        // Saida limpa: tenta fechar a sessao antes de terminar o programa.
        if (opcao == "6")
        {
            if (sessaoAtiva)
            {
                string endAoSair = $"END|{id}|USER_EXIT|{DateTime.Now:s}";
                bool endAceite = EnviarMensagem(endAoSair);
                if (endAceite) sessaoAtiva = false;
            }
            break;
        }

        string msg = "";
        // Inicia sessao com START.
        if (opcao == "1")
        {
            if (sessaoAtiva)
            {
                Console.WriteLine("Sessao ja iniciada.");
                continue;
            }

            msg = $"START|{id}|{DateTime.Now:s}";
            bool startAceite = EnviarMensagem(msg);
            if (startAceite) sessaoAtiva = true;
            continue;
        }

        // Fecha sessao com END por pedido do utilizador.
        if (opcao == "5")
        {
            if (!sessaoAtiva)
            {
                Console.WriteLine("Sessao nao iniciada. Use a opcao 1 primeiro.");
                continue;
            }

            msg = $"END|{id}|USER_REQUEST|{DateTime.Now:s}";
            bool endAceite = EnviarMensagem(msg);
            if (endAceite) sessaoAtiva = false;
            continue;
        }

        // DATA e HB exigem sessao ativa.
        if (!sessaoAtiva)
        {
            Console.WriteLine("Sessao nao iniciada. Use a opcao 1 primeiro.");
            continue;
        }

        // Heartbeat para manter sessao ativa no gateway/servidor.
        if (opcao == "4")
        {
            msg = $"HB|{id}|{DateTime.Now:s}";
            EnviarMensagem(msg);
            continue;
        }

        // Envio de dados de medicao conforme tipo selecionado.
        if (opcao == "2" || opcao == "3")
        {
            Console.Write("Valor: ");
            string valor = Console.ReadLine() ?? "0";
            string tipo = (opcao == "2") ? "TEMP" : "RUIDO";
            msg = $"DATA|{id}|{tipo}|{valor}|{DateTime.Now:s}";
            EnviarMensagem(msg);
            continue;
        }

        Console.WriteLine("Opcao invalida.");
    }
}
catch (Exception ex) { Console.WriteLine($"Erro: {ex.Message}"); }

/// <summary>
/// Envia uma mensagem TCP ao Gateway (127.0.0.1:5000) e devolve se foi aceite.
/// Abre uma ligacao TCP dedicada por mensagem (short-lived connection),
/// transmite os bytes ASCII e aguarda a resposta sincrona do Gateway.
/// Funcao local static: nao captura variaveis do scope exterior (id, sessaoAtiva).
/// </summary>
/// <param name="msg">
/// Mensagem formatada segundo o protocolo, e.g. "START|S101|2026-04-27T22:00:00".
/// </param>
/// <returns>
/// <c>true</c> se a resposta do Gateway comecar por "ACK|";
/// <c>false</c> para qualquer NACK ou ausencia de resposta.
/// </returns>
static bool EnviarMensagem(string msg)
{
    // Abre ligacao TCP curta ao Gateway (porta 5000); using garante Dispose automatico.
    using TcpClient client = new TcpClient("127.0.0.1", 5000);
    NetworkStream stream = client.GetStream();

    // Codifica e envia a mensagem como bytes ASCII.
    byte[] data = Encoding.ASCII.GetBytes(msg);
    stream.Write(data, 0, data.Length);
    Console.WriteLine($">>> Enviado: {msg}");

    // Le a resposta do Gateway (buffer de 1024 bytes e suficiente para ACK/NACK).
    byte[] respostaBuffer = new byte[1024];
    int respostaBytes = stream.Read(respostaBuffer, 0, respostaBuffer.Length);
    string resposta = respostaBytes > 0
        ? Encoding.ASCII.GetString(respostaBuffer, 0, respostaBytes)
        : "NACK|SEM_RESPOSTA";   // Fallback quando nao ha resposta.

    Console.WriteLine("<<< Resposta Gateway: " + resposta);
    // Devolve true apenas se a resposta indicar aceitacao (prefixo "ACK|").
    return resposta.StartsWith("ACK|");
}