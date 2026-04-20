using System.Net.Sockets;
using System.Text;

// Cliente de consola para simular um sensor e enviar mensagens ao gateway.
Console.WriteLine("--- SENSOR ONE HEALTH ---");

try
{
    // Le ID do sensor a usar na sessao atual.
    Console.Write("ID do Sensor: ");
    string id = Console.ReadLine() ?? "S101";
    bool sessaoAtiva = false;

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

// Envia uma mensagem para o gateway e devolve true apenas para respostas ACK.
static bool EnviarMensagem(string msg)
{
    using TcpClient client = new TcpClient("127.0.0.1", 5000);
    NetworkStream stream = client.GetStream();
    byte[] data = Encoding.ASCII.GetBytes(msg);
    stream.Write(data, 0, data.Length);
    Console.WriteLine($">>> Enviado: {msg}");

    byte[] respostaBuffer = new byte[1024];
    int respostaBytes = stream.Read(respostaBuffer, 0, respostaBuffer.Length);
    string resposta = respostaBytes > 0
        ? Encoding.ASCII.GetString(respostaBuffer, 0, respostaBytes)
        : "NACK|SEM_RESPOSTA";

    Console.WriteLine("<<< Resposta Gateway: " + resposta);
    return resposta.StartsWith("ACK|");
}