using System.Net.Sockets;
using System.Text;

Console.WriteLine("--- SENSOR ONE HEALTH ---");

try
{
    Console.Write("ID do Sensor: ");
    string id = Console.ReadLine() ?? "S101";

    while (true)
    {
        Console.WriteLine("\n1. Enviar Temperatura\n2. Enviar Ruído\n3. Enviar Heartbeat\n4. Sair");
        string opcao = Console.ReadLine() ?? "";

        if (opcao == "4") break;

        string msg = "";
        if (opcao == "3")
        {
            msg = $"HB|{id}"; // Mensagem de Heartbeat
        }
        else
        {
            Console.Write("Valor: ");
            string valor = Console.ReadLine() ?? "0";
            string tipo = (opcao == "1") ? "TEMP" : "RUIDO";
            msg = $"DATA|{id}|{tipo}|{valor}|{DateTime.Now:s}";
        }

        using TcpClient client = new TcpClient("127.0.0.1", 5000);
        NetworkStream stream = client.GetStream();
        byte[] data = Encoding.ASCII.GetBytes(msg);
        stream.Write(data, 0, data.Length);
        Console.WriteLine(">>> Enviado!");
    }
}
catch (Exception ex) { Console.WriteLine($"Erro: {ex.Message}"); }