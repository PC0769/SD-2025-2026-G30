using System.Net.Sockets;
using System.Text;

Console.WriteLine("--- SENSOR ONE HEALTH ---");

try
{
    Console.Write("Introduza o ID do Sensor: ");
    string id = Console.ReadLine() ?? "S101";

    while (true)
    {
        Console.WriteLine("\n1. Enviar Temperatura");
        Console.WriteLine("2. Enviar Ruído");
        Console.WriteLine("3. Sair (QUIT)");
        Console.Write("Escolha: ");
        string opcao = Console.ReadLine();

        if (opcao == "3") break;

        Console.Write("Valor da medição: ");
        string valor = Console.ReadLine();
        string tipo = (opcao == "1") ? "TEMP" : "RUIDO";

        // Formato: DATA|ID|TIPO|VALOR|TIMESTAMP
        string msg = $"DATA|{id}|{tipo}|{valor}|{DateTime.Now:s}";

        using TcpClient client = new TcpClient("127.0.0.1", 5000);
        NetworkStream stream = client.GetStream();
        byte[] data = Encoding.ASCII.GetBytes(msg);
        stream.Write(data, 0, data.Length);

        Console.WriteLine(">>> Enviado!");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Erro: {ex.Message}");
}
