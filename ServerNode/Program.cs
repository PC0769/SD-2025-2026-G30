using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("--- SERVIDOR CENTRAL ---");

TcpListener serverListener = new TcpListener(IPAddress.Any, 6000);
serverListener.Start();
Console.WriteLine("Aguardando dados na porta 6000...");

try
{
    while (true)
    {
        using TcpClient client = serverListener.AcceptTcpClient();
        NetworkStream stream = client.GetStream();

        byte[] buffer = new byte[1024];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);

        string data = Encoding.ASCII.GetString(buffer, 0, bytesRead);
        Console.WriteLine($"[ARMAZENAMENTO]: {data}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Erro: {ex.Message}");
}
