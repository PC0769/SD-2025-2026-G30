using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("--- GATEWAY ONE HEALTH ---");

TcpListener listener = new TcpListener(IPAddress.Any, 5000);
listener.Start();
Console.WriteLine("À escuta na porta 5000...");

while (true)
{
    try
    {
        using TcpClient sensorClient = listener.AcceptTcpClient();
        NetworkStream streamDoSensor = sensorClient.GetStream();

        byte[] buffer = new byte[1024];
        int bytesRead = streamDoSensor.Read(buffer, 0, buffer.Length);

        if (bytesRead > 0)
        {
            string mensagem = Encoding.ASCII.GetString(buffer, 0, bytesRead);
            Console.WriteLine($"[SENSOR -> GW]: {mensagem}");

            // ENCAMINHAMENTO PARA O SERVIDOR
            try
            {
                using TcpClient serverClient = new TcpClient("127.0.0.1", 6000);
                NetworkStream streamParaServidor = serverClient.GetStream();
                byte[] dadosParaEnviar = Encoding.ASCII.GetBytes(mensagem);
                streamParaServidor.Write(dadosParaEnviar, 0, dadosParaEnviar.Length);
                Console.WriteLine("[GW -> SERVER]: Dados encaminhados.");
            }
            catch
            {
                Console.WriteLine("[ERRO]: Servidor não está acessível!");
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Erro: {ex.Message}");
    }
}
