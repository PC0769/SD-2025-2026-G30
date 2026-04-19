using System.Net;
using System.Net.Sockets;
using System.Text;
using System.IO;

Console.WriteLine("--- SERVIDOR CENTRAL ---");
TcpListener serverListener = new TcpListener(IPAddress.Any, 6000);
serverListener.Start();
Console.WriteLine("Aguardando dados na porta 6000...");

while (true)
{
    try
    {
        using TcpClient client = serverListener.AcceptTcpClient();
        NetworkStream stream = client.GetStream();
        byte[] buffer = new byte[1024];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);

        if (bytesRead > 0)
        {
            string data = Encoding.ASCII.GetString(buffer, 0, bytesRead);
            string[] partes = data.Split('|');

            if (partes.Length >= 5)
            {
                string tipo = partes[2]; // Ex: TEMP

                // Nova estrutura: Timestamp ; ID ; Tipo ; Valor
                string linha = partes[4] + ";" + partes[1] + ";" + partes[2] + ";" + partes[3];

                // Grava no ficheiro TXT correspondente
                File.AppendAllText(tipo + ".txt", linha + Environment.NewLine);
                Console.WriteLine("[ARMAZENAMENTO]: Gravado em " + tipo + ".txt -> " + linha);
            }
        }
    }
    catch (Exception ex) { Console.WriteLine("Erro no Servidor: " + ex.Message); }
}