using System.Net;
using System.Net.Sockets;
using System.Text;
using System.IO;

Console.WriteLine("--- GATEWAY ONE HEALTH ---");

TcpListener listener = new TcpListener(IPAddress.Any, 5000);
listener.Start();
Console.WriteLine("A escuta no Porto 5000...");

while (true)
{
    try
    {
        using TcpClient sensorClient = listener.AcceptTcpClient();
        NetworkStream stream = sensorClient.GetStream();
        byte[] buffer = new byte[1024];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);

        if (bytesRead > 0)
        {
            string mensagem = Encoding.ASCII.GetString(buffer, 0, bytesRead);
            string[] partes = mensagem.Split('|');

            if (partes[0] == "DATA")
            {
                string id = partes[1];
                string tipo = partes[2];

                // VALIDACAO E ATUALIZACAO DO CSV
                if (ValidarEAtualizarCSV(id, tipo))
                {
                    Console.WriteLine("[VALIDADO]: Sensor " + id + " ok.");

                    // ENCAMINHAR PARA O SERVIDOR (Porto 6000)
                    EncaminharParaServidor(mensagem);
                    Console.WriteLine("Encaminhado para o Servidor.");
                }
                else
                {
                    Console.WriteLine("[RECUSADO]: Sensor " + id + " ou tipo " + tipo + " invalido.");
                }
            }
        }
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}

static bool ValidarEAtualizarCSV(string id, string tipo)
{
    string path = "config.csv";
    if (!File.Exists(path)) return false;
    var linhas = File.ReadAllLines(path).ToList();
    bool autorizado = false;
    for (int i = 0; i < linhas.Count; i++)
    {
        var campos = linhas[i].Split(':');
        if (campos[0] == id && campos[1] == "ativo" && campos[3].Contains(tipo))
        {
            autorizado = true;
            campos[4] = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            linhas[i] = string.Join(":", campos);
            break;
        }
    }
    if (autorizado) File.WriteAllLines(path, linhas);
    return autorizado;
}

static void EncaminharParaServidor(string msg)
{
    try
    {
        using TcpClient serverClient = new TcpClient("127.0.0.1", 6000);
        NetworkStream stream = serverClient.GetStream();
        byte[] dados = Encoding.ASCII.GetBytes(msg);
        stream.Write(dados, 0, dados.Length);
    }
    catch { Console.WriteLine("[ERRO]: Servidor offline!"); }
}