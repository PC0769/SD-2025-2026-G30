# =============================================================================
# Ficheiro : PreProcessingService/app.py
# Módulo   : Serviço RPC de Pré-processamento (Python/Flask)
# Porta    : 7000
# Invocado por: GatewayNode (via HTTP POST /preprocess)
# Protocolo TP2 §2.1 — Gateway -> Serviço de Pré-processamento
# =============================================================================

from flask import Flask, request, jsonify
import math

app = Flask(__name__)

# Gamas válidas por tipo de dado (min, max)
# Espelha as mesmas gamas definidas no GatewayNode (PROTOCOLO.md §5)
GAMAS_POR_TIPO = {
    "TEMP":  (-50.0,  80.0),
    "HUM":   (  0.0, 100.0),
    "RUIDO": (  0.0, 200.0),
    "PM2.5": (  0.0, 1000.0),
    "PM10":  (  0.0, 1000.0),
    "AR":    (  0.0, 1000.0),
}


def normalizar_valor(tipo: str, valor_raw: str) -> dict:
    """
    Normaliza um valor bruto recebido de um sensor.

    Passos:
      1. Substitui vírgula por ponto (separador decimal invariante)
      2. Converte para float
      3. Valida gama por tipo de dado
      4. Arredonda a 2 casas decimais

    Parâmetros:
        tipo      -- Tipo de dado (TEMP, HUM, RUIDO, PM2.5, PM10, AR)
        valor_raw -- Valor em string, pode ter ',' como separador decimal

    Retorna:
        dict com: valor (float|None), normalizado (bool), erro (str|None)
    """
    valor_str = str(valor_raw).strip().replace(",", ".")

    try:
        valor_float = float(valor_str)
    except ValueError:
        return {"valor": None, "normalizado": False, "erro": f"Valor '{valor_raw}' não é numérico"}

    tipo_upper = tipo.strip().upper()

    if tipo_upper in GAMAS_POR_TIPO:
        minimo, maximo = GAMAS_POR_TIPO[tipo_upper]
        if not (minimo <= valor_float <= maximo):
            return {
                "valor": None,
                "normalizado": False,
                "erro": f"Valor {valor_float} fora de gama para {tipo_upper} [{minimo}, {maximo}]"
            }

    valor_normalizado = round(valor_float, 2)
    return {"valor": valor_normalizado, "normalizado": True, "erro": None}


@app.route("/health", methods=["GET"])
def health():
    """
    Health check do serviço.
    Permite ao GatewayNode verificar se o serviço está disponível antes de chamar /preprocess.
    Retorna HTTP 200 com status 'ok'.
    """
    return jsonify({"status": "ok", "servico": "PreProcessingService", "porta": 7000})


@app.route("/preprocess", methods=["POST"])
def preprocess():
    """
    Endpoint RPC principal — pré-processamento de um registo DATA de sensor.

    Protocolo TP2 §2.1: O Gateway utiliza RPC para invocar este serviço externo
    que assegura a uniformização dos dados antes da agregação.

    Corpo do pedido (JSON):
        {
            "sensor_id": "S101",
            "tipo": "TEMP",
            "valor": "22,5",
            "timestamp": "2026-05-20T14:00:00"
        }

    Resposta de sucesso (HTTP 200):
        {
            "sensor_id": "S101",
            "tipo": "TEMP",
            "valor": 22.5,
            "timestamp": "2026-05-20T14:00:00",
            "normalizado": true,
            "erro": null
        }

    Resposta de erro de validação (HTTP 422):
        {
            "sensor_id": "S101",
            "tipo": "TEMP",
            "valor": null,
            "timestamp": "...",
            "normalizado": false,
            "erro": "Valor 99.0 fora de gama para TEMP [-50, 80]"
        }
    """
    dados = request.get_json(silent=True)

    if not dados:
        return jsonify({"erro": "Corpo do pedido ausente ou não é JSON válido"}), 400

    sensor_id = dados.get("sensor_id", "")
    tipo      = dados.get("tipo", "")
    valor_raw = dados.get("valor", "")
    timestamp = dados.get("timestamp", "")

    if not sensor_id or not tipo or valor_raw == "":
        return jsonify({"erro": "Campos obrigatórios em falta: sensor_id, tipo, valor"}), 400

    resultado = normalizar_valor(tipo, valor_raw)

    resposta = {
        "sensor_id": sensor_id,
        "tipo":      tipo.strip().upper(),
        "valor":     resultado["valor"],
        "timestamp": timestamp,
        "normalizado": resultado["normalizado"],
        "erro":      resultado["erro"],
    }

    status_http = 200 if resultado["normalizado"] else 422
    return jsonify(resposta), status_http


if __name__ == "__main__":
    print("=== PreProcessingService iniciado na porta 7000 ===")
    print("Endpoints disponíveis:")
    print("  GET  http://localhost:7000/health")
    print("  POST http://localhost:7000/preprocess")
    app.run(host="0.0.0.0", port=7000, debug=False)
