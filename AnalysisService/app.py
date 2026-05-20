# =============================================================================
# Ficheiro : AnalysisService/app.py
# Módulo   : Serviço RPC de Análise e Previsão (Python/Flask)
# Porta    : 7001
# Invocado por: ServerNode (via HTTP POST /analyze)
# Protocolo TP2 §2.1 — Servidor -> Serviço de Análise e Previsão
# =============================================================================

from flask import Flask, request, jsonify
import math
import statistics

app = Flask(__name__)

# Thresholds de alerta por tipo de dado (valores acima indicam risco)
# Protocolo TP2 §2.1: deteção de padrões de poluição e riscos para a saúde pública
THRESHOLDS_ALERTA = {
    "TEMP":  35.0,    # °C — risco de calor extremo
    "RUIDO": 80.0,    # dB — risco auditivo
    "PM2.5": 25.0,    # µg/m³ — OMS: limite diário
    "PM10":  50.0,    # µg/m³ — OMS: limite diário
    "HUM":   90.0,    # % — humidade excessiva
    "AR":    200.0,   # índice de qualidade do ar
}


def calcular_estatisticas(valores: list[float]) -> dict:
    """
    Calcula estatísticas descritivas para uma lista de valores numéricos.

    Parâmetros:
        valores -- lista de floats (médias de janelas AGGDATA)

    Retorna:
        dict com: media, desvio_padrao, minimo, maximo, contagem
    """
    if not valores:
        return {"media": None, "desvio_padrao": None, "minimo": None, "maximo": None, "contagem": 0}

    return {
        "media":        round(statistics.mean(valores), 4),
        "desvio_padrao": round(statistics.stdev(valores), 4) if len(valores) > 1 else 0.0,
        "minimo":       round(min(valores), 4),
        "maximo":       round(max(valores), 4),
        "contagem":     len(valores),
    }


def detetar_alertas(tipo: str, valores: list[float]) -> list[dict]:
    """
    Deteta valores que excedem os thresholds de alerta definidos para o tipo.

    Protocolo TP2 §2.1: deteção de padrões de poluição e previsão de riscos.

    Parâmetros:
        tipo    -- Tipo de dado (TEMP, RUIDO, PM2.5, ...)
        valores -- lista de médias de janelas AGGDATA

    Retorna:
        lista de dicts {indice, valor, threshold, mensagem} para cada violação
    """
    tipo_upper = tipo.strip().upper()
    threshold = THRESHOLDS_ALERTA.get(tipo_upper)
    alertas = []

    if threshold is None:
        return alertas

    for i, v in enumerate(valores):
        if v > threshold:
            alertas.append({
                "indice":    i,
                "valor":     round(v, 4),
                "threshold": threshold,
                "mensagem":  f"{tipo_upper} = {v:.2f} excede o limite de {threshold}"
            })

    return alertas


def calcular_tendencia(valores: list[float]) -> str:
    """
    Calcula a tendência temporal comparando a média da primeira metade
    com a média da segunda metade da série de valores.

    Retorna: 'subida', 'descida' ou 'estavel'
    """
    if len(valores) < 2:
        return "insuficiente"

    meio = len(valores) // 2
    media_primeira = statistics.mean(valores[:meio]) if meio > 0 else valores[0]
    media_segunda  = statistics.mean(valores[meio:])

    diferenca = media_segunda - media_primeira
    # Considera tendência significativa se a variação for > 5% da média global
    media_global = statistics.mean(valores)
    limiar = abs(media_global) * 0.05 if media_global != 0 else 0.1

    if diferenca > limiar:
        return "subida"
    elif diferenca < -limiar:
        return "descida"
    else:
        return "estavel"


@app.route("/health", methods=["GET"])
def health():
    """
    Health check do serviço.
    Permite ao ServerNode verificar disponibilidade antes de chamar /analyze.
    """
    return jsonify({"status": "ok", "servico": "AnalysisService", "porta": 7001})


@app.route("/analyze", methods=["POST"])
def analyze():
    """
    Endpoint RPC principal — análise estatística de registos AGGDATA.

    Protocolo TP2 §2.1: O Servidor invoca este serviço para realizar análises
    estatísticas, deteção de padrões de poluição e previsão de riscos.

    Corpo do pedido (JSON):
        {
            "sensor_id": "S101",
            "tipo": "TEMP",
            "registos": [
                {"media": 22.5, "minimo": 20.0, "maximo": 25.0, "contagem": 3,
                 "janela_inicio": "...", "janela_fim": "..."},
                ...
            ]
        }

    Resposta (HTTP 200):
        {
            "sensor_id": "S101",
            "tipo": "TEMP",
            "estatisticas": { "media": 22.5, "desvio_padrao": 1.2, ... },
            "alertas": [ { "valor": 36.0, "threshold": 35.0, "mensagem": "..." } ],
            "tendencia": "subida"
        }
    """
    dados = request.get_json(silent=True)

    if not dados:
        return jsonify({"erro": "Corpo do pedido ausente ou não é JSON válido"}), 400

    sensor_id = dados.get("sensor_id", "")
    tipo      = dados.get("tipo", "")
    registos  = dados.get("registos", [])

    if not sensor_id or not tipo:
        return jsonify({"erro": "Campos obrigatórios em falta: sensor_id, tipo"}), 400

    if not isinstance(registos, list) or len(registos) == 0:
        return jsonify({"erro": "Campo 'registos' deve ser uma lista não vazia"}), 400

    # Extrai as médias de cada registo AGGDATA para análise
    medias = []
    for r in registos:
        try:
            medias.append(float(r.get("media", 0)))
        except (TypeError, ValueError):
            pass

    estatisticas = calcular_estatisticas(medias)
    alertas      = detetar_alertas(tipo, medias)
    tendencia    = calcular_tendencia(medias)

    return jsonify({
        "sensor_id":    sensor_id,
        "tipo":         tipo.strip().upper(),
        "estatisticas": estatisticas,
        "alertas":      alertas,
        "tendencia":    tendencia,
        "num_registos": len(registos),
    }), 200


if __name__ == "__main__":
    print("=== AnalysisService iniciado na porta 7001 ===")
    print("Endpoints disponíveis:")
    print("  GET  http://localhost:7001/health")
    print("  POST http://localhost:7001/analyze")
    app.run(host="0.0.0.0", port=7001, debug=False)
