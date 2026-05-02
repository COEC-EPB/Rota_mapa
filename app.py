from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests

app = Flask(__name__)
CORS(app)

# 🔧 DIVIDIR EM PARTES
def dividir(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]

# 🧠 PROCESSAMENTO
def processar_arquivos(file1, file2):
    df1 = pd.read_excel(file1)
    df2 = pd.read_excel(file2)

    df = pd.concat([df1, df2], ignore_index=True)

    df = df[df["Status"].isin(["CRIADA", "A REPROGRAMAR"])]

    df = df[[
        "OS Manutencao",
        "Status",
        "Data Criacao",
        "Base Operacao",
        "Qtd Clientes Cadastrados",
        "Nome do Alimentador",
        "Latitude",
        "Longitude",
        "Trecho",
        "Tag Ativo",
        "Criticidade",
        "Tipo Manutencao"
    ]]

    df.columns = [
        "os", "status", "data_criacao", "base",
        "qtd_clientes", "alimentador",
        "lat", "lon", "trecho", "tag",
        "criticidade", "tipo"
    ]

    df["lat"] = df["lat"].astype(str).str.replace(",", ".").astype(float)
    df["lon"] = df["lon"].astype(str).str.replace(",", ".").astype(float)

    return df.to_dict(orient="records")

# 🚀 UPLOAD
@app.route("/upload", methods=["POST"])
def upload():
    try:
        print("Recebendo arquivos...")

        file1 = request.files.get("file1")
        file2 = request.files.get("file2")

        if not file1 or not file2:
            return jsonify({"erro": "Envie os dois arquivos"}), 400

        print("Processando...")

        dados = processar_arquivos(file1, file2)

        print(f"Total de registros: {len(dados)}")

        url_worker = "https://despacholinhaviva.pedro-fillype.workers.dev/salvar"

        # 🔥 ENVIO EM LOTES
        for i, chunk in enumerate(dividir(dados, 500)):
            print(f"Enviando lote {i+1} com {len(chunk)} registros...")

            response = requests.post(url_worker, json=chunk)

            if response.status_code != 200:
                print("Erro no Worker:", response.text)
                return jsonify({
                    "erro": "Falha ao enviar lote",
                    "detalhe": response.text
                }), 500

        print("Finalizado com sucesso")

        return jsonify({
            "ok": True,
            "total": len(dados)
        })

    except Exception as e:
        print("ERRO:", str(e))
        return jsonify({
            "erro": "Falha geral",
            "detalhe": str(e)
        }), 500


# 🚀 HEALTH CHECK
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
