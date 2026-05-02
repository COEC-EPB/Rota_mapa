from flask_cors import CORS
from flask import Flask, request, jsonify
import pandas as pd
import requests

app = Flask(__name__)
CORS(app)


# 🧠 PROCESSAMENTO
def processar_arquivos(file1, file2):
    df1 = pd.read_excel(file1)
    df2 = pd.read_excel(file2)

    # CONCATENAR
    df = pd.concat([df1, df2], ignore_index=True)

    # FILTRAR STATUS
    df = df[df["Status"].isin(["CRIADA", "A REPROGRAMAR"])]

    # SELECIONAR COLUNAS
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

    # RENOMEAR
    df.columns = [
        "os",
        "status",
        "data_criacao",
        "base",
        "qtd_clientes",
        "alimentador",
        "lat",
        "lon",
        "trecho",
        "tag",
        "criticidade",
        "tipo"
    ]

    # LIMPAR COORDENADAS
    df["lat"] = df["lat"].astype(str).str.replace(",", ".").astype(float)
    df["lon"] = df["lon"].astype(str).str.replace(",", ".").astype(float)

    return df.to_dict(orient="records")


# 🚀 UPLOAD
@app.route("/upload", methods=["POST"])
def upload():
    print("Recebendo arquivos...")

    file1 = request.files.get("file1")
    file2 = request.files.get("file2")

    print("Arquivos recebidos")

    dados = processar_arquivos(file1, file2)

    print("Processado:", len(dados))

    response = requests.post(
        "https://despacholinhaviva.pedro-fillype.workers.dev/salvar",
        json=dados
    )

    print("Enviado pro worker:", response.status_code)

    return jsonify({"ok": True})


# 🚀 HEALTH CHECK (IMPORTANTE PRA RAILWAY)
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok"})


# 🚀 START
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
