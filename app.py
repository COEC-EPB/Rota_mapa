from flask import Flask, request, jsonify
import pandas as pd
import psycopg2
import os
import requests

app = Flask(__name__)

# 🔌 CONEXÃO DATABASE (Railway)
conn = psycopg2.connect(
    os.environ.get("DATABASE_URL")
)
cursor = conn.cursor()


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
    file1 = request.files.get("file1")
    file2 = request.files.get("file2")

    dados = processar_arquivos(file1, file2)

    # envia pro Cloudflare Worker
    requests.post(
        "https://despacholinhaviva.pedro-fillype.workers.dev/salvar",
        json=dados
    )

    return jsonify({
        "ok": True,
        "total": len(dados)
    })

# 📊 DADOS PARA O MAPA
@app.route("/dados", methods=["GET"])
def dados():
    cursor.execute("SELECT * FROM servicos")
    rows = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    result = []
    for row in rows:
        result.append(dict(zip(colnames, row)))

    return jsonify(result)


# 🚀 START
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
