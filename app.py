app.post("/upload", upload.fields([
  { name: "file1" },
  { name: "file2" }
]), async (req, res) => {

  const dados = processarArquivos(
    req.files.file1[0].path,
    req.files.file2[0].path
  );

  // limpa tabela antes
  await db.query("DELETE FROM servicos");

  for (const item of dados) {
    await db.query(`
      INSERT INTO servicos (
        os, status, data_criacao, base,
        qtd_clientes, alimentador,
        lat, lon, trecho, tag, criticidade, tipo
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    `, [
      item.os,
      item.status,
      item.data_criacao,
      item.base,
      item.qtd_clientes,
      item.alimentador,
      item.lat,
      item.lon,
      item.trecho,
      item.tag,
      item.criticidade,
      item.tipo
    ]);
  }

  res.json({ ok: true, total: dados.length });
});
