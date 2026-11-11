const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const RENDER_URL = 'https://finart-wms.onrender.com';
const GITHUB_REPO = 'https://github.com/homepython/finart-wms.git';

app.use(cors());
app.use(express.json());

// Serve arquivos tanto da pasta /public quanto da raiz (à prova de falhas no upload do GitHub)
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  const caminhoPublic = path.join(__dirname, 'public', 'index.html');
  const caminhoRaiz = path.join(__dirname, 'index.html');
  if (fs.existsSync(caminhoPublic)) return res.sendFile(caminhoPublic);
  if (fs.existsSync(caminhoRaiz)) return res.sendFile(caminhoRaiz);
  res.send('Servidor FINART Online');
});

app.get('/coletor.html', (req, res) => {
  const caminhoPublic = path.join(__dirname, 'public', 'coletor.html');
  const caminhoRaiz = path.join(__dirname, 'coletor.html');
  if (fs.existsSync(caminhoPublic)) return res.sendFile(caminhoPublic);
  if (fs.existsSync(caminhoRaiz)) return res.sendFile(caminhoRaiz);
  res.status(404).send('Arquivo coletor.html não encontrado');
});

// Lista de conexões para atualização instantânea em tempo real (SSE)
let clientesTempoReal = [];

function notificarTelasInstantaneamente() {
  clientesTempoReal.forEach((cliente) => {
    cliente.res.write(`data: ${JSON.stringify({ atualizar: true, timestamp: Date.now() })}\n\n`);
  });
}

app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const novoCliente = { id: Date.now(), res };
  clientesTempoReal.push(novoCliente);

  req.on('close', () => {
    clientesTempoReal = clientesTempoReal.filter((c) => c.id !== novoCliente.id);
  });
});

// Banco de dados SQLite FINART
const db = new sqlite3.Database('./finart.db', (err) => {
  if (err) console.error('Erro ao abrir banco:', err.message);
  else console.log(`✅ Banco FINART ativo | Render: ${RENDER_URL} | Repo: ${GITHUB_REPO}`);
});

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS catalogo_itens (
      item TEXT PRIMARY KEY,
      cliente TEXT NOT NULL,
      produto TEXT NOT NULL,
      padrao_volumes_palete INTEGER NOT NULL,
      tipo_embalagem TEXT DEFAULT 'CAIXA'
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS paletes_bipados (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pedido TEXT NOT NULL,
      lote TEXT NOT NULL,
      item TEXT NOT NULL,
      cliente TEXT NOT NULL,
      produto TEXT NOT NULL,
      qtd_pecas_por_volume INTEGER NOT NULL,
      qtd_volumes_palete INTEGER NOT NULL,
      saldo_total_pecas INTEGER NOT NULL,
      volume_etiqueta TEXT DEFAULT '',
      data_hora DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  const stmt = db.prepare(`
    INSERT OR IGNORE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, tipo_embalagem)
    VALUES (?, ?, ?, ?, ?)
  `);
  stmt.run('FR-12588', 'DEVINTEX COSMETICOS', 'FRA 300 ML CÔNICO ATIV. R 28 410 TP PUSH PULL BRANCO', 15, 'CAIXA');
  stmt.run('719935', 'FLORA PRODS DE HIG E LIM', 'FRA 300 ML FLORA 24 410 PUSH PULL ROSA', 20, 'CAIXA');
  stmt.run('PT-7908', 'DEVINTEX COSMETICOS', 'PT 1000 GRS REDONDO 3 ENTRADAS BRANCO', 22, 'SACARIA');
  stmt.finalize();
});

function calcularPadraoPalete(cliente = '', item = '') {
  const cliUpper = cliente.toUpperCase();
  const itemUpper = item.toUpperCase();
  if (cliUpper.includes('DEVINTEX') && itemUpper === 'PT-7908') return 22;
  if (cliUpper.includes('DEVINTEX')) return 15;
  return 20;
}

// 1. INTERPRETAR BIPE DA ETIQUETA
app.post('/api/interpretar-bipe', (req, res) => {
  const { codigoLido } = req.body;
  if (!codigoLido) return res.status(400).json({ erro: 'Código vazio' });

  const partes = codigoLido.trim().split(';');
  if (partes.length < 3) {
    return res.status(400).json({
      erro: 'Formato inválido. Bipe o código de barras inferior (ITEM;LOTE;QTD).'
    });
  }

  const item = partes[0].trim();
  const loteBruto = partes[1].trim();
  const qtdPorVolume = parseInt(partes[2].trim(), 10) || 0;

  const pedido = loteBruto.length > 1 ? loteBruto.slice(0, -1) : loteBruto;
  const loteFormatado = loteBruto.length > 1
    ? `${loteBruto.slice(0, -1)}.${loteBruto.slice(-1)}`
    : loteBruto;

  db.get(`SELECT * FROM catalogo_itens WHERE item = ?`, [item], (err, row) => {
    if (err) return res.status(500).json({ erro: err.message });

    const cliente = row ? row.cliente : 'OUTROS CLIENTES';
    const produto = row ? row.produto : `PRODUTO ITEM ${item}`;
    const tipoEmbalagem = row ? row.tipo_embalagem : 'CAIXA';
    const padraoVolumes = row ? row.padrao_volumes_palete : calcularPadraoPalete(cliente, item);

    res.json({
      item,
      pedido,
      lote: loteFormatado,
      cliente,
      produto,
      tipoEmbalagem,
      qtd_pecas_por_volume: qtdPorVolume,
      qtd_volumes_sugerida: padraoVolumes,
      saldo_previsto: qtdPorVolume * padraoVolumes
    });
  });
});

// 2. SALVAR PALETE E ATUALIZAR INSTANTANEAMENTE
app.post('/api/paletes', (req, res) => {
  const {
    pedido,
    lote,
    item,
    cliente,
    produto,
    qtd_pecas_por_volume,
    qtd_volumes_palete,
    volume_etiqueta
  } = req.body;

  const pecasUn = Number(qtd_pecas_por_volume);
  const volumes = Number(qtd_volumes_palete);

  if (!lote || !item || pecasUn <= 0 || volumes <= 0) {
    return res.status(400).json({ erro: 'Dados incompletos ou valores zerados.' });
  }

  const saldoTotalPecas = pecasUn * volumes;

  if (volume_etiqueta && volume_etiqueta.trim() !== '') {
    db.get(
      `SELECT id FROM paletes_bipados WHERE lote = ? AND item = ? AND volume_etiqueta = ?`,
      [lote, item, volume_etiqueta.trim()],
      (err, duplicado) => {
        if (duplicado) {
          return res.status(409).json({
            erro: `Atenção: O volume ${volume_etiqueta} do Lote ${lote} já foi bipado!`
          });
        }
        inserirPalete();
      }
    );
  } else {
    inserirPalete();
  }

  function inserirPalete() {
    const padrao = calcularPadraoPalete(cliente, item);
    db.run(
      `INSERT OR IGNORE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete) VALUES (?, ?, ?, ?)`,
      [item, cliente, produto, padrao]
    );

    db.run(
      `INSERT INTO paletes_bipados 
      (pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [pedido, lote, item, cliente, produto, pecasUn, volumes, saldoTotalPecas, volume_etiqueta || ''],
      function (err) {
        if (err) return res.status(500).json({ erro: err.message });
        notificarTelasInstantaneamente();
        res.json({
          sucesso: true,
          id: this.lastID,
          saldo_total_pecas: saldoTotalPecas
        });
      }
    );
  }
});

// 3. DASHBOARD
app.get('/api/dashboard', (req, res) => {
  const queryConsolidado = `
    SELECT 
      lote, pedido, item, cliente, produto, qtd_pecas_por_volume,
      COUNT(id) AS total_paletes,
      SUM(qtd_volumes_palete) AS total_caixas_ou_sacaria,
      SUM(saldo_total_pecas) AS saldo_total_pecas
    FROM paletes_bipados
    GROUP BY lote, item, cliente, qtd_pecas_por_volume
    ORDER BY MAX(id) DESC
  `;

  const queryHistorico = `SELECT * FROM paletes_bipados ORDER BY id DESC LIMIT 200`;

  db.all(queryConsolidado, [], (err, consolidado) => {
    if (err) return res.status(500).json({ erro: err.message });
    db.all(queryHistorico, [], (err2, historico) => {
      if (err2) return res.status(500).json({ erro: err2.message });
      res.json({ consolidado, historico });
    });
  });
});

// 4. EDITAR FALHA OPERACIONAL
app.put('/api/paletes/:id', (req, res) => {
  const { id } = req.params;
  const { lote, pedido, cliente, item, produto, qtd_pecas_por_volume, qtd_volumes_palete, volume_etiqueta } = req.body;

  const pecasUn = Number(qtd_pecas_por_volume);
  const volumes = Number(qtd_volumes_palete);
  if (pecasUn <= 0 || volumes <= 0) return res.status(400).json({ erro: 'Valores inválidos.' });

  const novoSaldoTotal = pecasUn * volumes;

  db.run(
    `UPDATE paletes_bipados 
     SET lote = ?, pedido = ?, cliente = ?, item = ?, produto = ?, 
         qtd_pecas_por_volume = ?, qtd_volumes_palete = ?, saldo_total_pecas = ?, volume_etiqueta = ?
     WHERE id = ?`,
    [lote, pedido, cliente, item, produto, pecasUn, volumes, novoSaldoTotal, volume_etiqueta || '', id],
    function (err) {
      if (err) return res.status(500).json({ erro: err.message });
      notificarTelasInstantaneamente();
      res.json({ sucesso: true, saldo_recalculado: novoSaldoTotal });
    }
  );
});

// 5. EXCLUIR PALETE
app.delete('/api/paletes/:id', (req, res) => {
  db.run(`DELETE FROM paletes_bipados WHERE id = ?`, [req.params.id], function (err) {
    if (err) return res.status(500).json({ erro: err.message });
    notificarTelasInstantaneamente();
    res.json({ sucesso: true });
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Servidor FINART WMS rodando na porta ${PORT}`);
  console.log(`🌐 URL Online Render: ${RENDER_URL}`);
});