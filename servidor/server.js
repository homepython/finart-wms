const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '15mb' }));

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  const p1 = path.join(__dirname, 'public', 'index.html');
  const p2 = path.join(__dirname, 'index.html');
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  res.send('Servidor FINART Online');
});

app.get('/coletor.html', (req, res) => {
  const p1 = path.join(__dirname, 'public', 'coletor.html');
  const p2 = path.join(__dirname, 'coletor.html');
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  res.status(404).send('Arquivo coletor.html não encontrado');
});

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

const db = new sqlite3.Database('./finart.db', (err) => {
  if (err) console.error('Erro ao abrir banco:', err.message);
  else console.log('✅ Banco FINART conectado.');
});

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS catalogo_itens (
      item TEXT PRIMARY KEY,
      cliente TEXT NOT NULL,
      produto TEXT NOT NULL,
      padrao_volumes_palete INTEGER NOT NULL,
      qtd_padrao_pecas INTEGER DEFAULT 0,
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
    INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  stmt.run('FR-12588', 'DEVINTEX COSMETICOS', 'FRA 300 ML CÔNICO ATIV. R 28 410 TP PUSH PULL BRANCO', 15, 220, 'CAIXA');
  stmt.run('719935', 'FLORA PRODS DE HIG E LIM', 'FRA 300 ML FLORA 24 410 PUSH PULL ROSA', 20, 252, 'CAIXA');
  stmt.run('PT-7908', 'DEVINTEX COSMETICOS', 'PT 1000 GRS REDONDO 3 ENTRADAS BRANCO', 22, 59, 'SACARIA');
  stmt.finalize();
});

function calcularPadraoPalete(cliente = '', item = '', produto = '') {
  const cli = cliente.toUpperCase();
  const it = item.toUpperCase();
  const prod = produto.toUpperCase();

  if (cli.includes('DEVINTEX') && (it === 'PT-7908' || prod.includes('1000 GRS') || prod.includes('REDONDO'))) {
    return 22;
  }
  if (cli.includes('DEVINTEX')) {
    return 15;
  }
  return 20;
}

// 1. INTERPRETADOR TOLERANTE E RÁPIDO (LÊ CÂMERA AO VIVO, QR CODE, BARRAS OU FOTO MACRO)
app.post('/api/interpretar-bipe', (req, res) => {
  const { codigoLido, textoEtiqueta } = req.body;
  const brCode = (codigoLido || '').trim();
  const ocrRaw = (textoEtiqueta || '').trim();
  const tudoJunto = `${brCode}\n${ocrRaw}`.replace(/\r/g, '');

  if (!tudoJunto.trim()) {
    return res.status(400).json({ erro: 'Nenhuma leitura recebida.' });
  }

  const texto = tudoJunto
    .replace(/OTD/gi, 'QTD')
    .replace(/0TD/gi, 'QTD')
    .replace(/1TEM/gi, 'ITEM')
    .replace(/L0TE/gi, 'LOTE')
    .replace(/PED1DO/gi, 'PEDIDO')
    .replace(/CL1ENTE/gi, 'CLIENTE');

  let item = '';
  let pedido = '';
  let lote = '';
  let qtdPorVolume = 0;
  let clienteLido = '';
  let produtoLido = '';
  let volumeLido = '';

  // 1) Verifica se veio o padrão separado por ponto-e-vírgula (Ex: 719935;0060191;252 ou FR-12588;0065801;220)
  const mBarcode = texto.match(/([A-Z0-9\-]{4,10})\s*[;:,]\s*(\d{5,9})\s*[;:,]\s*(\d{1,4})/i);
  if (mBarcode) {
    item = mBarcode[1].trim().toUpperCase();
    if (item === '19935') item = '719935';
    const lb = mBarcode[2].trim();
    pedido = lb.length > 1 ? lb.slice(0, -1) : lb;
    lote = lb.length > 1 ? `${lb.slice(0, -1)}.${lb.slice(-1)}` : lb;
    qtdPorVolume = parseInt(mBarcode[3].trim(), 10) || 0;
  }

  // 2) Busca pelo Item no texto ou no QR Code mesmo que esteja meio borrado
  if (!item) {
    if (/719935|19935|22908/i.test(texto)) item = '719935';
    else if (/FR\s*-?\s*12588|12588/i.test(texto)) item = 'FR-12588';
    else if (/PT\s*-?\s*7908|7908/i.test(texto)) item = 'PT-7908';
    else {
      const mItem = texto.match(/\b(FR\s*-\s*\d{3,6}|PT\s*-\s*\d{3,6})\b/i) || texto.match(/ITEM[:.\s]*([A-Z0-9\-]{4,12})/i);
      if (mItem) item = mItem[1].replace(/\s+/g, '').toUpperCase();
    }
  }

  // 3) Busca Cliente no texto
  if (/FLORA\s*PRODS|FLORA\s*24|P[OÓ]S\s*DANOS/i.test(texto)) {
    clienteLido = 'FLORA PRODS DE HIG E LIM';
    if (!item) item = '719935';
  } else if (/DEVINTEX/i.test(texto)) {
    clienteLido = 'DEVINTEX COSMETICOS';
    if (!item && /1000\s*GRS|REDONDO/i.test(texto)) item = 'PT-7908';
    else if (!item && /300\s*ML|C[OÔ]NICO/i.test(texto)) item = 'FR-12588';
  } else {
    const mCli = texto.match(/CLIENTE[:.\s]*([A-Z0-9\s.\-&]{3,35})/i);
    if (mCli) clienteLido = mCli[1].split('\n')[0].trim().toUpperCase();
  }

  // 4) Busca Lote e Pedido (Ex: 006019.1, 006580.1, 006643.1)
  if (!lote) {
    const mLote = texto.match(/LOTE[:.\s]*(\d{5,8}\.\d+)/i) || texto.match(/\b(00\d{4}\.\d)\b/);
    if (mLote) lote = mLote[1].trim();
  }
  if (!pedido) {
    const mPed = texto.match(/PEDIDO[:.\s]*(\d{5,8})/i) || texto.match(/\b(006\d{3})\b/);
    if (mPed) pedido = mPed[1].trim();
  }

  // Se achou um pedido conhecido nas fotos, garante a associação correta
  if (pedido === '006019' && !item) item = '719935';
  if (pedido === '006580' && !item) item = 'FR-12588';
  if (pedido === '006643' && !item) item = 'PT-7908';

  if (!lote && pedido) lote = `${pedido}.1`;
  if (!pedido && lote && lote.includes('.')) pedido = lote.split('.')[0];

  // 5) Busca QTD por caixa e Volume da Etiqueta
  if (!qtdPorVolume) {
    const mQtd = texto.match(/QTD[:.\s]*(\d{2,4})\b/i);
    if (mQtd) qtdPorVolume = parseInt(mQtd[1], 10);
  }
  const mVol = texto.match(/\b(\d{3}\s*\/\s*\d{3})\b/);
  if (mVol) volumeLido = mVol[1].replace(/\s+/g, '');

  const mProd = texto.match(/PRODUTO[:.\s]*([^\n]+)/i);
  if (mProd) produtoLido = mProd[1].trim().toUpperCase();

  // 6) Cruza com o Catálogo para preencher tudo que faltar automaticamente
  db.get(`SELECT * FROM catalogo_itens WHERE item = ?`, [item], (err, row) => {
    if (err) return res.status(500).json({ erro: err.message });

    const clienteFinal = clienteLido || (row ? row.cliente : '');
    const produtoFinal = produtoLido || (row ? row.produto : '');
    const qtdPecasFinal = qtdPorVolume || (row ? row.qtd_padrao_pecas : 0);

    // Só recusa se não encontrou absolutamente nada (nem código, nem cliente, nem item, nem lote)
    if (!item && !clienteFinal && !pedido && !lote && !brCode) {
      return res.status(422).json({
        erro: 'Não foi possível ler os dados. Aproxime um pouco mais a câmera ou use o leitor ao vivo.'
      });
    }

    const padraoVolumes = calcularPadraoPalete(clienteFinal, item, produtoFinal);
    const tipoEmbalagem = padraoVolumes === 22 ? 'SACARIA' : 'CAIXA';

    res.json({
      item: item || (brCode ? brCode.slice(0, 12) : ''),
      pedido: pedido || '',
      lote: lote || '',
      cliente: clienteFinal || '',
      produto: produtoFinal || '',
      tipoEmbalagem,
      volume_etiqueta: volumeLido,
      qtd_pecas_por_volume: qtdPecasFinal,
      qtd_volumes_sugerida: padraoVolumes,
      saldo_previsto: qtdPecasFinal * padraoVolumes
    });
  });
});

// 2. SALVAR PALETE
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

  if (!lote || !item || !cliente || pecasUn <= 0 || volumes <= 0) {
    return res.status(400).json({ erro: 'Preencha Cliente, Lote, Item e Quantidades antes de confirmar.' });
  }

  const saldoTotalPecas = pecasUn * volumes;

  if (volume_etiqueta && volume_etiqueta.trim() !== '') {
    db.get(
      `SELECT id FROM paletes_bipados WHERE lote = ? AND item = ? AND volume_etiqueta = ?`,
      [lote, item, volume_etiqueta.trim()],
      (err, duplicado) => {
        if (duplicado) {
          return res.status(409).json({
            erro: `Atenção: O palete Volume ${volume_etiqueta} do Lote ${lote} já foi bipado!`
          });
        }
        inserirPalete();
      }
    );
  } else {
    inserirPalete();
  }

  function inserirPalete() {
    const padrao = calcularPadraoPalete(cliente, item, produto);
    db.run(
      `INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas) VALUES (?, ?, ?, ?, ?)`,
      [item, cliente, produto, padrao, pecasUn]
    );

    db.run(
      `INSERT INTO paletes_bipados 
      (pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [pedido || lote.split('.')[0], lote, item, cliente, produto, pecasUn, volumes, saldoTotalPecas, volume_etiqueta || ''],
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

// 3. ROTA DE AUTO-RESTAURAÇÃO DE BACKUP (GARANTE QUE OS SALDOS NUNCA SUMAM NO RENDER)
app.post('/api/restaurar-backup', (req, res) => {
  const { historico } = req.body;
  if (!Array.isArray(historico) || historico.length === 0) {
    return res.json({ restaurado: false });
  }

  db.get(`SELECT COUNT(*) AS total FROM paletes_bipados`, [], (err, row) => {
    if (err || (row && row.total > 0)) {
      return res.json({ restaurado: false });
    }

    db.serialize(() => {
      const stmt = db.prepare(`
        INSERT OR IGNORE INTO paletes_bipados
        (id, pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta, data_hora)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      historico.forEach((p) => {
        stmt.run(
          p.id, p.pedido, p.lote, p.item, p.cliente, p.produto,
          p.qtd_pecas_por_volume, p.qtd_volumes_palete, p.saldo_total_pecas,
          p.volume_etiqueta || '', p.data_hora
        );
      });
      stmt.finalize(() => {
        notificarTelasInstantaneamente();
        res.json({ restaurado: true, quantidade: historico.length });
      });
    });
  });
});

// 4. DASHBOARD
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

  const queryHistorico = `SELECT * FROM paletes_bipados ORDER BY id DESC LIMIT 300`;

  db.all(queryConsolidado, [], (err, consolidado) => {
    if (err) return res.status(500).json({ erro: err.message });
    db.all(queryHistorico, [], (err2, historico) => {
      if (err2) return res.status(500).json({ erro: err2.message });
      res.json({ consolidado, historico });
    });
  });
});

// 5. EDITAR FALHA OPERACIONAL
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

// 6. EXCLUIR PALETE
app.delete('/api/paletes/:id', (req, res) => {
  db.run(`DELETE FROM paletes_bipados WHERE id = ?`, [req.params.id], function (err) {
    if (err) return res.status(500).json({ erro: err.message });
    notificarTelasInstantaneamente();
    res.json({ sucesso: true });
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Servidor FINART WMS rodando na porta ${PORT}`);
});