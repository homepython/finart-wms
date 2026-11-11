const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const RENDER_URL = 'https://finart-wms.onrender.com';

app.use(cors());
app.use(express.json({ limit: '10mb' }));

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
  else console.log(`✅ Banco FINART ativo | ${RENDER_URL}`);
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

// 1. DIGITALIZADOR E INTERPRETADOR RIGOROSO DA ETIQUETA (ZERO FALSOS POSITIVOS)
app.post('/api/interpretar-bipe', (req, res) => {
  const { codigoLido, textoEtiqueta } = req.body;
  const brCode = (codigoLido || '').trim();
  const ocrRaw = (textoEtiqueta || '').trim();

  if (!brCode && !ocrRaw) {
    return res.status(400).json({ erro: 'Nenhuma imagem ou código recebido para leitura.' });
  }

  // Normaliza erros comuns de leitura óptica (OCR) em etiquetas
  const texto = ocrRaw
    .replace(/\r/g, '')
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

  // A) Extração pelo texto digitalizado da etiqueta (1º Plano)
  if (texto.length > 5) {
    // Cliente: busca na linha CLIENTE: ou reconhece nomes chave impressos na etiqueta
    const mCliente = texto.match(/CLIENTE[:.\s]*([A-Z0-9\s.\-&]{3,40})/i);
    if (mCliente) {
      clienteLido = mCliente[1].split('\n')[0].trim().toUpperCase();
    }
    if (texto.toUpperCase().includes('FLORA PRODS') || texto.toUpperCase().includes('FLORA ')) {
      clienteLido = 'FLORA PRODS DE HIG E LIM';
    } else if (texto.toUpperCase().includes('DEVINTEX')) {
      clienteLido = 'DEVINTEX COSMETICOS';
    }

    // Pedido (Ex: 006019, 006580, 006643)
    const mPedido = texto.match(/PEDIDO[:.\s]*(\d{5,8})/i);
    if (mPedido) pedido = mPedido[1].trim();

    // Lote (Ex: 006019.1, 006580.1, 006643.1)
    const mLote = texto.match(/LOTE[:.\s]*(\d{5,8}\.\d+)/i) || texto.match(/\b(\d{6}\.\d)\b/);
    if (mLote) lote = mLote[1].trim();

    // Item (Ex: FR-12588, PT-7908, 719935)
    const mItem =
      texto.match(/\b(FR\s*-\s*\d{4,6}|PT\s*-\s*\d{4,6})\b/i) ||
      texto.match(/ITEM[:.\s]*([A-Z0-9\-]{4,12})/i) ||
      texto.match(/\b(719935)\b/);
    if (mItem) item = mItem[1].replace(/\s+/g, '').toUpperCase();

    // Produto
    const mProduto = texto.match(/PRODUTO[:.\s]*([^\n]+(?:\n(?!(?:QTD|LOTE|VALIDADE|VOLUME))[^\n]+)*)/i);
    if (mProduto) {
      produtoLido = mProduto[1].replace(/\s+/g, ' ').trim().toUpperCase();
    }

    // QTD por caixa/saco (Ex: 252, 220, 59)
    const mQtd = texto.match(/QTD[:.\s]*(\d{1,4})\b/i);
    if (mQtd) qtdPorVolume = parseInt(mQtd[1], 10);

    // Volume (Ex: 153 / 278, 029 / 125, 002 / 560)
    const mVol = texto.match(/\b(\d{3}\s*\/\s*\d{3})\b/);
    if (mVol) volumeLido = mVol[1].replace(/\s+/g, '');
  }

  // B) Extração auxiliar por código de barras / linha inferior (Ex: 719935;0060191;252)
  const fonteCodigo = `${brCode}\n${texto}`;
  const mBarcode = fonteCodigo.match(/([A-Z0-9\-]{4,10})\s*[;:]\s*(\d{6,8})\s*[;:]\s*(\d{1,4})/i);
  if (mBarcode) {
    let itemCod = mBarcode[1].trim().toUpperCase();
    // Se o OCR cortou o primeiro dígito de 719935 (lendo 19935), corrige:
    if (itemCod === '19935') itemCod = '719935';
    if (!item) item = itemCod;

    const loteBruto = mBarcode[2].trim();
    if (!pedido && loteBruto.length > 1) pedido = loteBruto.slice(0, -1);
    if (!lote && loteBruto.length > 1) lote = `${loteBruto.slice(0, -1)}.${loteBruto.slice(-1)}`;
    if (!qtdPorVolume) qtdPorVolume = parseInt(mBarcode[3].trim(), 10);
  }

  // C) Sincroniza Pedido <-> Lote caso um dos dois tenha sido lido
  if (!lote && pedido) lote = `${pedido}.1`;
  if (!pedido && lote && lote.includes('.')) pedido = lote.split('.')[0];

  // D) Busca no catálogo para complementar dados do item reconhecido
  db.get(`SELECT * FROM catalogo_itens WHERE item = ?`, [item], (err, row) => {
    if (err) return res.status(500).json({ erro: err.message });

    const clienteFinal = clienteLido || (row ? row.cliente : '');
    const produtoFinal = produtoLido || (row ? row.produto : '');

    // TRAVA DE SEGURANÇA: Se não leu pelo menos o Item ou Cliente + Lote/Pedido, recusa a leitura!
    if ((!item && !clienteFinal) || (!lote && !pedido) || !qtdPorVolume) {
      return res.status(422).json({
        erro: 'Etiqueta não identificada na foto! Aproxime a câmera da etiqueta para focar as letras e tente novamente.'
      });
    }

    const padraoVolumes = calcularPadraoPalete(clienteFinal, item, produtoFinal);
    const tipoEmbalagem = padraoVolumes === 22 ? 'SACARIA' : 'CAIXA';

    res.json({
      item: item || 'VERIFICAR-ITEM',
      pedido: pedido,
      lote: lote,
      cliente: clienteFinal || 'VERIFICAR CLIENTE',
      produto: produtoFinal || `PRODUTO ITEM ${item}`,
      tipoEmbalagem,
      volume_etiqueta: volumeLido,
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

  if (!lote || !item || !cliente || pecasUn <= 0 || volumes <= 0) {
    return res.status(400).json({ erro: 'Preencha todos os dados da etiqueta antes de confirmar.' });
  }

  const saldoTotalPecas = pecasUn * volumes;

  if (volume_etiqueta && volume_etiqueta.trim() !== '') {
    db.get(
      `SELECT id FROM paletes_bipados WHERE lote = ? AND item = ? AND volume_etiqueta = ?`,
      [lote, item, volume_etiqueta.trim()],
      (err, duplicado) => {
        if (duplicado) {
          return res.status(409).json({
            erro: `Atenção: O palete com etiqueta Volume ${volume_etiqueta} do Lote ${lote} já foi bipado!`
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
      `INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete) VALUES (?, ?, ?, ?)`,
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
});