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
  stmt.run('FR-12588', 'DEVINTEX COSMETICOS', 'FRA 300 ML CÔNICO ATIV. R 28 410 TP PUSH PULL BRANCO 2515 -MP-FR-12588MP', 15, 220, 'CAIXA');
  stmt.run('719935', 'FLORA PRODS DE HIG E LIM', 'FRA 300 ML FLORA 24 410 PUSH PULL ROSA 22908/2 CPP PÓS DANOS SOL. MAR E', 20, 252, 'CAIXA');
  stmt.run('PT-7908', 'DEVINTEX COSMETICOS', 'PT 1000 GRS REDONDO 3 ENTRADAS BRANCO 2515 PT-7908MP', 22, 59, 'SACARIA');
  stmt.run('PT-12236', 'DEVINTEX COSMETICOS', 'PT 300 GRS MAIONESE MATIZADOR NATURAL PT-12236MP', 15, 180, 'CAIXA');
  stmt.finalize();
});

function calcularPadraoPalete(cliente = '', item = '', produto = '') {
  const cli = cliente.toUpperCase();
  const it = item.toUpperCase();
  const prod = produto.toUpperCase();

  // Exceção Devintex Sacaria: Pote 1000g Redondo (PT-7908) = 22 volumes
  if (cli.includes('DEVINTEX') && (it === 'PT-7908' || prod.includes('1000 GRS') || prod.includes('REDONDO'))) {
    return 22;
  }
  // Padrão Devintex Caixas = 15 volumes
  if (cli.includes('DEVINTEX')) {
    return 15;
  }
  // Demais clientes (Flora, etc.) = 20 volumes
  return 20;
}

// 1. MOTOR DE INTERPRETAÇÃO PROFUNDA DA ETIQUETA COMPLETA
app.post('/api/interpretar-bipe', (req, res) => {
  const { codigoLido, textoEtiqueta } = req.body;
  const brCode = (codigoLido || '').trim();
  const ocrRaw = (textoEtiqueta || '').trim();

  if (!brCode && !ocrRaw) {
    return res.status(400).json({ erro: 'Nenhuma imagem recebida.' });
  }

  // 1) LIMPEZA PESADA: Remove os códigos gigantes estampados no papelão da caixa (Ex: PAPTE1000125, PAFRA1000513, MQN 09, MQN 07)
  let texto = `${brCode}\n${ocrRaw}`
    .replace(/\r/g, '')
    .replace(/\bPA[A-Z]{2,5}\s*\d{4,9}\b/gi, ' ')
    .replace(/\bMQN\s*\d{1,4}\b/gi, ' ')
    .replace(/N[º°o]\s*PED\.?\s*CLIENTE\s*:?/gi, ' ')
    .replace(/OTD|0TD|QTO|QID|QT0/gi, 'QTD')
    .replace(/1TEM|lTEM|!TEM|ITEN/gi, 'ITEM')
    .replace(/L0TE|LQTE/gi, 'LOTE')
    .replace(/PED1DO|PEDID0/gi, 'PEDIDO')
    .replace(/CL1ENTE/gi, 'CLIENTE')
    .replace(/PI\s*-\s*(\d)/gi, 'PT-$1')
    .replace(/P1\s*-\s*(\d)/gi, 'PT-$1')
    .replace(/F8\s*-\s*(\d)/gi, 'FR-$1')
    .replace(/ER\s*-\s*(\d)/gi, 'FR-$1');

  let item = '';
  let pedido = '';
  let lote = '';
  let qtdPorVolume = 0;
  let clienteLido = '';
  let produtoLido = '';
  let volumeLido = '';

  // 2) Tenta extrair da linha auxiliar do código de barras caso esteja presente (Ex: PT-12236;0064371;180)
  const mBarcode = texto.match(/([A-Z]{2}\s*-\s*\d{4,6}|\d{5,7})\s*[;:,]\s*(\d{6,8})\s*[;:,]\s*(\d{2,4})/i);
  if (mBarcode) {
    item = mBarcode[1].replace(/\s+/g, '').toUpperCase();
    if (item === '19935') item = '719935';
    const lb = mBarcode[2].trim();
    pedido = lb.slice(0, -1);
    lote = `${lb.slice(0, -1)}.${lb.slice(-1)}`;
    qtdPorVolume = parseInt(mBarcode[3].trim(), 10) || 0;
  }

  // 3) EXTRAÇÃO DO LOTE E PEDIDO PELA ESTRUTURA DA ETIQUETA (Ex: 006437 e 006437.1)
  // Mesmo sem ler a palavra "LOTE:", qualquer número no formato 00XXXX.1 na etiqueta é o Lote!
  if (!lote) {
    const mLoteDireto =
      texto.match(/LOTE[:.\s]*(\d{5,7}\s*[\.,]\s*\d)/i) ||
      texto.match(/\b(00\d{4}\s*\.\s*\d)\b/);
    if (mLoteDireto) {
      lote = mLoteDireto[1].replace(/\s+/g, '').replace(',', '.');
      pedido = lote.split('.')[0];
    }
  }

  if (!pedido) {
    const mPed =
      texto.match(/PEDIDO[:.\s]*(\d{5,7})/i) ||
      texto.match(/\b(006\d{3})\b/);
    if (mPed) {
      pedido = mPed[1].trim();
      if (!lote) lote = `${pedido}.1`;
    }
  }

  // 4) EXTRAÇÃO DO CÓDIGO DO ITEM (Ex: PT-12236, FR-12588, PT-7908, 719935)
  // Nas etiquetas Devintex, o item aparece tanto em "ITEM: PT-12236" quanto no fim do produto "PT-12236MP"
  if (!item) {
    const mItemPrefixado = texto.match(/\b((?:PT|FR)\s*-\s*\d{4,6})(?:MP)?\b/i) ||
                           texto.match(/\b((?:PT|FR)\d{4,6})(?:MP)?\b/i);
    if (mItemPrefixado) {
      let codigoBruto = mItemPrefixado[1].replace(/\s+/g, '').toUpperCase();
      if (!codigoBruto.includes('-')) {
        codigoBruto = codigoBruto.slice(0, 2) + '-' + codigoBruto.slice(2);
      }
      item = codigoBruto;
    } else {
      const mItemNum = texto.match(/ITEM[:.\s]*([A-Z0-9\-]{5,10})/i) || texto.match(/\b(719935|19935)\b/);
      if (mItemNum) {
        let cand = mItemNum[1].trim().toUpperCase();
        if (cand === '19935') cand = '719935';
        if (cand !== pedido && !cand.startsWith('006')) item = cand;
      }
    }
  }

  // 5) EXTRAÇÃO DO CLIENTE
  if (/DEVINTEX|COSMETICOS/i.test(texto) || item.startsWith('PT-') || item.startsWith('FR-')) {
    clienteLido = 'DEVINTEX COSMETICOS';
  } else if (/FLORA\s*PRODS|FLORA\s*24|HIG\s*E\s*LIM|P[OÓ]S\s*DANOS/i.test(texto) || item === '719935') {
    clienteLido = 'FLORA PRODS DE HIG E LIM';
  } else {
    const mCli = texto.match(/CLIENTE[:.\s]*([A-Z0-9\s.\-&]{3,35})/i);
    if (mCli) clienteLido = mCli[1].split('\n')[0].trim().toUpperCase();
  }

  // 6) EXTRAÇÃO DO PRODUTO COMPLETO (CAPTURA AS 2 LINHAS INTEIRAS!)
  // Pega tudo desde "PRODUTO:" (ou desde "PT 300 GRS" / "FRA 300 ML") até antes de "QTD" ou "LOTE"
  const mBlocoProduto =
    texto.match(/PRODUTO[:.\s]*([\s\S]{5,140}?)(?=\bQTD\b|\bLOTE\b|\bVALIDADE\b|\b00\d{4}\.\d\b|$)/i) ||
    texto.match(/\b((?:PT|FRA)\s+\d{2,4}\s*(?:GRS|ML)[\s\S]{5,110}?)(?=\bQTD\b|\bLOTE\b|\bVALIDADE\b|$)/i);

  if (mBlocoProduto) {
    let prodLimpo = mBlocoProduto[1]
      .replace(/\n+/g, ' ')
      // Remove qualquer número isolado de QTD que tenha ficado colado no final antes do Lote
      .replace(/\s+\d{2,3}\s*$/, '')
      .replace(/[^A-Z0-9À-Ú\s.\-\/]/gi, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim()
      .toUpperCase();

    // Garante que se o item terminar com MP (ex: PT-12236MP), corta qualquer lixo após o MP
    const mAteMP = prodLimpo.match(/^(.+?(?:PT|FR)-\d{4,6}MP)/i);
    if (mAteMP) prodLimpo = mAteMP[1].toUpperCase();

    if (prodLimpo.length >= 6) {
      produtoLido = prodLimpo;
    }
  }

  // 7) EXTRAÇÃO DA QTD DE PEÇAS NA CAIXA (Ex: 180, 220, 252, 59)
  if (!qtdPorVolume) {
    // Tentativa A: Número logo após a palavra QTD (mesmo que haja quebra de linha)
    const mQtdDireta = texto.match(/QTD[:.\s\n]*(\d{2,4})\b/i);
    if (mQtdDireta) {
      const val = parseInt(mQtdDireta[1], 10);
      if (val >= 10 && val <= 2500 && String(val) !== pedido) {
        qtdPorVolume = val;
      }
    }
  }

  if (!qtdPorVolume) {
    // Tentativa B: Na etiqueta, a QTD fica exatamente entre o fim do PRODUTO (ex: PT-12236MP) e o LOTE (ex: 006437.1)
    const mEntreProdELote = texto.match(/(?:MP|MAR\s*E)[\s\S]{0,30}?\b(\d{2,4})\b[\s\S]{0,30}?(?:LOTE|00\d{4})/i);
    if (mEntreProdELote) {
      qtdPorVolume = parseInt(mEntreProdELote[1], 10);
    }
  }

  if (!qtdPorVolume && lote) {
    // Tentativa C: Número de 2 a 3 dígitos imediatamente anterior ao número do Lote
    const regexAntesDoLote = new RegExp(`\\b(\\d{2,3})\\b[\\s\\nLOTE:]*${lote.replace('.', '\\.')}`, 'i');
    const mAntes = texto.match(regexAntesDoLote);
    if (mAntes) qtdPorVolume = parseInt(mAntes[1], 10);
  }

  // 8) EXTRAÇÃO DO VOLUME DA ETIQUETA (Ex: 370 / 400, 029 / 125, 153 / 278)
  const mVol = texto.match(/\b(\d{2,3}\s*\/\s*\d{3})\b/);
  if (mVol) volumeLido = mVol[1].replace(/\s+/g, '');

  // 9) CONSULTA O CATÁLOGO PARA COMPLEMENTAR OU CONFIRMAR DADOS
  db.get(`SELECT * FROM catalogo_itens WHERE item = ?`, [item], (err, row) => {
    if (err) return res.status(500).json({ erro: err.message });

    const clienteFinal = clienteLido || (row ? row.cliente : '');
    const produtoFinal = produtoLido || (row ? row.produto : '');
    const qtdPecasFinal = qtdPorVolume || (row ? row.qtd_padrao_pecas : 0);

    // Se a imagem não tinha nenhum dado de etiqueta válido
    if (!item && !pedido && !lote && !produtoFinal) {
      return res.status(422).json({
        erro: 'Etiqueta não enquadrada. Aponte para a etiqueta inteira e tente novamente.'
      });
    }

    const padraoVolumes = calcularPadraoPalete(clienteFinal, item, produtoFinal);
    const tipoEmbalagem = padraoVolumes === 22 ? 'SACARIA' : 'CAIXA';

    res.json({
      item: item || (row ? row.item : ''),
      pedido: pedido || '',
      lote: lote || (pedido ? `${pedido}.1` : ''),
      cliente: clienteFinal || 'DEVINTEX COSMETICOS',
      produto: produtoFinal || (item ? `PRODUTO ITEM ${item}` : ''),
      tipoEmbalagem,
      volume_etiqueta: volumeLido,
      qtd_pecas_por_volume: qtdPecasFinal,
      qtd_volumes_sugerida: padraoVolumes,
      saldo_previsto: qtdPecasFinal * padraoVolumes
    });
  });
});

// 2. SALVAR PALETE E ATUALIZAR TELAS INSTANTANEAMENTE
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
    return res.status(400).json({ erro: 'Confirme Cliente, Lote, Item e Quantidades antes de salvar.' });
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
    // O sistema aprende automaticamente qualquer produto novo que você bipar!
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

// 3. AUTO-RESTAURAÇÃO DE BACKUP (OS SALDOS NUNCA SOMEM)
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