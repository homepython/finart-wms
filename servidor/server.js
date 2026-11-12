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

// TRAVA ESTRITA DE PEDIDO (Apenas dígitos, sem ponto, máx 6 caracteres: ex "006437")
function sanitizarPedido(valor = '') {
  const semPonto = String(valor).trim().split('.')[0];
  return semPonto.replace(/\D/g, '').slice(0, 6);
}

// TRAVA ESTRITA DE LOTE (Máx 8 caracteres e exatamente 1 ponto: ex "006437.1", nunca "006437.1.1")
function sanitizarLote(valor = '', pedidoFallback = '') {
  let limpo = String(valor).trim().replace(/,/g, '.').replace(/[^0-9.]/g, '');
  const partes = limpo.split('.').filter(Boolean);

  if (partes.length === 0) {
    const ped = sanitizarPedido(pedidoFallback);
    return ped ? `${ped}.1` : '';
  }

  if (partes.length === 1) {
    const num = partes[0];
    if (num.length === 7) {
      return `${num.slice(0, 6)}.${num.slice(6, 7)}`;
    }
    const base = num.slice(0, 6);
    return base ? `${base}.1` : '';
  }

  const base = partes[0].slice(0, 6);
  const sufixo = partes[1].slice(0, 1) || '1';
  return `${base}.${sufixo}`.slice(0, 8);
}

const db = new sqlite3.Database('./finart.db', (err) => {
  if (err) console.error('Erro ao abrir banco:', err.message);
  else console.log('✅ Banco FINART conectado.');
});

db.serialize(() => {
  // Catálogo Inteligente de Itens (Aprende novos códigos automaticamente)
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

  // Memória Inteligente de Clientes (Aprende o padrão de caixas de clientes novos)
  db.run(`
    CREATE TABLE IF NOT EXISTS clientes_regras (
      cliente TEXT PRIMARY KEY,
      padrao_volumes_palete INTEGER NOT NULL
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
    INSERT OR IGNORE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  stmt.run('FR-12588', 'DEVINTEX COSMETICOS', 'FRA 300 ML CÔNICO ATIV. R 28 410 TP PUSH PULL BRANCO 2515 -MP-FR-12588MP', 15, 220, 'CAIXA');
  stmt.run('719935', 'FLORA PRODS DE HIG E LIM', 'FRA 300 ML FLORA 24 410 PUSH PULL ROSA 22908/2 CPP PÓS DANOS SOL. MAR E', 20, 252, 'CAIXA');
  stmt.run('PT-7908', 'DEVINTEX COSMETICOS', 'PT 1000 GRS REDONDO 3 ENTRADAS BRANCO 2515 PT-7908MP', 22, 59, 'SACARIA');
  stmt.run('PT-12236', 'DEVINTEX COSMETICOS', 'PT 300 GRS MAIONESE MATIZADOR NATURAL PT-12236MP', 15, 180, 'CAIXA');
  stmt.finalize();

  // Auto-correção de registros antigos que tenham ficado com ".1.1" no banco
  db.all(`SELECT id, pedido, lote FROM paletes_bipados`, [], (err, rows) => {
    if (!err && rows) {
      rows.forEach((r) => {
        const pedCorreto = sanitizarPedido(r.pedido || r.lote);
        const loteCorreto = sanitizarLote(r.lote, pedCorreto);
        if (r.pedido !== pedCorreto || r.lote !== loteCorreto) {
          db.run(`UPDATE paletes_bipados SET pedido = ?, lote = ? WHERE id = ?`, [pedCorreto, loteCorreto, r.id]);
        }
      });
    }
  });
});

function calcularPadraoPalete(cliente = '', item = '', produto = '', padraoSalvo = 0) {
  const cli = cliente.toUpperCase();
  const it = item.toUpperCase();
  const prod = produto.toUpperCase();

  if (cli.includes('DEVINTEX') && (it === 'PT-7908' || prod.includes('1000 GRS') || prod.includes('REDONDO'))) {
    return 22;
  }
  if (padraoSalvo && padraoSalvo > 0) {
    return padraoSalvo;
  }
  if (cli.includes('DEVINTEX')) {
    return 15;
  }
  return 20;
}

// 1. INTERPRETADOR INTELIGENTE COM MEMÓRIA APRENDIDA
app.post('/api/interpretar-bipe', (req, res) => {
  const { codigoLido, textoEtiqueta } = req.body;
  const brCode = (codigoLido || '').trim();
  const ocrRaw = (textoEtiqueta || '').trim();

  if (!brCode && !ocrRaw) {
    return res.status(400).json({ erro: 'Nenhuma imagem recebida.' });
  }

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

  // A) Linha de código de barras (Ex: PT-12236;0064371;180)
  const mBarcode = texto.match(/([A-Z]{2}\s*-\s*\d{4,6}|\d{5,7})\s*[;:,]\s*(\d{6,8})\s*[;:,]\s*(\d{2,4})/i);
  if (mBarcode) {
    item = mBarcode[1].replace(/\s+/g, '').toUpperCase();
    if (item === '19935') item = '719935';
    const lb = mBarcode[2].trim();
    pedido = sanitizarPedido(lb.slice(0, -1));
    lote = sanitizarLote(`${lb.slice(0, -1)}.${lb.slice(-1)}`, pedido);
    qtdPorVolume = parseInt(mBarcode[3].trim(), 10) || 0;
  }

  // B) Lote e Pedido com proteção contra ".1.1"
  if (!lote) {
    const mLoteDireto =
      texto.match(/LOTE[:.\s]*(\d{5,6}\s*[\.,]\s*\d)/i) ||
      texto.match(/\b(00\d{4}\s*\.\s*\d)\b/);
    if (mLoteDireto) {
      lote = sanitizarLote(mLoteDireto[1]);
      pedido = sanitizarPedido(lote);
    }
  }

  if (!pedido) {
    // Nunca captura o ".1" dentro do pedido
    const mPed =
      texto.match(/PEDIDO[:.\s]*(\d{5,6})(?!\.\d)/i) ||
      texto.match(/\b(00\d{4})\b/);
    if (mPed) {
      pedido = sanitizarPedido(mPed[1]);
      if (!lote) lote = sanitizarLote('', pedido);
    }
  }

  // C) Código do Item (reconhece PT-XXXX, FR-XXXX ou qualquer código aprendido no catálogo)
  if (!item) {
    const mItemPrefixado =
      texto.match(/\b((?:PT|FR)\s*-\s*\d{4,6})(?:MP)?\b/i) ||
      texto.match(/\b((?:PT|FR)\d{4,6})(?:MP)?\b/i);
    if (mItemPrefixado) {
      let codigoBruto = mItemPrefixado[1].replace(/\s+/g, '').toUpperCase();
      if (!codigoBruto.includes('-')) {
        codigoBruto = codigoBruto.slice(0, 2) + '-' + codigoBruto.slice(2);
      }
      item = codigoBruto;
    } else {
      const mItemGenerico = texto.match(/ITEM[:.\s]*([A-Z0-9\-]{4,12})/i) || texto.match(/\b(719935|19935)\b/);
      if (mItemGenerico) {
        let cand = mItemGenerico[1].trim().toUpperCase();
        if (cand === '19935') cand = '719935';
        if (cand !== pedido && !cand.startsWith('006')) item = cand;
      }
    }
  }

  // D) Cliente
  if (/DEVINTEX|COSMETICOS/i.test(texto)) {
    clienteLido = 'DEVINTEX COSMETICOS';
  } else if (/FLORA\s*PRODS|FLORA\s*24|HIG\s*E\s*LIM|P[OÓ]S\s*DANOS/i.test(texto)) {
    clienteLido = 'FLORA PRODS DE HIG E LIM';
  } else {
    const mCli = texto.match(/CLIENTE[:.\s]*([A-Z0-9\s.\-&]{3,35})/i);
    if (mCli) clienteLido = mCli[1].split('\n')[0].trim().toUpperCase();
  }

  // E) Produto Completo (2 linhas)
  const mBlocoProduto =
    texto.match(/PRODUTO[:.\s]*([\s\S]{5,140}?)(?=\bQTD\b|\bLOTE\b|\bVALIDADE\b|\b00\d{4}\.\d\b|$)/i) ||
    texto.match(/\b((?:PT|FRA)\s+\d{2,4}\s*(?:GRS|ML)[\s\S]{5,110}?)(?=\bQTD\b|\bLOTE\b|\bVALIDADE\b|$)/i);

  if (mBlocoProduto) {
    let prodLimpo = mBlocoProduto[1]
      .replace(/\n+/g, ' ')
      .replace(/\s+\d{2,3}\s*$/, '')
      .replace(/[^A-Z0-9À-Ú\s.\-\/]/gi, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim()
      .toUpperCase();

    const mAteMP = prodLimpo.match(/^(.+?(?:PT|FR)-\d{4,6}MP)/i);
    if (mAteMP) prodLimpo = mAteMP[1].toUpperCase();
    if (prodLimpo.length >= 6) produtoLido = prodLimpo;
  }

  // F) QTD por Caixa
  if (!qtdPorVolume) {
    const mQtdDireta = texto.match(/QTD[:.\s\n]*(\d{2,4})\b/i);
    if (mQtdDireta) {
      const val = parseInt(mQtdDireta[1], 10);
      if (val >= 10 && val <= 2500 && String(val) !== pedido) qtdPorVolume = val;
    }
  }
  if (!qtdPorVolume) {
    const mEntreProdELote = texto.match(/(?:MP|MAR\s*E)[\s\S]{0,30}?\b(\d{2,4})\b[\s\S]{0,30}?(?:LOTE|00\d{4})/i);
    if (mEntreProdELote) qtdPorVolume = parseInt(mEntreProdELote[1], 10);
  }
  if (!qtdPorVolume && lote) {
    const regexAntesDoLote = new RegExp(`\\b(\\d{2,3})\\b[\\s\\nLOTE:]*${lote.replace('.', '\\.')}`, 'i');
    const mAntes = texto.match(regexAntesDoLote);
    if (mAntes) qtdPorVolume = parseInt(mAntes[1], 10);
  }

  // G) Volume da Etiqueta
  const mVol = texto.match(/\b(\d{2,3}\s*\/\s*\d{3})\b/);
  if (mVol) volumeLido = mVol[1].replace(/\s+/g, '');

  // H) Cruza com toda a memória aprendida no banco (Catálogo de Itens + Clientes)
  db.all(`SELECT * FROM catalogo_itens`, [], (err, catalogoRows) => {
    if (err) return res.status(500).json({ erro: err.message });

    // Se o código do item não foi pego diretamente pela regex, verifica se algum código já aprendido aparece no texto da etiqueta!
    let itemMemoria = null;
    if (item) {
      itemMemoria = catalogoRows.find((r) => r.item.toUpperCase() === item.toUpperCase());
    } else {
      itemMemoria = catalogoRows.find((r) => r.item.length >= 4 && texto.toUpperCase().includes(r.item.toUpperCase()));
      if (itemMemoria) item = itemMemoria.item;
    }

    const clienteFinal = clienteLido || (itemMemoria ? itemMemoria.cliente : (item.startsWith('PT-') || item.startsWith('FR-') ? 'DEVINTEX COSMETICOS' : ''));
    const produtoFinal = produtoLido || (itemMemoria ? itemMemoria.produto : '');
    const qtdPecasFinal = qtdPorVolume || (itemMemoria ? itemMemoria.qtd_padrao_pecas : 0);

    if (!item && !pedido && !lote && !produtoFinal) {
      return res.status(422).json({
        erro: 'Etiqueta não enquadrada. Aponte para a etiqueta inteira e tente novamente.'
      });
    }

    const pedidoLimpo = sanitizarPedido(pedido || lote);
    const loteLimpo = sanitizarLote(lote, pedidoLimpo);
    const padraoVolumes = calcularPadraoPalete(
      clienteFinal,
      item,
      produtoFinal,
      itemMemoria ? itemMemoria.padrao_volumes_palete : 0
    );
    const tipoEmbalagem = padraoVolumes === 22 ? 'SACARIA' : 'CAIXA';

    res.json({
      item: item || (itemMemoria ? itemMemoria.item : ''),
      pedido: pedidoLimpo,
      lote: loteLimpo,
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

// 2. SALVAR PALETE E ENSINAR NOVOS ITENS/CLIENTES AO SISTEMA
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
  const itemLimpo = String(item || '').trim().toUpperCase();
  const clienteLimpo = String(cliente || '').trim().toUpperCase();
  const produtoLimpo = String(produto || '').trim().toUpperCase();
  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);

  if (!loteLimpo || !itemLimpo || !clienteLimpo || pecasUn <= 0 || volumes <= 0) {
    return res.status(400).json({ erro: 'Confirme Cliente, Lote, Item e Quantidades antes de salvar.' });
  }

  const saldoTotalPecas = pecasUn * volumes;

  if (volume_etiqueta && volume_etiqueta.trim() !== '') {
    db.get(
      `SELECT id FROM paletes_bipados WHERE lote = ? AND item = ? AND volume_etiqueta = ?`,
      [loteLimpo, itemLimpo, volume_etiqueta.trim()],
      (err, duplicado) => {
        if (duplicado) {
          return res.status(409).json({
            erro: `Atenção: O palete Volume ${volume_etiqueta} do Lote ${loteLimpo} já foi bipado!`
          });
        }
        inserirPalete();
      }
    );
  } else {
    inserirPalete();
  }

  function inserirPalete() {
    // APRENDIZADO INTELIGENTE: Se não for picadeira (ex: >= 12 volumes), aprende o padrão do item e do cliente!
    const padraoParaAprender = volumes >= 12
      ? volumes
      : calcularPadraoPalete(clienteLimpo, itemLimpo, produtoLimpo);
    const tipoEmb = padraoParaAprender === 22 ? 'SACARIA' : 'CAIXA';

    db.run(
      `INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [itemLimpo, clienteLimpo, produtoLimpo, padraoParaAprender, pecasUn, tipoEmb]
    );

    db.run(
      `INSERT OR REPLACE INTO clientes_regras (cliente, padrao_volumes_palete) VALUES (?, ?)`,
      [clienteLimpo, padraoParaAprender]
    );

    db.run(
      `INSERT INTO paletes_bipados 
      (pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [pedidoLimpo, loteLimpo, itemLimpo, clienteLimpo, produtoLimpo, pecasUn, volumes, saldoTotalPecas, volume_etiqueta || ''],
      function (err) {
        if (err) return res.status(500).json({ erro: err.message });
        notificarTelasInstantaneamente();
        res.json({
          sucesso: true,
          id: this.lastID,
          pedido: pedidoLimpo,
          lote: loteLimpo,
          saldo_total_pecas: saldoTotalPecas
        });
      }
    );
  }
});

// 3. AUTO-RESTAURAÇÃO DE BACKUP (SALDOS + CATÁLOGO APRENDIDO NUNCA SOMEM)
app.post('/api/restaurar-backup', (req, res) => {
  const { historico, catalogo } = req.body;

  db.serialize(() => {
    if (Array.isArray(catalogo) && catalogo.length > 0) {
      const stmtCat = db.prepare(`
        INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
        VALUES (?, ?, ?, ?, ?, ?)
      `);
      catalogo.forEach((c) => {
        stmtCat.run(c.item, c.cliente, c.produto, c.padrao_volumes_palete || 15, c.qtd_padrao_pecas || 0, c.tipo_embalagem || 'CAIXA');
      });
      stmtCat.finalize();
    }

    if (!Array.isArray(historico) || historico.length === 0) {
      return res.json({ restaurado: false });
    }

    db.get(`SELECT COUNT(*) AS total FROM paletes_bipados`, [], (err, row) => {
      if (err || (row && row.total > 0)) {
        return res.json({ restaurado: false });
      }

      const stmt = db.prepare(`
        INSERT OR IGNORE INTO paletes_bipados
        (id, pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta, data_hora)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      historico.forEach((p) => {
        const pedOk = sanitizarPedido(p.pedido || p.lote);
        const loteOk = sanitizarLote(p.lote, pedOk);
        stmt.run(
          p.id, pedOk, loteOk, p.item, p.cliente, p.produto,
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

// 4. DASHBOARD + CATÁLOGO INTELIGENTE
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

  const queryHistorico = `SELECT * FROM paletes_bipados ORDER BY id DESC LIMIT 400`;
  const queryCatalogo = `SELECT * FROM catalogo_itens ORDER BY item ASC`;

  db.all(queryConsolidado, [], (err, consolidado) => {
    if (err) return res.status(500).json({ erro: err.message });
    db.all(queryHistorico, [], (err2, historico) => {
      if (err2) return res.status(500).json({ erro: err2.message });
      db.all(queryCatalogo, [], (err3, catalogo) => {
        res.json({ consolidado, historico, catalogo: catalogo || [] });
      });
    });
  });
});

// 5. EDITAR FALHA OPERACIONAL (COM TRAVA DE LOTE E APRENDIZADO)
app.put('/api/paletes/:id', (req, res) => {
  const { id } = req.params;
  const { lote, pedido, cliente, item, produto, qtd_pecas_por_volume, qtd_volumes_palete, volume_etiqueta } = req.body;

  const pecasUn = Number(qtd_pecas_por_volume);
  const volumes = Number(qtd_volumes_palete);
  if (pecasUn <= 0 || volumes <= 0) return res.status(400).json({ erro: 'Valores inválidos.' });

  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);
  const itemLimpo = String(item || '').trim().toUpperCase();
  const clienteLimpo = String(cliente || '').trim().toUpperCase();
  const produtoLimpo = String(produto || '').trim().toUpperCase();
  const novoSaldoTotal = pecasUn * volumes;

  db.run(
    `UPDATE paletes_bipados 
     SET lote = ?, pedido = ?, cliente = ?, item = ?, produto = ?, 
         qtd_pecas_por_volume = ?, qtd_volumes_palete = ?, saldo_total_pecas = ?, volume_etiqueta = ?
     WHERE id = ?`,
    [loteLimpo, pedidoLimpo, clienteLimpo, itemLimpo, produtoLimpo, pecasUn, volumes, novoSaldoTotal, volume_etiqueta || '', id],
    function (err) {
      if (err) return res.status(500).json({ erro: err.message });

      // Atualiza também o aprendizado do catálogo quando você edita um item
      const padrao = volumes >= 12 ? volumes : calcularPadraoPalete(clienteLimpo, itemLimpo, produtoLimpo);
      db.run(
        `INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas) VALUES (?, ?, ?, ?, ?)`,
        [itemLimpo, clienteLimpo, produtoLimpo, padrao, pecasUn]
      );

      notificarTelasInstantaneamente();
      res.json({ sucesso: true, saldo_recalculado: novoSaldoTotal });
    }
  );
});

// 6. EXCLUIR MÚLTIPLOS PALETES OU SELECIONAR TUDO DE UMA VEZ
app.post('/api/paletes/excluir-multiplos', (req, res) => {
  const { ids, todos } = req.body;

  if (todos) {
    db.run(`DELETE FROM paletes_bipados`, [], function (err) {
      if (err) return res.status(500).json({ erro: err.message });
      notificarTelasInstantaneamente();
      res.json({ sucesso: true, excluidos: this.changes });
    });
    return;
  }

  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ erro: 'Nenhum palete selecionado.' });
  }

  const placeholders = ids.map(() => '?').join(',');
  db.run(`DELETE FROM paletes_bipados WHERE id IN (${placeholders})`, ids, function (err) {
    if (err) return res.status(500).json({ erro: err.message });
    notificarTelasInstantaneamente();
    res.json({ sucesso: true, excluidos: this.changes });
  });
});

// 7. EXCLUIR UM ÚNICO PALETE
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