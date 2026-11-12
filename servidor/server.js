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
// Controla se o usuário já limpou/excluiu dados para que o backup do celular nunca restaure itens apagados
let versaoExclusao = Date.now();

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

function sanitizarPedido(valor = '') {
  const semPonto = String(valor).trim().split('.')[0];
  return semPonto.replace(/\D/g, '').slice(0, 6);
}

function sanitizarLote(valor = '', pedidoFallback = '') {
  let limpo = String(valor).trim().replace(/,/g, '.').replace(/[^0-9.]/g, '');
  const partes = limpo.split('.').filter(Boolean);

  if (partes.length === 0) {
    const ped = sanitizarPedido(pedidoFallback);
    return ped ? `${ped}.1` : '';
  }
  if (partes.length === 1) {
    const num = partes[0];
    if (num.length === 7) return `${num.slice(0, 6)}.${num.slice(6, 7)}`;
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

// Catálogo base oficial (protegido contra erros de leitura de câmera)
const CATALOGO_OFICIAL = {
  'FR-12588': {
    cliente: 'DEVINTEX COSMETICOS',
    produto: 'FRA 300 ML CÔNICO ATIV. R 28 410 TP PUSH PULL BRANCO 2515 -MP-FR-12588MP',
    padrao_volumes: 15,
    qtd_pecas: 220,
    embalagem: 'CAIXA'
  },
  '719935': {
    cliente: 'FLORA PRODS DE HIG E LIM',
    produto: 'FRA 300 ML FLORA 24 410 PUSH PULL ROSA 22908/2 CPP PÓS DANOS SOL. MAR E',
    padrao_volumes: 20,
    qtd_pecas: 252,
    embalagem: 'CAIXA'
  },
  'PT-7908': {
    cliente: 'DEVINTEX COSMETICOS',
    produto: 'PT 1000 GRS REDONDO 3 ENTRADAS BRANCO 2515 PT-7908MP',
    padrao_volumes: 22,
    qtd_pecas: 59,
    embalagem: 'SACARIA'
  },
  'PT-12236': {
    cliente: 'DEVINTEX COSMETICOS',
    produto: 'PT 300 GRS MAIONESE MATIZADOR NATURAL PT-12236MP',
    padrao_volumes: 15,
    qtd_pecas: 180,
    embalagem: 'CAIXA'
  }
};

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

  // Tabela que grava IDs excluídos para que nenhum celular restaure itens apagados
  db.run(`
    CREATE TABLE IF NOT EXISTS ids_excluidos (
      id INTEGER PRIMARY KEY
    )
  `);

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  Object.entries(CATALOGO_OFICIAL).forEach(([cod, d]) => {
    stmt.run(cod, d.cliente, d.produto, d.padrao_volumes, d.qtd_pecas, d.embalagem);
  });
  stmt.finalize();

  // Unifica automaticamente registros antigos divergentes do mesmo Item/Lote (ex: PT-7908 com 50 vs 59 peças)
  Object.entries(CATALOGO_OFICIAL).forEach(([cod, d]) => {
    db.run(
      `UPDATE paletes_bipados 
       SET cliente = ?, produto = ?, qtd_pecas_por_volume = ?, saldo_total_pecas = qtd_volumes_palete * ?
       WHERE UPPER(TRIM(item)) = ?`,
      [d.cliente, d.produto, d.qtd_pecas, d.qtd_pecas, cod]
    );
  });
});

function calcularPadraoPalete(cliente = '', item = '', produto = '', padraoSalvo = 0) {
  const cli = cliente.toUpperCase();
  const it = item.toUpperCase();
  const prod = produto.toUpperCase();

  if (cli.includes('DEVINTEX') && (it === 'PT-7908' || prod.includes('1000 GRS') || prod.includes('REDONDO'))) {
    return 22;
  }
  if (padraoSalvo && padraoSalvo > 0) return padraoSalvo;
  if (cli.includes('DEVINTEX')) return 15;
  return 20;
}

// 1. INTERPRETADOR COM UNIFICAÇÃO INTELIGENTE POR LOTE E CÓDIGO DO ITEM
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

  const mBarcode = texto.match(/([A-Z]{2}\s*-\s*\d{4,6}|\d{5,7})\s*[;:,]\s*(\d{6,8})\s*[;:,]\s*(\d{2,4})/i);
  if (mBarcode) {
    item = mBarcode[1].replace(/\s+/g, '').toUpperCase();
    if (item === '19935') item = '719935';
    const lb = mBarcode[2].trim();
    pedido = sanitizarPedido(lb.slice(0, -1));
    lote = sanitizarLote(`${lb.slice(0, -1)}.${lb.slice(-1)}`, pedido);
    qtdPorVolume = parseInt(mBarcode[3].trim(), 10) || 0;
  }

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
    const mPed =
      texto.match(/PEDIDO[:.\s]*(\d{5,6})(?!\.\d)/i) ||
      texto.match(/\b(00\d{4})\b/);
    if (mPed) {
      pedido = sanitizarPedido(mPed[1]);
      if (!lote) lote = sanitizarLote('', pedido);
    }
  }

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

  if (/DEVINTEX|COSMETICOS/i.test(texto)) {
    clienteLido = 'DEVINTEX COSMETICOS';
  } else if (/FLORA\s*PRODS|FLORA\s*24|HIG\s*E\s*LIM|P[OÓ]S\s*DANOS/i.test(texto)) {
    clienteLido = 'FLORA PRODS DE HIG E LIM';
  } else {
    const mCli = texto.match(/CLIENTE[:.\s]*([A-Z0-9\s.\-&]{3,35})/i);
    if (mCli) clienteLido = mCli[1].split('\n')[0].trim().toUpperCase();
  }

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

  const mVol = texto.match(/\b(\d{2,3}\s*\/\s*\d{3})\b/);
  if (mVol) volumeLido = mVol[1].replace(/\s+/g, '');

  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);

  // INTELIGÊNCIA DE UNIFICAÇÃO:
  // 1º Verifica se já existe um palete bipado com esse mesmo Lote (ou Pedido) -> Usa exatamente os mesmos dados!
  // 2º Verifica se o Código do Item já existe no Catálogo -> Trava o nome do produto e a quantidade oficial!
  db.get(
    `SELECT * FROM paletes_bipados WHERE (lote = ? AND lote != '') OR (item = ? AND item != '') ORDER BY id DESC LIMIT 1`,
    [loteLimpo, item],
    (errP, paleteExistente) => {
      db.all(`SELECT * FROM catalogo_itens`, [], (err, catalogoRows) => {
        if (err) return res.status(500).json({ erro: err.message });

        let itemMemoria = null;
        if (item) {
          itemMemoria = catalogoRows.find((r) => r.item.toUpperCase() === item.toUpperCase());
        } else {
          itemMemoria = catalogoRows.find((r) => r.item.length >= 4 && texto.toUpperCase().includes(r.item.toUpperCase()));
          if (itemMemoria) item = itemMemoria.item;
        }

        // Se o Lote já foi bipado antes e o item bate (ou não foi lido), herda 100% do lote já aberto
        const mesmoLoteAberto = paleteExistente && paleteExistente.lote === loteLimpo && (!item || paleteExistente.item === item);

        const itemFinal = item || (mesmoLoteAberto ? paleteExistente.item : (itemMemoria ? itemMemoria.item : ''));

        // PRIORIDADE TOTAL AO CATÁLOGO / LOTE ABERTO para nunca dividir saldo por causa de 1 número lido diferente no nome ou QTD
        const clienteFinal =
          (itemMemoria && itemMemoria.cliente) ||
          (mesmoLoteAberto && paleteExistente.cliente) ||
          clienteLido ||
          (itemFinal.startsWith('PT-') || itemFinal.startsWith('FR-') ? 'DEVINTEX COSMETICOS' : '');

        const produtoFinal =
          (itemMemoria && itemMemoria.produto) ||
          (mesmoLoteAberto && paleteExistente.produto) ||
          produtoLido ||
          '';

        const qtdPecasFinal =
          (itemMemoria && itemMemoria.qtd_padrao_pecas > 0 ? itemMemoria.qtd_padrao_pecas : 0) ||
          (mesmoLoteAberto && paleteExistente.qtd_pecas_por_volume > 0 ? paleteExistente.qtd_pecas_por_volume : 0) ||
          qtdPorVolume ||
          0;

        if (!itemFinal && !pedidoLimpo && !loteLimpo && !produtoFinal) {
          return res.status(422).json({
            erro: 'Etiqueta não enquadrada. Aponte para a etiqueta inteira e tente novamente.'
          });
        }

        const padraoVolumes = calcularPadraoPalete(
          clienteFinal,
          itemFinal,
          produtoFinal,
          itemMemoria ? itemMemoria.padrao_volumes_palete : 0
        );
        const tipoEmbalagem = padraoVolumes === 22 ? 'SACARIA' : 'CAIXA';

        res.json({
          item: itemFinal,
          pedido: pedidoLimpo,
          lote: loteLimpo,
          cliente: clienteFinal || 'DEVINTEX COSMETICOS',
          produto: produtoFinal || (itemFinal ? `PRODUTO ITEM ${itemFinal}` : ''),
          tipoEmbalagem,
          volume_etiqueta: volumeLido,
          qtd_pecas_por_volume: qtdPecasFinal,
          qtd_volumes_sugerida: padraoVolumes,
          saldo_previsto: qtdPecasFinal * padraoVolumes
        });
      });
    }
  );
});

// 2. SALVAR PALETE E UNIFICAR SALDO AUTOMATICAMENTE SE FOR DO MESMO LOTE + ITEM
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

  const volumes = Number(qtd_volumes_palete);
  const itemLimpo = String(item || '').trim().toUpperCase();
  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);

  // Verifica se já existe esse Item no catálogo ou no mesmo Lote para padronizar Nome e Peças/Volume
  db.get(`SELECT * FROM catalogo_itens WHERE UPPER(TRIM(item)) = ?`, [itemLimpo], (errCat, catRow) => {
    db.get(
      `SELECT * FROM paletes_bipados WHERE UPPER(TRIM(lote)) = ? AND UPPER(TRIM(item)) = ? LIMIT 1`,
      [loteLimpo, itemLimpo],
      (errLote, loteRow) => {
        const clienteUnificado = (loteRow && loteRow.cliente) || (catRow && catRow.cliente) || String(cliente || '').trim().toUpperCase();
        const produtoUnificado = (loteRow && loteRow.produto) || (catRow && catRow.produto) || String(produto || '').trim().toUpperCase();
        const pecasUnificada = Number(qtd_pecas_por_volume) || (loteRow && loteRow.qtd_pecas_por_volume) || (catRow && catRow.qtd_padrao_pecas) || 0;

        if (!loteLimpo || !itemLimpo || !clienteUnificado || pecasUnificada <= 0 || volumes <= 0) {
          return res.status(400).json({ erro: 'Confirme Cliente, Lote, Item e Quantidades antes de salvar.' });
        }

        const saldoTotalPecas = pecasUnificada * volumes;

        const padraoParaAprender = volumes >= 12
          ? volumes
          : calcularPadraoPalete(clienteUnificado, itemLimpo, produtoUnificado, catRow ? catRow.padrao_volumes_palete : 0);
        const tipoEmb = padraoParaAprender === 22 ? 'SACARIA' : 'CAIXA';

        // Salva no catálogo caso seja um produto novo
        db.run(
          `INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [itemLimpo, clienteUnificado, produtoUnificado, padraoParaAprender, pecasUnificada, tipoEmb]
        );

        // Garante que todos os paletes já bipados deste mesmo Lote + Item tenham exatamente o mesmo nome e peças/vol
        db.run(
          `UPDATE paletes_bipados 
           SET cliente = ?, produto = ?, qtd_pecas_por_volume = ?, saldo_total_pecas = qtd_volumes_palete * ?
           WHERE UPPER(TRIM(lote)) = ? AND UPPER(TRIM(item)) = ?`,
          [clienteUnificado, produtoUnificado, pecasUnificada, pecasUnificada, loteLimpo, itemLimpo]
        );

        db.run(
          `INSERT INTO paletes_bipados 
          (pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [pedidoLimpo, loteLimpo, itemLimpo, clienteUnificado, produtoUnificado, pecasUnificada, volumes, saldoTotalPecas, volume_etiqueta || ''],
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
    );
  });
});

// 3. AUTO-RESTAURAÇÃO DE BACKUP (RESPEITA ITENS EXCLUÍDOS PARA NUNCA VOLTAREM)
app.post('/api/restaurar-backup', (req, res) => {
  const { historico, catalogo } = req.body;

  db.serialize(() => {
    if (Array.isArray(catalogo) && catalogo.length > 0) {
      const stmtCat = db.prepare(`
        INSERT OR IGNORE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
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

      db.all(`SELECT id FROM ids_excluidos`, [], (errEx, excluidosRows) => {
        const setExcluidos = new Set((excluidosRows || []).map((r) => Number(r.id)));
        const validos = historico.filter((p) => !setExcluidos.has(Number(p.id)));

        if (validos.length === 0) {
          return res.json({ restaurado: false });
        }

        const stmt = db.prepare(`
          INSERT OR IGNORE INTO paletes_bipados
          (id, pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta, data_hora)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        validos.forEach((p) => {
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
          res.json({ restaurado: true, quantidade: validos.length });
        });
      });
    });
  });
});

// 4. DASHBOARD: AGRUPA ESTRITAMENTE POR (LOTE + ITEM) PARA NUNCA DIVIDIR O MESMO PRODUTO EM 2 LINHAS
app.get('/api/dashboard', (req, res) => {
  const queryConsolidado = `
    SELECT 
      UPPER(TRIM(lote)) AS lote,
      MAX(pedido) AS pedido,
      UPPER(TRIM(item)) AS item,
      MAX(cliente) AS cliente,
      MAX(produto) AS produto,
      MAX(qtd_pecas_por_volume) AS qtd_pecas_por_volume,
      COUNT(id) AS total_paletes,
      SUM(qtd_volumes_palete) AS total_caixas_ou_sacaria,
      SUM(saldo_total_pecas) AS saldo_total_pecas
    FROM paletes_bipados
    GROUP BY UPPER(TRIM(lote)), UPPER(TRIM(item))
    ORDER BY MAX(id) DESC
  `;

  const queryHistorico = `SELECT * FROM paletes_bipados ORDER BY id DESC LIMIT 400`;
  const queryCatalogo = `SELECT * FROM catalogo_itens ORDER BY item ASC`;

  db.all(queryConsolidado, [], (err, consolidado) => {
    if (err) return res.status(500).json({ erro: err.message });
    db.all(queryHistorico, [], (err2, historico) => {
      if (err2) return res.status(500).json({ erro: err2.message });
      db.all(queryCatalogo, [], (err3, catalogo) => {
        res.json({
          consolidado,
          historico,
          catalogo: catalogo || [],
          versaoExclusao
        });
      });
    });
  });
});

// 5. EDITAR PALETE (E UNIFICAR DADOS DO MESMO LOTE + ITEM)
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

      // Padroniza qualquer outro palete do mesmo Lote + Item para manter 1 única linha somada
      db.run(
        `UPDATE paletes_bipados 
         SET cliente = ?, produto = ?, qtd_pecas_por_volume = ?, saldo_total_pecas = qtd_volumes_palete * ?
         WHERE UPPER(TRIM(lote)) = ? AND UPPER(TRIM(item)) = ?`,
        [clienteLimpo, produtoLimpo, pecasUn, pecasUn, loteLimpo, itemLimpo]
      );

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
  versaoExclusao = Date.now();

  if (todos) {
    db.all(`SELECT id FROM paletes_bipados`, [], (err, rows) => {
      db.serialize(() => {
        const stmt = db.prepare(`INSERT OR IGNORE INTO ids_excluidos (id) VALUES (?)`);
        (rows || []).forEach((r) => stmt.run(r.id));
        stmt.finalize();

        db.run(`DELETE FROM paletes_bipados`, [], function (errDel) {
          if (errDel) return res.status(500).json({ erro: errDel.message });
          notificarTelasInstantaneamente();
          res.json({ sucesso: true, excluidos: this.changes, versaoExclusao });
        });
      });
    });
    return;
  }

  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ erro: 'Nenhum palete selecionado.' });
  }

  const idsNum = ids.map((n) => Number(n)).filter(Boolean);
  db.serialize(() => {
    const stmt = db.prepare(`INSERT OR IGNORE INTO ids_excluidos (id) VALUES (?)`);
    idsNum.forEach((id) => stmt.run(id));
    stmt.finalize();

    const placeholders = idsNum.map(() => '?').join(',');
    db.run(`DELETE FROM paletes_bipados WHERE id IN (${placeholders})`, idsNum, function (err) {
      if (err) return res.status(500).json({ erro: err.message });
      notificarTelasInstantaneamente();
      res.json({ sucesso: true, excluidos: this.changes, versaoExclusao });
    });
  });
});

// 7. EXCLUIR UM ÚNICO PALETE
app.delete('/api/paletes/:id', (req, res) => {
  const idNum = Number(req.params.id);
  versaoExclusao = Date.now();

  db.serialize(() => {
    db.run(`INSERT OR IGNORE INTO ids_excluidos (id) VALUES (?)`, [idNum]);
    db.run(`DELETE FROM paletes_bipados WHERE id = ?`, [idNum], function (err) {
      if (err) return res.status(500).json({ erro: err.message });
      notificarTelasInstantaneamente();
      res.json({ sucesso: true, versaoExclusao });
    });
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Servidor FINART WMS rodando na porta ${PORT}`);
});