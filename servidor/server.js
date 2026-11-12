const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '15mb' }));

const publicDir = path.join(__dirname, 'public');
app.use(express.static(publicDir));
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  const p1 = path.join(publicDir, 'index.html');
  const p2 = path.join(__dirname, 'index.html');
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  res.send('Servidor FINART Online');
});

app.get('/coletor.html', (req, res) => {
  const p1 = path.join(publicDir, 'coletor.html');
  const p2 = path.join(__dirname, 'coletor.html');
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  res.status(404).send('Arquivo coletor.html não encontrado');
});

// ============================================================================
// 1. CANAL SSE (SERVER-SENT EVENTS) COM HEARTBEAT E ANTI-BUFFERING (0ms)
// ============================================================================
let clientesTempoReal = [];
let sequenciaClienteSSE = 1;
let versaoExclusao = Date.now();
let restaurandoBackupMutex = false;

function notificarTelasInstantaneamente(eventoExtra = {}) {
  const payload = JSON.stringify({
    atualizar: true,
    timestamp: Date.now(),
    versaoExclusao,
    ...eventoExtra
  });
  clientesTempoReal.forEach((cliente) => {
    try {
      cliente.res.write(`data: ${payload}\n\n`);
    } catch (e) {}
  });
}

app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');

  const clienteId = `${Date.now()}_${sequenciaClienteSSE++}`;
  const novoCliente = { id: clienteId, res };
  clientesTempoReal.push(novoCliente);

  res.write(`data: ${JSON.stringify({ conectado: true, timestamp: Date.now(), versaoExclusao })}\n\n`);

  const timerHeartbeat = setInterval(() => {
    try {
      res.write(`: heartbeat ${Date.now()}\n\n`);
    } catch (e) {
      clearInterval(timerHeartbeat);
    }
  }, 20000);

  req.on('close', () => {
    clearInterval(timerHeartbeat);
    clientesTempoReal = clientesTempoReal.filter((c) => c.id !== clienteId);
  });
});

// ============================================================================
// 2. MOTOR DE SANITIZAÇÃO, TIPAGEM ESTRITA E FUZZY MATCHING (BASE FIEL 80%)
// ============================================================================
function sanitizarPedido(valor = '') {
  // Remove datas antes de sanitizar pedido
  const semDatas = String(valor || '')
    .replace(/\b\d{1,2}\s*[\/.\-]\s*\d{1,2}\s*[\/.\-]\s*\d{2,4}\b/g, '')
    .trim();
  const semPonto = semDatas.split('.')[0];
  return semPonto.replace(/\D/g, '').slice(0, 6);
}

function sanitizarLote(valor = '', pedidoFallback = '') {
  const semDatas = String(valor || '')
    .replace(/\b\d{1,2}\s*\/\s*\d{1,2}(?:\s*\/\s*\d{2,4})?\b/g, '')
    .trim();
  let limpo = semDatas.replace(/,/g, '.').replace(/[^0-9.]/g, '');
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

function validarInteiroPaletizacao(valor, min = 1, max = 50000) {
  if (valor === null || valor === undefined) return 0;
  const str = String(valor).trim();
  // Rejeita imediatamente se contiver barra de data (ex: 12/11)
  if (str.includes('/')) return 0;
  const num = parseInt(str.replace(/\D/g, ''), 10);
  if (!Number.isInteger(num) || num < min || num > max) return 0;
  return num;
}

function corrigirCodigoItem(itemBruto = '', produtoTexto = '') {
  let cod = String(itemBruto || '').trim().toUpperCase().replace(/\s+/g, '');
  const prod = String(produtoTexto || '').toUpperCase();

  // Rejeita se o código de item parecer uma data
  if (/^\d{1,2}[\/.\-]\d{1,2}/.test(cod)) {
    cod = '';
  }

  // Extrai código terminado em MP no final da descrição, se existir
  const matchMP = prod.match(/\b((?:PT|FR|IT|CD|MP|EM)-[A-Z0-9]{3,8})MP\b/i) || prod.match(/\b((?:PT|FR)-\d{4,6})MP\b/i);
  if (matchMP) {
    cod = matchMP[1].toUpperCase();
  }

  if (cod === 'PT-7808' || cod === 'PT7808') {
    return 'PT-7908';
  }
  if (cod === '19935') return '719935';
  if (cod === '21467') return '721467';
  return cod;
}

function distanciaLevenshtein(a = '', b = '') {
  const s1 = String(a || '').toUpperCase().trim();
  const s2 = String(b || '').toUpperCase().trim();
  if (s1 === s2) return 0;
  if (!s1.length) return s2.length;
  if (!s2.length) return s1.length;

  const matriz = [];
  for (let i = 0; i <= s2.length; i++) matriz[i] = [i];
  for (let j = 0; j <= s1.length; j++) matriz[0][j] = j;

  for (let i = 1; i <= s2.length; i++) {
    for (let j = 1; j <= s1.length; j++) {
      if (s2.charAt(i - 1) === s1.charAt(j - 1)) {
        matriz[i][j] = matriz[i - 1][j - 1];
      } else {
        matriz[i][j] = Math.min(
          matriz[i - 1][j - 1] + 1,
          matriz[i][j - 1] + 1,
          matriz[i - 1][j] + 1
        );
      }
    }
  }
  return matriz[s2.length][s1.length];
}

// Retorna percentual de similaridade entre 0 e 100
function calcularSimilaridadeFuzzy(a = '', b = '') {
  const s1 = String(a || '').toUpperCase().trim().replace(/\s+/g, ' ');
  const s2 = String(b || '').toUpperCase().trim().replace(/\s+/g, ' ');
  if (!s1 && !s2) return 100;
  if (!s1 || !s2) return 0;
  if (s1 === s2) return 100;
  const dist = distanciaLevenshtein(s1, s2);
  const maxLen = Math.max(s1.length, s2.length);
  return Math.max(0, Math.round(((maxLen - dist) / maxLen) * 100));
}

// Avalia um candidato de leitura contra um registro da Base Fiel (limiar >= 80%)
function avaliarMatchBaseFiel(leitura, base) {
  if (!base) return null;

  const simLote = leitura.lote && base.lote ? calcularSimilaridadeFuzzy(leitura.lote, base.lote) : 0;
  const simPedido = leitura.pedido && base.pedido ? calcularSimilaridadeFuzzy(leitura.pedido, base.pedido) : 0;
  const simItem = leitura.item && base.item ? calcularSimilaridadeFuzzy(leitura.item, base.item) : 0;
  const simProduto = leitura.produto && base.produto ? calcularSimilaridadeFuzzy(leitura.produto, base.produto) : 0;
  const simCliente = leitura.cliente && base.cliente ? calcularSimilaridadeFuzzy(leitura.cliente, base.cliente) : 0;

  // Calcula a maior similaridade de identificador chave (Lote, Pedido, Item) + composição
  const metricasValidas = [];
  if (simLote > 0) metricasValidas.push(simLote * 1.25);
  if (simItem > 0) metricasValidas.push(simItem * 1.25);
  if (simPedido > 0) metricasValidas.push(simPedido * 1.1);
  if (simProduto > 0) metricasValidas.push(simProduto);
  if (simCliente > 0) metricasValidas.push(simCliente);

  const melhorChave = Math.max(simLote, simItem, simPedido, simProduto);
  const mediaPonderada =
    metricasValidas.length > 0
      ? Math.min(100, Math.round(metricasValidas.reduce((acc, v) => acc + v, 0) / metricasValidas.length))
      : 0;

  const scoreFinal = Math.max(melhorChave, mediaPonderada);

  // Verifica se pertence ao mesmo contexto (Item igual ou >= 80% E Lote/Pedido >= 80%, ou Lote idêntico)
  const mesmoContexto =
    (simItem >= 80 && (simLote >= 75 || !leitura.lote)) ||
    (simLote >= 80 && (simItem >= 75 || !leitura.item)) ||
    (simItem === 100 && simLote >= 70) ||
    (simLote === 100 && simItem >= 70);

  if (!mesmoContexto || scoreFinal < 80) return null;

  const divergencias = [];
  if (leitura.lote && leitura.lote !== base.lote) {
    divergencias.push(`Lote lido "${leitura.lote}" → Base Fiel "${base.lote}"`);
  }
  if (leitura.pedido && leitura.pedido !== base.pedido) {
    divergencias.push(`Pedido lido "${leitura.pedido}" → Base Fiel "${base.pedido}"`);
  }
  if (leitura.item && leitura.item.toUpperCase() !== base.item.toUpperCase()) {
    divergencias.push(`Item lido "${leitura.item}" → Base Fiel "${base.item}"`);
  }
  if (leitura.cliente && leitura.cliente.toUpperCase() !== base.cliente.toUpperCase()) {
    divergencias.push(`Cliente lido "${leitura.cliente}" → Base Fiel "${base.cliente}"`);
  }
  if (leitura.qtd_pecas && Number(leitura.qtd_pecas) !== Number(base.qtd_pecas_por_volume)) {
    divergencias.push(`Peças/Vol lido "${leitura.qtd_pecas}" → Base Fiel "${base.qtd_pecas_por_volume}"`);
  }

  return {
    similaridade: scoreFinal,
    temDivergencia: divergencias.length > 0,
    divergencias,
    base
  };
}

// ============================================================================
// 3. BANCO DE DADOS SQLITE3 (AGNÓSTICO E AUTO-APRENDIZADO CONTÍNUO)
// ============================================================================
const dbPath = path.join(__dirname, 'finart.db');
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) console.error('Erro ao abrir banco:', err.message);
  else console.log(`✅ Banco FINART conectado em: ${dbPath}`);
});

const SEMENTE_INICIAL_CATALOGO = {
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
  '721467': {
    cliente: 'FLORA PRODS DE HIG E LIM',
    produto: 'FRA 300 ML FLORA 24 410 PUSH PULL VERDE CLARO 04.001.04.0498 ATIVADOR',
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
  },
  'PT-12590': {
    cliente: 'DEVINTEX COSMETICOS',
    produto: 'PT 500 GRS REDONDO 3 ENTRADAS BRANCO 2515 PT-12590MP',
    padrao_volumes: 15,
    qtd_pecas: 120,
    embalagem: 'CAIXA'
  }
};

db.serialize(() => {
  db.run(`PRAGMA journal_mode = WAL`);
  db.run(`PRAGMA synchronous = NORMAL`);
  db.run(`PRAGMA busy_timeout = 5000`);

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

  db.run(`
    CREATE TABLE IF NOT EXISTS ids_excluidos (
      id INTEGER PRIMARY KEY
    )
  `);

  db.run(`CREATE INDEX IF NOT EXISTS idx_paletes_lote_item ON paletes_bipados (lote, item)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_paletes_item ON paletes_bipados (item)`);

  db.run(`DELETE FROM catalogo_itens WHERE item = 'PT-7808' OR item = 'PT7808'`);
  db.run(`UPDATE paletes_bipados SET item = 'PT-7908' WHERE item = 'PT-7808' OR item = 'PT7808'`);

  // Usa INSERT OR IGNORE para preservar padrões aprendidos/editados pelo operador
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  Object.entries(SEMENTE_INICIAL_CATALOGO).forEach(([cod, d]) => {
    stmt.run(cod, d.cliente, d.produto, d.padrao_volumes, d.qtd_pecas, d.embalagem);
  });
  stmt.finalize();

  // Higieniza registros antigos que possam ter datas no campo volume_etiqueta ou pontos duplicados
  db.all(`SELECT id, pedido, lote, item, produto, volume_etiqueta FROM paletes_bipados`, [], (err, rows) => {
    if (!err && Array.isArray(rows) && rows.length > 0) {
      rows.forEach((r) => {
        const pedOk = sanitizarPedido(r.pedido || r.lote);
        const loteOk = sanitizarLote(r.lote, pedOk);
        const itemOk = corrigirCodigoItem(r.item, r.produto);
        const volLimpo = /^\d{1,2}\/\d{1,2}(\/\d{2,4})?$/.test(String(r.volume_etiqueta || '').trim())
          ? ''
          : r.volume_etiqueta;
        if (pedOk !== r.pedido || loteOk !== r.lote || itemOk !== r.item || volLimpo !== r.volume_etiqueta) {
          db.run(
            `UPDATE paletes_bipados SET pedido = ?, lote = ?, item = ?, volume_etiqueta = ? WHERE id = ?`,
            [pedOk, loteOk, itemOk, volLimpo, r.id]
          );
        }
      });
    }
  });
});

// Cálculo 100% dinâmico de padrão de volumes (baseado no histórico/catálogo do item ou cliente)
function calcularPadraoPaleteDinamico(item = '', cliente = '', padraoSalvoItem = 0, catalogoRows = []) {
  if (padraoSalvoItem && Number(padraoSalvoItem) > 0) {
    return Number(padraoSalvoItem);
  }

  const itUpper = String(item || '').toUpperCase().trim();
  const cliUpper = String(cliente || '').toUpperCase().trim();

  if (itUpper) {
    const achouItem = catalogoRows.find((c) => c.item.toUpperCase() === itUpper);
    if (achouItem && achouItem.padrao_volumes_palete > 0) {
      return Number(achouItem.padrao_volumes_palete);
    }
  }

  if (cliUpper) {
    const itensDoCliente = catalogoRows.filter(
      (c) => c.cliente.toUpperCase() === cliUpper && c.padrao_volumes_palete >= 10
    );
    if (itensDoCliente.length > 0) {
      // Usa a moda (padrão mais frequente daquele cliente)
      const contagem = {};
      let melhorPadrao = itensDoCliente[0].padrao_volumes_palete;
      let maxFreq = 0;
      itensDoCliente.forEach((c) => {
        const v = c.padrao_volumes_palete;
        contagem[v] = (contagem[v] || 0) + 1;
        if (contagem[v] > maxFreq) {
          maxFreq = contagem[v];
          melhorPadrao = v;
        }
      });
      return Number(melhorPadrao);
    }
  }

  return 15;
}

// ============================================================================
// 4. PARSER DE OCR RECONSTRUÍDO (SEPARAÇÃO ESTRITA DATA vs VOLUME + AGNÓSTICO)
// ============================================================================
function higienizarDatasERuidosOCR(textoBruto = '') {
  const datasEncontradas = [];

  let limpo = String(textoBruto || '')
    .replace(/\r/g, '')
    // Remove marcações de papelão/caixa e máquina
    .replace(/\bPA[A-Z]{2,5}\s*\d{4,9}\b/gi, ' ')
    .replace(/\bMQN\s*\d{1,4}\b/gi, ' ')
    .replace(/N[º°o]\s*PED\.?\s*CLIENTE\s*:?/gi, ' ')
    // Remove blocos explícitos de DATA, FABRICAÇÃO, VALIDADE, VENCIMENTO, EMISSÃO e HORA
    .replace(
      /\b(?:DATA(?:\s*DE\s*FAB(?:RICA[CÇ][AÃ]O)?)?|DT\.?\s*FAB|FAB(?:RICA[CÇ][AÃ]O)?|VAL(?:IDADE)?|VENC(?:IMENTO)?|EMISS[AÃ]O)\s*[:.\-]?\s*(\d{1,2}\s*[\/.\-]\s*\d{1,2}(?:\s*[\/.\-]\s*\d{2,4})?)/gi,
      (m, d) => {
        datasEncontradas.push(d);
        return ' [DATA_REMOVIDA] ';
      }
    )
    // Remove qualquer data completa DD/MM/AAAA, DD/MM/AA, DD.MM.AAAA ou AAAA-MM-DD
    .replace(/\b(0?[1-9]|[12]\d|3[01])\s*[\/.\-]\s*(0?[1-9]|1[0-2])\s*[\/.\-]\s*(20\d{2}|\d{2})\b/g, (m) => {
      datasEncontradas.push(m);
      return ' [DATA_REMOVIDA] ';
    })
    // Remove datas parciais MM/AAAA (ex: 11/2026, 08/2025)
    .replace(/\b(0?[1-9]|1[0-2])\s*\/\s*(20[2-3]\d)\b/g, (m) => {
      datasEncontradas.push(m);
      return ' [DATA_REMOVIDA] ';
    })
    // Remove horários HH:MM(:SS)
    .replace(/\b([01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d)?\b/g, ' ')
    // Normaliza cabeçalhos comuns de etiquetas logísticas com ruído de OCR
    .replace(/\b(?:OTD|0TD|QTO|QID|QT0|QTD\.|QUANT(?:IDADE)?)\s*:?/gi, 'QTD:')
    .replace(/\b(?:1TEM|lTEM|!TEM|ITEN|IIEM|\|TEM|TEM:|COD(?:IGO)?\s*ITEM)\s*:?/gi, 'ITEM:')
    .replace(/\b(?:PR0DUTO|PRODUT0|PRODUTO\.|DESCRI[CÇ][AÃ]O)\s*:?/gi, 'PRODUTO:')
    .replace(/\b(?:L0TE|LQTE)\s*:?/gi, 'LOTE:')
    .replace(/\b(?:PED1DO|PEDID0|PEDIDO\.)\s*:?/gi, 'PEDIDO:')
    .replace(/\b(?:CL1ENTE|CLlENTE|DESTINAT[AÁ]RIO|EMPRESA)\s*:?/gi, 'CLIENTE:')
    .replace(/PI\s*-\s*(\d)/gi, 'PT-$1')
    .replace(/P1\s*-\s*(\d)/gi, 'PT-$1')
    .replace(/F8\s*-\s*(\d)/gi, 'FR-$1')
    .replace(/ER\s*-\s*(\d)/gi, 'FR-$1');

  return { textoLimpo: limpo, datasEncontradas };
}

// Extrai o nome de QUALQUER cliente da etiqueta de forma 100% agnóstica
function extrairClienteAgnostico(textoLimpo = '', clientesConhecidos = []) {
  // 1. Verifica se algum cliente já conhecido no banco está escrito na etiqueta (exato ou fuzzy >= 85%)
  const textoUpper = textoLimpo.toUpperCase();
  for (const cliBanco of clientesConhecidos) {
    const nomeLimpo = String(cliBanco || '').toUpperCase().trim();
    if (nomeLimpo.length >= 3 && textoUpper.includes(nomeLimpo)) {
      return nomeLimpo;
    }
    // Verifica primeira palavra forte do cliente (ex: >= 5 letras)
    const primeiraPalavra = nomeLimpo.split(/\s+/)[0];
    if (primeiraPalavra.length >= 6 && textoUpper.includes(primeiraPalavra)) {
      return nomeLimpo;
    }
  }

  // 2. Extração explícita via campo CLIENTE: / DESTINATÁRIO: / EMPRESA:
  const mCliExplicito = textoLimpo.match(
    /CLIENTE\s*[:.\-]?\s*([A-ZÀ-Ú0-9][A-ZÀ-Ú0-9\s.\-&\/]{2,55}?)(?=\bPEDIDO|\bITEM|\bPRODUTO|\bLOTE|\bQTD|\bVOL|\bCNPJ|\[DATA_REMOVIDA\]|\n|$)/i
  );
  if (mCliExplicito) {
    const candidato = mCliExplicito[1]
      .replace(/[^A-ZÀ-Ú0-9\s.\-&]/gi, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim()
      .toUpperCase();
    if (candidato.length >= 3 && !/^(PEDIDO|ITEM|PRODUTO|LOTE|QTD|DATA|VOLUME)$/.test(candidato)) {
      // Verifica se tem alta similaridade (>= 82%) com algum cliente já cadastrado para evitar duplicar por 1 letra
      for (const cliBanco of clientesConhecidos) {
        if (calcularSimilaridadeFuzzy(candidato, cliBanco) >= 82) {
          return cliBanco.toUpperCase();
        }
      }
      return candidato;
    }
  }

  // 3. Heurística de Linha de Cabeçalho (para etiquetas onde o nome do cliente vem no topo sem o rótulo "CLIENTE:")
  const linhas = textoLimpo
    .split('\n')
    .map((l) => l.replace(/\[DATA_REMOVIDA\]/g, '').trim().toUpperCase())
    .filter((l) => l.length >= 4);

  const palavrasReservadas = /^(PEDIDO|ITEM|PRODUTO|LOTE|QTD|QUANTIDADE|VOLUME|PESO|CAIXA|PALETE|FINART|CONTROLE|ETIQUETA|TURNO|OPERADOR|DATA|VALIDADE|FAB|PT-|FR-|\d+)/i;

  for (let i = 0; i < Math.min(linhas.length, 4); i++) {
    const linha = linhas[i];
    if (palavrasReservadas.test(linha)) continue;
    // Deve conter pelo menos uma palavra alfabética de >= 4 letras e não ser descrição de frasco/pote
    if (/[A-ZÀ-Ú]{4,}/.test(linha) && !/\b(GRS|ML|MG|KG|LITROS|REDONDO|CONICO|PUSH\s*PULL)\b/i.test(linha)) {
      const limpo = linha
        .replace(/[^A-ZÀ-Ú0-9\s.\-&]/gi, ' ')
        .replace(/\s{2,}/g, ' ')
        .trim()
        .slice(0, 45);
      if (limpo.length >= 4) {
        for (const cliBanco of clientesConhecidos) {
          if (calcularSimilaridadeFuzzy(limpo, cliBanco) >= 82) {
            return cliBanco.toUpperCase();
          }
        }
        return limpo;
      }
    }
  }

  return '';
}

app.post('/api/interpretar-bipe', (req, res) => {
  const { codigoLido, textoEtiqueta, baseFielCliente } = req.body;
  const brCode = (codigoLido || '').trim();
  const ocrRaw = (textoEtiqueta || '').trim();

  if (!brCode && !ocrRaw) {
    return res.status(400).json({ erro: 'Nenhuma imagem recebida.' });
  }

  // ETAPA 1: Higienização estrita de Datas (DD/MM/AAAA, FAB, VAL) e ruídos
  const { textoLimpo: texto } = higienizarDatasERuidosOCR(`${brCode}\n${ocrRaw}`);

  let candidatosItem = [];
  let candidatosPedido = [];
  let candidatosLote = [];
  let qtdPorVolume = 0;
  let qtdVolumesEtiqueta = 0;
  let produtoLido = '';
  let volumeSequencialEtiqueta = '';

  // 1. Código de Barras / QR estruturado (ITEM;LOTE;QTD)
  const mBarcode = texto.match(/([A-Z]{2,4}\s*-\s*[A-Z0-9]{3,8}|\d{5,8})\s*[;:,]\s*(\d{5,8})\s*[;:,]\s*(\d{1,5})/i);
  if (mBarcode) {
    candidatosItem.push(mBarcode[1].replace(/\s+/g, '').toUpperCase());
    const lb = mBarcode[2].trim();
    const pedBar = sanitizarPedido(lb.slice(0, -1));
    const loteBar = sanitizarLote(`${lb.slice(0, -1)}.${lb.slice(-1)}`, pedBar);
    if (pedBar) candidatosPedido.push(pedBar);
    if (loteBar) candidatosLote.push(loteBar);
    qtdPorVolume = validarInteiroPaletizacao(mBarcode[3], 1, 25000);
  }

  // 2. PEDIDO explícito
  const regexPedidos = /PEDIDO\s*[:.\-]?\s*(\d{4,6})(?!\.\d)/gi;
  let mPed;
  while ((mPed = regexPedidos.exec(texto)) !== null) {
    const p = sanitizarPedido(mPed[1]);
    if (p.length >= 4) {
      candidatosPedido.push(p);
      candidatosLote.push(sanitizarLote('', p));
    }
  }

  // 3. LOTE explícito ou padrão numérico com sufixo .1 / .2
  const regexLotes = /(?:LOTE\s*[:.\-]?\s*(\d{4,6}\s*[\.,]\s*\d)|\b(00\d{4}\s*[\.,]\s*\d)\b)/gi;
  let mLote;
  while ((mLote = regexLotes.exec(texto)) !== null) {
    const rawL = mLote[1] || mLote[2];
    const l = sanitizarLote(rawL);
    const p = sanitizarPedido(l);
    if (l) candidatosLote.push(l);
    if (p) candidatosPedido.push(p);
  }

  // 4. Padrão numérico de pedido (00XXXX)
  const regexZeroZero = /\b(00\d{4})\b/g;
  let mZZ;
  while ((mZZ = regexZeroZero.exec(texto)) !== null) {
    const p = sanitizarPedido(mZZ[1]);
    candidatosPedido.push(p);
    candidatosLote.push(sanitizarLote('', p));
  }

  // 5. Códigos de item alfanuméricos (PT-XXXX, FR-XXXX, ou qualquer XX-XXXX)
  const regexTodosPrefixados = /\b([A-Z]{2,3}\s*-\s*\d{3,7})(?:MP)?\b/gi;
  let matchPref;
  while ((matchPref = regexTodosPrefixados.exec(texto)) !== null) {
    let c = matchPref[1].replace(/\s+/g, '').toUpperCase();
    candidatosItem.push(c);
  }

  // 6. Campo ITEM explícito (aceita códigos de qualquer cliente novo)
  const mItemExplicito = texto.match(/ITEM\s*[:.\-]?\s*([A-Z0-9\-]{3,15})/i);
  if (mItemExplicito) {
    const cand = mItemExplicito[1].trim().toUpperCase();
    if (!cand.startsWith('00') && !/^(DATA|LOTE|PEDIDO|PRODUTO|QTD)$/.test(cand)) {
      candidatosItem.push(cand);
    }
  }

  // 7. Códigos numéricos de 5 a 7 dígitos (excluindo anos e pedidos)
  const regexDigitosItem = /\b([1-9]\d{4,6})\b/g;
  let mDig;
  while ((mDig = regexDigitosItem.exec(texto)) !== null) {
    const numItem = mDig[1];
    if (!candidatosPedido.includes(numItem) && !/^202\d/.test(numItem)) {
      candidatosItem.push(numItem);
    }
  }

  // 8. Captura Multilinha da Descrição do Produto
  const mBlocoProduto =
    texto.match(/PRODUTO\s*[:.\-]?\s*([\s\S]{4,160}?)(?=\bQTD[:\s]|\bLOTE[:\s]|\bVOL|\bPESO|\[DATA_REMOVIDA\]|\b00\d{4}\.\d\b|$)/i) ||
    texto.match(/\b((?:PT|FRA|POTE|FRASCO|TAMPA|CAIXA)\s+\d{2,4}\s*(?:GRS|ML|G|KG)[\s\S]{4,140}?)(?=\bQTD[:\s]|\bLOTE[:\s]|\[DATA_REMOVIDA\]|\b00\d{4}\.\d\b|$)/i);

  if (mBlocoProduto) {
    let prodLimpo = mBlocoProduto[1]
      .replace(/\[DATA_REMOVIDA\]/g, ' ')
      .replace(/\n+/g, ' ')
      .replace(/[^A-Z0-9À-Ú\s.\-\/]/gi, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim()
      .toUpperCase();

    const mAteMP = prodLimpo.match(/^(.+?[A-Z]{2,3}-\d{3,6}MP)/i);
    if (mAteMP) prodLimpo = mAteMP[1].toUpperCase();
    if (prodLimpo.length >= 4) produtoLido = prodLimpo;
  }

  // 9. Extração ESTRITA de Quantidade de Peças por Volume (Ignorando anos 2020-2035 e datas!)
  if (!qtdPorVolume) {
    const mQtdDireta = texto.match(/QTD\s*[:.\-\s]*(\d{1,5})\b(?!\s*[\/.\-]\s*\d)/i);
    if (mQtdDireta) {
      const val = validarInteiroPaletizacao(mQtdDireta[1], 1, 20000);
      const ehAno = val >= 2020 && val <= 2035;
      if (val >= 5 && !ehAno && !candidatosPedido.includes(String(val))) {
        qtdPorVolume = val;
      }
    }
  }
  if (!qtdPorVolume) {
    const mPecasUnid = texto.match(/\b(\d{2,4})\s*(?:P[CÇ]S|PE[CÇ]AS|UN(?:ID)?)\b/i);
    if (mPecasUnid) {
      qtdPorVolume = validarInteiroPaletizacao(mPecasUnid[1], 5, 20000);
    }
  }

  // 10. Extração ESTRITA de Volume / Caixas (SOMENTE se explícito com VOL/CAIXAS, NUNCA aceitando datas!)
  const mVolInteiro = texto.match(/\b(?:VOL(?:UMES?)?|CAIXAS|SACOS|SACARIAS)\s*[:.\-]?\s*(\d{1,3})\b(?!\s*\/\s*\d{1,2}\s*\/)/i);
  if (mVolInteiro) {
    const vInt = validarInteiroPaletizacao(mVolInteiro[1], 1, 150);
    if (vInt > 0) qtdVolumesEtiqueta = vInt;
  }

  // Sequencial de volume (ex: VOL: 112/160), exigindo que NÃO seja data (denominador > 12 quando sem rótulo VOL)
  const mVolSeq = texto.match(/\bVOL(?:UME)?\s*[:.\-]?\s*(\d{1,4}\s*\/\s*\d{1,4})\b/i);
  if (mVolSeq) {
    volumeSequencialEtiqueta = mVolSeq[1].replace(/\s+/g, '');
  }

  db.all(`SELECT * FROM catalogo_itens WHERE item != 'PT-7808'`, [], (errCat, catalogoRows) => {
    if (errCat) return res.status(500).json({ erro: errCat.message });

    db.all(`SELECT * FROM paletes_bipados ORDER BY id DESC LIMIT 250`, [], (errHist, histRows) => {
      const historico = histRows || [];

      // Lista dinâmica de todos os clientes já conhecidos no banco
      const clientesConhecidos = [
        ...new Set([
          ...catalogoRows.map((c) => c.cliente),
          ...historico.map((h) => h.cliente)
        ].filter(Boolean))
      ];

      const clienteExtraidoEtiqueta = extrairClienteAgnostico(texto, clientesConhecidos);

      // Escolha do código do item (Exato ou Fuzzy >= 85% contra o catálogo/histórico)
      let itemEscolhido = corrigirCodigoItem(candidatosItem[0] || '', produtoLido);

      for (const candBruto of candidatosItem) {
        const cand = corrigirCodigoItem(candBruto, produtoLido);
        const exatoCat = catalogoRows.find((r) => r.item.toUpperCase() === cand.toUpperCase());
        const exatoHist = historico.find((h) => h.item.toUpperCase() === cand.toUpperCase());
        if (exatoCat || exatoHist) {
          itemEscolhido = (exatoCat || exatoHist).item;
          break;
        }
      }

      // Se não achou item pelo código mas leu a descrição do produto, tenta cruzar pela descrição (>= 80% fuzzy)
      if (!itemEscolhido && produtoLido.length >= 8) {
        for (const cat of catalogoRows) {
          if (calcularSimilaridadeFuzzy(produtoLido, cat.produto) >= 80 || cat.produto.toUpperCase().includes(produtoLido.slice(0, 15))) {
            itemEscolhido = cat.item;
            break;
          }
        }
      }

      let loteLidoNaCamera = candidatosLote[0] || '';
      let pedidoLidoNaCamera = candidatosPedido[0] || sanitizarPedido(loteLidoNaCamera);

      for (const candL of candidatosLote) {
        if (historico.some((h) => h.lote === candL)) {
          loteLidoNaCamera = candL;
          pedidoLidoNaCamera = sanitizarPedido(candL);
          break;
        }
      }

      // ======================================================================
      // MOTOR DE "BASE FIEL" COM FUZZY MATCHING (>= 80% DE SIMILARIDADE)
      // ======================================================================
      const leituraBruta = {
        item: itemEscolhido,
        lote: loteLidoNaCamera,
        pedido: pedidoLidoNaCamera,
        cliente: clienteExtraidoEtiqueta,
        produto: produtoLido,
        qtd_pecas: qtdPorVolume
      };

      // Candidatos a Base Fiel: 1º a Base Fiel enviada pelo Coletor na sessão, depois os lotes ativos no banco
      const basesCandidatas = [];
      if (baseFielCliente && baseFielCliente.item && baseFielCliente.lote) {
        basesCandidatas.push(baseFielCliente);
      }
      historico.forEach((h) => {
        if (!basesCandidatas.some((b) => b.item === h.item && b.lote === h.lote)) {
          basesCandidatas.push(h);
        }
      });

      let melhorMatchFiel = null;
      for (const baseCand of basesCandidatas) {
        const avaliacao = avaliarMatchBaseFiel(leituraBruta, baseCand);
        if (avaliacao && (!melhorMatchFiel || avaliacao.similaridade > melhorMatchFiel.similaridade)) {
          melhorMatchFiel = avaliacao;
        }
      }

      let sugestaoDivergencia = null;
      let loteFinal = loteLidoNaCamera;
      let pedidoFinal = pedidoLidoNaCamera;

      if (melhorMatchFiel && melhorMatchFiel.similaridade >= 80) {
        const bf = melhorMatchFiel.base;
        if (melhorMatchFiel.temDivergencia) {
          sugestaoDivergencia = {
            tipo: 'BASE_FIEL_FUZZY_80',
            similaridade: melhorMatchFiel.similaridade,
            auto_corrigido: false,
            divergencias: melhorMatchFiel.divergencias,
            mensagem: `Similaridade de ${melhorMatchFiel.similaridade}% com a Base Fiel ativa (${bf.item} • Lote ${bf.lote}). Foram detectados ruídos de OCR (${melhorMatchFiel.divergencias.join(' | ')}). Toque abaixo para Auto-Corrigir com 1 clique!`,
            item_lido: itemEscolhido || '(não lido)',
            lote_lido: loteLidoNaCamera || '(não lido)',
            pedido_lido: pedidoLidoNaCamera || '(não lido)',
            cliente_lido: clienteExtraidoEtiqueta || '(não lido)',
            produto_lido: produtoLido || '(não lido)',
            qtd_pecas_lida: qtdPorVolume || 0,
            // Dados perfeitos da Base Fiel para substituição com 1 clique
            item_sugerido: bf.item,
            cliente_sugerido: bf.cliente,
            produto_sugerido: bf.produto,
            lote_sugerido: bf.lote,
            pedido_sugerido: bf.pedido,
            qtd_pecas_sugerida: bf.qtd_pecas_por_volume,
            qtd_volumes_sugerida: bf.qtd_volumes_palete
          };
        } else {
          // 100% compatível com a Base Fiel: preenche campos ausentes automaticamente
          itemEscolhido = bf.item;
          loteFinal = bf.lote;
          pedidoFinal = bf.pedido;
        }
      }

      const paletesDesteItem = itemEscolhido
        ? historico.filter((h) => h.item.toUpperCase() === itemEscolhido.toUpperCase())
        : [];
      const catItem = itemEscolhido
        ? catalogoRows.find((r) => r.item.toUpperCase() === itemEscolhido.toUpperCase())
        : null;
      const ultimoPaleteDoItem = paletesDesteItem[0] || null;
      const ultimoPaletePadraoDoItem = paletesDesteItem.find((p) => p.qtd_volumes_palete >= 10) || ultimoPaleteDoItem;
      const refItemConfiavel = ultimoPaleteDoItem || catItem;

      // Trava 422 contra leitura vazia
      if (!itemEscolhido && !pedidoFinal && !loteFinal && !produtoLido && !clienteExtraidoEtiqueta) {
        return res.status(422).json({
          erro: 'Etiqueta não identificada na imagem. Enquadre a etiqueta com boa iluminação e tente novamente.'
        });
      }

      // Resolução 100% Agnóstica do Cliente (Prioriza Base Confiável do Item -> Etiqueta Lida -> Vazio p/ operador confirmar)
      const clienteFinal =
        (refItemConfiavel && refItemConfiavel.cliente) ||
        clienteExtraidoEtiqueta ||
        '';

      const produtoFinal =
        (catItem && catItem.produto) ||
        (ultimoPaleteDoItem && ultimoPaleteDoItem.produto) ||
        produtoLido ||
        '';

      const qtdPecasFinal =
        (ultimoPaleteDoItem && ultimoPaleteDoItem.qtd_pecas_por_volume > 0 ? ultimoPaleteDoItem.qtd_pecas_por_volume : 0) ||
        (catItem && catItem.qtd_padrao_pecas > 0 ? catItem.qtd_padrao_pecas : 0) ||
        qtdPorVolume ||
        0;

      const volumesDaBaseConfiavel =
        (ultimoPaletePadraoDoItem && ultimoPaletePadraoDoItem.qtd_volumes_palete > 0
          ? ultimoPaletePadraoDoItem.qtd_volumes_palete
          : 0) ||
        (catItem && catItem.padrao_volumes_palete > 0 ? catItem.padrao_volumes_palete : 0) ||
        qtdVolumesEtiqueta;

      const padraoVolumes = calcularPadraoPaleteDinamico(
        itemEscolhido,
        clienteFinal,
        volumesDaBaseConfiavel,
        catalogoRows
      );
      const tipoEmbalagem = (catItem && catItem.tipo_embalagem) || (padraoVolumes === 22 ? 'SACARIA' : 'CAIXA');

      res.json({
        item: itemEscolhido,
        pedido: pedidoFinal,
        lote: loteFinal,
        cliente: clienteFinal,
        produto: produtoFinal || (itemEscolhido ? `PRODUTO ITEM ${itemEscolhido}` : ''),
        tipoEmbalagem,
        volume_etiqueta: volumeSequencialEtiqueta,
        qtd_pecas_por_volume: qtdPecasFinal,
        qtd_volumes_sugerida: padraoVolumes,
        saldo_previsto: qtdPecasFinal * padraoVolumes,
        sugestao_divergencia: sugestaoDivergencia
      });
    });
  });
});

// ============================================================================
// 5. SALVAR PALETE E GRAVAR NOVOS CLIENTES/ITENS NA BASE FIEL AUTOMATICAMENTE
// ============================================================================
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

  const volumes = validarInteiroPaletizacao(qtd_volumes_palete, 1, 500);
  const pecasInput = validarInteiroPaletizacao(qtd_pecas_por_volume, 1, 100000);
  const itemLimpo = corrigirCodigoItem(item, produto);
  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);

  db.get(`SELECT * FROM catalogo_itens WHERE UPPER(TRIM(item)) = ?`, [itemLimpo], (errCat, catRow) => {
    db.get(
      `SELECT * FROM paletes_bipados WHERE UPPER(TRIM(lote)) = ? AND UPPER(TRIM(item)) = ? ORDER BY id DESC LIMIT 1`,
      [loteLimpo, itemLimpo],
      (errLote, loteRow) => {
        const clienteUnificado =
          String(cliente || '').trim().toUpperCase() ||
          (loteRow && loteRow.cliente) ||
          (catRow && catRow.cliente) ||
          '';
        const produtoUnificado =
          String(produto || '').trim().toUpperCase() ||
          (loteRow && loteRow.produto) ||
          (catRow && catRow.produto) ||
          `PRODUTO ${itemLimpo}`;
        const pecasUnificada =
          pecasInput ||
          (loteRow && loteRow.qtd_pecas_por_volume) ||
          (catRow && catRow.qtd_padrao_pecas) ||
          0;

        if (!loteLimpo || !itemLimpo || !clienteUnificado || pecasUnificada <= 0 || volumes <= 0) {
          return res.status(400).json({
            erro: 'Confirme Cliente, Lote, Item e Quantidades inteiras válidas antes de salvar.'
          });
        }

        const saldoTotalPecas = pecasUnificada * volumes;

        // Aprende automaticamente qualquer novo cliente, item e padrão de paletização
        const padraoParaAprender =
          volumes >= 10
            ? volumes
            : catRow
              ? catRow.padrao_volumes_palete
              : volumes;
        const tipoEmb = padraoParaAprender === 22 ? 'SACARIA' : 'CAIXA';

        // Garante que volume_etiqueta nunca salve uma data acidental
        const volEtiquetaLimpo = /^\d{1,2}\/\d{1,2}(\/\d{2,4})?$/.test(String(volume_etiqueta || '').trim())
          ? ''
          : String(volume_etiqueta || '').trim();

        db.serialize(() => {
          db.run(
            `INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [itemLimpo, clienteUnificado, produtoUnificado, padraoParaAprender, pecasUnificada, tipoEmb]
          );

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
            [
              pedidoLimpo,
              loteLimpo,
              itemLimpo,
              clienteUnificado,
              produtoUnificado,
              pecasUnificada,
              volumes,
              saldoTotalPecas,
              volEtiquetaLimpo
            ],
            function (err) {
              if (err) return res.status(500).json({ erro: err.message });
              const baseFielAtualizada = {
                id: this.lastID,
                cliente: clienteUnificado,
                pedido: pedidoLimpo,
                lote: loteLimpo,
                item: itemLimpo,
                produto: produtoUnificado,
                qtd_volumes_palete: volumes,
                qtd_pecas_por_volume: pecasUnificada,
                saldo_total_pecas: saldoTotalPecas
              };
              notificarTelasInstantaneamente({
                acao: 'novo_palete',
                id: this.lastID,
                item: itemLimpo,
                lote: loteLimpo,
                baseFiel: baseFielAtualizada
              });
              res.json({
                sucesso: true,
                id: this.lastID,
                pedido: pedidoLimpo,
                lote: loteLimpo,
                saldo_total_pecas: saldoTotalPecas,
                base_fiel: baseFielAtualizada
              });
            }
          );
        });
      }
    );
  });
});

// ============================================================================
// 6. AUTO-RESTAURAÇÃO DE BACKUP BLINDADA CONTRA CONCORRÊNCIA
// ============================================================================
app.post('/api/restaurar-backup', (req, res) => {
  if (restaurandoBackupMutex) {
    return res.json({ restaurado: false, motivo: 'Restauração já em andamento.' });
  }

  const { historico, catalogo } = req.body;
  restaurandoBackupMutex = true;

  db.serialize(() => {
    if (Array.isArray(catalogo) && catalogo.length > 0) {
      const stmtCat = db.prepare(`
        INSERT OR IGNORE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
        VALUES (?, ?, ?, ?, ?, ?)
      `);
      catalogo.forEach((c) => {
        const codCorrigido = corrigirCodigoItem(c.item, c.produto);
        if (codCorrigido && codCorrigido !== 'PT-7808') {
          stmtCat.run(
            codCorrigido,
            String(c.cliente || '').toUpperCase(),
            String(c.produto || '').toUpperCase(),
            c.padrao_volumes_palete || 15,
            c.qtd_padrao_pecas || 0,
            c.tipo_embalagem || 'CAIXA'
          );
        }
      });
      stmtCat.finalize();
    }

    if (!Array.isArray(historico) || historico.length === 0) {
      restaurandoBackupMutex = false;
      return res.json({ restaurado: false });
    }

    db.get(`SELECT COUNT(*) AS total FROM paletes_bipados`, [], (err, row) => {
      if (err || (row && row.total > 0)) {
        restaurandoBackupMutex = false;
        return res.json({ restaurado: false });
      }

      db.all(`SELECT id FROM ids_excluidos`, [], (errEx, excluidosRows) => {
        const setExcluidos = new Set((excluidosRows || []).map((r) => Number(r.id)));
        const validos = historico.filter((p) => !setExcluidos.has(Number(p.id)));

        if (validos.length === 0) {
          restaurandoBackupMutex = false;
          return res.json({ restaurado: false });
        }

        db.run(`BEGIN TRANSACTION`);
        const stmt = db.prepare(`
          INSERT OR IGNORE INTO paletes_bipados
          (id, pedido, lote, item, cliente, produto, qtd_pecas_por_volume, qtd_volumes_palete, saldo_total_pecas, volume_etiqueta, data_hora)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        validos.forEach((p) => {
          const pedOk = sanitizarPedido(p.pedido || p.lote);
          const loteOk = sanitizarLote(p.lote, pedOk);
          const itemOk = corrigirCodigoItem(p.item, p.produto);
          stmt.run(
            p.id,
            pedOk,
            loteOk,
            itemOk,
            String(p.cliente || '').toUpperCase(),
            String(p.produto || '').toUpperCase(),
            p.qtd_pecas_por_volume,
            p.qtd_volumes_palete,
            p.saldo_total_pecas,
            p.volume_etiqueta || '',
            p.data_hora
          );
        });
        stmt.finalize(() => {
          db.run(`COMMIT`, () => {
            restaurandoBackupMutex = false;
            notificarTelasInstantaneamente({ acao: 'backup_restaurado' });
            res.json({ restaurado: true, quantidade: validos.length });
          });
        });
      });
    });
  });
});

// ============================================================================
// 7. DASHBOARD CONSOLIDADO + HISTÓRICO + CATÁLOGO + CLIENTES DINÂMICOS
// ============================================================================
app.get('/api/dashboard', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');

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
      SUM(saldo_total_pecas) AS saldo_total_pecas,
      MAX(data_hora) AS ultima_atualizacao
    FROM paletes_bipados
    GROUP BY UPPER(TRIM(lote)), UPPER(TRIM(item))
    ORDER BY MAX(id) DESC
  `;

  const queryHistorico = `SELECT * FROM paletes_bipados ORDER BY id DESC LIMIT 400`;
  const queryCatalogo = `SELECT * FROM catalogo_itens WHERE item != 'PT-7808' ORDER BY item ASC`;

  db.all(queryConsolidado, [], (err, consolidado) => {
    if (err) return res.status(500).json({ erro: err.message });
    db.all(queryHistorico, [], (err2, historico) => {
      if (err2) return res.status(500).json({ erro: err2.message });
      db.all(queryCatalogo, [], (err3, catalogo) => {
        res.json({
          consolidado: consolidado || [],
          historico: historico || [],
          catalogo: catalogo || [],
          versaoExclusao
        });
      });
    });
  });
});

// ============================================================================
// 8. EDITAR PALETE E ATUALIZAR PADRÃO NA BASE FIEL / CATÁLOGO
// ============================================================================
app.put('/api/paletes/:id', (req, res) => {
  const { id } = req.params;
  const { lote, pedido, cliente, item, produto, qtd_pecas_por_volume, qtd_volumes_palete, volume_etiqueta } = req.body;

  const pecasUn = validarInteiroPaletizacao(qtd_pecas_por_volume, 1, 100000);
  const volumes = validarInteiroPaletizacao(qtd_volumes_palete, 1, 500);
  if (pecasUn <= 0 || volumes <= 0) return res.status(400).json({ erro: 'Valores inválidos.' });

  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);
  const itemLimpo = corrigirCodigoItem(item, produto);
  const clienteLimpo = String(cliente || '').trim().toUpperCase();
  const produtoLimpo = String(produto || '').trim().toUpperCase();
  const novoSaldoTotal = pecasUn * volumes;

  db.serialize(() => {
    db.run(
      `UPDATE paletes_bipados 
       SET lote = ?, pedido = ?, cliente = ?, item = ?, produto = ?, 
           qtd_pecas_por_volume = ?, qtd_volumes_palete = ?, saldo_total_pecas = ?, volume_etiqueta = ?
       WHERE id = ?`,
      [
        loteLimpo,
        pedidoLimpo,
        clienteLimpo,
        itemLimpo,
        produtoLimpo,
        pecasUn,
        volumes,
        novoSaldoTotal,
        volume_etiqueta || '',
        id
      ],
      function (err) {
        if (err) return res.status(500).json({ erro: err.message });

        db.run(
          `UPDATE paletes_bipados 
           SET cliente = ?, produto = ?, qtd_pecas_por_volume = ?, saldo_total_pecas = qtd_volumes_palete * ?
           WHERE UPPER(TRIM(lote)) = ? AND UPPER(TRIM(item)) = ?`,
          [clienteLimpo, produtoLimpo, pecasUn, pecasUn, loteLimpo, itemLimpo]
        );

        const padraoCorrigido = volumes >= 10 ? volumes : 15;
        const tipoEmb = padraoCorrigido === 22 ? 'SACARIA' : 'CAIXA';

        db.run(
          `INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [itemLimpo, clienteLimpo, produtoLimpo, padraoCorrigido, pecasUn, tipoEmb],
          () => {
            notificarTelasInstantaneamente({ acao: 'palete_editado', id: Number(id), item: itemLimpo, lote: loteLimpo });
            res.json({ sucesso: true, saldo_recalculado: novoSaldoTotal });
          }
        );
      }
    );
  });
});

// ============================================================================
// 9. EXCLUSÃO INDIVIDUAL E EM LOTE
// ============================================================================
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
          notificarTelasInstantaneamente({ acao: 'exclusao_massa' });
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
      notificarTelasInstantaneamente({ acao: 'exclusao_multipla', excluidos: this.changes });
      res.json({ sucesso: true, excluidos: this.changes, versaoExclusao });
    });
  });
});

app.delete('/api/paletes/:id', (req, res) => {
  const idNum = Number(req.params.id);
  versaoExclusao = Date.now();

  db.serialize(() => {
    db.run(`INSERT OR IGNORE INTO ids_excluidos (id) VALUES (?)`, [idNum]);
    db.run(`DELETE FROM paletes_bipados WHERE id = ?`, [idNum], function (err) {
      if (err) return res.status(500).json({ erro: err.message });
      notificarTelasInstantaneamente({ acao: 'exclusao_unica', id: idNum });
      res.json({ sucesso: true, versaoExclusao });
    });
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Servidor FINART WMS rodando na porta ${PORT}`);
});
