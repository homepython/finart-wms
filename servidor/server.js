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

function corrigirCodigoItem(itemBruto = '', produtoTexto = '') {
  let cod = String(itemBruto || '').trim().toUpperCase().replace(/\s+/g, '');
  const prod = String(produtoTexto || '').toUpperCase();

  const matchMP = prod.match(/\b((?:PT|FR)-\d{4,6})MP\b/i);
  if (matchMP) {
    cod = matchMP[1].toUpperCase();
  }

  if (cod === 'PT-7808' || cod === 'PT7808' || (prod.includes('1000 GRS') && prod.includes('REDONDO'))) {
    return 'PT-7908';
  }
  if (cod === '19935') return '719935';
  return cod;
}

// Calcula quantos dígitos ou letras diferem entre dois códigos/lotes (Ex: "008332.1" vs "006332.1" = distância 1)
function distanciaSimilaridade(a = '', b = '') {
  const s1 = String(a).toUpperCase().trim();
  const s2 = String(b).toUpperCase().trim();
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

const db = new sqlite3.Database('./finart.db', (err) => {
  if (err) console.error('Erro ao abrir banco:', err.message);
  else console.log('✅ Banco FINART conectado.');
});

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

  db.run(`
    CREATE TABLE IF NOT EXISTS ids_excluidos (
      id INTEGER PRIMARY KEY
    )
  `);

  db.run(`DELETE FROM catalogo_itens WHERE item = 'PT-7808' OR item = 'PT7808'`);
  db.run(`UPDATE paletes_bipados SET item = 'PT-7908' WHERE item = 'PT-7808' OR item = 'PT7808'`);

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  Object.entries(CATALOGO_OFICIAL).forEach(([cod, d]) => {
    stmt.run(cod, d.cliente, d.produto, d.padrao_volumes, d.qtd_pecas, d.embalagem);
  });
  stmt.finalize();
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

// 1. INTERPRETADOR COM VOTAÇÃO TRIPLA E FONTE CONFIÁVEL DOS PALETES JÁ APONTADOS
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

  let candidatosItem = [];
  let candidatosPedido = [];
  let candidatosLote = [];
  let qtdPorVolume = 0;
  let clienteLido = '';
  let produtoLido = '';
  let volumeLido = '';

  // FONTE 1 DA ETIQUETA: Linha do código de barras inferior (Ex: PT-7908;0063321;59)
  const mBarcode = texto.match(/([A-Z]{2}\s*-\s*\d{4,6}|\d{5,7})\s*[;:,]\s*(\d{6,8})\s*[;:,]\s*(\d{2,4})/i);
  if (mBarcode) {
    candidatosItem.push(mBarcode[1].replace(/\s+/g, '').toUpperCase());
    const lb = mBarcode[2].trim();
    const pedBar = sanitizarPedido(lb.slice(0, -1));
    const loteBar = sanitizarLote(`${lb.slice(0, -1)}.${lb.slice(-1)}`, pedBar);
    if (pedBar) candidatosPedido.push(pedBar);
    if (loteBar) candidatosLote.push(loteBar);
    qtdPorVolume = parseInt(mBarcode[3].trim(), 10) || 0;
  }

  // FONTE 2 DA ETIQUETA: Campo "PEDIDO: 006332" (Extraído separadamente do Lote!)
  const regexPedidos = /PEDIDO[:.\s]*(\d{4,6})(?!\.\d)/gi;
  let mPed;
  while ((mPed = regexPedidos.exec(texto)) !== null) {
    const p = sanitizarPedido(mPed[1]);
    if (p.length >= 4) {
      candidatosPedido.push(p);
      candidatosLote.push(sanitizarLote('', p));
    }
  }

  // FONTE 3 DA ETIQUETA: Campo "LOTE: 006332.1"
  const regexLotes = /(?:LOTE[:.\s]*(\d{4,6}\s*[\.,]\s*\d)|\b(00\d{4}\s*\.\s*\d)\b)/gi;
  let mLote;
  while ((mLote = regexLotes.exec(texto)) !== null) {
    const rawL = mLote[1] || mLote[2];
    const l = sanitizarLote(rawL);
    const p = sanitizarPedido(l);
    if (l) candidatosLote.push(l);
    if (p) candidatosPedido.push(p);
  }

  // Captura números de 6 dígitos iniciados por 00 caso o título "PEDIDO:" tenha saído cortado
  const regexZeroZero = /\b(00\d{4})\b/g;
  let mZZ;
  while ((mZZ = regexZeroZero.exec(texto)) !== null) {
    const p = sanitizarPedido(mZZ[1]);
    candidatosPedido.push(p);
    candidatosLote.push(sanitizarLote('', p));
  }

  // Coleta todos os candidatos de Código do Item na etiqueta
  const regexTodosPrefixados = /\b((?:PT|FR)\s*-?\s*\d{4,6})(?:MP)?\b/gi;
  let matchPref;
  while ((matchPref = regexTodosPrefixados.exec(texto)) !== null) {
    let c = matchPref[1].replace(/\s+/g, '').toUpperCase();
    if (!c.includes('-')) c = c.slice(0, 2) + '-' + c.slice(2);
    candidatosItem.push(c);
  }

  const mItemGenerico = texto.match(/ITEM[:.\s]*([A-Z0-9\-]{4,12})/i) || texto.match(/\b(719935|19935)\b/);
  if (mItemGenerico) {
    let cand = mItemGenerico[1].trim().toUpperCase();
    if (!cand.startsWith('00')) candidatosItem.push(cand);
  }

  // Cliente
  if (/DEVINTEX|COSMETICOS/i.test(texto)) {
    clienteLido = 'DEVINTEX COSMETICOS';
  } else if (/FLORA\s*PRODS|FLORA\s*24|HIG\s*E\s*LIM|P[OÓ]S\s*DANOS/i.test(texto)) {
    clienteLido = 'FLORA PRODS DE HIG E LIM';
  } else {
    const mCli = texto.match(/CLIENTE[:.\s]*([A-Z0-9\s.\-&]{3,35})/i);
    if (mCli) clienteLido = mCli[1].split('\n')[0].trim().toUpperCase();
  }

  // Produto
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

  // QTD por Caixa
  if (!qtdPorVolume) {
    const mQtdDireta = texto.match(/QTD[:.\s\n]*(\d{2,4})\b/i);
    if (mQtdDireta) {
      const val = parseInt(mQtdDireta[1], 10);
      if (val >= 10 && val <= 2500 && !candidatosPedido.includes(String(val))) {
        qtdPorVolume = val;
      }
    }
  }
  if (!qtdPorVolume) {
    const mEntreProdELote = texto.match(/(?:MP|MAR\s*E)[\s\S]{0,30}?\b(\d{2,4})\b[\s\S]{0,30}?(?:LOTE|00\d{4})/i);
    if (mEntreProdELote) qtdPorVolume = parseInt(mEntreProdELote[1], 10);
  }

  const mVol = texto.match(/\b(\d{2,3}\s*\/\s*\d{3})\b/);
  if (mVol) volumeLido = mVol[1].replace(/\s+/g, '');

  // CRUZA COM A FONTE CONFIÁVEL (PALETES JÁ BIPADOS + CATÁLOGO)
  db.all(`SELECT * FROM catalogo_itens WHERE item != 'PT-7808'`, [], (errCat, catalogoRows) => {
    if (errCat) return res.status(500).json({ erro: errCat.message });

    db.all(`SELECT * FROM paletes_bipados ORDER BY id DESC LIMIT 150`, [], (errHist, histRows) => {
      const historico = histRows || [];

      // 1º: Identifica o Item Oficial
      let itemEscolhido = corrigirCodigoItem(candidatosItem[0] || '', produtoLido);
      let itemMemoriaExato = null;

      for (const candBruto of candidatosItem) {
        const cand = corrigirCodigoItem(candBruto, produtoLido);
        const achouNoHistorico = historico.find((h) => h.item.toUpperCase() === cand.toUpperCase());
        const achouNoCatalogo = catalogoRows.find((r) => r.item.toUpperCase() === cand.toUpperCase());
        if (achouNoHistorico || achouNoCatalogo) {
          itemEscolhido = (achouNoCatalogo || achouNoHistorico).item;
          itemMemoriaExato = achouNoCatalogo || achouNoHistorico;
          break;
        }
      }

      // 2º: Identifica o Lote e Pedido Lidos na Etiqueta (Votação entre os candidatos da etiqueta)
      let loteLidoNaCamera = candidatosLote[0] || '';
      let pedidoLidoNaCamera = candidatosPedido[0] || sanitizarPedido(loteLidoNaCamera);

      // Se um dos candidatos lidos na própria etiqueta já bate 100% com um lote ativo no histórico, ele vence!
      for (const candL of candidatosLote) {
        if (historico.some((h) => h.lote === candL)) {
          loteLidoNaCamera = candL;
          pedidoLidoNaCamera = sanitizarPedido(candL);
          break;
        }
      }

      let loteFinal = loteLidoNaCamera;
      let pedidoFinal = pedidoLidoNaCamera;
      let sugestaoDivergencia = null;

      // 3º: INTELIGÊNCIA DE FONTE CONFIÁVEL PARA LOTES E PEDIDOS (Funciona para qualquer cliente e qualquer lote!)
      // Busca todos os lotes que já foram apontados para este Item (ou no histórico recente)
      const paletesDesteItem = itemEscolhido
        ? historico.filter((h) => h.item.toUpperCase() === itemEscolhido.toUpperCase())
        : [];

      if (paletesDesteItem.length > 0) {
        // Último lote confiável já apontado para este produto (ex: 006332.1)
        const fonteConfiavel = paletesDesteItem[0];
        const jaExisteExatoParaOItem = paletesDesteItem.some((h) => h.lote === loteLidoNaCamera);

        if (!jaExisteExatoParaOItem && loteLidoNaCamera) {
          // Procura se algum lote já apontado para este item tem distância de 1 ou 2 dígitos (ex: leu 008332.1 mas o confiável é 006332.1)
          const loteConfiavelParecido = paletesDesteItem.find(
            (h) => distanciaSimilaridade(loteLidoNaCamera, h.lote) <= 2 ||
                   distanciaSimilaridade(pedidoLidoNaCamera, h.pedido) <= 2
          ) || fonteConfiavel;

          const dist = distanciaSimilaridade(loteLidoNaCamera, loteConfiavelParecido.lote);

          // AUTO-CORRIGE IMEDIATAMENTE PARA A FONTE CONFIÁVEL E MOSTRA O ALERTA NA TELA!
          loteFinal = loteConfiavelParecido.lote;
          pedidoFinal = loteConfiavelParecido.pedido;

          sugestaoDivergencia = {
            tipo: 'LOTE_AUTO_CORRIGIDO_PELA_BASE',
            auto_corrigido: true,
            mensagem: dist <= 2
              ? `A câmera leu Pedido "${pedidoLidoNaCamera}" / Lote "${loteLidoNaCamera}", mas você já apontou este item na Fonte Confiável Lote "${loteConfiavelParecido.lote}" (Ped: "${loteConfiavelParecido.pedido}"). O sistema corrigiu automaticamente para "${loteConfiavelParecido.lote}"!`
              : `Este item (${itemEscolhido}) já está sendo apontado no Lote "${loteConfiavelParecido.lote}", mas a câmera leu "${loteLidoNaCamera}". O sistema manteve "${loteConfiavelParecido.lote}" por segurança.`,
            item_lido: itemEscolhido,
            item_sugerido: loteConfiavelParecido.item,
            cliente_sugerido: loteConfiavelParecido.cliente,
            produto_sugerido: loteConfiavelParecido.produto,
            qtd_pecas_sugerida: loteConfiavelParecido.qtd_pecas_por_volume,
            qtd_volumes_sugerida: loteConfiavelParecido.qtd_volumes_palete,
            lote_lido: loteLidoNaCamera,
            pedido_lido: pedidoLidoNaCamera,
            lote_sugerido: loteConfiavelParecido.lote,
            pedido_sugerido: loteConfiavelParecido.pedido
          };
        } else if (!loteLidoNaCamera) {
          // Se a câmera leu o item (ex: PT-7908) mas o lote saiu borrado em branco, puxa direto da Fonte Confiável!
          loteFinal = fonteConfiavel.lote;
          pedidoFinal = fonteConfiavel.pedido;
        }
      } else if (loteLidoNaCamera && historico.length > 0) {
        // Caso o Item ainda não tenha sido identificado ou tenha lido diferente, verifica se o Lote lido é 1 dígito diferente de QUALQUER lote ativo no histórico
        const loteGlobalParecido = historico.find((h) => {
          const d = distanciaSimilaridade(loteLidoNaCamera, h.lote);
          return d >= 1 && d <= 2;
        });

        if (loteGlobalParecido) {
          loteFinal = loteGlobalParecido.lote;
          pedidoFinal = loteGlobalParecido.pedido;
          if (!itemEscolhido) itemEscolhido = loteGlobalParecido.item;

          sugestaoDivergencia = {
            tipo: 'LOTE_AUTO_CORRIGIDO_PELA_BASE',
            auto_corrigido: true,
            mensagem: `A câmera leu o Lote "${loteLidoNaCamera}", que é muito parecido com o Lote ativo "${loteGlobalParecido.lote}" (Ped: "${loteGlobalParecido.pedido}"). O sistema ajustou para "${loteGlobalParecido.lote}".`,
            item_lido: itemEscolhido,
            item_sugerido: loteGlobalParecido.item,
            cliente_sugerido: loteGlobalParecido.cliente,
            produto_sugerido: loteGlobalParecido.produto,
            qtd_pecas_sugerida: loteGlobalParecido.qtd_pecas_por_volume,
            qtd_volumes_sugerida: loteGlobalParecido.qtd_volumes_palete,
            lote_lido: loteLidoNaCamera,
            pedido_lido: pedidoLidoNaCamera,
            lote_sugerido: loteGlobalParecido.lote,
            pedido_sugerido: loteGlobalParecido.pedido
          };
        }
      }

      // 4º: Verifica divergência no Código do Item (Ex: leu código 1 ou 2 letras diferente de um item do catálogo/histórico)
      if (!itemMemoriaExato && itemEscolhido && !sugestaoDivergencia) {
        let melhorCandidato = null;
        let menorDist = 99;

        const baseCompleta = [...catalogoRows];
        historico.forEach((h) => {
          if (!baseCompleta.some((c) => c.item === h.item)) {
            baseCompleta.push({
              item: h.item,
              cliente: h.cliente,
              produto: h.produto,
              padrao_volumes_palete: h.qtd_volumes_palete,
              qtd_padrao_pecas: h.qtd_pecas_por_volume
            });
          }
        });

        baseCompleta.forEach((cat) => {
          const dist = distanciaSimilaridade(itemEscolhido, cat.item);
          if (dist > 0 && dist <= 2 && dist < menorDist) {
            menorDist = dist;
            melhorCandidato = cat;
          }
        });

        if (melhorCandidato) {
          const itemLidoOriginal = itemEscolhido;
          itemEscolhido = melhorCandidato.item;
          itemMemoriaExato = melhorCandidato;

          sugestaoDivergencia = {
            tipo: 'ITEM_AUTO_CORRIGIDO',
            auto_corrigido: true,
            mensagem: `A câmera leu o item "${itemLidoOriginal}", mas a Fonte Confiável possui "${melhorCandidato.item}" (${melhorCandidato.produto}). Corrigido automaticamente para "${melhorCandidato.item}"!`,
            item_lido: itemLidoOriginal,
            item_sugerido: melhorCandidato.item,
            cliente_sugerido: melhorCandidato.cliente,
            produto_sugerido: melhorCandidato.produto,
            qtd_pecas_sugerida: melhorCandidato.qtd_padrao_pecas || qtdPorVolume,
            qtd_volumes_sugerida: calcularPadraoPalete(melhorCandidato.cliente, melhorCandidato.item, melhorCandidato.produto, melhorCandidato.padrao_volumes_palete),
            lote_lido: loteFinal,
            pedido_lido: pedidoFinal,
            lote_sugerido: loteFinal,
            pedido_sugerido: pedidoFinal
          };
        }
      }

      const refItemConfiavel = paletesDesteItem[0] || itemMemoriaExato;

      const clienteFinal =
        (refItemConfiavel && refItemConfiavel.cliente) ||
        clienteLido ||
        (itemEscolhido.startsWith('PT-') || itemEscolhido.startsWith('FR-') ? 'DEVINTEX COSMETICOS' : '');

      const produtoFinal =
        (refItemConfiavel && refItemConfiavel.produto) ||
        produtoLido ||
        '';

      const qtdPecasFinal =
        (paletesDesteItem[0] && paletesDesteItem[0].qtd_pecas_por_volume > 0 ? paletesDesteItem[0].qtd_pecas_por_volume : 0) ||
        (itemMemoriaExato && itemMemoriaExato.qtd_padrao_pecas > 0 ? itemMemoriaExato.qtd_padrao_pecas : 0) ||
        qtdPorVolume ||
        0;

      if (!itemEscolhido && !pedidoFinal && !loteFinal && !produtoFinal) {
        return res.status(422).json({
          erro: 'Etiqueta não enquadrada. Aponte para a etiqueta inteira e tente novamente.'
        });
      }

      const padraoVolumes = calcularPadraoPalete(
        clienteFinal,
        itemEscolhido,
        produtoFinal,
        (paletesDesteItem[0] && paletesDesteItem[0].qtd_volumes_palete >= 12)
          ? paletesDesteItem[0].qtd_volumes_palete
          : (itemMemoriaExato ? itemMemoriaExato.padrao_volumes_palete : 0)
      );
      const tipoEmbalagem = padraoVolumes === 22 ? 'SACARIA' : 'CAIXA';

      res.json({
        item: itemEscolhido,
        pedido: pedidoFinal,
        lote: loteFinal,
        cliente: clienteFinal || 'DEVINTEX COSMETICOS',
        produto: produtoFinal || (itemEscolhido ? `PRODUTO ITEM ${itemEscolhido}` : ''),
        tipoEmbalagem,
        volume_etiqueta: volumeLido,
        qtd_pecas_por_volume: qtdPecasFinal,
        qtd_volumes_sugerida: padraoVolumes,
        saldo_previsto: qtdPecasFinal * padraoVolumes,
        sugestao_divergencia: sugestaoDivergencia
      });
    });
  });
});

// 2. SALVAR PALETE E ATUALIZAR FONTE CONFIÁVEL
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
  const itemLimpo = corrigirCodigoItem(item, produto);
  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);

  db.get(`SELECT * FROM catalogo_itens WHERE UPPER(TRIM(item)) = ?`, [itemLimpo], (errCat, catRow) => {
    db.get(
      `SELECT * FROM paletes_bipados WHERE UPPER(TRIM(lote)) = ? AND UPPER(TRIM(item)) = ? LIMIT 1`,
      [loteLimpo, itemLimpo],
      (errLote, loteRow) => {
        const clienteUnificado = String(cliente || '').trim().toUpperCase() || (loteRow && loteRow.cliente) || (catRow && catRow.cliente);
        const produtoUnificado = String(produto || '').trim().toUpperCase() || (loteRow && loteRow.produto) || (catRow && catRow.produto);
        const pecasUnificada = Number(qtd_pecas_por_volume) || (loteRow && loteRow.qtd_pecas_por_volume) || (catRow && catRow.qtd_padrao_pecas) || 0;

        if (!loteLimpo || !itemLimpo || !clienteUnificado || pecasUnificada <= 0 || volumes <= 0) {
          return res.status(400).json({ erro: 'Confirme Cliente, Lote, Item e Quantidades antes de salvar.' });
        }

        const saldoTotalPecas = pecasUnificada * volumes;

        const padraoParaAprender = volumes >= 12
          ? volumes
          : calcularPadraoPalete(clienteUnificado, itemLimpo, produtoUnificado, catRow ? catRow.padrao_volumes_palete : 0);
        const tipoEmb = padraoParaAprender === 22 ? 'SACARIA' : 'CAIXA';

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

// 3. AUTO-RESTAURAÇÃO DE BACKUP
app.post('/api/restaurar-backup', (req, res) => {
  const { historico, catalogo } = req.body;

  db.serialize(() => {
    if (Array.isArray(catalogo) && catalogo.length > 0) {
      const stmtCat = db.prepare(`
        INSERT OR IGNORE INTO catalogo_itens (item, cliente, produto, padrao_volumes_palete, qtd_padrao_pecas, tipo_embalagem)
        VALUES (?, ?, ?, ?, ?, ?)
      `);
      catalogo.forEach((c) => {
        const codCorrigido = corrigirCodigoItem(c.item, c.produto);
        if (codCorrigido !== 'PT-7808') {
          stmtCat.run(codCorrigido, c.cliente, c.produto, c.padrao_volumes_palete || 15, c.qtd_padrao_pecas || 0, c.tipo_embalagem || 'CAIXA');
        }
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
          const itemOk = corrigirCodigoItem(p.item, p.produto);
          stmt.run(
            p.id, pedOk, loteOk, itemOk, p.cliente, p.produto,
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

// 4. DASHBOARD
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
  const queryCatalogo = `SELECT * FROM catalogo_itens WHERE item != 'PT-7808' ORDER BY item ASC`;

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

// 5. EDITAR PALETE
app.put('/api/paletes/:id', (req, res) => {
  const { id } = req.params;
  const { lote, pedido, cliente, item, produto, qtd_pecas_por_volume, qtd_volumes_palete, volume_etiqueta } = req.body;

  const pecasUn = Number(qtd_pecas_por_volume);
  const volumes = Number(qtd_volumes_palete);
  if (pecasUn <= 0 || volumes <= 0) return res.status(400).json({ erro: 'Valores inválidos.' });

  const pedidoLimpo = sanitizarPedido(pedido || lote);
  const loteLimpo = sanitizarLote(lote, pedidoLimpo);
  const itemLimpo = corrigirCodigoItem(item, produto);
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

// 6. EXCLUIR MÚLTIPLOS PALETES
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