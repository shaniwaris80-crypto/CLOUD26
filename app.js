// app.js (ESM)
import { firebaseConfig } from './firebase-config.js';

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.4/firebase-app.js";
import {
  getAuth, onAuthStateChanged, signInWithEmailAndPassword,
  createUserWithEmailAndPassword, signOut
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-auth.js";

import {
  getFirestore, doc, getDoc, setDoc, addDoc, updateDoc, deleteDoc,
  collection, query, where, orderBy, getDocs, onSnapshot, serverTimestamp
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-firestore.js";

import {
  getStorage, ref, uploadBytes, getDownloadURL
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-storage.js";

/* =========================================================
   INIT
========================================================= */
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);

/* =========================================================
   DOM HELPERS
========================================================= */
const $ = (s, r=document) => r.querySelector(s);
const $$ = (s, r=document) => Array.from(r.querySelectorAll(s));
const fmtEUR = (n) => new Intl.NumberFormat('es-ES', { style:'currency', currency:'EUR' }).format(Number(n||0));
const todayISO = () => new Date().toISOString().slice(0,10);
const setMsg = (el, text, type='') => {
  if (!el) return;
  el.className = 'msg' + (type ? ` ${type}` : '');
  el.textContent = text || '';
};
const escapeHtml = (s) => (s ?? '').toString()
  .replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')
  .replaceAll('"','&quot;').replaceAll("'","&#039;");

function uid(){ return auth.currentUser?.uid || null; }

/* =========================================================
   STATE
========================================================= */
const S = {
  orgId: null,
  org: null,
  accounts: [],
  rules: [],
  transactions: [],
  invoices: [],
  matches: [],
  selMovId: null,
  selInvId: null,
  unsub: [],
};

function cleanupSubs(){
  S.unsub.forEach(fn => { try{ fn(); } catch{} });
  S.unsub = [];
}

function show(el, yes=true){
  if (!el) return;
  el.classList.toggle('hidden', !yes);
}

function switchTab(tab){
  $$('.tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  const ids = ['dashboard','cuentas','movs','reglas','facturas','conciliacion','ajustes'];
  ids.forEach(id => show($('#tab-'+id), id === tab));
}

/* =========================================================
   FIRESTORE PATHS
========================================================= */
const paths = {
  user: (uid)=>doc(db, 'users', uid),
  org: (orgId)=>doc(db, 'orgs', orgId),
  accounts: (orgId)=>collection(db, 'orgs', orgId, 'accounts'),
  rules: (orgId)=>collection(db, 'orgs', orgId, 'rules'),
  txs: (orgId)=>collection(db, 'orgs', orgId, 'transactions'),
  invoices: (orgId)=>collection(db, 'orgs', orgId, 'invoices'),
  matches: (orgId)=>collection(db, 'orgs', orgId, 'matches'),
};

/* =========================================================
   AUTH UI
========================================================= */
const authView = $('#authView');
const appView  = $('#appView');
const userBadge = $('#userBadge');
const btnLogout = $('#btnLogout');

$('#btnLogin').addEventListener('click', async ()=>{
  const email = $('#authEmail').value.trim();
  const pass = $('#authPass').value;
  const msg = $('#authMsg');
  setMsg(msg,'');
  try{
    await signInWithEmailAndPassword(auth, email, pass);
  }catch(e){
    setMsg(msg, e.message, 'err');
  }
});

$('#btnRegister').addEventListener('click', async ()=>{
  const email = $('#authEmail').value.trim();
  const pass = $('#authPass').value;
  const msg = $('#authMsg');
  setMsg(msg,'');
  try{
    await createUserWithEmailAndPassword(auth, email, pass);
  }catch(e){
    setMsg(msg, e.message, 'err');
  }
});

btnLogout.addEventListener('click', ()=>signOut(auth));

/* =========================================================
   BOOTSTRAP USER/ORG
========================================================= */
async function ensureUserOrg(){
  const u = auth.currentUser;
  if (!u) return null;

  const uref = paths.user(u.uid);
  const usnap = await getDoc(uref);

  if (usnap.exists() && usnap.data()?.orgId){
    return usnap.data().orgId;
  }

  // Crear org por defecto
  const orgId = `org_${u.uid}`;
  await setDoc(paths.org(orgId), {
    name: 'Mi Empresa',
    ownerUid: u.uid,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  }, { merge:true });

  await setDoc(uref, {
    email: u.email,
    orgId,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  }, { merge:true });

  return orgId;
}

async function loadOrg(orgId){
  const snap = await getDoc(paths.org(orgId));
  S.org = snap.exists() ? snap.data() : { name:'Mi Empresa' };
  $('#orgName').textContent = S.org.name || '—';
  $('#orgNameInput').value = S.org.name || '';
}

/* =========================================================
   LIVE SUBSCRIPTIONS
========================================================= */
function subscribeAll(){
  cleanupSubs();
  const orgId = S.orgId;

  // Accounts
  S.unsub.push(onSnapshot(
    query(paths.accounts(orgId), orderBy('createdAt','desc')),
    (qs)=>{
      S.accounts = qs.docs.map(d=>({id:d.id, ...d.data()}));
      renderAccounts();
      renderDash();
      renderAccountFilter();
    }
  ));

  // Rules
  S.unsub.push(onSnapshot(
    query(paths.rules(orgId), orderBy('priority','desc')),
    (qs)=>{
      S.rules = qs.docs.map(d=>({id:d.id, ...d.data()}));
      renderRules();
    }
  ));

  // Transactions
  S.unsub.push(onSnapshot(
    query(paths.txs(orgId), orderBy('date','desc')),
    (qs)=>{
      S.transactions = qs.docs.map(d=>({id:d.id, ...d.data()}));
      renderMovs();
      renderConciliationLists();
      renderDash();
    }
  ));

  // Invoices
  S.unsub.push(onSnapshot(
    query(paths.invoices(orgId), orderBy('date','desc')),
    (qs)=>{
      S.invoices = qs.docs.map(d=>({id:d.id, ...d.data()}));
      renderInvoices();
      renderConciliationLists();
      renderDash();
    }
  ));

  // Matches
  S.unsub.push(onSnapshot(
    query(paths.matches(orgId), orderBy('createdAt','desc')),
    (qs)=>{
      S.matches = qs.docs.map(d=>({id:d.id, ...d.data()}));
      renderMatches();
      renderConciliationLists();
      renderDash();
    }
  ));
}

/* =========================================================
   TAB NAV
========================================================= */
$$('.tab').forEach(b => b.addEventListener('click', ()=>switchTab(b.dataset.tab)));
switchTab('dashboard');

/* =========================================================
   ACCOUNTS
========================================================= */
$('#btnAddAccount').addEventListener('click', async ()=>{
  const name = $('#accName').value.trim();
  const ccy = $('#accCcy').value.trim() || 'EUR';
  const opening = Number($('#accOpening').value || 0);
  if (!name) return;

  await addDoc(paths.accounts(S.orgId), {
    name, ccy,
    openingBalance: opening,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  });

  $('#accName').value = '';
  $('#accOpening').value = '';
});

function calcAccountBalance(accountId){
  const acc = S.accounts.find(a=>a.id===accountId);
  const opening = Number(acc?.openingBalance || 0);

  const sumTx = S.transactions
    .filter(t=>t.accountId===accountId)
    .reduce((a,t)=>a + Number(t.amount||0), 0);

  return opening + sumTx;
}

function renderAccounts(){
  const wrap = $('#accountsList');
  wrap.innerHTML = '';

  if (!S.accounts.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin cuentas</div><div class="s">Añade una cuenta para empezar.</div></div>`;
    return;
  }

  S.accounts.forEach(acc=>{
    const bal = calcAccountBalance(acc.id);
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(acc.name||'')}</div>
          <div class="s">Moneda: ${escapeHtml(acc.ccy||'EUR')} · Saldo: <b>${fmtEUR(bal)}</b></div>
        </div>
        <div class="row">
          <button class="btn" data-del="${acc.id}">Eliminar</button>
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      const id = btn.getAttribute('data-del');
      // Nota: si hay movimientos, no lo borramos para evitar líos.
      const hasTx = S.transactions.some(t=>t.accountId===id);
      if (hasTx){
        alert('Esta cuenta tiene movimientos. Borra o mueve primero los movimientos.');
        return;
      }
      await deleteDoc(doc(db,'orgs',S.orgId,'accounts',id));
    });
  });
}

function renderAccountFilter(){
  const sel = $('#movAccountFilter');
  sel.innerHTML = '';
  const optAll = document.createElement('option');
  optAll.value = '';
  optAll.textContent = 'Todas las cuentas';
  sel.appendChild(optAll);

  S.accounts.forEach(a=>{
    const o = document.createElement('option');
    o.value = a.id;
    o.textContent = a.name;
    sel.appendChild(o);
  });
}

/* =========================================================
   RULES
========================================================= */
$('#btnAddRule').addEventListener('click', async ()=>{
  const needle = $('#ruleNeedle').value.trim();
  const category = $('#ruleCat').value.trim();
  const party = $('#ruleParty').value.trim();
  const type = $('#ruleType').value;
  const priority = Number($('#rulePrio').value || 10);

  if (!needle || !category) return;

  await addDoc(paths.rules(S.orgId), {
    needle,
    category,
    party: party || null,
    type,
    priority,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  });

  $('#ruleNeedle').value = '';
  $('#ruleCat').value = '';
  $('#ruleParty').value = '';
  $('#rulePrio').value = '10';
});

function renderRules(){
  const wrap = $('#rulesList');
  wrap.innerHTML = '';
  if (!S.rules.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin reglas</div><div class="s">Crea reglas para auto-categorizar movimientos.</div></div>`;
    return;
  }

  S.rules.forEach(r=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(r.needle)} → ${escapeHtml(r.category)}</div>
          <div class="s">Tipo: ${escapeHtml(r.type)} · Prioridad: ${escapeHtml(r.priority)} ${r.party ? `· Contraparte: ${escapeHtml(r.party)}` : ''}</div>
        </div>
        <div class="row">
          <button class="btn" data-del="${r.id}">Eliminar</button>
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      const id = btn.getAttribute('data-del');
      await deleteDoc(doc(db,'orgs',S.orgId,'rules',id));
    });
  });
}

/* =========================================================
   CSV IMPORT
========================================================= */
function parseCSV(text){
  // Parser simple (sin comillas complejas). Para bancos típicos suele bastar.
  const lines = text.split(/\r?\n/).map(l=>l.trim()).filter(Boolean);
  if (!lines.length) return { headers:[], rows:[] };

  const split = (line) => line.split(',').map(x=>x.trim().replace(/^"|"$/g,''));
  const headers = split(lines[0]).map(h=>h.toLowerCase());

  const rows = [];
  for (let i=1;i<lines.length;i++){
    rows.push(split(lines[i]));
  }
  return { headers, rows };
}

function detectCol(headers, candidates){
  const idx = headers.findIndex(h => candidates.some(c => h.includes(c)));
  return idx >= 0 ? idx : null;
}

function normalizeDate(s){
  // Acepta YYYY-MM-DD o DD/MM/YYYY
  const v = (s||'').trim();
  if (!v) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(v)){
    const [d,m,y] = v.split('/');
    return `${y}-${m}-${d}`;
  }
  // fallback: Date parse
  const dt = new Date(v);
  if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0,10);
  return null;
}

function normalizeAmount(s){
  // Soporta "1.234,56" o "1234.56"
  let v = (s||'').toString().trim();
  if (!v) return 0;
  v = v.replace(/\s/g,'');
  // si contiene coma decimal (es-ES)
  if (v.includes(',') && v.lastIndexOf(',') > v.lastIndexOf('.')){
    v = v.replaceAll('.','').replace(',','.');
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

async function importCsvToAccount(file, accountId){
  const msgEl = $('#csvMsg');
  setMsg(msgEl,'');

  const text = await file.text();
  const { headers, rows } = parseCSV(text);

  const cDate = detectCol(headers, ['date','fecha','f. valor','valor']);
  const cAmt  = detectCol(headers, ['amount','importe','importe (€)','cantidad','monto']);
  const cDesc = detectCol(headers, ['description','concepto','detalle','texto','descripcion']);

  if (cDate===null || cAmt===null || cDesc===null){
    throw new Error('CSV no reconocido. Necesita columnas tipo fecha/importe/concepto.');
  }

  // Insertamos con dedupe básico por hash
  let added = 0;

  // Traer hashes existentes recientes (últimos 90 días aprox) para evitar duplicados
  const qx = query(paths.txs(S.orgId), where('accountId','==', accountId), orderBy('date','desc'));
  const snap = await getDocs(qx);
  const existing = new Set(snap.docs.map(d => d.data()?.hash).filter(Boolean));

  for (const r of rows){
    const date = normalizeDate(r[cDate]);
    const amount = normalizeAmount(r[cAmt]);
    const description = (r[cDesc]||'').toString().trim();
    if (!date || !description) continue;

    const hash = `${accountId}|${date}|${amount}|${description}`.slice(0,500);
    if (existing.has(hash)) continue;

    const base = {
      accountId,
      date,
      amount,
      description,
      party: null,
      category: null,
      matched: false,
      hash,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    };

    const enriched = applyRulesToTx(base, S.rules);
    await addDoc(paths.txs(S.orgId), enriched);
    existing.add(hash);
    added++;
  }

  setMsg(msgEl, `Importados ${added} movimientos (dedupe activo).`, 'ok');
}

$('#btnImportCsv').addEventListener('click', async ()=>{
  const file = $('#csvFile').files?.[0];
  if (!file) return;

  const accountId = $('#movAccountFilter').value || S.accounts[0]?.id;
  if (!accountId){
    setMsg($('#csvMsg'), 'Primero crea una cuenta.', 'err');
    return;
  }

  try{
    await importCsvToAccount(file, accountId);
  }catch(e){
    setMsg($('#csvMsg'), e.message || 'Error importando CSV', 'err');
  }
});

/* =========================================================
   RULE ENGINE
========================================================= */
function applyRulesToTx(tx, rules){
  const desc = (tx.description||'').toLowerCase();
  const isIn = Number(tx.amount||0) > 0;
  const isOut = Number(tx.amount||0) < 0;

  let best = null;
  for (const r of rules){
    const needle = (r.needle||'').toLowerCase();
    if (!needle) continue;
    if (!desc.includes(needle)) continue;

    const type = r.type || 'any';
    if (type === 'in' && !isIn) continue;
    if (type === 'out' && !isOut) continue;

    if (!best || Number(r.priority||0) > Number(best.priority||0)){
      best = r;
    }
  }

  if (!best) return tx;

  return {
    ...tx,
    category: best.category || tx.category || null,
    party: best.party || tx.party || null,
  };
}

$('#btnAutoRules').addEventListener('click', async ()=>{
  const filtered = getFilteredMovs().filter(t => !t.matched);
  if (!filtered.length) return;

  let updated = 0;
  for (const t of filtered){
    const newTx = applyRulesToTx(t, S.rules);
    if (newTx.category !== t.category || newTx.party !== t.party){
      await updateDoc(doc(db,'orgs',S.orgId,'transactions',t.id), {
        category: newTx.category || null,
        party: newTx.party || null,
        updatedAt: serverTimestamp(),
      });
      updated++;
    }
  }
  alert(`Reglas aplicadas: ${updated} movimientos actualizados.`);
});

/* =========================================================
   MOVS RENDER + EDIT
========================================================= */
$('#movAccountFilter').addEventListener('change', renderMovs);
$('#movSearch').addEventListener('input', renderMovs);

function getFilteredMovs(){
  const accId = $('#movAccountFilter').value;
  const q = ($('#movSearch').value || '').trim().toLowerCase();

  return S.transactions.filter(t=>{
    if (accId && t.accountId !== accId) return false;
    if (!q) return true;
    const blob = `${t.description||''} ${t.party||''} ${t.category||''}`.toLowerCase();
    return blob.includes(q);
  });
}

function renderMovs(){
  const wrap = $('#movsList');
  const movs = getFilteredMovs();
  $('#movCount').textContent = String(movs.length);

  wrap.innerHTML = '';
  if (!movs.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin movimientos</div><div class="s">Importa un CSV para empezar.</div></div>`;
    return;
  }

  movs.forEach(t=>{
    const acc = S.accounts.find(a=>a.id===t.accountId);
    const signTag = Number(t.amount||0) >= 0 ? 'ok' : 'danger';
    const matchedTag = t.matched ? 'white' : '';
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(t.description||'')}</div>
          <div class="s">
            ${escapeHtml(t.date||'')} · ${escapeHtml(acc?.name||'Cuenta')} ·
            <b>${fmtEUR(t.amount||0)}</b>
          </div>
          <div class="tags">
            <span class="tag ${signTag}">${Number(t.amount||0) >= 0 ? 'Ingreso' : 'Gasto'}</span>
            <span class="tag ${matchedTag}">${t.matched ? 'Conciliado' : 'Sin conciliar'}</span>
            ${t.category ? `<span class="tag">Cat: ${escapeHtml(t.category)}</span>` : `<span class="tag">Cat: —</span>`}
            ${t.party ? `<span class="tag">Con: ${escapeHtml(t.party)}</span>` : `<span class="tag">Con: —</span>`}
          </div>
        </div>
        <div class="row">
          <button class="btn" data-edit="${t.id}">Editar</button>
          <button class="btn" data-del="${t.id}">Eliminar</button>
        </div>
      </div>
      <div class="edit hidden" id="edit-${t.id}">
        <div class="grid2">
          <div>
            <label>Categoría</label>
            <input data-cat="${t.id}" value="${escapeHtml(t.category||'')}"/>
          </div>
          <div>
            <label>Contraparte</label>
            <input data-party="${t.id}" value="${escapeHtml(t.party||'')}"/>
          </div>
        </div>
        <div class="row">
          <button class="btn primary" data-save="${t.id}">Guardar</button>
          <button class="btn" data-cancel="${t.id}">Cerrar</button>
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-edit]', wrap).forEach(b=>{
    b.addEventListener('click', ()=>{
      const id = b.getAttribute('data-edit');
      show($('#edit-'+id), true);
    });
  });
  $$('button[data-cancel]', wrap).forEach(b=>{
    b.addEventListener('click', ()=>{
      const id = b.getAttribute('data-cancel');
      show($('#edit-'+id), false);
    });
  });
  $$('button[data-save]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      const id = b.getAttribute('data-save');
      const cat = $(`input[data-cat="${id}"]`)?.value?.trim() || null;
      const party = $(`input[data-party="${id}"]`)?.value?.trim() || null;
      await updateDoc(doc(db,'orgs',S.orgId,'transactions',id), {
        category: cat,
        party: party,
        updatedAt: serverTimestamp(),
      });
      show($('#edit-'+id), false);
    });
  });
  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      const id = b.getAttribute('data-del');
      const tx = S.transactions.find(x=>x.id===id);
      if (tx?.matched){
        alert('Este movimiento está conciliado. Deshaz la conciliación primero.');
        return;
      }
      await deleteDoc(doc(db,'orgs',S.orgId,'transactions',id));
    });
  });
}

/* =========================================================
   INVOICES
========================================================= */
$('#invDate').value = todayISO();

$('#btnAddInvoice').addEventListener('click', async ()=>{
  const party = $('#invParty').value.trim();
  const number = $('#invNumber').value.trim();
  const total = Number($('#invTotal').value || 0);
  const date = $('#invDate').value || todayISO();
  const due = $('#invDue').value || null;
  const status = $('#invStatus').value;

  const file = $('#invPdf').files?.[0];
  const msg = $('#invMsg');
  setMsg(msg,'');

  if (!party || !number || !total){
    setMsg(msg,'Completa proveedor/cliente, número y total.', 'err');
    return;
  }
  if (!file){
    setMsg(msg,'Sube el PDF de la factura.', 'err');
    return;
  }

  try{
    const invId = `inv_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const storagePath = `orgs/${S.orgId}/invoices/${invId}.pdf`;
    const r = ref(storage, storagePath);
    await uploadBytes(r, file, { contentType:'application/pdf' });
    const url = await getDownloadURL(r);

    await addDoc(paths.invoices(S.orgId), {
      party, number, total, date, due,
      status,
      pdfUrl: url,
      pdfPath: storagePath,
      matched: false,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    });

    setMsg(msg,'Factura subida correctamente.', 'ok');
    $('#invParty').value = '';
    $('#invNumber').value = '';
    $('#invTotal').value = '';
    $('#invPdf').value = '';
  }catch(e){
    setMsg(msg, e.message || 'Error subiendo factura', 'err');
  }
});

function renderInvoices(){
  const wrap = $('#invoicesList');
  wrap.innerHTML = '';

  if (!S.invoices.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin facturas</div><div class="s">Sube PDFs para conciliar con pagos.</div></div>`;
    return;
  }

  S.invoices.forEach(inv=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(inv.party||'')} · ${escapeHtml(inv.number||'')}</div>
          <div class="s">${escapeHtml(inv.date||'')} · Total: <b>${fmtEUR(inv.total||0)}</b> ${inv.due ? `· Vence: ${escapeHtml(inv.due)}` : ''}</div>
          <div class="tags">
            <span class="tag ${inv.status==='paid' ? 'white' : ''}">Estado: ${escapeHtml(inv.status||'pending')}</span>
            <span class="tag ${inv.matched ? 'white' : ''}">${inv.matched ? 'Conciliada' : 'Sin conciliar'}</span>
          </div>
        </div>
        <div class="row">
          ${inv.pdfUrl ? `<a class="btn" href="${inv.pdfUrl}" target="_blank" rel="noreferrer">Ver PDF</a>` : ''}
          <button class="btn" data-markpaid="${inv.id}">Marcar pagada</button>
          <button class="btn" data-del="${inv.id}">Eliminar</button>
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-markpaid]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      const id = b.getAttribute('data-markpaid');
      await updateDoc(doc(db,'orgs',S.orgId,'invoices',id), { status:'paid', updatedAt: serverTimestamp() });
    });
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      const id = b.getAttribute('data-del');
      const inv = S.invoices.find(x=>x.id===id);
      if (inv?.matched){
        alert('Esta factura está conciliada. Deshaz la conciliación primero.');
        return;
      }
      await deleteDoc(doc(db,'orgs',S.orgId,'invoices',id));
    });
  });
}

/* =========================================================
   CONCILIATION
========================================================= */
function renderConciliationLists(){
  const wrapMov = $('#unmatchedMovs');
  const wrapInv = $('#pendingInvoices');
  wrapMov.innerHTML = '';
  wrapInv.innerHTML = '';

  const matchedMovIds = new Set(S.matches.map(m=>m.movId));
  const matchedInvIds = new Set(S.matches.map(m=>m.invId));

  const movs = S.transactions
    .filter(t => !matchedMovIds.has(t.id))
    .filter(t => !t.matched)
    .slice(0, 40);

  const invs = S.invoices
    .filter(i => !matchedInvIds.has(i.id))
    .filter(i => !i.matched && (i.status !== 'canceled'))
    .slice(0, 40);

  if (!movs.length) wrapMov.innerHTML = `<div class="item"><div class="h">Sin pendientes</div><div class="s">Todo conciliado (o no hay movimientos).</div></div>`;
  if (!invs.length) wrapInv.innerHTML = `<div class="item"><div class="h">Sin pendientes</div><div class="s">Sube facturas o revisa estados.</div></div>`;

  movs.forEach(t=>{
    const el = document.createElement('div');
    el.className = 'item';
    const hint = suggestInvoicesForMov(t, invs);
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(t.description||'')}</div>
          <div class="s">${escapeHtml(t.date||'')} · <b>${fmtEUR(t.amount||0)}</b></div>
          ${hint ? `<div class="s">Sugerencia: ${escapeHtml(hint)}</div>` : ''}
        </div>
        <div class="row">
          <button class="btn ${S.selMovId===t.id ? 'primary' : ''}" data-selmov="${t.id}">Seleccionar</button>
        </div>
      </div>
    `;
    wrapMov.appendChild(el);
  });

  invs.forEach(i=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(i.party||'')} · ${escapeHtml(i.number||'')}</div>
          <div class="s">${escapeHtml(i.date||'')} · <b>${fmtEUR(i.total||0)}</b> ${i.due ? `· Vence: ${escapeHtml(i.due)}` : ''}</div>
        </div>
        <div class="row">
          <button class="btn ${S.selInvId===i.id ? 'primary' : ''}" data-selinv="${i.id}">Seleccionar</button>
        </div>
      </div>
    `;
    wrapInv.appendChild(el);
  });

  $$('button[data-selmov]', wrapMov).forEach(b=>{
    b.addEventListener('click', ()=>{
      S.selMovId = b.getAttribute('data-selmov');
      $('#selMov').textContent = S.selMovId;
      renderConciliationLists();
    });
  });

  $$('button[data-selinv]', wrapInv).forEach(b=>{
    b.addEventListener('click', ()=>{
      S.selInvId = b.getAttribute('data-selinv');
      $('#selInv').textContent = S.selInvId;
      renderConciliationLists();
    });
  });
}

function suggestInvoicesForMov(mov, invoices){
  // sugerencia simple por importe absoluto y ventana 20 días
  const amt = Math.abs(Number(mov.amount||0));
  const d0 = new Date(mov.date);
  const cand = invoices
    .map(i=>{
      const diffAmt = Math.abs(Math.abs(Number(i.total||0)) - amt);
      const dd = Math.abs((new Date(i.date).getTime() - d0.getTime()) / (1000*3600*24));
      const score = diffAmt*2 + dd; // peso
      return { i, score, diffAmt, dd };
    })
    .sort((a,b)=>a.score-b.score)[0];

  if (!cand) return '';
  if (cand.diffAmt <= 0.5 && cand.dd <= 20){
    return `${cand.i.party} · ${cand.i.number}`;
  }
  return '';
}

$('#btnMatch').addEventListener('click', async ()=>{
  const msg = $('#matchMsg');
  setMsg(msg,'');

  if (!S.selMovId || !S.selInvId){
    setMsg(msg,'Selecciona un movimiento y una factura.', 'err');
    return;
  }

  const mov = S.transactions.find(t=>t.id===S.selMovId);
  const inv = S.invoices.find(i=>i.id===S.selInvId);
  if (!mov || !inv){
    setMsg(msg,'Selección inválida.', 'err');
    return;
  }

  await addDoc(paths.matches(S.orgId), {
    movId: mov.id,
    invId: inv.id,
    createdAt: serverTimestamp(),
    createdBy: uid(),
    movAmount: mov.amount,
    invTotal: inv.total,
  });

  await updateDoc(doc(db,'orgs',S.orgId,'transactions',mov.id), {
    matched: true,
    updatedAt: serverTimestamp(),
  });
  await updateDoc(doc(db,'orgs',S.orgId,'invoices',inv.id), {
    matched: true,
    status: 'paid',
    updatedAt: serverTimestamp(),
  });

  S.selMovId = null;
  S.selInvId = null;
  $('#selMov').textContent = '—';
  $('#selInv').textContent = '—';
  setMsg(msg,'Conciliación creada.', 'ok');
});

$('#btnUnmatch').addEventListener('click', async ()=>{
  // deshacer por IDs seleccionados (si existen)
  const msg = $('#matchMsg');
  setMsg(msg,'');

  if (!S.selMovId || !S.selInvId){
    setMsg(msg,'Selecciona el movimiento y la factura que quieres deshacer.', 'err');
    return;
  }

  const match = S.matches.find(m => m.movId===S.selMovId && m.invId===S.selInvId);
  if (!match){
    setMsg(msg,'No existe conciliación exacta con esa selección.', 'err');
    return;
  }

  await deleteDoc(doc(db,'orgs',S.orgId,'matches',match.id));

  await updateDoc(doc(db,'orgs',S.orgId,'transactions',match.movId), {
    matched: false,
    updatedAt: serverTimestamp(),
  });

  await updateDoc(doc(db,'orgs',S.orgId,'invoices',match.invId), {
    matched: false,
    status: 'pending',
    updatedAt: serverTimestamp(),
  });

  setMsg(msg,'Conciliación deshecha.', 'ok');
});

function renderMatches(){
  const wrap = $('#matchesList');
  wrap.innerHTML = '';

  if (!S.matches.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin conciliaciones</div><div class="s">Conciliar te deja todo cuadrado.</div></div>`;
    return;
  }

  S.matches.forEach(m=>{
    const mov = S.transactions.find(t=>t.id===m.movId);
    const inv = S.invoices.find(i=>i.id===m.invId);
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(inv?.party||'—')} · ${escapeHtml(inv?.number||'—')}</div>
          <div class="s">Mov: ${escapeHtml(m.movId)} (${fmtEUR(m.movAmount||0)}) ↔ Fac: ${escapeHtml(m.invId)} (${fmtEUR(m.invTotal||0)})</div>
        </div>
        <div class="row">
          <button class="btn" data-del="${m.id}">Eliminar</button>
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      const id = b.getAttribute('data-del');
      const m = S.matches.find(x=>x.id===id);
      if (!m) return;

      await deleteDoc(doc(db,'orgs',S.orgId,'matches',id));
      // revert flags
      await updateDoc(doc(db,'orgs',S.orgId,'transactions',m.movId), { matched:false, updatedAt: serverTimestamp() });
      await updateDoc(doc(db,'orgs',S.orgId,'invoices',m.invId), { matched:false, status:'pending', updatedAt: serverTimestamp() });
    });
  });
}

/* =========================================================
   DASHBOARD
========================================================= */
function renderDash(){
  $('#orgName').textContent = S.org?.name || '—';

  const saldoTotal = S.accounts.reduce((a,acc)=>a + calcAccountBalance(acc.id), 0);

  // movimientos 30 días
  const now = Date.now();
  const mov30 = S.transactions.filter(t=>{
    const dt = new Date(t.date).getTime();
    return (now - dt) <= 30*24*3600*1000;
  }).length;

  const pendientes = S.transactions.filter(t=>!t.matched).length;

  $('#kpiSaldoTotal').textContent = fmtEUR(saldoTotal);
  $('#kpiMov30').textContent = String(mov30);
  $('#kpiPendientes').textContent = String(pendientes);

  const wrap = $('#dashAccounts');
  wrap.innerHTML = '';
  if (!S.accounts.length){
    wrap.innerHTML = `<div class="item"><div class="h">Crea una cuenta</div><div class="s">Luego importa un CSV.</div></div>`;
    return;
  }

  S.accounts.forEach(acc=>{
    const bal = calcAccountBalance(acc.id);
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(acc.name||'')}</div>
          <div class="s">Saldo: <b>${fmtEUR(bal)}</b> · Apertura: ${fmtEUR(acc.openingBalance||0)}</div>
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });
}

/* =========================================================
   ORG SETTINGS
========================================================= */
$('#btnSaveOrg').addEventListener('click', async ()=>{
  const name = $('#orgNameInput').value.trim();
  const msg = $('#orgMsg');
  setMsg(msg,'');

  if (!name){
    setMsg(msg,'Nombre inválido.', 'err');
    return;
  }
  await updateDoc(paths.org(S.orgId), { name, updatedAt: serverTimestamp() });
  setMsg(msg,'Org actualizada.', 'ok');
});

/* =========================================================
   AUTH STATE
========================================================= */
onAuthStateChanged(auth, async (u)=>{
  if (!u){
    cleanupSubs();
    S.orgId = null;
    S.org = null;
    show(authView,true);
    show(appView,false);
    show(userBadge,false);
    show(btnLogout,false);
    return;
  }

  show(authView,false);
  show(appView,true);
  show(userBadge,true);
  show(btnLogout,true);
  userBadge.textContent = u.email || u.uid;

  S.orgId = await ensureUserOrg();
  await loadOrg(S.orgId);

  // defaults
  $('#invDate').value = todayISO();

  subscribeAll();
  switchTab('dashboard');
});
