// app.js (ESM) — CloudTrack PRO v2
import { firebaseConfig } from './firebase-config.js';

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.4/firebase-app.js";
import {
  getAuth, onAuthStateChanged, signInWithEmailAndPassword,
  createUserWithEmailAndPassword, signOut
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-auth.js";

import {
  getFirestore, doc, getDoc, setDoc, addDoc, updateDoc, deleteDoc,
  collection, query, where, orderBy, limit, getDocs, onSnapshot,
  serverTimestamp, writeBatch
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-firestore.js";

import {
  getStorage, ref, uploadBytes, getDownloadURL
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-storage.js";

import { getAnalytics } from "https://www.gstatic.com/firebasejs/10.12.4/firebase-analytics.js";

/* =========================================================
   INIT
========================================================= */
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);

// Analytics: puede fallar en localhost / entornos sin https (no rompe la app)
try { getAnalytics(app); } catch {}

/* =========================================================
   HELPERS
========================================================= */
const $ = (s, r=document) => r.querySelector(s);
const $$ = (s, r=document) => Array.from(r.querySelectorAll(s));
const fmtEUR = (n) => new Intl.NumberFormat('es-ES', { style:'currency', currency:'EUR' }).format(Number(n||0));
const todayISO = () => new Date().toISOString().slice(0,10);
const escapeHtml = (s) => (s ?? '').toString()
  .replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')
  .replaceAll('"','&quot;').replaceAll("'","&#039;");
const setMsg = (el, text, type='') => {
  if (!el) return;
  el.className = 'msg' + (type ? ` ${type}` : '');
  el.textContent = text || '';
};
const show = (el, yes=true) => el && el.classList.toggle('hidden', !yes);

function uid(){ return auth.currentUser?.uid || null; }
function userEmail(){ return auth.currentUser?.email || ''; }

function idNow(prefix){
  return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

/* =========================================================
   STATE
========================================================= */
const S = {
  orgId: null,
  org: null,
  meRole: 'empleado',
  storeFilter: 'ALL',

  stores: [],
  accounts: [],
  rules: [],
  transactions: [],
  invoices: [],
  cashClosings: [],
  recoGroups: [],

  selMovIds: new Set(),
  selInvIds: new Set(),

  unsub: [],
};

function cleanupSubs(){
  S.unsub.forEach(fn => { try{ fn(); } catch{} });
  S.unsub = [];
}

/* =========================================================
   PATHS (ORG multiusuario)
   Estructura:
   orgs/{orgId}
     members/{uid} -> {role}
     stores/{storeId}
     accounts/{accountId}
     rules/{ruleId}
     transactions/{txId}
     invoices/{invId}
     cashClosings/{closeId}
     recoGroups/{groupId} -> {movIds[], invIds[], totals...}
========================================================= */
const P = {
  user: (uid)=>doc(db,'users',uid),
  org: (orgId)=>doc(db,'orgs',orgId),
  member: (orgId, uid)=>doc(db,'orgs',orgId,'members',uid),
  members: (orgId)=>collection(db,'orgs',orgId,'members'),
  stores: (orgId)=>collection(db,'orgs',orgId,'stores'),
  accounts: (orgId)=>collection(db,'orgs',orgId,'accounts'),
  rules: (orgId)=>collection(db,'orgs',orgId,'rules'),
  txs: (orgId)=>collection(db,'orgs',orgId,'transactions'),
  invoices: (orgId)=>collection(db,'orgs',orgId,'invoices'),
  cash: (orgId)=>collection(db,'orgs',orgId,'cashClosings'),
  groups: (orgId)=>collection(db,'orgs',orgId,'recoGroups'),
};

/* =========================================================
   UI: Tabs
========================================================= */
function switchTab(tab){
  $$('.tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  const ids = ['dashboard','stores','accounts','movs','rules','invoices','cash','reco','settings'];
  ids.forEach(id => show($('#tab-'+id), id === tab));
}

$$('.tab').forEach(b => b.addEventListener('click', ()=>switchTab(b.dataset.tab)));
switchTab('dashboard');

/* =========================================================
   MODAL
========================================================= */
function openModal(title, html){
  $('#modalTitle').textContent = title;
  $('#modalBody').innerHTML = html;
  show($('#modal'), true);
}
$('#btnCloseModal').addEventListener('click', ()=>show($('#modal'), false));
$('#modal').addEventListener('click', (e)=>{
  if (e.target === $('#modal')) show($('#modal'), false);
});

/* =========================================================
   ROLE / PERMS
========================================================= */
function isAdmin(){ return S.meRole === 'admin'; }
function isContable(){ return S.meRole === 'contable' || isAdmin(); }
function canEditCore(){ return isContable(); }  // cuentas, reglas, borrar, etc.

function refreshRoleBadge(){
  const rb = $('#roleBadge');
  rb.textContent = `ROL: ${S.meRole.toUpperCase()}`;
  show(rb, true);
}

/* =========================================================
   AUTH
========================================================= */
const authView = $('#authView');
const appView  = $('#appView');
const userBadge = $('#userBadge');
const btnLogout = $('#btnLogout');

$('#btnLogin').addEventListener('click', async ()=>{
  const email = $('#authEmail').value.trim();
  const pass = $('#authPass').value;
  setMsg($('#authMsg'), '');
  try{
    await signInWithEmailAndPassword(auth, email, pass);
  }catch(e){
    setMsg($('#authMsg'), e.message, 'err');
  }
});

$('#btnRegister').addEventListener('click', async ()=>{
  const email = $('#authEmail').value.trim();
  const pass = $('#authPass').value;
  setMsg($('#authMsg'), '');
  try{
    await createUserWithEmailAndPassword(auth, email, pass);
  }catch(e){
    setMsg($('#authMsg'), e.message, 'err');
  }
});

btnLogout.addEventListener('click', ()=>signOut(auth));

/* =========================================================
   ORG BOOTSTRAP (100% cloud)
   - Primer login: crea org propia y te pone admin.
   - Puedes crear otra org y añadir miembros.
========================================================= */
async function ensureUserDoc(){
  const u = auth.currentUser;
  if (!u) return;

  const uref = P.user(u.uid);
  const snap = await getDoc(uref);
  if (!snap.exists()){
    await setDoc(uref, {
      email: u.email,
      defaultOrgId: null,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    }, { merge:true });
  }
}

async function createNewOrg(name){
  const u = auth.currentUser;
  const orgId = idNow('org');
  const batch = writeBatch(db);

  batch.set(P.org(orgId), {
    name: name || 'Mi Empresa',
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
    ownerUid: u.uid,
  }, { merge:true });

  batch.set(P.member(orgId, u.uid), {
    uid: u.uid,
    email: u.email,
    role: 'admin',
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  }, { merge:true });

  // tiendas por defecto (puedes borrar luego)
  const s1 = doc(P.stores(orgId));
  const s2 = doc(P.stores(orgId));
  const s3 = doc(P.stores(orgId));
  batch.set(s1, { name:'San Pablo', alias:'SP', createdAt: serverTimestamp() });
  batch.set(s2, { name:'San Lesmes', alias:'SL', createdAt: serverTimestamp() });
  batch.set(s3, { name:'Santiago', alias:'ST', createdAt: serverTimestamp() });

  await batch.commit();

  // set defaultOrgId
  await updateDoc(P.user(u.uid), { defaultOrgId: orgId, updatedAt: serverTimestamp() });
  return orgId;
}

async function loadDefaultOrgId(){
  const u = auth.currentUser;
  const snap = await getDoc(P.user(u.uid));
  return snap.exists() ? (snap.data().defaultOrgId || null) : null;
}

async function setDefaultOrg(orgId){
  const u = auth.currentUser;
  await updateDoc(P.user(u.uid), { defaultOrgId: orgId, updatedAt: serverTimestamp() });
}

async function loadOrg(orgId){
  const o = await getDoc(P.org(orgId));
  if (!o.exists()) throw new Error('ORG no existe');
  S.org = o.data();
  $('#orgName').textContent = S.org.name || '—';
}

async function loadMyRole(orgId){
  const u = auth.currentUser;
  const ms = await getDoc(P.member(orgId, u.uid));
  if (!ms.exists()) throw new Error('No tienes acceso a esta ORG');
  S.meRole = ms.data().role || 'empleado';
  refreshRoleBadge();
}

/* =========================================================
   SUBSCRIPTIONS
========================================================= */
function applyStoreFilter(items){
  if (S.storeFilter === 'ALL') return items;
  return items.filter(x => x.storeId === S.storeFilter);
}

function subscribeAll(){
  cleanupSubs();
  const orgId = S.orgId;

  // stores
  S.unsub.push(onSnapshot(query(P.stores(orgId), orderBy('name','asc')), (qs)=>{
    S.stores = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderStores();
    renderStoreFilter();
    renderRuleStoreSelect();
    renderDash();
  }));

  // accounts
  S.unsub.push(onSnapshot(query(P.accounts(orgId), orderBy('createdAt','desc')), (qs)=>{
    S.accounts = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderAccounts();
    renderAccountFilter();
    renderDash();
  }));

  // rules
  S.unsub.push(onSnapshot(query(P.rules(orgId), orderBy('priority','desc')), (qs)=>{
    S.rules = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderRules();
  }));

  // txs
  S.unsub.push(onSnapshot(query(P.txs(orgId), orderBy('date','desc'), limit(1500)), (qs)=>{
    S.transactions = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderMovs();
    renderRecoLists();
    renderDash();
  }));

  // invoices
  S.unsub.push(onSnapshot(query(P.invoices(orgId), orderBy('date','desc'), limit(1500)), (qs)=>{
    S.invoices = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderInvoices();
    renderRecoLists();
    renderDash();
  }));

  // cash closings
  S.unsub.push(onSnapshot(query(P.cash(orgId), orderBy('date','desc'), limit(300)), (qs)=>{
    S.cashClosings = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderCash();
    renderDash();
  }));

  // reco groups
  S.unsub.push(onSnapshot(query(P.groups(orgId), orderBy('createdAt','desc'), limit(300)), (qs)=>{
    S.recoGroups = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderGroups();
    renderRecoLists();
    renderDash();
  }));
}

/* =========================================================
   STORE FILTER (GLOBAL)
========================================================= */
function renderStoreFilter(){
  const sel = $('#storeFilter');
  sel.innerHTML = '';
  const oAll = document.createElement('option');
  oAll.value = 'ALL';
  oAll.textContent = 'Todas';
  sel.appendChild(oAll);

  S.stores.forEach(s=>{
    const o = document.createElement('option');
    o.value = s.id;
    o.textContent = s.name;
    sel.appendChild(o);
  });

  sel.value = S.storeFilter;
}

$('#storeFilter').addEventListener('change', ()=>{
  S.storeFilter = $('#storeFilter').value || 'ALL';
  renderMovs();
  renderInvoices();
  renderCash();
  renderRecoLists();
  renderDash();
});

/* =========================================================
   STORES CRUD
========================================================= */
$('#btnAddStore').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const name = $('#storeName').value.trim();
  const alias = $('#storeAlias').value.trim();
  if (!name) return;

  await addDoc(P.stores(S.orgId), {
    name,
    alias: alias || null,
    createdAt: serverTimestamp(),
  });

  $('#storeName').value = '';
  $('#storeAlias').value = '';
});

function renderStores(){
  const wrap = $('#storesList');
  wrap.innerHTML = '';
  if (!S.stores.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin tiendas</div><div class="s">Crea una tienda para filtrar y controlar caja.</div></div>`;
    return;
  }

  S.stores.forEach(s=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(s.name||'')}</div>
          <div class="s">Alias: ${escapeHtml(s.alias||'—')}</div>
        </div>
        <div class="row">
          ${canEditCore() ? `<button class="btn" data-del="${s.id}">Eliminar</button>` : ''}
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      const id = b.getAttribute('data-del');
      await deleteDoc(doc(db,'orgs',S.orgId,'stores',id));
      if (S.storeFilter === id) { S.storeFilter = 'ALL'; renderStoreFilter(); }
    });
  });
}

function storeNameById(id){
  return S.stores.find(s=>s.id===id)?.name || '—';
}

/* =========================================================
   ACCOUNTS
========================================================= */
$('#btnAddAccount').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const name = $('#accName').value.trim();
  const ccy = $('#accCcy').value.trim() || 'EUR';
  const opening = Number($('#accOpening').value || 0);
  if (!name) return;

  await addDoc(P.accounts(S.orgId), {
    name, ccy, openingBalance: opening,
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
    .filter(t=>t.type==='bank' && t.accountId===accountId)
    .reduce((a,t)=>a + Number(t.amount||0), 0);
  return opening + sumTx;
}

function renderAccounts(){
  const wrap = $('#accountsList');
  wrap.innerHTML = '';
  if (!S.accounts.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin cuentas</div><div class="s">Añade una cuenta para importar CSV.</div></div>`;
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
        <div class="row">
          ${canEditCore() ? `<button class="btn" data-del="${acc.id}">Eliminar</button>` : ''}
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      if (!canEditCore()) return;
      const id = btn.getAttribute('data-del');
      const hasTx = S.transactions.some(t=>t.accountId===id);
      if (hasTx) return alert('Esta cuenta tiene movimientos. Borra/mueve movimientos primero.');
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
function renderRuleStoreSelect(){
  const sel = $('#ruleStore');
  sel.innerHTML = '';
  const oAll = document.createElement('option');
  oAll.value = '';
  oAll.textContent = 'Todas';
  sel.appendChild(oAll);
  S.stores.forEach(s=>{
    const o = document.createElement('option');
    o.value = s.id;
    o.textContent = s.name;
    sel.appendChild(o);
  });
}

$('#btnAddRule').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const needle = $('#ruleNeedle').value.trim();
  const category = $('#ruleCat').value.trim();
  const party = $('#ruleParty').value.trim();
  const type = $('#ruleType').value;
  const priority = Number($('#rulePrio').value || 10);
  const storeId = $('#ruleStore').value || null;

  if (!needle || !category) return;

  await addDoc(P.rules(S.orgId), {
    needle,
    category,
    party: party || null,
    type,
    priority,
    storeId,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  });

  $('#ruleNeedle').value = '';
  $('#ruleCat').value = '';
  $('#ruleParty').value = '';
  $('#rulePrio').value = '10';
  $('#ruleStore').value = '';
});

function renderRules(){
  const wrap = $('#rulesList');
  wrap.innerHTML = '';
  if (!S.rules.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin reglas</div><div class="s">Crea reglas para auto-categorizar.</div></div>`;
    return;
  }

  S.rules.forEach(r=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(r.needle)} → ${escapeHtml(r.category)}</div>
          <div class="s">
            Tipo: ${escapeHtml(r.type)} · Pri: ${escapeHtml(r.priority)}
            ${r.party ? `· Con: ${escapeHtml(r.party)}` : ''}
            · Tienda: ${r.storeId ? escapeHtml(storeNameById(r.storeId)) : 'Todas'}
          </div>
        </div>
        <div class="row">
          ${canEditCore() ? `<button class="btn" data-del="${r.id}">Eliminar</button>` : ''}
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      if (!canEditCore()) return;
      const id = btn.getAttribute('data-del');
      await deleteDoc(doc(db,'orgs',S.orgId,'rules',id));
    });
  });
}

/* =========================================================
   CSV PARSER (robusto: comillas, ; o ,)
========================================================= */
function detectDelimiter(text){
  const sample = text.split(/\r?\n/).slice(0,5).join('\n');
  const cComma = (sample.match(/,/g) || []).length;
  const cSemi  = (sample.match(/;/g) || []).length;
  return cSemi > cComma ? ';' : ',';
}

function parseCSV(text){
  const delim = detectDelimiter(text);
  const rows = [];
  let cur = '';
  let row = [];
  let inQuotes = false;

  for (let i=0;i<text.length;i++){
    const ch = text[i];
    if (ch === '"'){
      if (inQuotes && text[i+1] === '"'){ cur += '"'; i++; }
      else inQuotes = !inQuotes;
      continue;
    }
    if (!inQuotes && (ch === '\n' || ch === '\r')){
      if (cur.length || row.length){
        row.push(cur);
        rows.push(row.map(x=>x.trim()));
      }
      cur = ''; row = [];
      // skip \r\n
      if (ch === '\r' && text[i+1] === '\n') i++;
      continue;
    }
    if (!inQuotes && ch === delim){
      row.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (cur.length || row.length){
    row.push(cur);
    rows.push(row.map(x=>x.trim()));
  }

  const headers = (rows[0] || []).map(h => (h||'').toLowerCase());
  const data = rows.slice(1).filter(r => r.some(x=>String(x||'').trim() !== ''));
  return { headers, data, delim };
}

function detectCol(headers, candidates){
  const idx = headers.findIndex(h => candidates.some(c => h.includes(c)));
  return idx >= 0 ? idx : null;
}

function normalizeDate(s){
  const v = (s||'').trim();
  if (!v) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(v)){
    const [d,m,y] = v.split('/');
    return `${y}-${m}-${d}`;
  }
  const dt = new Date(v);
  if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0,10);
  return null;
}

function normalizeAmount(s){
  let v = (s||'').toString().trim();
  if (!v) return 0;
  v = v.replace(/\s/g,'');
  if (v.includes(',') && v.lastIndexOf(',') > v.lastIndexOf('.')){
    v = v.replaceAll('.','').replace(',','.');
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/* =========================================================
   RULE ENGINE
========================================================= */
function applyRulesToTx(tx){
  const desc = (tx.description||'').toLowerCase();
  const isIn = Number(tx.amount||0) > 0;
  const isOut = Number(tx.amount||0) < 0;

  let best = null;
  for (const r of S.rules){
    const needle = (r.needle||'').toLowerCase();
    if (!needle) continue;
    if (!desc.includes(needle)) continue;

    if (r.storeId && tx.storeId && r.storeId !== tx.storeId) continue;

    const type = r.type || 'any';
    if (type === 'in' && !isIn) continue;
    if (type === 'out' && !isOut) continue;

    if (!best || Number(r.priority||0) > Number(best.priority||0)) best = r;
  }

  if (!best) return tx;

  return {
    ...tx,
    category: best.category || tx.category || null,
    party: best.party || tx.party || null,
  };
}

/* =========================================================
   MOVS: filters + render + edit
========================================================= */
$('#movTypeFilter').addEventListener('change', renderMovs);
$('#movAccountFilter').addEventListener('change', renderMovs);
$('#movSearch').addEventListener('input', renderMovs);

function getFilteredMovs(){
  const type = $('#movTypeFilter').value;
  const accId = $('#movAccountFilter').value;
  const q = ($('#movSearch').value || '').trim().toLowerCase();

  let list = S.transactions;

  // tienda global
  list = applyStoreFilter(list);

  if (type) list = list.filter(t=>t.type===type);
  if (accId) list = list.filter(t=>t.accountId===accId);

  if (q){
    list = list.filter(t=>{
      const blob = `${t.description||''} ${t.party||''} ${t.category||''}`.toLowerCase();
      return blob.includes(q);
    });
  }
  return list;
}

function renderMovs(){
  const wrap = $('#movsList');
  const movs = getFilteredMovs();
  $('#movCount').textContent = String(movs.length);

  wrap.innerHTML = '';
  if (!movs.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin movimientos</div><div class="s">Importa CSV o registra caja.</div></div>`;
    return;
  }

  movs.slice(0, 300).forEach(t=>{
    const acc = S.accounts.find(a=>a.id===t.accountId);
    const signTag = Number(t.amount||0) >= 0 ? 'ok' : 'danger';

    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(t.description||'(sin concepto)')}</div>
          <div class="s">
            ${escapeHtml(t.date||'')} · ${escapeHtml(t.type||'bank').toUpperCase()}
            ${t.type==='bank' ? ` · ${escapeHtml(acc?.name||'Cuenta')}` : ''}
            · Tienda: ${escapeHtml(t.storeId ? storeNameById(t.storeId) : '—')}
            · <b>${fmtEUR(t.amount||0)}</b>
          </div>
          <div class="tags">
            <span class="tag ${signTag}">${Number(t.amount||0) >= 0 ? 'Ingreso' : 'Gasto'}</span>
            <span class="tag ${t.recoStatus==='reconciled' ? 'white' : ''}">${t.recoStatus==='reconciled' ? 'Conciliado' : 'Sin conciliar'}</span>
            <span class="tag">Cat: ${escapeHtml(t.category||'—')}</span>
            <span class="tag">Con: ${escapeHtml(t.party||'—')}</span>
          </div>
        </div>
        <div class="row">
          <button class="btn" data-edit="${t.id}">Editar</button>
          ${canEditCore() ? `<button class="btn" data-del="${t.id}">Eliminar</button>` : ''}
        </div>
      </div>

      <div class="edit hidden" id="edit-${t.id}">
        <div class="grid3">
          <div>
            <label>Categoría</label>
            <input data-cat="${t.id}" value="${escapeHtml(t.category||'')}"/>
          </div>
          <div>
            <label>Contraparte</label>
            <input data-party="${t.id}" value="${escapeHtml(t.party||'')}"/>
          </div>
          <div>
            <label>Tienda</label>
            <select data-store="${t.id}"></select>
          </div>
        </div>
        <div class="row">
          <button class="btn primary" data-save="${t.id}">Guardar</button>
          <button class="btn" data-cancel="${t.id}">Cerrar</button>
        </div>
      </div>
    `;

    wrap.appendChild(el);

    // fill store select
    const sel = $(`select[data-store="${t.id}"]`, el);
    sel.innerHTML = '';
    const o0 = document.createElement('option'); o0.value=''; o0.textContent='—';
    sel.appendChild(o0);
    S.stores.forEach(s=>{
      const o = document.createElement('option');
      o.value = s.id; o.textContent = s.name;
      sel.appendChild(o);
    });
    sel.value = t.storeId || '';
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
      const storeId = $(`select[data-store="${id}"]`)?.value || null;

      await updateDoc(doc(db,'orgs',S.orgId,'transactions',id), {
        category: cat,
        party: party,
        storeId: storeId || null,
        updatedAt: serverTimestamp(),
      });
      show($('#edit-'+id), false);
    });
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      if (!canEditCore()) return;
      const id = b.getAttribute('data-del');
      const tx = S.transactions.find(x=>x.id===id);
      if (tx?.recoStatus === 'reconciled') return alert('Este movimiento está conciliado. Borra el grupo primero.');
      await deleteDoc(doc(db,'orgs',S.orgId,'transactions',id));
    });
  });
}

/* =========================================================
   IMPORT CSV (bank txs)
========================================================= */
$('#btnImportCsv').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const file = $('#csvFile').files?.[0];
  if (!file) return;

  const accountId = $('#movAccountFilter').value || S.accounts[0]?.id;
  if (!accountId) return setMsg($('#csvMsg'),'Primero crea una cuenta.', 'err');

  try{
    const text = await file.text();
    const { headers, data } = parseCSV(text);

    const cDate = detectCol(headers, ['date','fecha','valor','f. valor','f.valor']);
    const cAmt  = detectCol(headers, ['amount','importe','cantidad','monto','import']);
    const cDesc = detectCol(headers, ['description','concepto','detalle','texto','descripcion','descripción']);

    if (cDate===null || cAmt===null || cDesc===null){
      throw new Error('CSV no reconocido. Necesita columnas tipo fecha/importe/concepto.');
    }

    // dedupe por hash: account|date|amount|desc
    const existingHashes = new Set(S.transactions.filter(t=>t.type==='bank' && t.accountId===accountId).map(t=>t.hash).filter(Boolean));

    let added = 0;
    for (const r of data){
      const date = normalizeDate(r[cDate]);
      const amount = normalizeAmount(r[cAmt]);
      const description = (r[cDesc]||'').toString().trim();
      if (!date || !description) continue;

      const hash = `${accountId}|${date}|${amount}|${description}`.slice(0,500);
      if (existingHashes.has(hash)) continue;

      const base = {
        type: 'bank',
        accountId,
        storeId: (S.storeFilter !== 'ALL') ? S.storeFilter : null,
        date,
        amount,
        description,
        party: null,
        category: null,
        recoStatus: 'open',
        hash,
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
      };

      const enriched = applyRulesToTx(base);
      await addDoc(P.txs(S.orgId), enriched);
      existingHashes.add(hash);
      added++;
    }

    setMsg($('#csvMsg'), `Importados ${added} movimientos (dedupe activo).`, 'ok');
    $('#csvFile').value = '';
  }catch(e){
    setMsg($('#csvMsg'), e.message || 'Error importando CSV', 'err');
  }
});

$('#btnApplyRules').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const filtered = getFilteredMovs().filter(t => t.recoStatus !== 'reconciled');
  if (!filtered.length) return;

  let updated = 0;
  for (const t of filtered){
    const newTx = applyRulesToTx(t);
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
   INVOICES
========================================================= */
$('#invDate').value = todayISO();

$('#btnAddInvoice').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const party = $('#invParty').value.trim();
  const number = $('#invNumber').value.trim();
  const total = Number($('#invTotal').value || 0);
  const date = $('#invDate').value || todayISO();
  const due = $('#invDue').value || null;
  const status = $('#invStatus').value;

  const file = $('#invPdf').files?.[0];
  const msg = $('#invMsg');
  setMsg(msg,'');

  if (!party || !number || !total) return setMsg(msg,'Completa proveedor/cliente, número y total.', 'err');
  if (!file) return setMsg(msg,'Sube el PDF de la factura.', 'err');

  try{
    const invId = idNow('inv');
    const storagePath = `orgs/${S.orgId}/invoices/${invId}.pdf`;
    const r = ref(storage, storagePath);
    await uploadBytes(r, file, { contentType:'application/pdf' });
    const url = await getDownloadURL(r);

    await addDoc(P.invoices(S.orgId), {
      storeId: (S.storeFilter !== 'ALL') ? S.storeFilter : null,
      party, number, total, date, due,
      status,
      pdfUrl: url,
      pdfPath: storagePath,
      recoStatus: 'open',
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

function getFilteredInvoices(){
  let list = S.invoices;
  list = applyStoreFilter(list);
  return list;
}

function renderInvoices(){
  const wrap = $('#invoicesList');
  wrap.innerHTML = '';
  const invs = getFilteredInvoices();
  if (!invs.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin facturas</div><div class="s">Sube PDFs para conciliar.</div></div>`;
    return;
  }

  invs.slice(0, 300).forEach(inv=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(inv.party||'')} · ${escapeHtml(inv.number||'')}</div>
          <div class="s">
            ${escapeHtml(inv.date||'')} · Tienda: ${escapeHtml(inv.storeId ? storeNameById(inv.storeId) : '—')}
            · Total: <b>${fmtEUR(inv.total||0)}</b>
            ${inv.due ? `· Vence: ${escapeHtml(inv.due)}` : ''}
          </div>
          <div class="tags">
            <span class="tag ${inv.status==='paid' ? 'white' : ''}">Estado: ${escapeHtml(inv.status||'pending')}</span>
            <span class="tag ${inv.recoStatus==='reconciled' ? 'white' : ''}">${inv.recoStatus==='reconciled' ? 'Conciliada' : 'Pendiente'}</span>
          </div>
        </div>
        <div class="row">
          ${inv.pdfUrl ? `<a class="btn" href="${inv.pdfUrl}" target="_blank" rel="noreferrer">Ver PDF</a>` : ''}
          ${canEditCore() ? `<button class="btn" data-paid="${inv.id}">Pagada</button>` : ''}
          ${canEditCore() ? `<button class="btn" data-del="${inv.id}">Eliminar</button>` : ''}
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-paid]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      if (!canEditCore()) return;
      const id = b.getAttribute('data-paid');
      await updateDoc(doc(db,'orgs',S.orgId,'invoices',id), { status:'paid', updatedAt: serverTimestamp() });
    });
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      if (!canEditCore()) return;
      const id = b.getAttribute('data-del');
      const inv = S.invoices.find(x=>x.id===id);
      if (inv?.recoStatus==='reconciled') return alert('Factura conciliada. Borra el grupo primero.');
      await deleteDoc(doc(db,'orgs',S.orgId,'invoices',id));
    });
  });
}

/* =========================================================
   CASH CLOSINGS (Caja diaria por tienda)
========================================================= */
$('#cashDate').value = todayISO();

$('#btnSaveCash').addEventListener('click', async ()=>{
  if (!isContable()) return alert('Sin permisos');
  const storeId = (S.storeFilter !== 'ALL') ? S.storeFilter : null;
  if (!storeId) return setMsg($('#cashMsg'),'Selecciona una tienda arriba (no “Todas”).', 'err');

  const date = $('#cashDate').value || todayISO();
  const cash = Number($('#cashCash').value || 0);
  const card = Number($('#cashCard').value || 0);
  const exp = Number($('#cashExp').value || 0);
  const notes = $('#cashNotes').value.trim() || null;

  await addDoc(P.cash(S.orgId), {
    storeId,
    date,
    cash,
    card,
    expenses: exp,
    total: (cash + card - exp),
    notes,
    createdAt: serverTimestamp(),
    createdBy: uid(),
  });

  setMsg($('#cashMsg'),'Cierre guardado.', 'ok');
  $('#cashCash').value = '';
  $('#cashCard').value = '';
  $('#cashExp').value = '';
  $('#cashNotes').value = '';
});

function renderCash(){
  const wrap = $('#cashList');
  wrap.innerHTML = '';

  let list = S.cashClosings;
  list = applyStoreFilter(list);

  if (!list.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin cierres</div><div class="s">Guarda cierres diarios por tienda.</div></div>`;
    return;
  }

  list.slice(0,200).forEach(c=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(c.date)} · ${escapeHtml(storeNameById(c.storeId))}</div>
          <div class="s">Efectivo: ${fmtEUR(c.cash||0)} · Tarjeta: ${fmtEUR(c.card||0)} · Gastos: ${fmtEUR(c.expenses||0)}</div>
          <div class="s"><b>Total:</b> ${fmtEUR(c.total||0)} ${c.notes ? `· ${escapeHtml(c.notes)}` : ''}</div>
        </div>
        <div class="row">
          ${isContable() ? `<button class="btn" data-del="${c.id}">Eliminar</button>` : ''}
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      if (!isContable()) return;
      const id = b.getAttribute('data-del');
      await deleteDoc(doc(db,'orgs',S.orgId,'cashClosings',id));
    });
  });
}

/* =========================================================
   RECONCILIATION PRO (Grupos)
========================================================= */
function isTxReconciled(txId){
  return S.transactions.find(t=>t.id===txId)?.recoStatus === 'reconciled';
}
function isInvReconciled(invId){
  return S.invoices.find(i=>i.id===invId)?.recoStatus === 'reconciled';
}

function renderRecoLists(){
  const wrapM = $('#recoMovs');
  const wrapI = $('#recoInvs');
  wrapM.innerHTML = '';
  wrapI.innerHTML = '';

  const groupedMovIds = new Set(S.recoGroups.flatMap(g=>g.movIds || []));
  const groupedInvIds = new Set(S.recoGroups.flatMap(g=>g.invIds || []));

  let movs = S.transactions.filter(t=>t.type==='bank' && !groupedMovIds.has(t.id) && t.recoStatus !== 'reconciled');
  movs = applyStoreFilter(movs).slice(0,60);

  let invs = S.invoices.filter(i=>i.status!=='canceled' && !groupedInvIds.has(i.id) && i.recoStatus !== 'reconciled');
  invs = applyStoreFilter(invs).slice(0,60);

  if (!movs.length) wrapM.innerHTML = `<div class="item"><div class="h">Sin pendientes</div><div class="s">Todo conciliado o no hay movimientos.</div></div>`;
  if (!invs.length) wrapI.innerHTML = `<div class="item"><div class="h">Sin pendientes</div><div class="s">Sube facturas o revisa estados.</div></div>`;

  movs.forEach(t=>{
    const checked = S.selMovIds.has(t.id);
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(t.description||'(sin concepto)')}</div>
          <div class="s">${escapeHtml(t.date)} · <b>${fmtEUR(t.amount||0)}</b> · ${escapeHtml(t.party||'—')}</div>
        </div>
        <div class="row">
          <button class="btn ${checked ? 'primary' : ''}" data-tx="${t.id}">${checked ? 'Quitar' : 'Añadir'}</button>
        </div>
      </div>
    `;
    wrapM.appendChild(el);
  });

  invs.forEach(i=>{
    const checked = S.selInvIds.has(i.id);
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">${escapeHtml(i.party||'')} · ${escapeHtml(i.number||'')}</div>
          <div class="s">${escapeHtml(i.date)} · <b>${fmtEUR(i.total||0)}</b> · ${escapeHtml(i.due||'')}</div>
        </div>
        <div class="row">
          <button class="btn ${checked ? 'primary' : ''}" data-inv="${i.id}">${checked ? 'Quitar' : 'Añadir'}</button>
        </div>
      </div>
    `;
    wrapI.appendChild(el);
  });

  $$('button[data-tx]', wrapM).forEach(b=>{
    b.addEventListener('click', ()=>{
      const id = b.getAttribute('data-tx');
      if (S.selMovIds.has(id)) S.selMovIds.delete(id); else S.selMovIds.add(id);
      updateSelCounts();
      renderRecoLists();
    });
  });

  $$('button[data-inv]', wrapI).forEach(b=>{
    b.addEventListener('click', ()=>{
      const id = b.getAttribute('data-inv');
      if (S.selInvIds.has(id)) S.selInvIds.delete(id); else S.selInvIds.add(id);
      updateSelCounts();
      renderRecoLists();
    });
  });
}

function updateSelCounts(){
  $('#selMovsCount').textContent = String(S.selMovIds.size);
  $('#selInvsCount').textContent = String(S.selInvIds.size);
}

$('#btnClearSel').addEventListener('click', ()=>{
  S.selMovIds.clear();
  S.selInvIds.clear();
  updateSelCounts();
  renderRecoLists();
});

$('#btnCreateGroup').addEventListener('click', async ()=>{
  if (!isContable()) return alert('Sin permisos');
  const msg = $('#recoMsg'); setMsg(msg,'');

  if (!S.selMovIds.size || !S.selInvIds.size){
    return setMsg(msg,'Selecciona al menos 1 movimiento y 1 factura.', 'err');
  }

  const movIds = Array.from(S.selMovIds);
  const invIds = Array.from(S.selInvIds);

  // Totales
  const movTotal = movIds.reduce((a,id)=>a + Number(S.transactions.find(t=>t.id===id)?.amount||0), 0);
  const invTotal = invIds.reduce((a,id)=>a + Number(S.invoices.find(i=>i.id===id)?.total||0), 0);
  const diff = movTotal - invTotal;

  const groupId = idNow('group');
  await setDoc(doc(db,'orgs',S.orgId,'recoGroups',groupId), {
    movIds,
    invIds,
    movTotal,
    invTotal,
    diff,
    storeId: (S.storeFilter !== 'ALL') ? S.storeFilter : null,
    createdAt: serverTimestamp(),
    createdBy: uid(),
  });

  // Marcar estados
  for (const id of movIds){
    await updateDoc(doc(db,'orgs',S.orgId,'transactions',id), { recoStatus:'reconciled', updatedAt: serverTimestamp() });
  }
  for (const id of invIds){
    await updateDoc(doc(db,'orgs',S.orgId,'invoices',id), { recoStatus:'reconciled', status:'paid', updatedAt: serverTimestamp() });
  }

  S.selMovIds.clear(); S.selInvIds.clear(); updateSelCounts();
  setMsg(msg, `Grupo creado. Diferencia: ${fmtEUR(diff)} (movs - facturas)`, 'ok');
});

function renderGroups(){
  const wrap = $('#groupsList');
  wrap.innerHTML = '';

  let groups = S.recoGroups;
  groups = applyStoreFilter(groups);

  if (!groups.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin grupos</div><div class="s">Crea grupos para pagos parciales o agrupados.</div></div>`;
    return;
  }

  groups.slice(0,200).forEach(g=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="top">
        <div>
          <div class="h">Grupo ${escapeHtml(g.id)}</div>
          <div class="s">Tienda: ${escapeHtml(g.storeId ? storeNameById(g.storeId) : '—')}</div>
          <div class="s">Movs: <b>${fmtEUR(g.movTotal||0)}</b> · Facturas: <b>${fmtEUR(g.invTotal||0)}</b> · Dif: <b>${fmtEUR(g.diff||0)}</b></div>
          <div class="s">MovIds: ${escapeHtml((g.movIds||[]).slice(0,6).join(', '))}${(g.movIds||[]).length>6?'…':''}</div>
          <div class="s">InvIds: ${escapeHtml((g.invIds||[]).slice(0,6).join(', '))}${(g.invIds||[]).length>6?'…':''}</div>
        </div>
        <div class="row">
          ${isContable() ? `<button class="btn" data-del="${g.id}">Eliminar grupo</button>` : ''}
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      if (!isContable()) return;
      const id = b.getAttribute('data-del');
      const g = S.recoGroups.find(x=>x.id===id);
      if (!g) return;

      // revert statuses
      for (const mid of (g.movIds||[])){
        await updateDoc(doc(db,'orgs',S.orgId,'transactions',mid), { recoStatus:'open', updatedAt: serverTimestamp() });
      }
      for (const iid of (g.invIds||[])){
        await updateDoc(doc(db,'orgs',S.orgId,'invoices',iid), { recoStatus:'open', status:'pending', updatedAt: serverTimestamp() });
      }
      await deleteDoc(doc(db,'orgs',S.orgId,'recoGroups',id));
    });
  });
}

/* =========================================================
   DASHBOARD
========================================================= */
function renderDash(){
  $('#orgName').textContent = S.org?.name || '—';

  // saldo bancos
  const bankBal = S.accounts.reduce((a,acc)=>a + calcAccountBalance(acc.id), 0);

  // caja hoy (último cierre de hoy por tienda seleccionada)
  const d = todayISO();
  let cashList = S.cashClosings.filter(c=>c.date===d);
  cashList = applyStoreFilter(cashList);
  const cashToday = cashList.reduce((a,c)=>a + Number(c.total||0), 0);

  // pendientes conciliar
  let txPend = S.transactions.filter(t=>t.type==='bank' && t.recoStatus!=='reconciled');
  let invPend = S.invoices.filter(i=>i.status!=='canceled' && i.recoStatus!=='reconciled');
  txPend = applyStoreFilter(txPend);
  invPend = applyStoreFilter(invPend);

  $('#kpiBank').textContent = fmtEUR(bankBal);
  $('#kpiCashToday').textContent = fmtEUR(cashToday);
  $('#kpiPending').textContent = String(txPend.length + invPend.length);

  // dash accounts
  const wrapA = $('#dashAccounts');
  wrapA.innerHTML = '';
  if (!S.accounts.length){
    wrapA.innerHTML = `<div class="item"><div class="h">Crea una cuenta</div><div class="s">Luego importa CSV.</div></div>`;
  } else {
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
      wrapA.appendChild(el);
    });
  }

  // dash cash
  const wrapC = $('#dashCash');
  wrapC.innerHTML = '';
  let cash = S.cashClosings.slice();
  cash = applyStoreFilter(cash).slice(0,6);
  if (!cash.length){
    wrapC.innerHTML = `<div class="item"><div class="h">Sin cierres</div><div class="s">Guarda cierres para verlos aquí.</div></div>`;
  } else {
    cash.forEach(c=>{
      const el = document.createElement('div');
      el.className = 'item';
      el.innerHTML = `
        <div class="top">
          <div>
            <div class="h">${escapeHtml(c.date)} · ${escapeHtml(storeNameById(c.storeId))}</div>
            <div class="s">Total: <b>${fmtEUR(c.total||0)}</b> (Efe ${fmtEUR(c.cash||0)} + Tar ${fmtEUR(c.card||0)} - Gast ${fmtEUR(c.expenses||0)})</div>
          </div>
        </div>
      `;
      wrapC.appendChild(el);
    });
  }
}

/* =========================================================
   ORG + MEMBERS UI
========================================================= */
$('#btnOpenOrg').addEventListener('click', async ()=>{
  const html = `
    <div class="card soft">
      <h3>Crear nueva ORG</h3>
      <label>Nombre</label>
      <input id="newOrgName" placeholder="Mi Empresa"/>
      <div class="row">
        <button class="btn primary" id="btnCreateOrg">Crear</button>
      </div>
      <div class="hr"></div>
      <h3>Cambiar ORG (por ID)</h3>
      <label>OrgId</label>
      <input id="orgIdInput" placeholder="org_..."/>
      <div class="row">
        <button class="btn" id="btnSetOrg">Usar esta ORG</button>
      </div>
      <p class="muted">Solo podrás entrar si eres miembro de esa ORG.</p>
    </div>
  `;
  openModal('Organización', html);

  $('#btnCreateOrg').addEventListener('click', async ()=>{
    const name = $('#newOrgName').value.trim() || 'Mi Empresa';
    const orgId = await createNewOrg(name);
    await boot(orgId);
    show($('#modal'), false);
  });

  $('#btnSetOrg').addEventListener('click', async ()=>{
    const orgId = $('#orgIdInput').value.trim();
    if (!orgId) return;
    await setDefaultOrg(orgId);
    await boot(orgId);
    show($('#modal'), false);
  });
});

$('#btnOpenMembers').addEventListener('click', async ()=>{
  if (!isAdmin()) return alert('Solo admin puede gestionar miembros');
  const orgId = S.orgId;

  const ms = await getDocs(query(P.members(orgId), orderBy('createdAt','desc')));
  const rows = ms.docs.map(d=>({id:d.id, ...d.data()}));

  const html = `
    <div class="card soft">
      <h3>Añadir miembro</h3>
      <p class="muted">Añade por UID (rápido). Si quieres, luego hacemos “invitación por email”.</p>
      <div class="grid2">
        <div>
          <label>UID del usuario</label>
          <input id="memUid" placeholder="uid..."/>
        </div>
        <div>
          <label>Rol</label>
          <select id="memRole">
            <option value="empleado">empleado</option>
            <option value="contable">contable</option>
            <option value="admin">admin</option>
          </select>
        </div>
      </div>
      <div class="row">
        <button class="btn primary" id="btnAddMem">Añadir</button>
      </div>

      <div class="hr"></div>
      <h3>Miembros</h3>
      <div class="list">
        ${rows.map(r=>`
          <div class="item">
            <div class="top">
              <div>
                <div class="h">${escapeHtml(r.email||'(sin email)')}</div>
                <div class="s">UID: ${escapeHtml(r.uid||r.id)} · Rol: <b>${escapeHtml(r.role||'empleado')}</b></div>
              </div>
              <div class="row">
                <button class="btn" data-remove="${escapeHtml(r.id)}">Quitar</button>
              </div>
            </div>
          </div>
        `).join('')}
      </div>
      <p class="muted">Tu ORG ID: <b>${escapeHtml(orgId)}</b></p>
    </div>
  `;

  openModal('Miembros', html);

  $('#btnAddMem').addEventListener('click', async ()=>{
    const memUid = $('#memUid').value.trim();
    const memRole = $('#memRole').value;
    if (!memUid) return;
    await setDoc(P.member(orgId, memUid), {
      uid: memUid,
      email: null,
      role: memRole,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    }, { merge:true });
    alert('Miembro añadido. Recarga miembros.');
  });

  $$('button[data-remove]', $('#modalBody')).forEach(b=>{
    b.addEventListener('click', async ()=>{
      const id = b.getAttribute('data-remove');
      if (id === uid()) return alert('No puedes quitarte a ti mismo.');
      await deleteDoc(P.member(orgId, id));
      alert('Eliminado. Recarga miembros.');
    });
  });
});

/* =========================================================
   BOOT
========================================================= */
async function boot(orgId){
  S.orgId = orgId;
  await loadOrg(orgId);
  await loadMyRole(orgId);
  subscribeAll();
  renderStoreFilter();
  updateSelCounts();
  renderDash();
  switchTab('dashboard');
}

/* =========================================================
   AUTH STATE
========================================================= */
onAuthStateChanged(auth, async (u)=>{
  if (!u){
    cleanupSubs();
    S.orgId = null;
    S.org = null;
    S.meRole = 'empleado';
    S.storeFilter = 'ALL';
    S.selMovIds.clear(); S.selInvIds.clear();

    show(authView,true);
    show(appView,false);
    show(userBadge,false);
    show(btnLogout,false);
    show($('#roleBadge'), false);
    return;
  }

  show(authView,false);
  show(appView,true);
  show(userBadge,true);
  show(btnLogout,true);
  userBadge.textContent = userEmail();

  await ensureUserDoc();

  let orgId = await loadDefaultOrgId();
  if (!orgId){
    orgId = await createNewOrg('Mi Empresa');
  }

  try{
    await boot(orgId);
  }catch(e){
    // si no tiene acceso al org por alguna razón, crea una nueva
    const newOrg = await createNewOrg('Mi Empresa');
    await boot(newOrg);
  }

  // defaults
  $('#invDate').value = todayISO();
  $('#cashDate').value = todayISO();
});
