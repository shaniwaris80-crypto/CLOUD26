// app.js — CloudTrack PRO (White Fintech) — 100% Cloud
import { firebaseConfig } from './firebase-config.js';

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.4/firebase-app.js";
import {
  getAuth, onAuthStateChanged,
  signInWithEmailAndPassword, createUserWithEmailAndPassword, signOut
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-auth.js";

import {
  getFirestore,
  doc, getDoc, setDoc, addDoc, updateDoc, deleteDoc,
  collection, query, orderBy, limit, where, getDocs, onSnapshot,
  serverTimestamp, writeBatch
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-firestore.js";

import {
  getStorage, ref, uploadBytes, getDownloadURL
} from "https://www.gstatic.com/firebasejs/10.12.4/firebase-storage.js";

// Optional analytics; no rompe si falla
import { getAnalytics } from "https://www.gstatic.com/firebasejs/10.12.4/firebase-analytics.js";

/* =========================================================
   INIT
========================================================= */
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);
try { getAnalytics(app); } catch {}

/* =========================================================
   HELPERS
========================================================= */
const $ = (s, r=document) => r.querySelector(s);
const $$ = (s, r=document) => Array.from(r.querySelectorAll(s));
const escapeHtml = (s) => (s ?? '').toString()
  .replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')
  .replaceAll('"','&quot;').replaceAll("'","&#039;");
const fmtEUR = (n) => new Intl.NumberFormat('es-ES', { style:'currency', currency:'EUR' }).format(Number(n||0));
const todayISO = () => new Date().toISOString().slice(0,10);
const toISO = (d) => new Date(d).toISOString().slice(0,10);
const setMsg = (el, text, type='') => {
  if (!el) return;
  el.className = `msg ${type}`.trim();
  el.textContent = text || '';
};
const show = (el, yes=true) => el && el.classList.toggle('hidden', !yes);

function uid(){ return auth.currentUser?.uid || null; }
function email(){ return auth.currentUser?.email || ''; }
function initialsFromEmail(e){
  const v = (e||'').split('@')[0].trim();
  return (v[0] || 'U').toUpperCase();
}
function idNow(prefix='id'){
  return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}
function clamp(n, a, b){ return Math.max(a, Math.min(b, n)); }

/* =========================================================
   STATE
========================================================= */
const S = {
  orgId: null,
  org: null,
  meRole: 'empleado',
  theme: 'light',

  storeFilter: 'ALL',
  dateFrom: null,
  dateTo: null,

  stores: [],
  accounts: [],
  rules: [],
  transactions: [],
  invoices: [],
  cashClosings: [],
  recoGroups: [],
  auditLogs: [],

  selMovIds: new Set(),
  selInvIds: new Set(),

  charts: { flow:null, cats:null },
  unsub: [],
};

function cleanupSubs(){
  S.unsub.forEach(fn => { try{ fn(); } catch{} });
  S.unsub = [];
}

/* =========================================================
   PATHS
========================================================= */
const P = {
  user: (uid)=>doc(db,'users',uid),
  org: (orgId)=>doc(db,'orgs',orgId),
  member: (orgId, uid)=>doc(db,'orgs',orgId,'members',uid),

  members: (orgId)=>collection(db,'orgs',orgId,'members'),
  invites: (orgId)=>collection(db,'orgs',orgId,'invites'),

  stores: (orgId)=>collection(db,'orgs',orgId,'stores'),
  accounts: (orgId)=>collection(db,'orgs',orgId,'accounts'),
  rules: (orgId)=>collection(db,'orgs',orgId,'rules'),
  txs: (orgId)=>collection(db,'orgs',orgId,'transactions'),
  invoices: (orgId)=>collection(db,'orgs',orgId,'invoices'),
  cash: (orgId)=>collection(db,'orgs',orgId,'cashClosings'),
  groups: (orgId)=>collection(db,'orgs',orgId,'recoGroups'),
  audit: (orgId)=>collection(db,'orgs',orgId,'auditLogs'),
};

/* =========================================================
   PERMISSIONS
========================================================= */
const isAdmin = () => S.meRole === 'admin';
const isContable = () => S.meRole === 'contable' || isAdmin();
const canEditCore = () => isContable();

/* =========================================================
   THEME (default WHITE)
========================================================= */
function applyTheme(theme){
  S.theme = theme === 'dark' ? 'dark' : 'light';
  document.body.setAttribute('data-theme', S.theme);
  const label = $('#themeLabel'); if (label) label.textContent = S.theme.toUpperCase();
}
async function saveThemePref(){
  const u = auth.currentUser;
  if (!u) return;
  await setDoc(P.user(u.uid), { prefs:{ theme:S.theme }, updatedAt: serverTimestamp() }, { merge:true });
}
async function loadThemePref(){
  const u = auth.currentUser;
  if (!u) return applyTheme('light');
  const snap = await getDoc(P.user(u.uid));
  const theme = snap.exists() ? (snap.data()?.prefs?.theme || 'light') : 'light';
  applyTheme(theme);
}

/* =========================================================
   NAV / TABS
========================================================= */
function switchTab(tab){
  $$('.navitem').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  const ids = ['dashboard','movs','invoices','cash','reco','accounts','stores','rules','exports','audit','settings'];
  ids.forEach(id => show($('#tab-'+id), id === tab));
  // algunos tabs necesitan render rápido
  if (tab === 'dashboard') renderDashboard();
  if (tab === 'movs') renderMovs();
  if (tab === 'invoices') renderInvoices();
  if (tab === 'cash') renderCash();
  if (tab === 'reco') renderRecoLists();
  if (tab === 'accounts') renderAccounts();
  if (tab === 'stores') renderStores();
  if (tab === 'rules') renderRules();
  if (tab === 'audit') renderAudit();
}
$$('.navitem').forEach(b => b.addEventListener('click', ()=>switchTab(b.dataset.tab)));
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
$('#modal').addEventListener('click', (e)=>{ if (e.target === $('#modal')) show($('#modal'), false); });

/* =========================================================
   AUTH UI
========================================================= */
const authView = $('#authView');
const appView  = $('#appView');

$('#btnLogin').addEventListener('click', async ()=>{
  setMsg($('#authMsg'), '');
  try{
    await signInWithEmailAndPassword(auth, $('#authEmail').value.trim(), $('#authPass').value);
  }catch(e){
    setMsg($('#authMsg'), e.message, 'err');
  }
});

$('#btnRegister').addEventListener('click', async ()=>{
  setMsg($('#authMsg'), '');
  try{
    await createUserWithEmailAndPassword(auth, $('#authEmail').value.trim(), $('#authPass').value);
  }catch(e){
    setMsg($('#authMsg'), e.message, 'err');
  }
});

$('#btnLogout').addEventListener('click', ()=>signOut(auth));

/* =========================================================
   GLOBAL FILTERS
========================================================= */
function initDefaultDateRange(){
  // último mes por defecto
  const now = new Date();
  const start = new Date(now); start.setDate(start.getDate()-30);
  S.dateFrom = toISO(start);
  S.dateTo = toISO(now);
  $('#dateFrom').value = S.dateFrom;
  $('#dateTo').value = S.dateTo;
}
initDefaultDateRange();

$('#dateFrom').addEventListener('change', ()=>{
  S.dateFrom = $('#dateFrom').value || null;
  refreshAllViews();
});
$('#dateTo').addEventListener('change', ()=>{
  S.dateTo = $('#dateTo').value || null;
  refreshAllViews();
});

$('#storeFilter').addEventListener('change', ()=>{
  S.storeFilter = $('#storeFilter').value || 'ALL';
  refreshAllViews();
});

$('#globalSearch').addEventListener('input', ()=>refreshAllViews());
$('#typeFilter').addEventListener('change', ()=>refreshAllViews());
$('#accountFilter').addEventListener('change', ()=>refreshAllViews());
$('#statusFilter').addEventListener('change', ()=>refreshAllViews());

function inRangeISO(dateISO){
  if (!dateISO) return false;
  const d = dateISO;
  if (S.dateFrom && d < S.dateFrom) return false;
  if (S.dateTo && d > S.dateTo) return false;
  return true;
}

function applyStoreFilter(items){
  if (S.storeFilter === 'ALL') return items;
  return items.filter(x => x.storeId === S.storeFilter);
}

function applySearch(items, fieldsFn){
  const q = ($('#globalSearch').value || '').trim().toLowerCase();
  if (!q) return items;
  return items.filter(x => fieldsFn(x).toLowerCase().includes(q));
}

function refreshAllViews(){
  renderDashboard();
  renderMovs();
  renderInvoices();
  renderCash();
  renderRecoLists();
  renderAudit();
}

/* =========================================================
   QUICK ACTIONS
========================================================= */
$('#btnQuickImport').addEventListener('click', ()=>switchTab('movs'));
$('#btnQuickInvoice').addEventListener('click', ()=>{
  switchTab('invoices');
  $('#invParty')?.focus();
});
$('#btnQuickCash').addEventListener('click', ()=>{
  switchTab('cash');
  $('#cashCash')?.focus();
});

/* =========================================================
   CSV PARSER (robusto)
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
      if (ch === '\r' && text[i+1] === '\n') i++;
      continue;
    }
    if (!inQuotes && ch === delim){
      row.push(cur); cur = ''; continue;
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

    const type = r.type || 'any';
    if (type === 'in' && !isIn) continue;
    if (type === 'out' && !isOut) continue;

    if (r.storeId && tx.storeId && r.storeId !== tx.storeId) continue;

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
   AUDIT LOG
========================================================= */
async function audit(action, details={}){
  if (!S.orgId || !uid()) return;
  try{
    await addDoc(P.audit(S.orgId), {
      action,
      details,
      byUid: uid(),
      byEmail: email(),
      storeId: (S.storeFilter !== 'ALL') ? S.storeFilter : null,
      createdAt: serverTimestamp(),
    });
  }catch{}
}

/* =========================================================
   ORG / MEMBERS / INVITES
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
      prefs: { theme: 'light' },
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
    ownerUid: u.uid,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  }, { merge:true });

  batch.set(P.member(orgId, u.uid), {
    uid: u.uid,
    email: u.email,
    role: 'admin',
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  }, { merge:true });

  // tiendas por defecto
  const s1 = doc(P.stores(orgId));
  const s2 = doc(P.stores(orgId));
  const s3 = doc(P.stores(orgId));
  batch.set(s1, { name:'San Pablo', alias:'SP', createdAt: serverTimestamp() });
  batch.set(s2, { name:'San Lesmes', alias:'SL', createdAt: serverTimestamp() });
  batch.set(s3, { name:'Santiago', alias:'ST', createdAt: serverTimestamp() });

  await batch.commit();
  await updateDoc(P.user(u.uid), { defaultOrgId: orgId, updatedAt: serverTimestamp() });
  await audit('ORG_CREATE', { orgId, name });

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
  const snap = await getDoc(P.org(orgId));
  if (!snap.exists()) throw new Error('ORG no existe');
  S.org = snap.data();
  $('#orgName').textContent = S.org.name || '—';
}
async function loadMyRole(orgId){
  const u = auth.currentUser;
  const ms = await getDoc(P.member(orgId, u.uid));
  if (!ms.exists()) throw new Error('No tienes acceso a esta ORG');
  S.meRole = ms.data().role || 'empleado';
  $('#roleBadge').textContent = `ROL: ${S.meRole.toUpperCase()}`;
}

$('#btnOrg').addEventListener('click', ()=>{
  const html = `
    <div class="card">
      <h3>Organización</h3>
      <p class="muted">Crea una nueva ORG o cambia a otra (si eres miembro).</p>

      <div class="grid2">
        <div>
          <label>Crear ORG</label>
          <input id="newOrgName" class="input" placeholder="Mi Empresa"/>
          <div class="row right">
            <button class="btn primary" id="btnCreateOrg">Crear</button>
          </div>
        </div>

        <div>
          <label>Cambiar ORG (OrgId)</label>
          <input id="orgIdInput" class="input" placeholder="org_..."/>
          <div class="row right">
            <button class="btn" id="btnUseOrg">Usar ORG</button>
          </div>
        </div>
      </div>

      <div class="hr"></div>
      <p class="muted">Tu ORG actual: <b>${escapeHtml(S.orgId||'—')}</b></p>
    </div>
  `;
  openModal('ORG', html);

  $('#btnCreateOrg').addEventListener('click', async ()=>{
    const name = $('#newOrgName').value.trim() || 'Mi Empresa';
    const orgId = await createNewOrg(name);
    await boot(orgId);
    show($('#modal'), false);
  });

  $('#btnUseOrg').addEventListener('click', async ()=>{
    const orgId = $('#orgIdInput').value.trim();
    if (!orgId) return;
    await setDefaultOrg(orgId);
    await boot(orgId);
    show($('#modal'), false);
  });
});

async function renderMembersModal(){
  if (!isAdmin()) return alert('Solo ADMIN');
  const orgId = S.orgId;
  const ms = await getDocs(query(P.members(orgId), orderBy('createdAt','desc')));
  const rows = ms.docs.map(d=>({id:d.id, ...d.data()}));

  const html = `
    <div class="card">
      <h3>Miembros</h3>
      <p class="muted">Admin gestiona roles. Para invitar: crea token por email y envíalo.</p>

      <div class="grid2">
        <div>
          <h3>Crear invitación</h3>
          <label>Email</label>
          <input id="invEmail" class="input" placeholder="usuario@email.com"/>
          <label>Rol</label>
          <select id="invRole" class="select">
            <option value="empleado">empleado</option>
            <option value="contable">contable</option>
            <option value="admin">admin</option>
          </select>
          <div class="row right">
            <button class="btn primary" id="btnMakeInvite">Crear token</button>
          </div>
          <div id="invOut" class="msg"></div>
        </div>

        <div>
          <h3>Añadir por UID (manual)</h3>
          <label>UID</label>
          <input id="memUid" class="input" placeholder="uid..."/>
          <label>Rol</label>
          <select id="memRole" class="select">
            <option value="empleado">empleado</option>
            <option value="contable">contable</option>
            <option value="admin">admin</option>
          </select>
          <div class="row right">
            <button class="btn" id="btnAddMem">Añadir</button>
          </div>
        </div>
      </div>

      <div class="hr"></div>
      <h3>Listado</h3>
      <div class="list">
        ${rows.map(r=>`
          <div class="item">
            <div class="itemtop">
              <div>
                <div class="h">${escapeHtml(r.email||'(sin email)')}</div>
                <div class="s">UID: ${escapeHtml(r.uid||r.id)} · Rol: <b>${escapeHtml(r.role||'empleado')}</b></div>
              </div>
              <div class="row">
                <button class="btn danger" data-remove="${escapeHtml(r.id)}">Quitar</button>
              </div>
            </div>
          </div>
        `).join('')}
      </div>

      <div class="hr"></div>
      <p class="muted">ORG ID: <b>${escapeHtml(orgId)}</b></p>
    </div>
  `;
  openModal('Miembros', html);

  $('#btnMakeInvite').addEventListener('click', async ()=>{
    const invEmail = $('#invEmail').value.trim().toLowerCase();
    const role = $('#invRole').value;
    if (!invEmail) return setMsg($('#invOut'), 'Email requerido', 'err');

    const token = `INV-${Math.random().toString(36).slice(2,8).toUpperCase()}-${Date.now().toString(36).toUpperCase()}`;
    await addDoc(P.invites(orgId), {
      token,
      email: invEmail,
      role,
      used: false,
      createdAt: serverTimestamp(),
      createdBy: email(),
    });
    await audit('INVITE_CREATE', { invEmail, role, token });
    setMsg($('#invOut'), `Token creado: ${token} (envíalo al usuario)`, 'ok');
  });

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
    await audit('MEMBER_ADD_UID', { memUid, memRole });
    alert('Añadido. Cierra y abre para refrescar.');
  });

  $$('button[data-remove]', $('#modalBody')).forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      const id = btn.getAttribute('data-remove');
      if (id === uid()) return alert('No puedes quitarte a ti mismo.');
      await deleteDoc(P.member(orgId, id));
      await audit('MEMBER_REMOVE', { memberId:id });
      alert('Eliminado. Cierra y abre para refrescar.');
    });
  });
}

$('#btnMembers').addEventListener('click', renderMembersModal);

/* Accept invite token (en Auth view) */
$('#btnAcceptInvite').addEventListener('click', async ()=>{
  setMsg($('#authMsg'), '');
  const token = ($('#inviteToken').value || '').trim();
  if (!token) return setMsg($('#authMsg'), 'Pega un token INV-...', 'err');
  if (!uid()) return setMsg($('#authMsg'), 'Primero inicia sesión con tu cuenta.', 'err');

  // buscamos token en orgs/*/invites: sin index global, hacemos búsqueda por colección group:
  // Para simplificar sin collectionGroup, pedimos orgId dentro del token? No.
  // SOLUCIÓN: invitación se hace desde modal y se comparte además con orgId.
  // Aquí soportamos formato: INV...|orgId
  if (!token.includes('|')) return setMsg($('#authMsg'), 'Token inválido. Formato esperado: INV-...|org_...', 'err');

  const [tok, orgId] = token.split('|').map(x=>x.trim());
  if (!tok || !orgId) return setMsg($('#authMsg'), 'Token inválido.', 'err');

  try{
    const qs = await getDocs(query(P.invites(orgId), where('token','==', tok), limit(1)));
    if (qs.empty) return setMsg($('#authMsg'), 'Token no encontrado.', 'err');
    const invDoc = qs.docs[0];
    const inv = invDoc.data();
    if (inv.used) return setMsg($('#authMsg'), 'Token ya usado.', 'err');
    const invEmail = (inv.email||'').toLowerCase();
    if (invEmail && invEmail !== email().toLowerCase()) {
      return setMsg($('#authMsg'), 'Este token es para otro email.', 'err');
    }

    await setDoc(P.member(orgId, uid()), {
      uid: uid(),
      email: email(),
      role: inv.role || 'empleado',
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    }, { merge:true });

    await updateDoc(invDoc.ref, { used:true, usedByUid: uid(), usedByEmail: email(), usedAt: serverTimestamp() });
    await setDefaultOrg(orgId);
    setMsg($('#authMsg'), 'Invitación aceptada. Entrando…', 'ok');
    await boot(orgId);
  }catch(e){
    setMsg($('#authMsg'), e.message || 'Error aceptando invitación', 'err');
  }
});

/* Theme toggle */
async function toggleTheme(){
  applyTheme(S.theme === 'dark' ? 'light' : 'dark');
  await saveThemePref();
}
$('#btnTheme').addEventListener('click', toggleTheme);
$('#btnTheme2').addEventListener('click', toggleTheme);

/* =========================================================
   SUBSCRIPTIONS
========================================================= */
function subscribeAll(){
  cleanupSubs();
  const orgId = S.orgId;

  S.unsub.push(onSnapshot(query(P.stores(orgId), orderBy('name','asc')), (qs)=>{
    S.stores = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderStoreFilter();
    renderRuleStoreSelect();
    refreshAllViews();
  }));

  S.unsub.push(onSnapshot(query(P.accounts(orgId), orderBy('createdAt','desc')), (qs)=>{
    S.accounts = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderAccountFilter();
    refreshAllViews();
  }));

  S.unsub.push(onSnapshot(query(P.rules(orgId), orderBy('priority','desc')), (qs)=>{
    S.rules = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderRules();
  }));

  S.unsub.push(onSnapshot(query(P.txs(orgId), orderBy('date','desc'), limit(2000)), (qs)=>{
    S.transactions = qs.docs.map(d=>({id:d.id, ...d.data()}));
    refreshAllViews();
  }));

  S.unsub.push(onSnapshot(query(P.invoices(orgId), orderBy('date','desc'), limit(2000)), (qs)=>{
    S.invoices = qs.docs.map(d=>({id:d.id, ...d.data()}));
    refreshAllViews();
  }));

  S.unsub.push(onSnapshot(query(P.cash(orgId), orderBy('date','desc'), limit(600)), (qs)=>{
    S.cashClosings = qs.docs.map(d=>({id:d.id, ...d.data()}));
    refreshAllViews();
  }));

  S.unsub.push(onSnapshot(query(P.groups(orgId), orderBy('createdAt','desc'), limit(600)), (qs)=>{
    S.recoGroups = qs.docs.map(d=>({id:d.id, ...d.data()}));
    refreshAllViews();
  }));

  S.unsub.push(onSnapshot(query(P.audit(orgId), orderBy('createdAt','desc'), limit(200)), (qs)=>{
    S.auditLogs = qs.docs.map(d=>({id:d.id, ...d.data()}));
    renderAudit();
  }));
}

/* =========================================================
   BOOT
========================================================= */
async function boot(orgId){
  S.orgId = orgId;
  await loadOrg(orgId);
  await loadMyRole(orgId);
  subscribeAll();
  $('#orgName').textContent = S.org?.name || '—';
  await audit('BOOT', { orgId });
  switchTab('dashboard');
}

/* =========================================================
   FILTERED DATA GETTERS
========================================================= */
function getFilteredMovs(){
  let list = S.transactions.slice();

  // rango por fecha
  list = list.filter(t => inRangeISO(t.date));

  // store global
  list = applyStoreFilter(list);

  // tipo
  const type = $('#typeFilter').value;
  if (type) list = list.filter(t => t.type === type);

  // cuenta
  const acc = $('#accountFilter').value;
  if (acc) list = list.filter(t => t.accountId === acc);

  // estado conciliación
  const st = $('#statusFilter').value;
  if (st === 'open') list = list.filter(t => t.recoStatus !== 'reconciled');
  if (st === 'reconciled') list = list.filter(t => t.recoStatus === 'reconciled');

  // search
  list = applySearch(list, (t)=>`${t.description||''} ${t.party||''} ${t.category||''}`);

  return list;
}

function getFilteredInvoices(){
  let list = S.invoices.slice();
  list = list.filter(i => inRangeISO(i.date));
  list = applyStoreFilter(list);

  const st = $('#statusFilter').value;
  if (st === 'open') list = list.filter(i => i.recoStatus !== 'reconciled');
  if (st === 'reconciled') list = list.filter(i => i.recoStatus === 'reconciled');

  list = applySearch(list, (i)=>`${i.party||''} ${i.number||''}`);

  return list;
}

function getFilteredCash(){
  let list = S.cashClosings.slice();
  list = list.filter(c => inRangeISO(c.date));
  list = applyStoreFilter(list);
  return list;
}

function storeNameById(id){
  return S.stores.find(s=>s.id===id)?.name || '—';
}

/* =========================================================
   STORE FILTER UI
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

function renderRuleStoreSelect(){
  const sel = $('#ruleStore');
  if (!sel) return;
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

function renderAccountFilter(){
  const sel = $('#accountFilter');
  sel.innerHTML = '';
  const oAll = document.createElement('option');
  oAll.value = '';
  oAll.textContent = 'Cuenta: Todas';
  sel.appendChild(oAll);
  S.accounts.forEach(a=>{
    const o = document.createElement('option');
    o.value = a.id;
    o.textContent = a.name;
    sel.appendChild(o);
  });
}

/* =========================================================
   DASHBOARD + CHARTS
========================================================= */
function calcAccountBalance(accountId){
  const acc = S.accounts.find(a=>a.id===accountId);
  const opening = Number(acc?.openingBalance || 0);
  const sumTx = S.transactions
    .filter(t => t.type==='bank' && t.accountId===accountId)
    .reduce((a,t)=>a + Number(t.amount||0), 0);
  return opening + sumTx;
}

function groupByDay(movs, cash){
  const map = new Map();
  const add = (d, amt) => map.set(d, (map.get(d)||0) + amt);
  movs.forEach(t => add(t.date, Number(t.amount||0)));
  cash.forEach(c => add(c.date, Number(c.total||0))); // cierre neto
  const keys = Array.from(map.keys()).sort();
  return { labels: keys, values: keys.map(k=>map.get(k)) };
}

function topCategories(movs){
  const map = new Map();
  movs.filter(t=>Number(t.amount||0) < 0).forEach(t=>{
    const k = t.category || '—';
    map.set(k, (map.get(k)||0) + Math.abs(Number(t.amount||0)));
  });
  const arr = Array.from(map.entries()).sort((a,b)=>b[1]-a[1]).slice(0,8);
  return { labels: arr.map(x=>x[0]), values: arr.map(x=>x[1]) };
}

function ensureCharts(){
  const ctx1 = $('#chartFlow');
  const ctx2 = $('#chartCats');
  if (!ctx1 || !ctx2 || !window.Chart) return;

  if (!S.charts.flow){
    S.charts.flow = new Chart(ctx1, {
      type:'line',
      data:{ labels:[], datasets:[{ label:'Cashflow', data:[], tension:.25 }] },
      options:{
        responsive:true,
        plugins:{ legend:{ display:false } },
        scales:{ y:{ ticks:{ callback:(v)=>fmtEUR(v) } } }
      }
    });
  }
  if (!S.charts.cats){
    S.charts.cats = new Chart(ctx2, {
      type:'bar',
      data:{ labels:[], datasets:[{ label:'Gastos', data:[] }] },
      options:{
        responsive:true,
        plugins:{ legend:{ display:false } },
        scales:{ y:{ ticks:{ callback:(v)=>fmtEUR(v) } } }
      }
    });
  }
}

function renderDashboard(){
  // KPIs
  const bankBal = S.accounts.reduce((a,acc)=>a + calcAccountBalance(acc.id), 0);

  const movs = getFilteredMovs();
  const invs = getFilteredInvoices();
  const cash = getFilteredCash();

  const flow = movs.reduce((a,t)=>a + Number(t.amount||0),0) + cash.reduce((a,c)=>a + Number(c.total||0),0);

  const lastCash = (cash[0] ? Number(cash[0].total||0) : 0);

  const txPending = S.transactions.filter(t=>t.recoStatus!=='reconciled' && inRangeISO(t.date));
  const invPending = S.invoices.filter(i=>i.recoStatus!=='reconciled' && inRangeISO(i.date));
  const recoCount = applyStoreFilter(txPending).length + applyStoreFilter(invPending).length;

  $('#kpiBank').textContent = fmtEUR(bankBal);
  $('#kpiFlow').textContent = fmtEUR(flow);
  $('#kpiCash').textContent = fmtEUR(lastCash);
  $('#kpiReco').textContent = String(recoCount);

  // badges
  const overdue = S.invoices.filter(i=>{
    if (!i.due || i.status==='paid' || i.status==='canceled') return false;
    return i.due < todayISO();
  });
  $('#badgePending').textContent = `Pendientes: ${invPending.length + txPending.length}`;
  $('#badgeOverdue').textContent = `Vencidas: ${overdue.length}`;
  $('#badgeOverdue').className = `badge ${overdue.length ? 'danger' : ''}`.trim();

  // accounts list
  const wa = $('#dashAccounts');
  wa.innerHTML = '';
  if (!S.accounts.length){
    wa.innerHTML = `<div class="item"><div class="h">Sin cuentas</div><div class="s">Crea una cuenta para importar CSV.</div></div>`;
  } else {
    S.accounts.forEach(acc=>{
      const bal = calcAccountBalance(acc.id);
      const el = document.createElement('div');
      el.className = 'item';
      el.innerHTML = `
        <div class="itemtop">
          <div>
            <div class="h">${escapeHtml(acc.name||'')}</div>
            <div class="s">Saldo: <b>${fmtEUR(bal)}</b> · Apertura: ${fmtEUR(acc.openingBalance||0)}</div>
          </div>
        </div>
      `;
      wa.appendChild(el);
    });
  }

  // cash list
  const wc = $('#dashCash');
  wc.innerHTML = '';
  const cashTop = cash.slice(0,6);
  if (!cashTop.length){
    wc.innerHTML = `<div class="item"><div class="h">Sin cierres</div><div class="s">Guarda cierres para verlos aquí.</div></div>`;
  } else {
    cashTop.forEach(c=>{
      const el = document.createElement('div');
      el.className = 'item';
      el.innerHTML = `
        <div class="itemtop">
          <div>
            <div class="h">${escapeHtml(c.date)} · ${escapeHtml(storeNameById(c.storeId))}</div>
            <div class="s">Total: <b>${fmtEUR(c.total||0)}</b> (Efe ${fmtEUR(c.cash||0)} + Tar ${fmtEUR(c.card||0)} − Gast ${fmtEUR(c.expenses||0)})</div>
          </div>
        </div>
      `;
      wc.appendChild(el);
    });
  }

  // charts
  ensureCharts();
  if (S.charts.flow && S.charts.cats){
    const day = groupByDay(movs, cash);
    S.charts.flow.data.labels = day.labels;
    S.charts.flow.data.datasets[0].data = day.values;
    S.charts.flow.update();

    const cats = topCategories(movs);
    S.charts.cats.data.labels = cats.labels;
    S.charts.cats.data.datasets[0].data = cats.values;
    S.charts.cats.update();
  }
}

/* =========================================================
   MOVIMIENTOS
========================================================= */
function renderMovs(){
  const wrap = $('#movsList');
  const list = getFilteredMovs();
  $('#movCount').textContent = String(list.length);

  wrap.innerHTML = '';
  if (!list.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin movimientos</div><div class="s">Importa CSV o crea cierres.</div></div>`;
    return;
  }

  list.slice(0,400).forEach(t=>{
    const acc = S.accounts.find(a=>a.id===t.accountId);
    const isIn = Number(t.amount||0) >= 0;
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(t.description||'(sin concepto)')}</div>
          <div class="s">
            ${escapeHtml(t.date||'')} · ${escapeHtml((t.type||'bank').toUpperCase())}
            ${t.type==='bank' ? ` · ${escapeHtml(acc?.name||'Cuenta')}` : ''}
            · Tienda: ${escapeHtml(t.storeId ? storeNameById(t.storeId) : '—')}
            · <b>${fmtEUR(t.amount||0)}</b>
          </div>
          <div class="tags">
            <span class="tag ${isIn ? 'ok' : 'danger'}">${isIn ? 'Ingreso' : 'Gasto'}</span>
            <span class="tag ${t.recoStatus==='reconciled' ? 'blue' : ''}">${t.recoStatus==='reconciled' ? 'Conciliado' : 'Sin conciliar'}</span>
            <span class="tag">Cat: ${escapeHtml(t.category||'—')}</span>
            <span class="tag">Con: ${escapeHtml(t.party||'—')}</span>
          </div>
        </div>
        <div class="row">
          <button class="btn" data-edit="${t.id}">Editar</button>
          ${canEditCore() ? `<button class="btn danger" data-del="${t.id}">Borrar</button>` : ''}
        </div>
      </div>

      <div class="hr"></div>

      <div class="grid4 hidden" id="edit-${t.id}">
        <div class="span2">
          <label>Categoría</label>
          <input class="input" data-cat="${t.id}" value="${escapeHtml(t.category||'')}"/>
        </div>
        <div>
          <label>Contraparte</label>
          <input class="input" data-party="${t.id}" value="${escapeHtml(t.party||'')}"/>
        </div>
        <div>
          <label>Tienda</label>
          <select class="select" data-store="${t.id}"></select>
        </div>

        <div class="span2">
          <label>Concepto</label>
          <input class="input" data-desc="${t.id}" value="${escapeHtml(t.description||'')}"/>
        </div>
        <div>
          <label>Fecha</label>
          <input class="input" type="date" data-date="${t.id}" value="${escapeHtml(t.date||'')}"/>
        </div>
        <div>
          <label>Importe</label>
          <input class="input" type="number" step="0.01" data-amt="${t.id}" value="${Number(t.amount||0)}"/>
        </div>

        <div class="span2 row right">
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
      o.value = s.id;
      o.textContent = s.name;
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
      if (!canEditCore()) return alert('Sin permisos');
      const id = b.getAttribute('data-save');
      const cat = ($(`input[data-cat="${id}"]`)?.value || '').trim() || null;
      const party = ($(`input[data-party="${id}"]`)?.value || '').trim() || null;
      const storeId = ($(`select[data-store="${id}"]`)?.value || '').trim() || null;
      const desc = ($(`input[data-desc="${id}"]`)?.value || '').trim() || null;
      const date = ($(`input[data-date="${id}"]`)?.value || '').trim() || null;
      const amt = Number($(`input[data-amt="${id}"]`)?.value || 0);

      await updateDoc(doc(db,'orgs',S.orgId,'transactions',id), {
        category: cat,
        party: party,
        storeId: storeId,
        description: desc,
        date: date,
        amount: amt,
        updatedAt: serverTimestamp(),
      });
      await audit('TX_UPDATE', { id, cat, party, storeId, date, amt });
      show($('#edit-'+id), false);
    });
  });
  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      if (!canEditCore()) return alert('Sin permisos');
      const id = b.getAttribute('data-del');
      const tx = S.transactions.find(x=>x.id===id);
      if (tx?.recoStatus === 'reconciled') return alert('Movimiento conciliado. Borra el grupo primero.');
      await deleteDoc(doc(db,'orgs',S.orgId,'transactions',id));
      await audit('TX_DELETE', { id });
    });
  });
}

/* Import CSV */
$('#btnImportCsv').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const file = $('#csvFile').files?.[0];
  if (!file) return;

  const accountId = $('#accountFilter').value || S.accounts[0]?.id;
  if (!accountId) return setMsg($('#csvMsg'), 'Crea una cuenta primero.', 'err');

  try{
    const text = await file.text();
    const { headers, data } = parseCSV(text);

    const cDate = detectCol(headers, ['date','fecha','valor','f. valor','f.valor']);
    const cAmt  = detectCol(headers, ['amount','importe','cantidad','monto','import']);
    const cDesc = detectCol(headers, ['description','concepto','detalle','texto','descripcion','descripción']);

    if (cDate===null || cAmt===null || cDesc===null){
      throw new Error('CSV no reconocido. Necesita columnas fecha/importe/concepto.');
    }

    const existingHashes = new Set(
      S.transactions
        .filter(t=>t.type==='bank' && t.accountId===accountId)
        .map(t=>t.hash)
        .filter(Boolean)
    );

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
    await audit('CSV_IMPORT', { accountId, added });
    $('#csvFile').value = '';
  }catch(e){
    setMsg($('#csvMsg'), e.message || 'Error importando CSV', 'err');
  }
});

$('#btnApplyRules').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const list = getFilteredMovs().filter(t => t.recoStatus !== 'reconciled');
  let updated = 0;
  for (const t of list){
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
  await audit('RULES_APPLY', { updated });
  alert(`Reglas aplicadas: ${updated} movimientos actualizados.`);
});

/* =========================================================
   FACTURAS
========================================================= */
$('#invDate').value = todayISO();

function clearInvoiceForm(){
  $('#invParty').value = '';
  $('#invNumber').value = '';
  $('#invTotal').value = '';
  $('#invStatus').value = 'pending';
  $('#invDate').value = todayISO();
  $('#invDue').value = '';
  $('#invPdf').value = '';
  setMsg($('#invMsg'), '');
}

$('#btnClearInvoice').addEventListener('click', clearInvoiceForm);

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

    await audit('INV_CREATE', { party, number, total, date, due, status });
    setMsg(msg,'Factura guardada.', 'ok');
    clearInvoiceForm();
  }catch(e){
    setMsg(msg, e.message || 'Error subiendo factura', 'err');
  }
});

function renderInvoices(){
  const wrap = $('#invoicesList');
  const invs = getFilteredInvoices();
  wrap.innerHTML = '';

  if (!invs.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin facturas</div><div class="s">Sube PDFs para conciliar.</div></div>`;
    return;
  }

  invs.slice(0,400).forEach(inv=>{
    const overdue = inv.due && inv.status!=='paid' && inv.status!=='canceled' && inv.due < todayISO();
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(inv.party||'')} · ${escapeHtml(inv.number||'')}</div>
          <div class="s">
            ${escapeHtml(inv.date||'')} · Tienda: ${escapeHtml(inv.storeId ? storeNameById(inv.storeId) : '—')}
            · Total: <b>${fmtEUR(inv.total||0)}</b>
            ${inv.due ? `· Vence: <b>${escapeHtml(inv.due)}</b>` : ''}
          </div>
          <div class="tags">
            <span class="tag ${inv.status==='paid' ? 'ok' : overdue ? 'danger' : ''}">Estado: ${escapeHtml(inv.status||'pending')}</span>
            <span class="tag ${inv.recoStatus==='reconciled' ? 'blue' : ''}">${inv.recoStatus==='reconciled' ? 'Conciliada' : 'Pendiente'}</span>
            ${overdue ? `<span class="tag danger">VENCIDA</span>` : ''}
          </div>
        </div>
        <div class="row">
          ${inv.pdfUrl ? `<a class="btn" href="${inv.pdfUrl}" target="_blank" rel="noreferrer">Ver PDF</a>` : ''}
          ${canEditCore() ? `<button class="btn" data-paid="${inv.id}">Marcar pagada</button>` : ''}
          ${canEditCore() ? `<button class="btn danger" data-del="${inv.id}">Borrar</button>` : ''}
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
      await audit('INV_MARK_PAID', { id });
    });
  });

  $$('button[data-del]', wrap).forEach(b=>{
    b.addEventListener('click', async ()=>{
      if (!canEditCore()) return;
      const id = b.getAttribute('data-del');
      const inv = S.invoices.find(x=>x.id===id);
      if (inv?.recoStatus==='reconciled') return alert('Factura conciliada. Borra el grupo primero.');
      await deleteDoc(doc(db,'orgs',S.orgId,'invoices',id));
      await audit('INV_DELETE', { id });
    });
  });
}

/* =========================================================
   CAJA
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
    createdByUid: uid(),
    createdByEmail: email(),
  });

  await audit('CASH_CLOSE', { storeId, date, cash, card, exp });
  setMsg($('#cashMsg'),'Cierre guardado.', 'ok');
  $('#cashCash').value = '';
  $('#cashCard').value = '';
  $('#cashExp').value = '';
  $('#cashNotes').value = '';
});

function renderCash(){
  const wrap = $('#cashList');
  const list = getFilteredCash();
  wrap.innerHTML = '';

  if (!list.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin cierres</div><div class="s">Guarda cierres diarios por tienda.</div></div>`;
    return;
  }

  list.slice(0,400).forEach(c=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(c.date)} · ${escapeHtml(storeNameById(c.storeId))}</div>
          <div class="s">Efectivo: ${fmtEUR(c.cash||0)} · Tarjeta: ${fmtEUR(c.card||0)} · Gastos: ${fmtEUR(c.expenses||0)}</div>
          <div class="s"><b>Total:</b> ${fmtEUR(c.total||0)} ${c.notes ? `· ${escapeHtml(c.notes)}` : ''}</div>
        </div>
        <div class="row">
          ${isContable() ? `<button class="btn danger" data-del="${c.id}">Borrar</button>` : ''}
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
      await audit('CASH_DELETE', { id });
    });
  });
}

/* =========================================================
   CUENTAS
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
  await audit('ACC_CREATE', { name, opening });
  $('#accName').value = '';
  $('#accOpening').value = '';
});

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
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(acc.name||'')}</div>
          <div class="s">Saldo: <b>${fmtEUR(bal)}</b> · Apertura: ${fmtEUR(acc.openingBalance||0)}</div>
        </div>
        <div class="row">
          ${canEditCore() ? `<button class="btn danger" data-del="${acc.id}">Borrar</button>` : ''}
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
      await audit('ACC_DELETE', { id });
    });
  });
}

/* =========================================================
   TIENDAS
========================================================= */
$('#btnAddStore').addEventListener('click', async ()=>{
  if (!canEditCore()) return alert('Sin permisos');
  const name = $('#storeName').value.trim();
  const alias = $('#storeAlias').value.trim();
  if (!name) return;

  await addDoc(P.stores(S.orgId), {
    name, alias: alias || null,
    createdAt: serverTimestamp(),
  });
  await audit('STORE_CREATE', { name, alias });
  $('#storeName').value = '';
  $('#storeAlias').value = '';
});

function renderStores(){
  const wrap = $('#storesList');
  wrap.innerHTML = '';
  if (!S.stores.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin tiendas</div><div class="s">Crea tiendas para filtrar.</div></div>`;
    return;
  }
  S.stores.forEach(s=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(s.name||'')}</div>
          <div class="s">Alias: ${escapeHtml(s.alias||'—')}</div>
        </div>
        <div class="row">
          ${canEditCore() ? `<button class="btn danger" data-del="${s.id}">Borrar</button>` : ''}
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });

  $$('button[data-del]', wrap).forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      if (!canEditCore()) return;
      const id = btn.getAttribute('data-del');
      await deleteDoc(doc(db,'orgs',S.orgId,'stores',id));
      await audit('STORE_DELETE', { id });
      if (S.storeFilter === id){ S.storeFilter='ALL'; renderStoreFilter(); }
    });
  });
}

/* =========================================================
   REGLAS
========================================================= */
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
    needle, category,
    party: party || null,
    type, priority, storeId,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  });
  await audit('RULE_CREATE', { needle, category, type, priority, storeId });

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
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(r.needle)} → ${escapeHtml(r.category)}</div>
          <div class="s">Tipo: ${escapeHtml(r.type)} · Pri: ${escapeHtml(r.priority)} · Tienda: ${r.storeId ? escapeHtml(storeNameById(r.storeId)) : 'Todas'}</div>
          <div class="tags">
            ${r.party ? `<span class="tag">Con: ${escapeHtml(r.party)}</span>` : ''}
          </div>
        </div>
        <div class="row">
          ${canEditCore() ? `<button class="btn danger" data-del="${r.id}">Borrar</button>` : ''}
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
      await audit('RULE_DELETE', { id });
    });
  });
}

/* =========================================================
   CONCILIACIÓN
========================================================= */
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

function renderRecoLists(){
  const wrapM = $('#recoMovs');
  const wrapI = $('#recoInvs');
  wrapM.innerHTML = '';
  wrapI.innerHTML = '';

  // solo pendientes, dentro de rango
  let movs = S.transactions.filter(t => t.type==='bank' && t.recoStatus!=='reconciled' && inRangeISO(t.date));
  movs = applyStoreFilter(movs);
  movs = applySearch(movs, (t)=>`${t.description||''} ${t.party||''} ${t.category||''}`).slice(0,80);

  let invs = S.invoices.filter(i => i.status!=='canceled' && i.recoStatus!=='reconciled' && inRangeISO(i.date));
  invs = applyStoreFilter(invs);
  invs = applySearch(invs, (i)=>`${i.party||''} ${i.number||''}`).slice(0,80);

  if (!movs.length) wrapM.innerHTML = `<div class="item"><div class="h">Sin pendientes</div><div class="s">No hay movimientos pendientes con estos filtros.</div></div>`;
  if (!invs.length) wrapI.innerHTML = `<div class="item"><div class="h">Sin pendientes</div><div class="s">No hay facturas pendientes con estos filtros.</div></div>`;

  movs.forEach(t=>{
    const checked = S.selMovIds.has(t.id);
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
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
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(i.party||'')} · ${escapeHtml(i.number||'')}</div>
          <div class="s">${escapeHtml(i.date)} · <b>${fmtEUR(i.total||0)}</b> ${i.due ? `· Vence: ${escapeHtml(i.due)}` : ''}</div>
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

  renderGroups();
}

$('#btnCreateGroup').addEventListener('click', async ()=>{
  if (!isContable()) return alert('Sin permisos');
  const msg = $('#recoMsg'); setMsg(msg,'');

  if (!S.selMovIds.size || !S.selInvIds.size){
    return setMsg(msg,'Selecciona al menos 1 movimiento y 1 factura.', 'err');
  }

  const movIds = Array.from(S.selMovIds);
  const invIds = Array.from(S.selInvIds);

  const movTotal = movIds.reduce((a,id)=>a + Number(S.transactions.find(t=>t.id===id)?.amount||0), 0);
  const invTotal = invIds.reduce((a,id)=>a + Number(S.invoices.find(i=>i.id===id)?.total||0), 0);
  const diff = movTotal - invTotal;

  const groupId = idNow('group');
  await setDoc(doc(db,'orgs',S.orgId,'recoGroups',groupId), {
    movIds, invIds, movTotal, invTotal, diff,
    storeId: (S.storeFilter !== 'ALL') ? S.storeFilter : null,
    createdAt: serverTimestamp(),
    createdByUid: uid(),
    createdByEmail: email(),
  });

  for (const id of movIds){
    await updateDoc(doc(db,'orgs',S.orgId,'transactions',id), { recoStatus:'reconciled', updatedAt: serverTimestamp() });
  }
  for (const id of invIds){
    await updateDoc(doc(db,'orgs',S.orgId,'invoices',id), { recoStatus:'reconciled', status:'paid', updatedAt: serverTimestamp() });
  }

  await audit('RECO_GROUP_CREATE', { groupId, movTotal, invTotal, diff });

  S.selMovIds.clear();
  S.selInvIds.clear();
  updateSelCounts();
  setMsg(msg, `Grupo creado. Diferencia: ${fmtEUR(diff)} (movs - facturas)`, diff === 0 ? 'ok' : 'err');
});

function renderGroups(){
  const wrap = $('#groupsList');
  if (!wrap) return;
  wrap.innerHTML = '';

  let groups = S.recoGroups.slice();
  groups = groups.filter(g => {
    const d = (g.createdAt?.toDate ? toISO(g.createdAt.toDate()) : null);
    // no filtramos por fecha de createdAt (porque puede venir serverTimestamp)
    return true;
  });
  groups = applyStoreFilter(groups).slice(0,200);

  if (!groups.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin grupos</div><div class="s">Crea grupos para pagos parciales o agrupados.</div></div>`;
    return;
  }

  groups.forEach(g=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
        <div>
          <div class="h">Grupo ${escapeHtml(g.id)}</div>
          <div class="s">Movs: <b>${fmtEUR(g.movTotal||0)}</b> · Facturas: <b>${fmtEUR(g.invTotal||0)}</b> · Dif: <b>${fmtEUR(g.diff||0)}</b></div>
          <div class="tags">
            <span class="tag">MovIds: ${(g.movIds||[]).length}</span>
            <span class="tag">InvIds: ${(g.invIds||[]).length}</span>
            ${g.storeId ? `<span class="tag">Tienda: ${escapeHtml(storeNameById(g.storeId))}</span>` : `<span class="tag">Tienda: Todas</span>`}
          </div>
        </div>
        <div class="row">
          ${isContable() ? `<button class="btn danger" data-del="${g.id}">Eliminar grupo</button>` : ''}
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

      for (const mid of (g.movIds||[])){
        await updateDoc(doc(db,'orgs',S.orgId,'transactions',mid), { recoStatus:'open', updatedAt: serverTimestamp() });
      }
      for (const iid of (g.invIds||[])){
        await updateDoc(doc(db,'orgs',S.orgId,'invoices',iid), { recoStatus:'open', status:'pending', updatedAt: serverTimestamp() });
      }
      await deleteDoc(doc(db,'orgs',S.orgId,'recoGroups',id));
      await audit('RECO_GROUP_DELETE', { id });
    });
  });
}

/* Sugerencias */
$('#btnHints').addEventListener('click', ()=>{
  const days = clamp(Number($('#hintDays').value || 7), 1, 60);
  const tol = Math.max(0, Number($('#hintTol').value || 0.5));

  const movs = S.transactions
    .filter(t => t.type==='bank' && t.recoStatus!=='reconciled' && inRangeISO(t.date));
  const invs = S.invoices
    .filter(i => i.status!=='canceled' && i.recoStatus!=='reconciled' && inRangeISO(i.date));

  const fm = applyStoreFilter(movs);
  const fi = applyStoreFilter(invs);

  const hints = [];
  for (const t of fm){
    for (const i of fi){
      const diff = Math.abs(Number(t.amount||0) - Number(i.total||0));
      if (diff > tol) continue;
      const dt = new Date(t.date);
      const di = new Date(i.date);
      const dd = Math.abs((dt - di) / (1000*60*60*24));
      if (dd > days) continue;

      hints.push({
        txId: t.id, invId: i.id,
        score: (tol - diff) + (days - dd) * 0.1,
        tx: t, inv: i,
        diff, dd
      });
    }
  }
  hints.sort((a,b)=>b.score - a.score);
  renderHints(hints.slice(0,20));
});

function renderHints(hints){
  const box = $('#hintsBox');
  box.innerHTML = '';
  if (!hints.length){
    box.innerHTML = `<div class="item"><div class="h">Sin sugerencias</div><div class="s">Prueba ampliar días o tolerancia.</div></div>`;
    return;
  }
  hints.forEach(h=>{
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(h.tx.description||'')} ↔ ${escapeHtml(h.inv.party||'')} ${escapeHtml(h.inv.number||'')}</div>
          <div class="s">TX: ${escapeHtml(h.tx.date)} ${fmtEUR(h.tx.amount||0)} · INV: ${escapeHtml(h.inv.date)} ${fmtEUR(h.inv.total||0)}</div>
          <div class="tags">
            <span class="tag">Δ ${fmtEUR(h.diff)}</span>
            <span class="tag">Días ${Math.round(h.dd)}</span>
          </div>
        </div>
        <div class="row">
          <button class="btn" data-pick="${h.txId}|${h.invId}">Seleccionar</button>
        </div>
      </div>
    `;
    box.appendChild(el);
  });

  $$('button[data-pick]', box).forEach(b=>{
    b.addEventListener('click', ()=>{
      const [txId, invId] = b.getAttribute('data-pick').split('|');
      S.selMovIds.add(txId);
      S.selInvIds.add(invId);
      updateSelCounts();
      renderRecoLists();
      switchTab('reco');
    });
  });
}

/* =========================================================
   EXPORT CSV
========================================================= */
function downloadCSV(filename, rows){
  const esc = (v)=> `"${String(v??'').replaceAll('"','""')}"`;
  const csv = rows.map(r => r.map(esc).join(';')).join('\n');
  const blob = new Blob([csv], { type:'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportMovs(){
  const list = getFilteredMovs();
  const rows = [
    ['date','type','account','store','amount','description','party','category','recoStatus']
  ];
  list.forEach(t=>{
    const acc = S.accounts.find(a=>a.id===t.accountId)?.name || '';
    rows.push([
      t.date||'', t.type||'', acc,
      t.storeId ? storeNameById(t.storeId) : '',
      Number(t.amount||0),
      t.description||'', t.party||'', t.category||'',
      t.recoStatus||''
    ]);
  });
  downloadCSV(`movimientos_${todayISO()}.csv`, rows);
}
function exportInvs(){
  const list = getFilteredInvoices();
  const rows = [['date','due','store','party','number','total','status','recoStatus','pdfUrl']];
  list.forEach(i=>{
    rows.push([
      i.date||'', i.due||'',
      i.storeId ? storeNameById(i.storeId) : '',
      i.party||'', i.number||'',
      Number(i.total||0),
      i.status||'', i.recoStatus||'',
      i.pdfUrl||''
    ]);
  });
  downloadCSV(`facturas_${todayISO()}.csv`, rows);
}
function exportCash(){
  const list = getFilteredCash();
  const rows = [['date','store','cash','card','expenses','total','notes','createdByEmail']];
  list.forEach(c=>{
    rows.push([
      c.date||'',
      c.storeId ? storeNameById(c.storeId) : '',
      Number(c.cash||0), Number(c.card||0), Number(c.expenses||0), Number(c.total||0),
      c.notes||'', c.createdByEmail||''
    ]);
  });
  downloadCSV(`caja_${todayISO()}.csv`, rows);
}
function exportGroups(){
  const list = applyStoreFilter(S.recoGroups.slice());
  const rows = [['groupId','store','movTotal','invTotal','diff','movCount','invCount','createdByEmail']];
  list.forEach(g=>{
    rows.push([
      g.id,
      g.storeId ? storeNameById(g.storeId) : '',
      Number(g.movTotal||0), Number(g.invTotal||0), Number(g.diff||0),
      (g.movIds||[]).length, (g.invIds||[]).length,
      g.createdByEmail||''
    ]);
  });
  downloadCSV(`conciliacion_${todayISO()}.csv`, rows);
}

$('#btnExportMovs').addEventListener('click', exportMovs);
$('#btnExportInvs').addEventListener('click', exportInvs);
$('#btnExportCash').addEventListener('click', exportCash);
$('#btnExportGroups').addEventListener('click', exportGroups);

$('#btnExportMovs2').addEventListener('click', exportMovs);
$('#btnExportInvs2').addEventListener('click', exportInvs);
$('#btnExportCash2').addEventListener('click', exportCash);
$('#btnExportGroups2').addEventListener('click', exportGroups);

/* =========================================================
   AUDIT VIEW
========================================================= */
function renderAudit(){
  const wrap = $('#auditList');
  if (!wrap) return;
  wrap.innerHTML = '';

  const list = S.auditLogs.slice(0,200);
  if (!list.length){
    wrap.innerHTML = `<div class="item"><div class="h">Sin eventos</div><div class="s">Aquí verás acciones clave.</div></div>`;
    return;
  }
  list.forEach(a=>{
    const when = a.createdAt?.toDate ? a.createdAt.toDate().toLocaleString('es-ES') : '';
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `
      <div class="itemtop">
        <div>
          <div class="h">${escapeHtml(a.action||'')}</div>
          <div class="s">${escapeHtml(when)} · ${escapeHtml(a.byEmail||'')}</div>
          <div class="s">${escapeHtml(JSON.stringify(a.details||{}))}</div>
        </div>
      </div>
    `;
    wrap.appendChild(el);
  });
}

/* =========================================================
   BUTTONS / THEME LABEL INIT
========================================================= */
applyTheme('light');

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
    S.selMovIds.clear();
    S.selInvIds.clear();

    show(authView, true);
    show(appView, false);
    $('#userEmail').textContent = '—';
    $('#avatar').textContent = 'U';
    $('#roleBadge').textContent = '—';
    return;
  }

  show(authView, false);
  show(appView, true);

  $('#userEmail').textContent = email();
  $('#avatar').textContent = initialsFromEmail(email());

  await ensureUserDoc();
  await loadThemePref();

  let orgId = await loadDefaultOrgId();
  if (!orgId){
    orgId = await createNewOrg('Mi Empresa');
  }

  try{
    await boot(orgId);
  }catch{
    const newOrg = await createNewOrg('Mi Empresa');
    await boot(newOrg);
  }

  $('#invDate').value = todayISO();
  $('#cashDate').value = todayISO();
  updateSelCounts();
});

/* =========================================================
   INVITE TOKEN HELP: token|orgId
   - En el modal de miembros, cuando creas token, envíalo así:
     TOKEN|ORGID
========================================================= */
(function injectInviteHint(){
  const m = $('#authMsg');
  if (m) {
    // no mostramos nada por defecto
  }
})();
