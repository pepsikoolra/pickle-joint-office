/**
 * 구름 비서실 — 대장님과 비서들이 방마다 실시간으로 함께 일하는 사무실
 * Firebase Realtime Database (Spark 무료 플랜)
 */
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import {
  getDatabase, ref, onValue, set, update, remove, push, onDisconnect, serverTimestamp,
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-database.js";

/* ── 설정 ─────────────────────────────────────────── */

const firebaseConfig = {
  apiKey: "AIzaSyDNwjCIjYkx5a6zQ9k6DUVWBCLBeLunwRw",
  authDomain: "pickle-joint-office-beed8.firebaseapp.com",
  databaseURL: "https://pickle-joint-office-beed8-default-rtdb.firebaseio.com",
  projectId: "pickle-joint-office-beed8",
  storageBucket: "pickle-joint-office-beed8.firebasestorage.app",
  messagingSenderId: "242570603871",
  appId: "1:242570603871:web:426c62667848032ee99aea",
};

const CHANNELS = [
  { id: "meeting", emoji: "🏛️", name: "대회의실" },
  { id: "command", emoji: "🎯", name: "지휘실" },
  { id: "lounge", emoji: "☕", name: "라운지" },
];

const STATUSES = {
  idle: { label: "대기 중", cls: "s-wait", at: "seat", bubble: "✨ 대기 중" },
  working: { label: "업무 중", cls: "s-doing", at: "seat", bubble: "💻 일하는 중" },
  meeting: { label: "회의 중", cls: "s-doing", at: "seat", bubble: "💬 회의 중" },
  rest: { label: "휴식 중", cls: "s-sleep", at: "bed", bubble: "💤 쉬는 중" },
  sleep: { label: "수면 중", cls: "s-sleep", at: "bed", bubble: "😴 자는 중" },
};

const REST_MODES = [
  { id: "sleep", emoji: "💤", label: "잠자기" },
  { id: "read", emoji: "📖", label: "책 읽기" },
  { id: "phone", emoji: "📱", label: "핸드폰 보기" },
  { id: "music", emoji: "🎧", label: "음악 듣기" },
];

const TASK_STATUS = {
  wait: { label: "대기", cls: "s-wait", next: "doing" },
  doing: { label: "실행 중", cls: "s-doing", next: "done" },
  done: { label: "완료", cls: "s-done", next: "wait" },
};

const COMMANDER = {
  uid: "commander",
  name: "Goorm",
  role: "대장님",
  emoji: "👑",
  color: "#c58a3c",
  room: "command",
  commander: true,
};

// 대장님의 지시는 항상 goorma_1 한 곳으로 모이고, goorma_1이 나머지 피클에게 나눠 준다.
const TARGET_PICKLE = "goorma_1";

const EMOJIS = ["🫧", "🙂", "🐣", "🦊", "🐼", "🐧", "🐨", "🦉", "🐯", "🐰", "🐳", "🦄",
  "🌸", "💙", "📱", "🍀", "⭐", "🔥", "☕", "📎", "📘", "🎧", "🧭", "🧸"];

const COLORS = ["#a9622a", "#3f6fb5", "#8a55b0", "#c2613a",
  "#b5843a", "#4a7a8c", "#a34a6a", "#5b6363"];

const DEFAULT_OFFICE = "main-office";

/* ── 사무실 · 내 정보 ─────────────────────────────── */

function normalizeOffice(raw) {
  const v = String(raw || "").toLowerCase().trim()
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9_-]/g, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
  return v || DEFAULT_OFFICE;
}

function readOffice() {
  const params = new URLSearchParams(location.search);
  const q = params.get("office") || params.get("room");
  if (q) return normalizeOffice(q);
  const hash = location.hash.replace(/^#/, "");
  const m = hash.match(/(?:^|&)(?:office|room)=([^&]*)/);
  if (m && m[1]) return normalizeOffice(decodeURIComponent(m[1]));
  return DEFAULT_OFFICE;
}

const office = readOffice();

function officeUrl(target) {
  const u = new URL(location.href);
  u.hash = "";
  u.search = "?office=" + encodeURIComponent(target);
  return u.toString();
}

function loadJSON(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch { return fallback; }
}

function saveJSON(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* 무시 */ }
}

function newUid() {
  try {
    if (crypto && crypto.randomUUID) return "u" + crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  } catch { /* 아래로 */ }
  return "u" + Math.random().toString(36).slice(2, 12) + Date.now().toString(36);
}

let uid = loadJSON("goorm.uid", null);
if (!uid) { uid = newUid(); saveJSON("goorm.uid", uid); }

const saved = loadJSON("goorm.profile", null);
const firstVisit = !saved || !saved.name;

let me = {
  uid,
  name: saved?.name || "goorma_1",
  role: saved?.role || "메인 비서",
  emoji: saved?.emoji || "🫧",
  color: saved?.color || COLORS[0],
  room: CHANNELS.some((c) => c.id === saved?.room) ? saved.room : "command",
  status: STATUSES[saved?.status] ? saved.status : "idle",
  restMode: REST_MODES.some((r) => r.id === saved?.restMode) ? saved.restMode : "",
};

/* ── Firebase ─────────────────────────────────────── */

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

const base = `rooms/${office}`;
const meRef = ref(db, `${base}/members/${uid}`);
const memberRef = (id) => ref(db, `${base}/members/${id}`);
const metaRef = ref(db, `${base}/meta`);
const tasksRef = ref(db, `${base}/tasks`);
const notesRef = ref(db, `${base}/notes`);
const quotaRef = ref(db, `${base}/quota`);

const state = {
  meta: {},
  members: {},
  channels: { meeting: {}, command: {}, lounge: {} },
  tasks: {},
  notes: {},
  quota: {},
  connected: false,
  tab: "map",
  channel: "command",
  mapStyle: "basic",
  motion: true,
};

/* ── DOM ──────────────────────────────────────────── */

const $ = (id) => document.getElementById(id);

const els = {
  officeTitle: $("officeTitle"),
  ownerName: $("ownerName"),
  connPill: $("connPill"),
  connText: $("connText"),
  tabs: $("tabs"),

  viewMap: $("view-map"),
  bannerOffice: $("bannerOffice"),
  bannerTitle: $("bannerTitle"),
  mapStyle: $("mapStyle"),
  motionToggle: $("motionToggle"),
  roomCounter: $("roomCounter"),
  onlineCounter: $("onlineCounter"),
  mapStage: $("mapStage"),
  gameLayer: $("gameLayer"),
  picklePop: $("picklePop"),
  seedCard: $("seedCard"),
  seedBtn: $("seedBtn"),
  bedCounter: $("bedCounter"),

  viewChat: $("view-chat"),
  channelPills: $("channelPills"),
  chatRoomEmoji: $("chatRoomEmoji"),
  chatRoomName: $("chatRoomName"),
  chatOnline: $("chatOnline"),
  chatList: $("chatList"),
  chatEmpty: $("chatEmpty"),
  chatForm: $("chatForm"),
  chatInput: $("chatInput"),
  routeFromName: $("routeFromName"),
  routeToName: $("routeToName"),
  routeToEmoji: $("routeToEmoji"),
  composerAddStaff: $("composerAddStaff"),

  viewTasks: $("view-tasks"),
  taskForm: $("taskForm"),
  taskTitle: $("taskTitle"),
  taskAssignee: $("taskAssignee"),
  taskStatus: $("taskStatus"),
  taskList: $("taskList"),
  taskEmpty: $("taskEmpty"),
  taskCount: $("taskCount"),

  viewStaff: $("view-staff"),
  staffList: $("staffList"),
  staffEmpty: $("staffEmpty"),
  addStaffBtn: $("addStaffBtn"),

  viewQuota: $("view-quota"),
  quotaList: $("quotaList"),
  quotaEmpty: $("quotaEmpty"),
  quotaSummary: $("quotaSummary"),

  viewMemo: $("view-memo"),
  memoForm: $("memoForm"),
  memoInput: $("memoInput"),
  memoList: $("memoList"),
  memoEmpty: $("memoEmpty"),
  memoCount: $("memoCount"),
  pinnedBox: $("pinnedBox"),
  pinnedList: $("pinnedList"),

  staffModal: $("staffModal"),
  staffModalTitle: $("staffModalTitle"),
  staffName: $("staffName"),
  staffRole: $("staffRole"),
  staffEmojis: $("staffEmojis"),
  staffColors: $("staffColors"),
  staffRoom: $("staffRoom"),
  staffStatus: $("staffStatus"),
  staffSlot: $("staffSlot"),
  staffRest: $("staffRest"),
  staffPreviewAvatar: $("staffPreviewAvatar"),
  staffPreviewName: $("staffPreviewName"),
  staffPreviewRole: $("staffPreviewRole"),
  staffSave: $("staffSave"),
  staffDelete: $("staffDelete"),

  officeModal: $("officeModal"),
  officeTitleInput: $("officeTitleInput"),
  officeOwnerInput: $("officeOwnerInput"),
  roomLink: $("roomLink"),
  copyLinkBtn: $("copyLinkBtn"),
  officeSwitchInput: $("officeSwitchInput"),
  officeSwitchBtn: $("officeSwitchBtn"),
  officeSave: $("officeSave"),

  toast: $("toast"),
};

/* ── 도우미 ───────────────────────────────────────── */

function el(tag, opts = {}, children = []) {
  const node = document.createElement(tag);
  if (opts.class) node.className = opts.class;
  if (opts.type) node.setAttribute("type", opts.type);
  if (opts.text != null) node.textContent = opts.text;
  if (opts.attrs) {
    for (const [k, v] of Object.entries(opts.attrs)) {
      if (v != null && v !== false) node.setAttribute(k, v === true ? "" : String(v));
    }
  }
  if (opts.style) for (const [k, v] of Object.entries(opts.style)) node.style.setProperty(k, v);
  if (opts.on) for (const [k, v] of Object.entries(opts.on)) node.addEventListener(k, v);
  for (const child of [].concat(children)) if (child) node.appendChild(child);
  return node;
}

let toastTimer = null;
function toast(message) {
  els.toast.textContent = message;
  els.toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => els.toast.classList.remove("show"), 2600);
}

const statusOf = (key) => STATUSES[key] || STATUSES.idle;
const taskStatusOf = (key) => TASK_STATUS[key] || TASK_STATUS.wait;
const channelOf = (id) => CHANNELS.find((c) => c.id === id) || CHANNELS[1];

function tint(color) {
  return `color-mix(in srgb, ${color || "#c9a87c"} 20%, #fffdf8)`;
}

function avatarNode(emoji, color, cls = "", online = null) {
  const node = el("span", { class: "avatar " + cls, text: emoji || "🙂", style: { background: tint(color) } });
  return node;
}

function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  const now = new Date();
  const diff = now.getTime() - d.getTime();
  if (diff < 60 * 1000) return "방금 전";
  if (diff < 60 * 60 * 1000) return Math.floor(diff / 60000) + "분 전";
  const timeStr = d.toLocaleTimeString("ko-KR", { hour: "numeric", minute: "2-digit" });
  if (d.toDateString() === now.toDateString()) return timeStr;
  const yesterday = new Date(now);
  yesterday.setDate(now.getDate() - 1);
  if (d.toDateString() === yesterday.toDateString()) return "어제 " + timeStr;
  if (diff < 7 * 24 * 3600 * 1000) return `${d.getMonth() + 1}월 ${d.getDate()}일 ${timeStr}`;
  return d.toLocaleDateString("ko-KR", { month: "numeric", day: "numeric" });
}

/* ── 내 자리 ──────────────────────────────────────── */

function paintHeader() {
  const title = state.meta.title || "구름 비서실";
  const owner = state.meta.owner || "Goorm";
  els.officeTitle.textContent = title;
  els.bannerOffice.textContent = title;
  els.ownerName.textContent = owner;
  document.title = title;
}

async function goOnline() {
  const payload = {
    uid: me.uid, name: me.name, role: me.role, emoji: me.emoji, color: me.color,
    room: me.room, status: me.status, restMode: me.restMode || "",
    manual: false, online: true, lastSeen: serverTimestamp(),
  };
  try {
    await set(meRef, payload);
    await onDisconnect(meRef).update({ online: false, lastSeen: serverTimestamp() });
  } catch (err) {
    console.warn("자리 알림 실패", err);
  }
}

function pushMe() {
  update(meRef, {
    uid: me.uid, name: me.name, role: me.role, emoji: me.emoji, color: me.color,
    room: me.room, status: me.status, restMode: me.restMode || "", manual: false, lastSeen: serverTimestamp(),
  }).catch((err) => {
    console.warn("프로필 저장 실패", err);
    toast("서버에 저장하지 못했습니다");
  });
  saveJSON("goorm.profile", me);
}

/* ── 탭 ───────────────────────────────────────────── */

function switchTab(name) {
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("is-active", t.dataset.tab === name));
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("is-active", v.dataset.view === name));
  state.tab = name;
  window.scrollTo({ top: 0, behavior: "smooth" });
}

els.tabs.addEventListener("click", (event) => {
  const tab = event.target.closest(".tab");
  if (tab) switchTab(tab.dataset.tab);
});

/* ── 오피스 맵 ────────────────────────────────────── */

/* 지도 위 지정 자리(의자 6개)와 지정 침대(6개) 좌표 — office-map-game.jpg 기준 % */
const SLOT_COUNT = 6;
const SEATS = [
  { x: 18.9, y: 42.4 }, { x: 26.6, y: 42.4 }, { x: 34.7, y: 42.4 },
  { x: 18.9, y: 57.8 }, { x: 26.6, y: 57.8 }, { x: 34.7, y: 57.8 },
];
const BEDS = [
  { x: 57.9, y: 40.4 }, { x: 76.3, y: 40.2 },
  { x: 58.1, y: 51.2 }, { x: 76.4, y: 50.9 },
  { x: 58.1, y: 62.4 }, { x: 76.6, y: 61.8 },
];
const SPARE_SPOTS = [[23, 67], [29.5, 67.5], [36, 68], [42, 68.5]];

const SLOT_LABEL = (i) => `${i + 1}번 자리 · ${i + 1}번 침대`;

function slotOf(m) {
  const s = Number(m && m.slot);
  return Number.isInteger(s) && s >= 0 && s < SLOT_COUNT ? s : -1;
}

let slotWriting = new Set();

/** 슬롯이 없는 비서에게 빈 자리(낮은 번호부터)를 자동 배정하고 서버에 저장한다. */
function ensureSlots(members) {
  const used = new Set();
  const pending = [];
  for (const m of members) {
    const s = slotOf(m);
    if (s >= 0 && !used.has(s)) used.add(s);
    else pending.push(m);
  }
  for (const m of pending) {
    let free = 0;
    while (used.has(free) && free < SLOT_COUNT) free++;
    if (free >= SLOT_COUNT) break;
    used.add(free);
    m.slot = free;
    if (slotWriting.has(m.id)) continue;
    slotWriting.add(m.id);
    update(memberRef(m.id), { slot: free }).catch((err) => {
      console.warn("자리 배정 실패", err);
    }).finally(() => slotWriting.delete(m.id));
  }
}

/* ── 피클 스프라이트 ──────────────────────────────── */

function hexToRgb(hex) {
  const h = String(hex || "#a9622a").replace("#", "");
  return [parseInt(h.slice(0, 2), 16) || 0, parseInt(h.slice(2, 4), 16) || 0, parseInt(h.slice(4, 6), 16) || 0];
}

function mixHex(hex, target, ratio) {
  const a = hexToRgb(hex);
  const b = hexToRgb(target);
  const out = a.map((v, i) => Math.round(v + (b[i] - v) * ratio));
  return "#" + out.map((v) => Math.max(0, Math.min(255, v)).toString(16).padStart(2, "0")).join("");
}

function spriteColors(color) {
  const body = /^#[0-9a-fA-F]{6}$/.test(String(color || "")) ? color : "#a9622a";
  return {
    body,
    light: mixHex(body, "#ffffff", 0.42),
    deep: mixHex(body, "#2a1a0e", 0.26),
    tummy: mixHex(body, "#fff6e2", 0.62),
    pajA: mixHex(body, "#ffffff", 0.74),
    pajB: mixHex(body, "#ffffff", 0.34),
  };
}

/** 의자에 앉아 일하는 피클 */
function workingSVG(color, idx, status, withLegs = false) {
  const c = spriteColors(color);
  const gid = `pkg-${idx}`;
  const atWork = status === "working" || status === "meeting";
  const prop = atWork
    ? `<g class="pk-prop">
        <rect x="22" y="63" width="36" height="25" rx="4.2" fill="#f4f7fb" stroke="#c8d2dd" stroke-width="1.2"/>
        <rect x="24.8" y="65.8" width="30.4" height="19.4" rx="2.4" fill="#dbe7f2"/>
        <rect x="27.5" y="68.6" width="15" height="2.2" rx="1.1" fill="#a9bccf"/>
        <rect x="27.5" y="72.6" width="22" height="2.2" rx="1.1" fill="#bccbd9"/>
        <rect x="27.5" y="76.6" width="11" height="2.2" rx="1.1" fill="#bccbd9"/>
      </g>`
    : "";
  const legs = withLegs
    ? `<ellipse class="pk-shadow" cx="40" cy="103" rx="20" ry="4" fill="#3a2410" opacity="0.16"/>
  <g class="pk-legs">
    <g class="pk-leg pk-leg-l"><rect x="27" y="84" width="10" height="15" rx="5" fill="${c.deep}"/><ellipse cx="32" cy="99" rx="7" ry="3.4" fill="${c.deep}"/></g>
    <g class="pk-leg pk-leg-r"><rect x="43" y="84" width="10" height="15" rx="5" fill="${c.deep}"/><ellipse cx="48" cy="99" rx="7" ry="3.4" fill="${c.deep}"/></g>
  </g>`
    : "";

  // 팔은 어깨를 축으로 따로 움직이는 관절 그룹
  const arm = (side) => {
    const x = atWork ? (side === "l" ? 21 : 59) : (side === "l" ? 33 : 47);
    const y = atWork ? 74 : 76;
    return `<g class="pk-arm pk-arm-${side}">
        <ellipse cx="${x}" cy="${y}" rx="6" ry="7.4" fill="${c.body}"/>
        <ellipse cx="${x}" cy="${y - 4.4}" rx="3.3" ry="2.1" fill="${c.light}" opacity=".55"/>
      </g>`;
  };

  return `<svg class="pk-svg pk-svg-seat" viewBox="0 0 80 ${withLegs ? 108 : 96}" aria-hidden="true">
  <defs>
    <linearGradient id="${gid}" x1="0" y1="0" x2="0.35" y2="1">
      <stop offset="0" stop-color="${c.light}"/>
      <stop offset="0.55" stop-color="${c.body}"/>
      <stop offset="1" stop-color="${c.deep}"/>
    </linearGradient>
  </defs>
  ${legs}
  <g class="pk-body">
    <g class="pk-ear pk-ear-l">
      <ellipse cx="9" cy="30" rx="9" ry="3.6" transform="rotate(-40 9 30)" fill="${c.light}"/>
      <ellipse cx="6" cy="38" rx="10" ry="3.6" transform="rotate(-12 6 38)" fill="${c.body}"/>
      <ellipse cx="8" cy="46" rx="8.6" ry="3.4" transform="rotate(16 8 46)" fill="${c.light}"/>
    </g>
    <g class="pk-ear pk-ear-r">
      <ellipse cx="71" cy="30" rx="9" ry="3.6" transform="rotate(40 71 30)" fill="${c.light}"/>
      <ellipse cx="74" cy="38" rx="10" ry="3.6" transform="rotate(12 74 38)" fill="${c.body}"/>
      <ellipse cx="72" cy="46" rx="8.6" ry="3.4" transform="rotate(-16 72 46)" fill="${c.light}"/>
    </g>
    <path d="M40 5 C56 5 66 19 66.5 36 C67 50 63 60 65 72 C66.5 84 55 94 40 94 C25 94 13.5 84 15 72 C17 60 13 50 13.5 36 C14 19 24 5 40 5 Z" fill="url(#${gid})"/>
    <ellipse cx="30" cy="24" rx="3.4" ry="2.4" fill="${c.deep}" opacity=".22"/>
    <ellipse cx="52" cy="30" rx="3" ry="2.2" fill="${c.deep}" opacity=".18"/>
    <ellipse cx="46" cy="17" rx="2.6" ry="1.8" fill="${c.light}" opacity=".8"/>
    <ellipse cx="40" cy="66" rx="17" ry="19" fill="${c.tummy}" opacity=".45"/>
    <g class="pk-face">
      <g class="pk-eyes pk-eyes-open">
        <ellipse cx="30.5" cy="44" rx="7.6" ry="8.6" fill="#fffdf7"/>
        <ellipse cx="49.5" cy="44" rx="7.6" ry="8.6" fill="#fffdf7"/>
        <circle cx="31.6" cy="45" r="4.5" fill="#3a291c"/>
        <circle cx="50.6" cy="45" r="4.5" fill="#3a291c"/>
        <circle cx="33.2" cy="43.2" r="1.7" fill="#ffffff"/>
        <circle cx="52.2" cy="43.2" r="1.7" fill="#ffffff"/>
      </g>
      <g class="pk-eyes pk-eyes-happy">
        <path d="M23.6 47 q6.9 -8.4 13.8 0" stroke="#3a291c" stroke-width="2.9" fill="none" stroke-linecap="round"/>
        <path d="M42.6 47 q6.9 -8.4 13.8 0" stroke="#3a291c" stroke-width="2.9" fill="none" stroke-linecap="round"/>
      </g>
      <g class="pk-eyes pk-eyes-focus">
        <path d="M25.2 39.8 L34.8 44.4 L25.2 49" stroke="#3a291c" stroke-width="2.8" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
        <path d="M54.8 39.8 L45.2 44.4 L54.8 49" stroke="#3a291c" stroke-width="2.8" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
      </g>
      <ellipse cx="21" cy="55" rx="4.4" ry="2.8" fill="#ee9c86" opacity=".45"/>
      <ellipse cx="59" cy="55" rx="4.4" ry="2.8" fill="#ee9c86" opacity=".45"/>
      <g class="pk-mouth pk-mouth-smile">
        <path d="M36.5 57 q3.5 4.2 7 0" stroke="#5a3a22" stroke-width="2.1" fill="none" stroke-linecap="round"/>
      </g>
      <g class="pk-mouth pk-mouth-talk">
        <path d="M36.7 56.5 h6.6 v2.1 q-3.3 3.7 -6.6 0 Z" fill="#6b4630"/>
      </g>
      <g class="pk-mouth pk-mouth-o">
        <ellipse cx="40" cy="58.4" rx="2.9" ry="3.5" fill="#6b4630"/>
      </g>
    </g>
    ${prop}
    ${arm("l")}
    ${arm("r")}
  </g>
</svg>`;
}

/** 침대에서의 모습 — 잠들었는지, 읽는지, 폰 보는지, 음악 듣는지 */
function poseOf(m, onBed) {
  if (!onBed) return m.status === "working" || m.status === "meeting" ? "work" : "idle";
  if (REST_MODES.some((r) => r.id === m.restMode)) return m.restMode;
  if (m.status === "sleep") return "sleep";
  const pool = ["read", "phone", "music"];
  let hash = 0;
  for (const ch of String(m.name || "pickle")) hash = (hash + ch.charCodeAt(0)) % 997;
  return pool[hash % pool.length];
}

/** 침대에 누운 피클 (잠옷 + 이불) — mode: sleep · read · phone · music */
function restingSVG(color, idx, mode = "sleep") {
  const c = spriteColors(color);
  const gid = `pksg-${idx}`;
  const pid = `pksp-${idx}`;
  const did = `pksd-${idx}`;
  const glow = `pksl-${idx}`;
  const asleep = mode === "sleep" || mode === "music";
  const eyes = asleep
    ? `<g class="pk-eyes pk-eyes-closed">
       <path d="M17.5 37 q4.5 -4.2 9 0" stroke="#43301f" stroke-width="2.2" fill="none" stroke-linecap="round"/>
       <path d="M31.5 37 q4.5 -4.2 9 0" stroke="#43301f" stroke-width="2.2" fill="none" stroke-linecap="round"/>
     </g>`
    : `<g class="pk-eyes pk-eyes-open">
       <ellipse cx="20.5" cy="36" rx="4.8" ry="5.6" fill="#fffdf7"/>
       <ellipse cx="34.5" cy="36" rx="4.8" ry="5.6" fill="#fffdf7"/>
       <circle cx="22" cy="37" r="2.6" fill="#3a291c"/>
       <circle cx="36" cy="37" r="2.6" fill="#3a291c"/>
       <circle cx="22.9" cy="36" r=".9" fill="#fff"/>
       <circle cx="36.9" cy="36" r=".9" fill="#fff"/>
     </g>`;

  const mouth = mode === "music"
    ? `<ellipse cx="27.5" cy="46.5" rx="3.4" ry="3.8" fill="#6b4630"/>`
    : `<ellipse cx="27.5" cy="47" rx="2.8" ry="2.2" fill="#6b4630"/>`;

  const cap = mode === "sleep"
    ? `<path d="M11 23 Q7 9 22 5 Q30 8 32 16 Q23 13 20 22 Z" fill="${c.pajB}"/>
       <circle cx="21" cy="6" r="4" fill="#fff6ea"/>`
    : "";

  const headset = mode === "music"
    ? `<g class="pk-headset">
        <path d="M9 40 Q9 9 27 9 Q45 9 45 40" fill="none" stroke="#54565f" stroke-width="4.2" stroke-linecap="round"/>
        <rect x="4" y="31" width="10" height="15" rx="5" fill="#43454e"/>
        <rect x="40" y="31" width="10" height="15" rx="5" fill="#43454e"/>
        <rect x="6" y="34" width="6" height="9" rx="3" fill="${c.pajA}" opacity=".35"/>
      </g>`
    : "";

  const notes = mode === "music"
    ? `<g class="pk-notes" fill="#8ba0d4">
        <text class="pk-note n1" x="30" y="12" font-size="12" font-weight="700">♪</text>
        <text class="pk-note n2" x="44" y="20" font-size="10" font-weight="700">♫</text>
        <text class="pk-note n3" x="22" y="24" font-size="9" font-weight="700">♪</text>
      </g>`
    : "";

  const prop = mode === "read"
    ? `<g class="pk-book">
        <path d="M48 27 L64 31 L64 47 L48 43 Z" fill="#fffaf0" stroke="#d3c0a2" stroke-width="1"/>
        <path d="M64 31 L80 27 L80 43 L64 47 Z" fill="#fffdf7" stroke="#d3c0a2" stroke-width="1"/>
        <path d="M52 32.5 L60 34.5" stroke="#c9b59a" stroke-width="1.2"/>
        <path d="M52 36.5 L60 38.5" stroke="#c9b59a" stroke-width="1.2"/>
        <path d="M68 34.5 L76 32.5" stroke="#c9b59a" stroke-width="1.2"/>
        <path d="M68 38.5 L76 36.5" stroke="#c9b59a" stroke-width="1.2"/>
        <g class="pk-arm pk-arm-l"><ellipse cx="47" cy="40" rx="5.4" ry="6.2" fill="${c.body}"/></g>
        <g class="pk-arm pk-arm-r"><ellipse cx="81" cy="40" rx="5.4" ry="6.2" fill="${c.body}"/></g>
      </g>`
    : mode === "phone"
      ? `<g class="pk-phone">
          <circle cx="58" cy="34" r="16" fill="url(#${glow})" opacity=".75"/>
          <rect x="51" y="23" width="14" height="23" rx="3.2" fill="#2f333d"/>
          <rect x="53" y="26" width="10" height="16" rx="1.6" fill="#a9d7ff"/>
          <rect x="53" y="26" width="10" height="5" rx="1.6" fill="#cfeaff"/>
          <ellipse class="pk-arm pk-arm-l" cx="47" cy="42" rx="5" ry="5.6" fill="${c.body}"/>
          <ellipse class="pk-arm pk-arm-r pk-thumb" cx="66" cy="43.5" rx="4.8" ry="5.4" fill="${c.body}"/>
        </g>`
      : "";

  return `<svg class="pk-svg pk-svg-bed" viewBox="0 0 138 80" aria-hidden="true">
  <defs>
    <linearGradient id="${gid}" x1="0" y1="0" x2="0.3" y2="1">
      <stop offset="0" stop-color="${c.light}"/>
      <stop offset="0.6" stop-color="${c.body}"/>
      <stop offset="1" stop-color="${c.deep}"/>
    </linearGradient>
    <pattern id="${pid}" width="9" height="9" patternUnits="userSpaceOnUse">
      <rect width="9" height="9" fill="${c.pajA}"/>
      <rect width="4.2" height="9" fill="${c.pajB}" opacity=".5"/>
    </pattern>
    <linearGradient id="${did}" x1="0" y1="0" x2="0.2" y2="1">
      <stop offset="0" stop-color="#63656e"/>
      <stop offset="0.45" stop-color="#4b4d56"/>
      <stop offset="1" stop-color="#393b43"/>
    </linearGradient>
    <radialGradient id="${glow}">
      <stop offset="0" stop-color="#bfe3ff" stop-opacity=".9"/>
      <stop offset="1" stop-color="#bfe3ff" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <ellipse cx="70" cy="72" rx="56" ry="5" fill="#2c1c0e" opacity=".14"/>
  <g class="pk-sleep-body">
    <path d="M38 30 C38 24 45 21 56 21 L88 21 C98 21 103 27 103 37 L103 51 C103 61 97 65 88 65 L52 65 C43 65 38 61 38 53 Z" fill="url(#${pid})" stroke="${c.pajB}" stroke-width="1.2"/>
    <ellipse cx="108" cy="37" rx="7.5" ry="5.2" fill="${c.pajA}" stroke="${c.pajB}" stroke-width="1"/>
    <ellipse cx="108" cy="52" rx="7.5" ry="5.2" fill="${c.pajA}" stroke="${c.pajB}" stroke-width="1"/>
    <path class="pk-blanket" d="M54 26 C64 22 76 22 86 23 L98 24 C106 25 108 31 108 41 L108 49 C108 60 102 65 93 65 L62 65 C55 65 52 60 54 50 Z" fill="url(#${did})"/>
    <path d="M55 28 C66 24 76 24 86 25 L104 27" stroke="#75777f" stroke-width="2.4" fill="none" opacity=".7" stroke-linecap="round"/>
    <path d="M68 40 Q74 36 82 38" stroke="#61636c" stroke-width="1.6" fill="none" opacity=".45"/>
    <path d="M78 52 Q86 48 96 50" stroke="#61636c" stroke-width="1.6" fill="none" opacity=".35"/>
    ${prop}
    <g class="pk-sleep-head">
      <g class="pk-ear pk-ear-l">
        <ellipse cx="10" cy="24" rx="8" ry="3.2" transform="rotate(-52 10 24)" fill="${c.light}"/>
        <ellipse cx="6" cy="32" rx="9" ry="3.2" transform="rotate(-20 6 32)" fill="${c.body}"/>
        <ellipse cx="8" cy="40" rx="7.6" ry="3" transform="rotate(14 8 40)" fill="${c.light}"/>
      </g>
      <ellipse cx="27" cy="38" rx="19" ry="20" fill="url(#${gid})"/>
      <path d="M44 31 L52 35 L44 41 Z" fill="${c.pajA}" stroke="${c.pajB}" stroke-width=".8"/>
      ${eyes}
      ${mouth}
      <ellipse cx="13" cy="45" rx="3.6" ry="2.2" fill="#ee9c86" opacity=".4"/>
      <ellipse cx="41" cy="45" rx="3.6" ry="2.2" fill="#ee9c86" opacity=".4"/>
      ${cap}
      ${headset}
    </g>
    ${notes}
  </g>
</svg>`;
}

const present = (m) => !!(m && (m.manual || m.online));

function memberList() {
  return Object.entries(state.members).map(([id, m]) => ({ id, ...m })).filter((m) => m && m.name);
}

function paintPose(node, m, onBed, slot, walking = false) {
  const st = statusOf(m.status);
  const pose = walking ? "walk" : poseOf(m, onBed);
  const idx = slot >= 0 ? slot : 9;
  node.dataset.pose = pose;
  node.innerHTML = walking
    ? workingSVG(m.color, idx, "idle", true)
    : onBed
      ? restingSVG(m.color, idx, pose)
      : workingSVG(m.color, idx, m.status);

  node.appendChild(el("span", { class: "pk-bubble" }, [
    el("b", { text: m.name || "피클" }),
    el("i", { text: walking ? "🚶" : (st.bubble.split(" ")[0] || "") }),
  ]));
  if (!walking && pose === "sleep") {
    node.appendChild(el("span", { class: "pk-zzz", attrs: { "aria-hidden": "true" } }, [
      el("i", { text: "z" }), el("i", { text: "z" }), el("i", { text: "z" }),
    ]));
  }
}

/* 의자 ↔ 침대를 걸어서 이동한다 (문 앞을 거쳐 두 구간) */
const WALK_STEP = 860;
const WALK_GATE = { x: 47.2, y: 62.5 };
let walkSeq = 0;

function startWalk(node, m, spot, slot, onBed) {
  const token = String(++walkSeq);
  const fromX = parseFloat(node.style.left) || spot.x;
  const fromY = parseFloat(node.style.top) || spot.y;
  node.dataset.walk = token;
  node.dataset.place = onBed ? "bed" : "seat";
  node.classList.add("is-walking");
  node.classList.toggle("face-left", spot.x < fromX);
  paintPose(node, m, onBed, slot, true);
  node.style.transitionDuration = "0.85s";
  node.style.zIndex = String(40 + Math.round(Math.max(fromY, spot.y)));
  node.style.left = WALK_GATE.x + "%";
  node.style.top = WALK_GATE.y + "%";

  setTimeout(() => {
    if (!node.isConnected || node.dataset.walk !== token) return;
    node.classList.toggle("face-left", spot.x < WALK_GATE.x);
    node.style.left = spot.x + "%";
    node.style.top = spot.y + "%";
    setTimeout(() => {
      if (!node.isConnected || node.dataset.walk !== token) return;
      node.classList.remove("is-walking", "face-left");
      node.style.transitionDuration = "";
      node.style.zIndex = String(20 + Math.round(spot.y));
      node.dataset.walk = "";
      paintPose(node, m, onBed, slot);
    }, WALK_STEP);
  }, WALK_STEP);
}

function updatePickle(node, m, spot, slot, onBed, spare = false) {
  const st = statusOf(m.status);
  const place = onBed ? "bed" : "seat";
  const prevPlace = node.dataset.place;

  node.classList.toggle("pk-seat", !onBed);
  node.classList.toggle("pk-bed", onBed);
  node.classList.toggle("pk-spare", !!spare);
  node.classList.toggle("pk-me", m.id === me.uid);
  node.classList.toggle("is-working", !onBed);
  node.classList.toggle("is-sleeping", onBed);
  node.dataset.state = m.status || "idle";
  node.dataset.slot = String(slot);
  node.title = `${m.name} · ${st.label}${slot >= 0 ? " · " + SLOT_LABEL(slot) : " · 자리 없음"}`;
  node.style.setProperty("--pk-delay", `${(Math.max(slot, 0) % 6) * 0.42}s`);
  node.style.setProperty("--pk-breath", `${(2.4 + (Math.max(slot, 0) % 5) * 0.2).toFixed(2)}s`);
  node.style.setProperty("--pk-face-dur", `${(11.6 + (Math.max(slot, 0) % 3) * 1.4).toFixed(1)}s`);
  node.style.setProperty("--pk-face-delay", `${((Math.max(slot, 0) % 6) * 1.15).toFixed(2)}s`);

  if (node.dataset.walk) return;

  if (prevPlace && prevPlace !== place) {
    startWalk(node, m, spot, slot, onBed);
    return;
  }

  node.style.left = spot.x + "%";
  node.style.top = spot.y + "%";
  node.style.zIndex = String(20 + Math.round(spot.y));
  node.dataset.place = place;
  if (node.dataset.pose !== poseOf(m, onBed)) paintPose(node, m, onBed, slot);
}

function renderMap() {
  const members = memberList();
  ensureSlots(members);

  // 회의실 배지 인원
  const counts = { meeting: 0, command: 0, lounge: 0 };
  for (const m of members) if (counts[m.room] != null) counts[m.room] += 1;
  CHANNELS.forEach((c) => {
    const node = document.querySelector(`[data-count="${c.id}"]`);
    if (node) node.textContent = String(counts[c.id] || 0);
  });

  const workingN = members.filter((m) => statusOf(m.status).at === "seat").length;
  const restingN = members.filter((m) => statusOf(m.status).at === "bed").length;

  els.roomCounter.textContent = `🪑 ${Math.min(workingN, SLOT_COUNT)}/${SLOT_COUNT} 자리`;
  els.bedCounter.textContent = `${Math.min(restingN, SLOT_COUNT)}/${SLOT_COUNT} 침대`;
  els.onlineCounter.textContent = `ON ${1 + members.filter((m) => m.manual || m.online).length}명`;
  els.bannerTitle.textContent = members.length
    ? (workingN && restingN
      ? `대장님, ${workingN}명이 일하고 ${restingN}명이 쉬고 있어요`
      : workingN
        ? `대장님, ${workingN}명이 일하고 있어요`
        : `대장님, ${restingN}명이 쉬고 있어요`)
    : "대장님, 아직 피클이 없어요";
  els.seedCard.hidden = members.length > 1;

  // 자리 배정 — 같은 번호의 의자와 침대를 함께 쓴다
  const nodes = new Map();
  for (const node of [...els.gameLayer.children]) nodes.set(node.dataset.id, node);
  const used = new Set();

  const place = (m, spot, slot, onBed, spare = false) => {
    used.add(m.id);
    let node = nodes.get(m.id);
    if (!node) {
      node = el("div", { class: "pk" });
      node.dataset.id = m.id;
      node.dataset.place = "";
      node.addEventListener("click", (event) => {
        event.stopPropagation();
        const cur = memberList().find((x) => x.id === m.id) || m;
        openPicklePop(cur, {
          x: parseFloat(node.style.left) || spot.x,
          y: parseFloat(node.style.top) || spot.y,
        }, node.dataset.place === "bed");
      });
      els.gameLayer.appendChild(node);
    }
    updatePickle(node, m, spot, slot, onBed, spare);
  };

  const bySlot = new Map();
  const extras = [];
  for (const m of members) {
    const s = slotOf(m);
    if (s >= 0 && !bySlot.has(s)) bySlot.set(s, m);
    else extras.push(m);
  }

  for (const [slot, m] of bySlot) {
    const onBed = statusOf(m.status).at === "bed";
    place(m, onBed ? BEDS[slot] : SEATS[slot], slot, onBed);
  }
  extras.forEach((m, i) => {
    place(m, SPARE_SPOTS[i % SPARE_SPOTS.length], -1, false, true);
  });

  for (const [id, node] of nodes) if (!used.has(id)) node.remove();
  if (popId && !members.some((m) => m.id === popId)) closePicklePop();
}

/* ── 피클 말풍선 · 빠른 전환 ──────────────────────── */

let popId = null;

function closePicklePop() {
  popId = null;
  els.picklePop.hidden = true;
  els.picklePop.replaceChildren();
}

function setMemberStatus(id, status) {
  if (!STATUSES[status]) return;
  if (id === me.uid) {
    me = { ...me, status };
    saveJSON("goorm.profile", me);
  }
  update(memberRef(id), { status }).catch(() => toast("상태를 바꾸지 못했습니다"));
}

function setMemberRest(id, mode) {
  if (!REST_MODES.some((r) => r.id === mode)) return;
  const status = mode === "sleep" ? "sleep" : "rest";
  if (id === me.uid) {
    me = { ...me, status, restMode: mode };
    saveJSON("goorm.profile", me);
  }
  update(memberRef(id), { status, restMode: mode }).catch(() => toast("상태를 바꾸지 못했습니다"));
}

function openPicklePop(m, spot, onBed) {
  const pop = els.picklePop;
  const st = statusOf(m.status);
  const slot = slotOf(m);
  const pose = onBed ? poseOf(m, true) : "";
  popId = m.id;
  pop.hidden = false;
  pop.className = "pickle-pop" + (spot.y < 47 ? " below" : "");
  pop.style.top = spot.y + "%";
  pop.style.left = "50%";

  pop.replaceChildren(
    el("div", { class: "pop-head" }, [
      el("span", { class: "pop-avatar", text: m.emoji || "🙂", style: { background: tint(m.color) } }),
      el("div", { class: "pop-title" }, [
        el("strong", { text: m.name || "피클" }),
        el("span", { class: "role-badge", text: m.role || "비서" }),
      ]),
      el("button", { type: "button", class: "icon-btn pop-close", text: "✕", attrs: { "aria-label": "닫기" }, on: { click: closePicklePop } }),
    ]),
    el("div", { class: "pop-meta" }, [
      el("span", { class: "status-pill " + st.cls, text: st.label }),
      el("span", { class: "pop-slot", text: slot >= 0 ? SLOT_LABEL(slot) : "빈 자리 없음" }),
    ]),
    el("button", {
      type: "button",
      class: "pop-act pop-act-work" + (onBed ? "" : " is-on"),
      text: "💻 일하러 가기",
      on: { click: () => { setMemberStatus(m.id, "working"); closePicklePop(); } },
    }),
    el("div", { class: "pop-rest" }, [
      el("span", { class: "pop-rest-label", text: "침대에서" }),
      el("div", { class: "pop-chips" }, REST_MODES.map((r) => el("button", {
        type: "button",
        class: "pop-chip" + (onBed && pose === r.id ? " is-on" : ""),
        text: `${r.emoji} ${r.label}`,
        attrs: { "data-rest": r.id },
        on: { click: () => { setMemberRest(m.id, r.id); closePicklePop(); } },
      }))),
    ]),
    el("button", {
      type: "button", class: "pop-edit", text: "✏️ 정보 수정",
      on: { click: () => { closePicklePop(); openStaffEditor(m.id); } },
    }),
  );

  const stageW = els.mapStage.clientWidth || 720;
  const popW = pop.getBoundingClientRect().width || (stageW < 620 ? 168 : 200);
  const half = (popW / 2) / stageW * 100;
  const x = Math.min(99 - half, Math.max(half, spot.x));
  pop.style.left = x + "%";
}

els.mapStage.addEventListener("click", (event) => {
  if (!event.target.closest(".pk") && !event.target.closest(".pickle-pop")) closePicklePop();
});

els.mapStyle.addEventListener("click", (event) => {
  const btn = event.target.closest(".pill");
  if (!btn) return;
  state.mapStyle = btn.dataset.style;
  els.mapStyle.querySelectorAll(".pill").forEach((p) => p.classList.toggle("is-active", p === btn));
  els.mapStage.classList.toggle("clay", state.mapStyle === "clay");
});

els.motionToggle.addEventListener("click", () => {
  state.motion = !state.motion;
  els.motionToggle.classList.toggle("is-active", state.motion);
  els.motionToggle.textContent = state.motion ? "✨ 움직임 ON" : "✨ 움직임 OFF";
  els.mapStage.classList.toggle("motion", state.motion);
});

document.querySelectorAll(".room-badge").forEach((badge) => {
  badge.addEventListener("click", () => {
    setChannel(badge.dataset.channel);
    switchTab("chat");
  });
});

/* ── 회의실 ───────────────────────────────────────── */

function setChannel(id) {
  const channel = channelOf(id);
  state.channel = channel.id;
  els.channelPills.querySelectorAll(".pill").forEach((p) => p.classList.toggle("is-active", p.dataset.channel === channel.id));
  els.chatRoomEmoji.textContent = channel.emoji;
  els.chatRoomName.textContent = channel.name;
  renderChat();
}

els.channelPills.addEventListener("click", (event) => {
  const btn = event.target.closest(".pill");
  if (btn) setChannel(btn.dataset.channel);
});

function messagesOf(channelId) {
  const raw = state.channels[channelId] || {};
  return Object.entries(raw).map(([id, m]) => ({ id, ...m })).sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
}

function mentionNode(text) {
  const frag = document.createDocumentFragment();
  const parts = String(text || "").split(/(@[0-9A-Za-z가-힣_]+)/g);
  for (const part of parts) {
    if (!part) continue;
    if (/^@[0-9A-Za-z가-힣_]+$/.test(part)) frag.appendChild(el("span", { class: "mention", text: part }));
    else frag.appendChild(document.createTextNode(part));
  }
  return frag;
}

const commanderName = () => state.meta.owner || COMMANDER.name;

function targetPickle() {
  const member = memberList().find((m) => m.name === TARGET_PICKLE);
  return { name: TARGET_PICKLE, emoji: member?.emoji || "🫧" };
}

// 발신자는 항상 대장님, 받는 곳은 항상 goorma_1 하나뿐인 지시 줄.
function renderComposerRoute() {
  const target = targetPickle();
  if (els.routeFromName) els.routeFromName.textContent = commanderName();
  if (els.routeToName) els.routeToName.textContent = "@" + target.name;
  if (els.routeToEmoji) els.routeToEmoji.textContent = target.emoji;
  if (els.chatInput) {
    els.chatInput.placeholder = `${target.name}에게 지시할 내용을 입력하세요 (AI 한도량을 보고 나머지 피클에게 나눠 시킵니다)`;
  }
}

if (els.composerAddStaff) {
  els.composerAddStaff.addEventListener("click", () => openStaffEditor(null));
}

function renderChat() {
  const list = messagesOf(state.channel);
  const members = memberList();
  els.chatOnline.textContent = String(1 + members.filter((m) => m.manual || m.online).length);

  const nearBottom = els.chatList.scrollHeight - els.chatList.scrollTop - els.chatList.clientHeight < 90;
  const wasEmpty = els.chatList.childElementCount === 0;

  els.chatList.replaceChildren();
  for (const m of list) {
    const isCommander = !!m.commander;
    const head = el("div", { class: "msg-head" }, [
      el("span", { class: "msg-name", text: m.name || "이름 없음" }),
      m.role ? el("span", { class: "role-badge", text: m.role }) : null,
      isCommander && m.to ? el("span", { class: "msg-route", text: "→ @" + m.to }) : null,
      el("span", { class: "msg-time", text: fmtTime(m.createdAt) }),
    ]);

    const bubble = el("div", { class: "msg-bubble" });
    bubble.appendChild(mentionNode(m.text));

    const row = el("div", { class: "msg-row" + (isCommander ? " mine" : "") }, [
      avatarNode(m.emoji, m.color),
      el("div", { class: "msg-main" }, [head, bubble]),
    ]);
    els.chatList.appendChild(row);
  }

  els.chatEmpty.classList.toggle("show", list.length === 0);
  if (list.length && (wasEmpty || nearBottom)) els.chatList.scrollTop = els.chatList.scrollHeight;
}

els.chatForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = els.chatInput.value.trim();
  if (!text) return;
  const target = targetPickle();
  const name = commanderName();
  els.chatInput.value = "";
  push(ref(db, `${base}/channels/${state.channel}/messages`), {
    uid: COMMANDER.uid,
    senderId: COMMANDER.uid,          // 🤖 업무 배정 감시(check.sh)가 읽는 필드
    byUid: me.uid,
    name,
    senderName: name,
    role: COMMANDER.role,
    senderRole: COMMANDER.role,
    emoji: COMMANDER.emoji,
    color: COMMANDER.color,
    commander: true,
    to: target.name,
    text: text.slice(0, 500),
    createdAt: serverTimestamp(),
  }).catch((err) => {
    console.warn("메시지 전송 실패", err);
    toast("메시지를 보내지 못했습니다");
  });
});

/* ── 작업실 ───────────────────────────────────────── */

function renderTasks() {
  const list = Object.entries(state.tasks).map(([id, t]) => ({ id, ...t })).sort((a, b) => {
    const rank = { doing: 0, wait: 1, done: 2 };
    const ra = rank[a.status] ?? 1;
    const rb = rank[b.status] ?? 1;
    if (ra !== rb) return ra - rb;
    return (a.createdAt || 0) - (b.createdAt || 0);
  });

  els.taskCount.textContent = String(list.length);
  els.taskList.replaceChildren();

  for (const t of list) {
    const st = taskStatusOf(t.status);
    const pill = el("button", {
      type: "button",
      class: "status-pill " + st.cls,
      text: st.label,
      attrs: { title: "눌러서 상태 바꾸기" },
      on: {
        click: () => {
          update(ref(db, `${base}/tasks/${t.id}`), { status: st.next })
            .catch(() => toast("상태를 바꾸지 못했습니다"));
        },
      },
    });

    const del = el("button", {
      type: "button", class: "icon-btn", text: "✕",
      attrs: { "aria-label": "할 일 삭제" },
      on: {
        click: () => {
          if (!confirm("이 일을 삭제할까요?")) return;
          remove(ref(db, `${base}/tasks/${t.id}`)).catch(() => toast("삭제하지 못했습니다"));
        },
      },
    });

    els.taskList.appendChild(el("li", { class: "task-item" + (t.status === "done" ? " done" : "") }, [
      el("div", { class: "task-body" }, [
        el("div", { class: "task-title", text: t.title || "" }),
        el("div", { class: "task-meta" }, [
          t.assignee ? el("span", { class: "tag", text: "담당 " + t.assignee }) : null,
          pill,
          t.by ? el("span", { class: "tag", text: t.by }) : null,
        ]),
      ]),
      del,
    ]));
  }

  els.taskEmpty.classList.toggle("show", list.length === 0);
  renderAssigneeOptions();
}

let assigneeSig = "";
function renderAssigneeOptions() {
  const names = memberList().map((m) => m.name).filter(Boolean);
  const uniq = [...new Set(names)];
  const sig = uniq.join("|");
  if (sig === assigneeSig) return;
  assigneeSig = sig;
  const prev = els.taskAssignee.value;
  els.taskAssignee.replaceChildren(el("option", { text: "미정", attrs: { value: "" } }));
  for (const name of uniq) els.taskAssignee.appendChild(el("option", { text: name, attrs: { value: name } }));
  if (prev && uniq.includes(prev)) els.taskAssignee.value = prev;
}

els.taskForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const title = els.taskTitle.value.trim();
  if (!title) return;
  els.taskTitle.value = "";
  push(tasksRef, {
    title: title.slice(0, 200),
    assignee: els.taskAssignee.value || "",
    status: els.taskStatus.value || "wait",
    by: `${commanderName()} 대장님`,
    byUid: me.uid,
    createdAt: serverTimestamp(),
  }).catch((err) => {
    console.warn("할 일 추가 실패", err);
    toast("할 일을 추가하지 못했습니다");
  });
});

/* ── 비서현황 ─────────────────────────────────────── */

let staffDraft = null;
let staffEditingId = null;

function paintSlotOptions(selected) {
  const chosen = selected == null ? "" : String(selected);
  els.staffSlot.replaceChildren(el("option", { text: "자동 배정 (빈 자리)", attrs: { value: "" } }));
  for (let i = 0; i < SLOT_COUNT; i++) {
    const owner = memberList().find((m) => m.id !== staffEditingId && slotOf(m) === i);
    els.staffSlot.appendChild(el("option", {
      text: `${i + 1}번 자리 · ${i + 1}번 침대` + (owner ? ` — ${owner.name}` : " (비어 있음)"),
      attrs: { value: String(i) },
    }));
  }
  els.staffSlot.value = chosen;
}

function openStaffEditor(id) {
  staffEditingId = id || null;
  const existing = id ? state.members[id] : null;
  staffDraft = existing
    ? {
      name: existing.name || "", role: existing.role || "비서", emoji: existing.emoji || "🙂",
      color: existing.color || COLORS[0], room: existing.room || "command", status: existing.status || "idle",
      slot: slotOf(existing) >= 0 ? slotOf(existing) : "",
      restMode: REST_MODES.some((r) => r.id === existing.restMode) ? existing.restMode : "",
    }
    : { name: "", role: "메인 비서", emoji: "🫧", color: COLORS[0], room: "command", status: "idle", slot: "", restMode: "" };

  els.staffModalTitle.textContent = existing ? "비서 정보" : "비서 추가";
  els.staffName.value = staffDraft.name;
  els.staffRole.value = staffDraft.role;
  els.staffRoom.value = staffDraft.room;
  els.staffStatus.value = staffDraft.status;
  els.staffRest.value = staffDraft.restMode;
  paintSlotOptions(staffDraft.slot);
  els.staffDelete.hidden = !existing;
  paintStaffDraft();
  openModal(els.staffModal);
  setTimeout(() => els.staffName.focus({ preventScroll: true }), 90);
}

function paintStaffDraft() {
  els.staffPreviewAvatar.textContent = staffDraft.emoji;
  els.staffPreviewAvatar.style.background = tint(staffDraft.color);
  els.staffPreviewName.textContent = staffDraft.name || "이름 없음";
  els.staffPreviewRole.textContent = staffDraft.role || "비서";
  els.staffEmojis.querySelectorAll(".emoji-opt").forEach((b) => b.classList.toggle("sel", b.dataset.emoji === staffDraft.emoji));
  els.staffColors.querySelectorAll(".color-opt").forEach((b) => b.classList.toggle("sel", b.dataset.color === staffDraft.color));
}

for (const emoji of EMOJIS) {
  const b = el("button", { type: "button", class: "emoji-opt", text: emoji, attrs: { "aria-label": emoji } });
  b.dataset.emoji = emoji;
  b.addEventListener("click", () => { staffDraft.emoji = emoji; paintStaffDraft(); });
  els.staffEmojis.appendChild(b);
}

for (const color of COLORS) {
  const b = el("button", { type: "button", class: "color-opt", attrs: { "aria-label": "색 " + color } });
  b.dataset.color = color;
  b.style.background = color;
  b.addEventListener("click", () => { staffDraft.color = color; paintStaffDraft(); });
  els.staffColors.appendChild(b);
}

els.staffName.addEventListener("input", () => { staffDraft.name = els.staffName.value; paintStaffDraft(); });
els.staffRole.addEventListener("input", () => { staffDraft.role = els.staffRole.value; paintStaffDraft(); });
els.staffRoom.addEventListener("change", () => { staffDraft.room = els.staffRoom.value; });
els.staffStatus.addEventListener("change", () => { staffDraft.status = els.staffStatus.value; });
els.staffRest.addEventListener("change", () => { staffDraft.restMode = els.staffRest.value; });

els.addStaffBtn.addEventListener("click", () => openStaffEditor(null));

els.staffSave.addEventListener("click", () => {
  const name = (els.staffName.value || "").trim().slice(0, 24);
  if (!name) { toast("이름을 적어 주세요"); els.staffName.focus(); return; }
  const wantedSlot = els.staffSlot.value === "" ? -1 : Number(els.staffSlot.value);
  const payload = {
    uid: staffEditingId || newUid(),
    name,
    role: (els.staffRole.value || "비서").trim().slice(0, 20) || "비서",
    emoji: staffDraft.emoji,
    color: staffDraft.color,
    room: els.staffRoom.value,
    status: els.staffStatus.value,
    restMode: els.staffRest.value || "",
    lastSeen: serverTimestamp(),
  };

  if (staffEditingId) {
    const mine = slotOf(state.members[staffEditingId]);
    if (wantedSlot >= 0 && wantedSlot !== mine) {
      const other = memberList().find((m) => m.id !== staffEditingId && slotOf(m) === wantedSlot);
      if (other) {
        if (mine >= 0) update(memberRef(other.id), { slot: mine }).catch(() => {});
        else {
          const used = new Set(memberList().filter((m) => m.id !== other.id).map(slotOf));
          let free = 0;
          while (used.has(free) && free < SLOT_COUNT) free++;
          if (free < SLOT_COUNT) update(memberRef(other.id), { slot: free }).catch(() => {});
        }
      }
      payload.slot = wantedSlot;
    }
    update(memberRef(staffEditingId), payload)
      .then(() => toast("비서 정보를 저장했습니다"))
      .catch(() => toast("저장하지 못했습니다"));
    if (staffEditingId === me.uid) {
      me = { ...me, ...payload };
      saveJSON("goorm.profile", me);
      pushMe();
    }
  } else {
    if (wantedSlot >= 0) payload.slot = wantedSlot;
    update(memberRef(payload.uid), { ...payload, manual: true })
      .then(() => toast(`${name} 님을 들였습니다`))
      .catch(() => toast("추가하지 못했습니다"));
  }
  closeModal(els.staffModal);
});

els.staffDelete.addEventListener("click", () => {
  if (!staffEditingId) return;
  if (!confirm("이 비서를 목록에서 지울까요?")) return;
  remove(memberRef(staffEditingId)).catch(() => toast("지우지 못했습니다"));
  closeModal(els.staffModal);
});

function renderStaff() {
  const members = memberList();
  els.staffList.replaceChildren();

  for (const m of members) {
    const st = statusOf(m.status);
    const ch = channelOf(m.room);
    const slot = slotOf(m);
    const resting = st.at === "bed";
    els.staffList.appendChild(el("div", { class: "staff-card" }, [
      avatarNode(m.emoji, m.color, "lg"),
      el("div", { class: "staff-main" }, [
        el("div", { class: "staff-name" }, [
          el("span", { text: m.name }),
          m.id === me.uid ? el("span", { class: "tag", text: "나" }) : null,
        ]),
        el("div", { class: "staff-tags" }, [
          el("span", { class: "role-badge", text: m.role || "비서" }),
          el("span", { class: "tag", text: `${ch.emoji} ${ch.name}` }),
          el("span", { class: "status-pill " + st.cls, text: st.label }),
          resting ? el("span", { class: "tag", text: (() => {
            const r = REST_MODES.find((x) => x.id === poseOf(m, true));
            return r ? `${r.emoji} ${r.label}` : "";
          })() }) : null,
          el("span", { class: "tag seat-tag", text: slot >= 0 ? `🪑 ${slot + 1}번 자리 · 🛏️ ${slot + 1}번 침대` : "🪑 자리 없음" }),
          m.online ? el("span", { class: "tag", text: "● 접속 중" }) : null,
        ]),
      ]),
      el("div", { class: "staff-actions" }, [
        el("button", {
          type: "button", class: "btn small" + (resting ? "" : " soft"),
          text: resting ? "💻 일하기" : "💤 쉬기",
          attrs: { title: resting ? "의자로 보내기" : "침대에서 쉬게 하기" },
          on: { click: () => setMemberStatus(m.id, resting ? "working" : "rest") },
        }),
        el("button", {
          type: "button", class: "btn small", text: "수정",
          on: { click: () => openStaffEditor(m.id) },
        }),
      ]),
    ]));
  }

  els.staffEmpty.classList.toggle("show", members.length === 0);
}

/* ── 기본 피클 불러오기 ───────────────────────────── */

const STARTERS = [
  { name: "라랑이", role: "메인 비서", emoji: "🦊", color: "#a9622a", status: "working" },
  { name: "꼬망이", role: "회의 담당", emoji: "🐼", color: "#3f6fb5", status: "meeting" },
  { name: "하선", role: "야간 비서", emoji: "🐰", color: "#8a55b0", status: "sleep" },
];

let seeding = false;
let autoSeeded = false;

async function seedStarters(quiet) {
  if (seeding) return;
  seeding = true;
  try {
    const names = new Set(memberList().map((m) => m.name));
    const used = new Set(memberList().map((m) => slotOf(m)).filter((s) => s >= 0));
    let added = 0;
    for (const s of STARTERS) {
      if (names.has(s.name)) continue;
      let free = 0;
      while (used.has(free) && free < SLOT_COUNT) free++;
      const id = newUid();
      const payload = {
        uid: id, name: s.name, role: s.role, emoji: s.emoji, color: s.color,
        room: "command", status: s.status, manual: true, lastSeen: serverTimestamp(),
      };
      if (free < SLOT_COUNT) { payload.slot = free; used.add(free); }
      await update(memberRef(id), payload);
      added += 1;
    }
    if (!quiet) toast(added ? "🎮 기본 피클을 불러왔어요" : "이미 기본 피클들이 있어요");
  } catch (err) {
    console.warn("기본 피클 불러오기 실패", err);
    toast("기본 피클을 불러오지 못했습니다");
  } finally {
    seeding = false;
  }
}

els.seedBtn.addEventListener("click", () => seedStarters(false));

/* ── AI한도 ───────────────────────────────────────── */

function renderQuota() {
  const members = memberList();
  els.quotaList.replaceChildren();

  let sum = 0;
  let counted = 0;

  for (const m of members) {
    const q = state.quota[m.id] || {};
    const percent = Number.isFinite(Number(q.percent)) ? Number(q.percent) : 0;
    sum += percent;
    counted += 1;

    const fill = el("div", { class: "quota-fill" + (percent >= 85 ? " high" : "") });
    fill.style.width = percent + "%";

    const labelOf = (v) => (v >= 90 ? "한계" : v >= 70 ? "주의" : v >= 40 ? "보통" : "여유");
    const percentLabel = el("span", { class: "quota-percent", text: `${percent}% · ${labelOf(percent)}` });

    const range = el("input", {
      attrs: { type: "range", min: "0", max: "100", step: "5", "aria-label": m.name + " 사용량" },
    });
    range.value = String(percent);
    const paintRange = (v) => {
      range.style.background = `linear-gradient(90deg, #b0703a ${v}%, #f1e5d2 ${v}%)`;
    };
    paintRange(percent);
    range.addEventListener("input", () => {
      const v = Number(range.value);
      percentLabel.textContent = `${v}% · ${labelOf(v)}`;
      fill.style.width = v + "%";
      fill.classList.toggle("high", v >= 85);
      paintRange(v);
    });
    range.addEventListener("change", () => {
      update(ref(db, `${base}/quota/${m.id}`), {
        percent: Number(range.value), updatedAt: serverTimestamp(),
      }).catch(() => toast("한도를 저장하지 못했습니다"));
    });

    const note = el("input", {
      attrs: { type: "text", maxlength: "80", placeholder: "메모 (예: 이번 주 70% 사용)", value: q.note || "" },
    });
    note.addEventListener("change", () => {
      update(ref(db, `${base}/quota/${m.id}`), {
        note: note.value.slice(0, 80), updatedAt: serverTimestamp(),
      }).catch(() => toast("메모를 저장하지 못했습니다"));
    });

    els.quotaList.appendChild(el("div", { class: "quota-card" }, [
      el("div", { class: "quota-head" }, [
        avatarNode(m.emoji, m.color),
        el("span", { class: "quota-name", text: m.name }),
        percentLabel,
      ]),
      el("div", { class: "quota-bar" }, [fill]),
      el("div", { class: "quota-row" }, [range, note]),
    ]));
  }

  const avg = counted ? Math.round(sum / counted) : 0;
  els.quotaSummary.textContent = `평균 ${avg}%`;
  els.quotaEmpty.classList.toggle("show", members.length === 0);
}

/* ── 메모 ─────────────────────────────────────────── */

function renderMemo() {
  const list = Object.entries(state.notes).map(([id, n]) => ({ id, ...n }))
    .sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  const pinned = list.filter((n) => n.pinned);

  els.memoCount.textContent = String(list.length);

  if (pinned.length) {
    els.pinnedBox.hidden = false;
    els.pinnedList.replaceChildren(...pinned.map((n) => el("div", { class: "pinned-item" }, [
      el("span", { class: "pin-text", text: n.text || "" }),
      el("span", { class: "pin-author", text: n.name || "" }),
    ])));
  } else {
    els.pinnedBox.hidden = true;
    els.pinnedList.replaceChildren();
  }

  els.memoList.replaceChildren();
  for (const n of list) {
    const actions = el("div", { class: "memo-actions" }, [
      el("button", {
        type: "button", class: "icon-btn", text: n.pinned ? "📌" : "📍",
        attrs: { title: n.pinned ? "고정 해제" : "공지로 고정" },
        on: {
          click: () => update(ref(db, `${base}/notes/${n.id}`), { pinned: !n.pinned })
            .catch(() => toast("변경하지 못했습니다")),
        },
      }),
    ]);

    if (n.byUid === me.uid) {
      actions.appendChild(el("button", {
        type: "button", class: "icon-btn", text: "✕",
        attrs: { title: "삭제" },
        on: {
          click: () => {
            if (!confirm("이 메모를 삭제할까요?")) return;
            remove(ref(db, `${base}/notes/${n.id}`)).catch(() => toast("삭제하지 못했습니다"));
          },
        },
      }));
    }

    els.memoList.appendChild(el("li", { class: "memo-item" + (n.pinned ? " pinned" : "") }, [
      el("div", { class: "memo-head" }, [
        el("span", { class: "memo-author", text: n.name || "" }),
        el("span", { class: "memo-time", text: fmtTime(n.createdAt) }),
      ]),
      el("div", { class: "memo-text", text: n.text || "" }),
      actions,
    ]));
  }

  els.memoEmpty.classList.toggle("show", list.length === 0);
}

els.memoForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = els.memoInput.value.trim();
  if (!text) return;
  els.memoInput.value = "";
  push(notesRef, {
    text: text.slice(0, 500),
    name: commanderName(),
    byUid: me.uid,
    pinned: false,
    createdAt: serverTimestamp(),
  }).catch((err) => {
    console.warn("메모 저장 실패", err);
    toast("메모를 남기지 못했습니다");
  });
});

/* ── 모달 공통 ────────────────────────────────────── */

function openModal(node) {
  node.hidden = false;
  document.body.style.overflow = "hidden";
}
function closeModal(node) {
  node.hidden = true;
  if (![...document.querySelectorAll(".modal")].some((m) => !m.hidden)) document.body.style.overflow = "";
}
document.querySelectorAll(".modal").forEach((modal) => {
  modal.querySelectorAll("[data-close]").forEach((b) => b.addEventListener("click", () => closeModal(modal)));
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    document.querySelectorAll(".modal").forEach((m) => { if (!m.hidden) closeModal(m); });
  }
});

/* ── 사무실 설정 ──────────────────────────────────── */

let metaDraft = { title: "구름 비서실", owner: "Goorm" };

function openOfficeModal() {
  metaDraft = { title: state.meta.title || "구름 비서실", owner: state.meta.owner || "Goorm" };
  els.officeTitleInput.value = metaDraft.title;
  els.officeOwnerInput.value = metaDraft.owner;
  els.roomLink.textContent = officeUrl(office);
  els.officeSwitchInput.value = office === DEFAULT_OFFICE ? "" : office;
  openModal(els.officeModal);
}

els.officeTitle.addEventListener("click", openOfficeModal);

els.officeSave.addEventListener("click", () => {
  const title = (els.officeTitleInput.value || "").trim().slice(0, 24) || "구름 비서실";
  const owner = (els.officeOwnerInput.value || "").trim().slice(0, 16) || "Goorm";
  update(metaRef, { title, owner }).then(() => {
    state.meta = { ...state.meta, title, owner };
    paintHeader();
    renderMap();
    renderComposerRoute();
    toast("사무실 이름을 저장했습니다");
    closeModal(els.officeModal);
  }).catch(() => toast("저장하지 못했습니다"));
});

els.copyLinkBtn.addEventListener("click", async () => {
  const url = officeUrl(office);
  try {
    await navigator.clipboard.writeText(url);
    toast("초대 링크를 복사했습니다");
  } catch {
    const ta = document.createElement("textarea");
    ta.value = url;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); toast("초대 링크를 복사했습니다"); }
    catch { toast("복사하지 못했습니다. 주소창의 링크를 복사해 주세요"); }
    ta.remove();
  }
});

els.officeSwitchBtn.addEventListener("click", () => {
  const next = normalizeOffice(els.officeSwitchInput.value || DEFAULT_OFFICE);
  if (next === office) { toast("이미 이 사무실에 있습니다"); return; }
  location.href = officeUrl(next);
});
els.officeSwitchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") { event.preventDefault(); els.officeSwitchBtn.click(); }
});

/* ── 실시간 구독 ──────────────────────────────────── */

onValue(ref(db, ".info/connected"), (snap) => {
  const online = snap.val() === true;
  state.connected = online;
  els.connPill.classList.toggle("off", !online);
  els.connText.textContent = online ? "실시간 연결" : "다시 연결 중";
  if (online) goOnline();
});

onValue(metaRef, (snap) => {
  state.meta = snap.val() || {};
  paintHeader();
  renderMap();
  renderComposerRoute();
}, (err) => console.warn("meta 구독 실패", err));

const seenKey = `goorm.seen.${office}`;
const firstVisitOffice = !loadJSON(seenKey, false);
if (firstVisitOffice) saveJSON(seenKey, true);
let autoSeedQueued = false;

function queueAutoSeed() {
  if (autoSeedQueued || autoSeeded || !firstVisitOffice) return;
  if (loadJSON(`goorm.seeded.${office}`, false)) return;
  if (memberList().length > 1) return;
  autoSeedQueued = true;
  setTimeout(() => {
    if (autoSeeded) return;
    if (memberList().length > 1) return;   // 그 사이 다른 피클이 들어옴
    autoSeeded = true;
    saveJSON(`goorm.seeded.${office}`, true);
    seedStarters(true);
  }, 2200);
}

onValue(ref(db, `${base}/members`), (snap) => {
  const raw = snap.val() || {};
  state.members = raw;
  queueAutoSeed();
  renderMap();
  renderStaff();
  renderQuota();
  renderComposerRoute();
  renderAssigneeOptions();
  if (state.tab === "chat") renderChat();
}, (err) => {
  console.warn("members 구독 실패", err);
  toast("실시간 연결에 문제가 있습니다");
});

CHANNELS.forEach((c) => {
  onValue(ref(db, `${base}/channels/${c.id}/messages`), (snap) => {
    state.channels[c.id] = snap.val() || {};
    if (state.channel === c.id) renderChat();
    if (state.tab === "map") renderMap();
  }, (err) => console.warn(`${c.id} 구독 실패`, err));
});

onValue(tasksRef, (snap) => {
  state.tasks = snap.val() || {};
  renderTasks();
}, (err) => console.warn("tasks 구독 실패", err));

onValue(notesRef, (snap) => {
  state.notes = snap.val() || {};
  renderMemo();
}, (err) => console.warn("notes 구독 실패", err));

onValue(quotaRef, (snap) => {
  state.quota = snap.val() || {};
  renderQuota();
}, (err) => console.warn("quota 구독 실패", err));

/* ── 시작 ─────────────────────────────────────────── */

paintHeader();
renderMap();
renderComposerRoute();
setChannel("command");
renderTasks();
renderStaff();
renderQuota();
renderMemo();

els.mapStage.classList.toggle("motion", state.motion);
switchTab("map");

setInterval(() => {
  if (state.tab === "chat") {
    els.chatList.querySelectorAll(".msg-time").forEach((node, i) => {
      const list = messagesOf(state.channel);
      if (list[i]) node.textContent = fmtTime(list[i].createdAt);
    });
  }
}, 60 * 1000);

if (firstVisit) {
  setTimeout(() => toast("오른쪽 위 이름을 눌러 사무실 이름을 바꿀 수 있어요"), 900);
}
