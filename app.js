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
  idle: { label: "대기 중", cls: "s-wait" },
  working: { label: "업무 중", cls: "s-doing" },
  meeting: { label: "회의 중", cls: "s-doing" },
  rest: { label: "휴식 중", cls: "s-wait" },
};

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
  senderId: "commander",
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
  mapAvatars: $("mapAvatars"),

  viewChat: $("view-chat"),
  channelPills: $("channelPills"),
  chatRoomEmoji: $("chatRoomEmoji"),
  chatRoomName: $("chatRoomName"),
  chatOnline: $("chatOnline"),
  chatList: $("chatList"),
  chatEmpty: $("chatEmpty"),
  chatForm: $("chatForm"),
  chatInput: $("chatInput"),
  senderPills: $("senderPills"),
  mentionPills: $("mentionPills"),

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
    room: me.room, status: me.status, manual: false, online: true, lastSeen: serverTimestamp(),
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
    room: me.room, status: me.status, manual: false, lastSeen: serverTimestamp(),
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

const ROOM_SPOTS = {
  meeting: [[30, 46], [38, 54], [25, 57], [43, 44], [20, 51]],
  command: [[55, 24], [63, 29], [48, 23], [60, 36], [52, 40]],
  lounge: [[66, 69], [74, 73], [82, 69], [70, 79], [78, 79]],
};

const present = (m) => !!(m && (m.manual || m.online));

function memberList() {
  return Object.entries(state.members).map(([id, m]) => ({ id, ...m })).filter((m) => m && m.name);
}

function renderMap() {
  const members = memberList();
  const commander = { ...COMMANDER, uid: "commander", name: state.meta.owner || COMMANDER.name, online: true };

  // 배지 인원
  const counts = { meeting: 0, command: 0, lounge: 0 };
  for (const m of members) if (counts[m.room] != null) counts[m.room] += 1;
  counts.command += 1; // 대장님은 늘 지휘실
  CHANNELS.forEach((c) => {
    const node = document.querySelector(`[data-count="${c.id}"]`);
    if (node) node.textContent = String(counts[c.id] || 0);
  });

  // 카운터
  els.roomCounter.textContent = `${CHANNELS.length}개 방 가동`;

  const onlineCount = 1 + members.filter((m) => m.manual || m.online).length;
  els.onlineCounter.textContent = `ON ${onlineCount}명`;

  const waitCount = members.length || 1;
  els.bannerTitle.textContent = `대장님, 비서 ${waitCount}명이 대기 중이에요`;

  // 아바타
  els.mapAvatars.replaceChildren();
  const byRoom = { meeting: [], command: [commander], lounge: [] };
  for (const m of members) {
    const key = byRoom[m.room] ? m.room : "command";
    byRoom[key].push(m);
  }

  for (const channel of CHANNELS) {
    const spots = ROOM_SPOTS[channel.id] || ROOM_SPOTS.command;
    byRoom[channel.id].slice(0, spots.length).forEach((m, i) => {
      const [x, y] = spots[i];
      const isCommander = !!m.commander;
      const node = el("div", {
        class: "map-avatar" + (isCommander ? " commander" : "") + (present(m) || isCommander ? "" : " offline"),
        text: m.emoji || "🙂",
        style: { left: x + "%", top: y + "%", background: tint(m.color), borderColor: "#fffdf8" },
        attrs: { title: `${m.name} · ${statusOf(m.status).label}` },
        on: {
          click: () => {
            if (isCommander) return;
            openStaffEditor(m.id);
          },
        },
      });
      if (present(m) || isCommander) node.appendChild(el("span", { class: "presence-dot" }));
      els.mapAvatars.appendChild(node);
    });
  }
}

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

function senderList() {
  const members = memberList().map((m) => ({
    id: m.id, name: m.name, role: m.role || "비서", emoji: m.emoji || "🙂",
    color: m.color, commander: false,
  }));
  return [{ id: "commander", name: state.meta.owner || COMMANDER.name, role: "대장님", emoji: "👑", color: COMMANDER.color, commander: true }, ...members];
}

function currentSender() {
  return senderList().find((s) => s.id === state.senderId) || senderList()[0];
}

function renderSenders() {
  const list = senderList();
  if (!list.some((s) => s.id === state.senderId)) state.senderId = "commander";

  els.senderPills.replaceChildren();
  for (const s of list) {
    const chip = el("button", {
      type: "button",
      class: "chip" + (s.id === state.senderId ? " is-active" : ""),
      text: `${s.emoji} ${s.name}${s.commander ? " (대장님)" : ""}`,
      on: {
        click: () => {
          state.senderId = s.id;
          renderSenders();
        },
      },
    });
    els.senderPills.appendChild(chip);
  }
  els.senderPills.appendChild(el("button", {
    type: "button", class: "chip add", text: "＋ 비서 추가",
    on: { click: () => openStaffEditor(null) },
  }));

  els.mentionPills.replaceChildren();
  els.mentionPills.appendChild(el("button", {
    type: "button", class: "chip mention", text: "@all",
    on: { click: () => insertMention("all") },
  }));
  for (const m of memberList()) {
    els.mentionPills.appendChild(el("button", {
      type: "button", class: "chip mention", text: "@" + m.name,
      on: { click: () => insertMention(m.name) },
    }));
  }
}

function insertMention(name) {
  const value = els.chatInput.value;
  const needsSpace = value && !value.endsWith(" ") ? " " : "";
  els.chatInput.value = `${value}${needsSpace}@${name} `;
  els.chatInput.focus();
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
  const sender = currentSender();
  els.chatInput.value = "";
  push(ref(db, `${base}/channels/${state.channel}/messages`), {
    uid: sender.id,
    byUid: me.uid,
    name: sender.name,
    role: sender.role,
    emoji: sender.emoji,
    color: sender.color,
    commander: !!sender.commander,
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
  const sender = currentSender();
  els.taskTitle.value = "";
  push(tasksRef, {
    title: title.slice(0, 200),
    assignee: els.taskAssignee.value || "",
    status: els.taskStatus.value || "wait",
    by: sender.commander ? `${sender.name} 대장님` : sender.name,
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

function openStaffEditor(id) {
  staffEditingId = id || null;
  const existing = id ? state.members[id] : null;
  staffDraft = existing
    ? {
      name: existing.name || "", role: existing.role || "비서", emoji: existing.emoji || "🙂",
      color: existing.color || COLORS[0], room: existing.room || "command", status: existing.status || "idle",
    }
    : { name: "", role: "메인 비서", emoji: "🫧", color: COLORS[0], room: "command", status: "idle" };

  els.staffModalTitle.textContent = existing ? "비서 정보" : "비서 추가";
  els.staffName.value = staffDraft.name;
  els.staffRole.value = staffDraft.role;
  els.staffRoom.value = staffDraft.room;
  els.staffStatus.value = staffDraft.status;
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

els.addStaffBtn.addEventListener("click", () => openStaffEditor(null));

els.staffSave.addEventListener("click", () => {
  const name = (els.staffName.value || "").trim().slice(0, 24);
  if (!name) { toast("이름을 적어 주세요"); els.staffName.focus(); return; }
  const payload = {
    uid: staffEditingId || newUid(),
    name,
    role: (els.staffRole.value || "비서").trim().slice(0, 20) || "비서",
    emoji: staffDraft.emoji,
    color: staffDraft.color,
    room: els.staffRoom.value,
    status: els.staffStatus.value,
    lastSeen: serverTimestamp(),
  };

  if (staffEditingId) {
    update(memberRef(staffEditingId), payload)
      .then(() => toast("비서 정보를 저장했습니다"))
      .catch(() => toast("저장하지 못했습니다"));
    if (staffEditingId === me.uid) {
      me = { ...me, ...payload };
      saveJSON("goorm.profile", me);
      pushMe();
    }
  } else {
    update(memberRef(payload.uid), { ...payload, manual: true })
      .then(() => toast(`${name} 비서를 들였습니다`))
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
          present(m) ? el("span", { class: "tag", text: "● 접속 중" }) : null,
        ]),
      ]),
      el("button", {
        type: "button", class: "btn small", text: "수정",
        on: { click: () => openStaffEditor(m.id) },
      }),
    ]));
  }

  els.staffEmpty.classList.toggle("show", members.length === 0);
}

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

    const percentLabel = el("span", { class: "quota-percent", text: percent + "%" });

    const range = el("input", {
      attrs: { type: "range", min: "0", max: "100", step: "5", "aria-label": m.name + " 사용량" },
    });
    range.value = String(percent);
    range.addEventListener("input", () => {
      const v = Number(range.value);
      percentLabel.textContent = v + "%";
      fill.style.width = v + "%";
      fill.classList.toggle("high", v >= 85);
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
  const sender = currentSender();
  els.memoInput.value = "";
  push(notesRef, {
    text: text.slice(0, 500),
    name: sender.name,
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
    renderSenders();
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
  renderSenders();
}, (err) => console.warn("meta 구독 실패", err));

onValue(ref(db, `${base}/members`), (snap) => {
  state.members = snap.val() || {};
  renderMap();
  renderStaff();
  renderQuota();
  renderSenders();
  renderAssigneeOptions();
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
renderSenders();
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
