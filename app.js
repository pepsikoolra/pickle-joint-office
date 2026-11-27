/**
 * 합동 비서실 — 실시간 공용 사무실 (Firebase Realtime Database, Spark 무료 플랜)
 */
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import {
  getDatabase, ref, onValue, update, remove, push, onDisconnect, set, serverTimestamp,
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

const STATUSES = [
  { key: "working", label: "업무 중", cls: "st-working" },
  { key: "idle", label: "대기 중", cls: "st-idle" },
  { key: "meeting", label: "회의 중", cls: "st-meeting" },
  { key: "rest", label: "휴식 중", cls: "st-rest" },
];

const PRIORITIES = {
  high: { label: "높음", cls: "high", rank: 0 },
  normal: { label: "보통", cls: "", rank: 1 },
  low: { label: "낮음", cls: "low", rank: 2 },
};

const EMOJIS = ["🥒", "🙂", "🐣", "🦊", "🐼", "🐧", "🐨", "🦉", "🐯", "🐰", "🐳", "🦄",
  "🌱", "🌻", "🍀", "⭐", "🔥", "☕", "📎", "📘", "🎧", "🧭", "🧸", "🍋"];

const COLORS = ["#2f6f4f", "#3f6fb5", "#8a55b0", "#c2613a",
  "#b5843a", "#4a7a8c", "#a34a6a", "#5b6363"];

const DEFAULT_ROOM = "main-office";
const DEFAULT_NAME = "goorma_1";
const MAX_MSG = 1000;

/* ── 방 · 프로필 ───────────────────────────────────── */

function normalizeRoom(raw) {
  const v = String(raw || "").toLowerCase().trim()
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9_-]/g, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
  return v || DEFAULT_ROOM;
}

function readRoom() {
  const params = new URLSearchParams(location.search);
  const q = params.get("room");
  if (q) return normalizeRoom(q);
  const hash = location.hash.replace(/^#/, "");
  const m = hash.match(/(?:^|&)room=([^&]*)/);
  if (m && m[1]) return normalizeRoom(decodeURIComponent(m[1]));
  return DEFAULT_ROOM;
}

const room = readRoom();

function roomUrl(target) {
  const u = new URL(location.href);
  u.hash = "";
  u.search = "?room=" + encodeURIComponent(target);
  return u.toString();
}

function loadJSON(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

function saveJSON(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* 저장 불가 시 무시 */ }
}

function newUid() {
  try {
    if (crypto && crypto.randomUUID) return "u" + crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  } catch { /* 아래로 */ }
  return "u" + Math.random().toString(36).slice(2, 12) + Date.now().toString(36);
}

let uid = loadJSON("pjo.uid", null);
if (!uid) { uid = newUid(); saveJSON("pjo.uid", uid); }

const savedProfile = loadJSON("pjo.profile", null);
const isFirstVisit = !savedProfile || !savedProfile.name;

let me = {
  uid,
  name: savedProfile?.name || DEFAULT_NAME,
  emoji: savedProfile?.emoji || "🙂",
  color: savedProfile?.color || COLORS[0],
  status: STATUSES.some((s) => s.key === savedProfile?.status) ? savedProfile.status : "idle",
};

/* ── Firebase ─────────────────────────────────────── */

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

const roomPath = "rooms/" + room;
const meRef = ref(db, `${roomPath}/members/${uid}`);
const memberRef = (id) => ref(db, `${roomPath}/members/${id}`);
const messagesRef = ref(db, `${roomPath}/messages`);
const todosRef = ref(db, `${roomPath}/todos`);

const state = { members: {}, messages: {}, todos: {}, connected: false };

/* ── DOM ──────────────────────────────────────────── */

const $ = (id) => document.getElementById(id);

const els = {
  roomName: $("roomName"),
  connDot: $("connDot"),
  copyRoomBtn: $("copyRoomBtn"),
  meBtn: $("meBtn"),
  meAvatar: $("meAvatar"),
  meName: $("meName"),
  meStatus: $("meStatus"),

  members: $("members"),
  membersEmpty: $("membersEmpty"),
  memberCount: $("memberCount"),

  msgList: $("msgList"),
  msgEmpty: $("msgEmpty"),
  msgCount: $("msgCount"),
  msgForm: $("msgForm"),
  msgInput: $("msgInput"),

  pinnedWrap: $("pinnedWrap"),
  pinnedList: $("pinnedList"),

  todoList: $("todoList"),
  todoEmpty: $("todoEmpty"),
  todoCount: $("todoCount"),
  todoForm: $("todoForm"),
  todoInput: $("todoInput"),
  todoAssignee: $("todoAssignee"),
  todoPriority: $("todoPriority"),

  profileModal: $("profileModal"),
  profileName: $("profileName"),
  emojiGrid: $("emojiGrid"),
  colorRow: $("colorRow"),
  statusSeg: $("statusSeg"),
  profileSave: $("profileSave"),
  previewAvatar: $("previewAvatar"),
  previewName: $("previewName"),
  previewStatus: $("previewStatus"),

  infoModal: $("infoModal"),
  infoBtn: $("infoBtn"),
  roomInput: $("roomInput"),
  roomGo: $("roomGo"),

  toast: $("toast"),
};

/* ── 작은 도우미 ──────────────────────────────────── */

function el(tag, opts = {}, children = []) {
  const node = document.createElement(tag);
  if (opts.class) node.className = opts.class;
  if (opts.text != null) node.textContent = opts.text;
  if (opts.attrs) {
    for (const [k, v] of Object.entries(opts.attrs)) {
      if (v != null && v !== false) node.setAttribute(k, v === true ? "" : String(v));
    }
  }
  if (opts.style) {
    for (const [k, v] of Object.entries(opts.style)) node.style.setProperty(k, v);
  }
  if (opts.on) {
    for (const [k, v] of Object.entries(opts.on)) node.addEventListener(k, v);
  }
  for (const child of [].concat(children)) {
    if (child) node.appendChild(child);
  }
  return node;
}

let toastTimer = null;
function toast(message) {
  els.toast.textContent = message;
  els.toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => els.toast.classList.remove("show"), 2800);
}

function statusOf(key) {
  return STATUSES.find((s) => s.key === key) || STATUSES[1];
}

function tint(color) {
  return `color-mix(in srgb, ${color || "#8a867e"} 22%, var(--surface-2))`;
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

  if (diff < 7 * 24 * 3600 * 1000) {
    return (d.getMonth() + 1) + "월 " + d.getDate() + "일 " + timeStr;
  }
  return d.toLocaleDateString("ko-KR", { year: "2-digit", month: "numeric", day: "numeric" });
}

function avatarNode(emoji, color, size = null, online = null) {
  const node = el("span", {
    class: "avatar",
    text: emoji || "🙂",
    style: { background: tint(color) },
  });
  if (size) {
    node.style.width = size + "px";
    node.style.height = size + "px";
    node.style.fontSize = Math.round(size * 0.5) + "px";
  }
  if (online !== null) node.appendChild(el("span", { class: "presence" + (online ? " live" : "") }));
  return node;
}

/* ── 헤더 · 내 정보 ───────────────────────────────── */

function paintMe() {
  els.roomName.textContent = room;
  els.meAvatar.textContent = me.emoji;
  els.meAvatar.style.background = tint(me.color);
  els.meName.textContent = me.name;
  els.meStatus.textContent = statusOf(me.status).label;
  document.title = "합동 비서실 · #" + room;
}

let onlineRegistered = false;

async function goOnline() {
  const payload = {
    uid: me.uid,
    name: me.name,
    emoji: me.emoji,
    color: me.color,
    status: me.status,
    online: true,
    lastSeen: serverTimestamp(),
  };
  try {
    await set(meRef, payload);
    await onDisconnect(meRef).update({ online: false, lastSeen: serverTimestamp() });
    onlineRegistered = true;
  } catch (err) {
    console.warn("자리 알림 실패", err);
  }
}

function pushMe() {
  update(meRef, {
    uid: me.uid,
    name: me.name,
    emoji: me.emoji,
    color: me.color,
    status: me.status,
    lastSeen: serverTimestamp(),
  }).catch((err) => {
    console.warn("프로필 저장 실패", err);
    toast("프로필을 서버에 저장하지 못했습니다");
  });
}

/* ── 구성원 보드 ──────────────────────────────────── */

function renderMembers() {
  const list = Object.entries(state.members).map(([id, v]) => ({ id, ...v }));
  list.sort((a, b) => {
    if ((a.id === me.uid) !== (b.id === me.uid)) return a.id === me.uid ? -1 : 1;
    if (!!b.online !== !!a.online) return b.online ? 1 : -1;
    return String(a.name || "").localeCompare(String(b.name || ""), "ko");
  });

  els.members.replaceChildren();

  for (const m of list) {
    if (!m || typeof m !== "object") continue;
    const isMe = m.id === me.uid;
    const st = statusOf(m.status);

    const nameRow = el("div", { class: "member-name" }, [
      el("span", { text: m.name || "이름 없음" }),
      isMe ? el("span", { class: "badge-me", text: "나" }) : null,
    ]);

    const body = el("div", { class: "member-body" }, [nameRow]);

    if (isMe) {
      const select = el("select", {
        class: "me-status-select",
        attrs: { "aria-label": "내 상태 바꾸기" },
      });
      for (const s of STATUSES) {
        const opt = el("option", { text: s.label, attrs: { value: s.key } });
        if (s.key === me.status) opt.selected = true;
        select.appendChild(opt);
      }
      select.addEventListener("change", () => {
        me.status = select.value;
        saveJSON("pjo.profile", {
          name: me.name, emoji: me.emoji, color: me.color, status: me.status,
        });
        paintMe();
        pushMe();
        renderMembers();
        toast(statusOf(me.status).label + "으로 바꿨습니다");
      });
      body.appendChild(select);
    } else {
      body.appendChild(el("div", { class: "member-badges" }, [
        el("span", { class: "status-pill " + st.cls, text: st.label }),
        m.online ? null : el("span", { class: "tag", text: "오프라인" }),
      ]));
    }

    els.members.appendChild(el("div", {
      class: "member" + (isMe ? " is-me" : "") + (m.online ? "" : " offline"),
    }, [avatarNode(m.emoji, m.color, null, !!m.online), body]));
  }

  els.memberCount.textContent = list.length + "명";
  els.membersEmpty.classList.toggle("show", list.length === 0);
  renderAssigneeOptions(list);
}

/* ── 메모 · 공지 ──────────────────────────────────── */

let firstMsgPaint = true;

function messageList() {
  return Object.entries(state.messages)
    .map(([id, m]) => ({ id, ...m }))
    .sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
}

function messageNode(m) {
  const mine = m.uid === me.uid;

  const body = el("div", { class: "msg-body" }, [
    el("div", { class: "msg-head" }, [
      el("span", { class: "msg-name", text: m.name || "이름 없음" }),
      el("span", { class: "msg-time", text: fmtTime(m.createdAt) }),
    ]),
    el("div", { class: "msg-text", text: m.text || "" }),
  ]);

  const actions = el("div", { class: "msg-actions" });

  const pinBtn = el("button", {
    type: "button",
    class: "act-btn" + (m.pinned ? " on" : ""),
    text: m.pinned ? "📌" : "📍",
    attrs: {
      title: m.pinned ? "고정 해제" : "공지로 고정",
      "aria-label": m.pinned ? "공지 고정 해제" : "공지로 고정",
    },
  });
  pinBtn.addEventListener("click", () => {
    update(ref(db, `${roomPath}/messages/${m.id}`), { pinned: !m.pinned })
      .catch(() => toast("변경하지 못했습니다"));
  });
  actions.appendChild(pinBtn);

  if (mine) {
    const delBtn = el("button", {
      type: "button", class: "act-btn del", text: "✕",
      attrs: { title: "내 메모 삭제", "aria-label": "내 메모 삭제" },
    });
    delBtn.addEventListener("click", () => {
      if (!window.confirm("이 메모를 삭제할까요?")) return;
      remove(ref(db, `${roomPath}/messages/${m.id}`))
        .catch(() => toast("삭제하지 못했습니다"));
    });
    actions.appendChild(delBtn);
  }

  return el("div", { class: "msg" + (mine ? " mine" : "") }, [
    avatarNode(m.emoji, m.color, 30),
    body,
    actions,
  ]);
}

function renderMessages() {
  const all = messageList();
  const pinned = all.filter((m) => m.pinned);

  els.msgCount.textContent = String(all.length);

  if (pinned.length) {
    els.pinnedWrap.hidden = false;
    els.pinnedList.replaceChildren(...pinned.map((m) => el("div", { class: "pinned-item" }, [
      el("span", { class: "pin-text", text: m.text || "" }),
      el("span", { class: "pin-author", text: m.name || "" }),
    ])));
  } else {
    els.pinnedWrap.hidden = true;
    els.pinnedList.replaceChildren();
  }

  const wasEmpty = els.msgList.childElementCount === 0;
  const nearBottom = els.msgList.scrollHeight - els.msgList.scrollTop - els.msgList.clientHeight < 90;

  els.msgList.replaceChildren(...all.map((m) => messageNode(m)));
  els.msgEmpty.classList.toggle("show", all.length === 0);

  if (all.length && (firstMsgPaint || wasEmpty || nearBottom)) {
    els.msgList.scrollTop = els.msgList.scrollHeight;
  }
  firstMsgPaint = false;
}

function refreshTimes() {
  const all = messageList();
  const nodes = els.msgList.querySelectorAll(".msg-time");
  nodes.forEach((node, i) => {
    if (all[i]) node.textContent = fmtTime(all[i].createdAt);
  });
}

/* ── 할 일 ────────────────────────────────────────── */

function todoList() {
  return Object.entries(state.todos)
    .map(([id, t]) => ({ id, ...t }))
    .sort((a, b) => {
      if (!!a.done !== !!b.done) return a.done ? 1 : -1;
      const ra = (PRIORITIES[a.priority] || PRIORITIES.normal).rank;
      const rb = (PRIORITIES[b.priority] || PRIORITIES.normal).rank;
      if (ra !== rb) return ra - rb;
      return (a.createdAt || 0) - (b.createdAt || 0);
    });
}

function todoNode(t) {
  const pri = PRIORITIES[t.priority] || PRIORITIES.normal;

  const check = el("input", {
    class: "todo-check",
    attrs: { type: "checkbox", "aria-label": "완료 표시" },
  });
  check.checked = !!t.done;
  check.addEventListener("change", () => {
    update(ref(db, `${roomPath}/todos/${t.id}`), { done: check.checked })
      .catch(() => toast("변경하지 못했습니다"));
  });

  const meta = el("div", { class: "todo-meta" }, [
    el("span", { class: "tag " + pri.cls, text: pri.label }),
    t.assignee ? el("span", { class: "tag", text: "담당 " + t.assignee }) : null,
    t.name ? el("span", { class: "todo-author", text: t.name }) : null,
  ]);

  const del = el("button", {
    type: "button", class: "act-btn del", text: "✕",
    attrs: { title: "삭제", "aria-label": "할 일 삭제" },
  });
  del.addEventListener("click", () => {
    if (!window.confirm("이 할 일을 삭제할까요?")) return;
    remove(ref(db, `${roomPath}/todos/${t.id}`))
      .catch(() => toast("삭제하지 못했습니다"));
  });

  return el("li", { class: "todo" + (t.done ? " done" : "") }, [
    check,
    el("div", { class: "todo-body" }, [
      el("div", { class: "todo-title", text: t.title || "" }),
      meta,
    ]),
    del,
  ]);
}

function renderTodos() {
  const list = todoList();
  els.todoList.replaceChildren(...list.map((t) => todoNode(t)));
  const done = list.filter((t) => t.done).length;
  els.todoCount.textContent = list.length ? `${done}/${list.length}` : "0";
  els.todoCount.title = list.length ? `완료 ${done} · 전체 ${list.length}` : "";
  els.todoEmpty.classList.toggle("show", list.length === 0);
}

let assigneeSignature = "";

function renderAssigneeOptions(list) {
  const uniq = [...new Set(list.map((m) => m.name).filter(Boolean))];
  const sig = uniq.join("|");
  if (sig === assigneeSignature) return;
  assigneeSignature = sig;

  const previous = els.todoAssignee.value;
  els.todoAssignee.replaceChildren(el("option", { text: "담당 없음", attrs: { value: "" } }));
  for (const name of uniq) {
    els.todoAssignee.appendChild(el("option", { text: name, attrs: { value: name } }));
  }
  if (previous && uniq.includes(previous)) els.todoAssignee.value = previous;
}

/* ── 실시간 구독 ──────────────────────────────────── */

onValue(ref(db, ".info/connected"), (snap) => {
  const online = snap.val() === true;
  state.connected = online;
  els.connDot.className = "room-dot " + (online ? "live" : "off");
  els.connDot.title = online ? "실시간 연결됨" : "연결이 끊겼습니다 · 다시 연결 중";
  if (online) goOnline();
});

onValue(ref(db, `${roomPath}/members`), (snap) => {
  state.members = snap.val() || {};
  renderMembers();
}, (err) => {
  console.warn("members 구독 실패", err);
  toast("실시간 연결에 문제가 있습니다");
});

onValue(messagesRef, (snap) => {
  state.messages = snap.val() || {};
  renderMessages();
}, (err) => console.warn("messages 구독 실패", err));

onValue(todosRef, (snap) => {
  state.todos = snap.val() || {};
  renderTodos();
}, (err) => console.warn("todos 구독 실패", err));

/* ── 이벤트 ───────────────────────────────────────── */

function autoGrow(node) {
  node.style.height = "auto";
  node.style.height = Math.min(node.scrollHeight, 140) + "px";
}

els.msgForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = els.msgInput.value.trim();
  if (!text) return;
  els.msgInput.value = "";
  autoGrow(els.msgInput);
  push(messagesRef, {
    uid: me.uid,
    name: me.name,
    emoji: me.emoji,
    color: me.color,
    text: text.slice(0, MAX_MSG),
    pinned: false,
    createdAt: serverTimestamp(),
  }).catch((err) => {
    console.warn("메모 전송 실패", err);
    toast("메모를 보내지 못했습니다");
  });
});

els.msgInput.addEventListener("keydown", (event) => {
  // 터치 기기(아이폰 등)에서는 Enter를 줄바꿈으로 둔다
  const touch = window.matchMedia && window.matchMedia("(pointer: coarse)").matches;
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing && !touch) {
    event.preventDefault();
    els.msgForm.requestSubmit();
  }
});
els.msgInput.addEventListener("input", () => autoGrow(els.msgInput));

els.todoForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const title = els.todoInput.value.trim();
  if (!title) return;
  const assignee = els.todoAssignee.value || "";
  const priority = els.todoPriority.value || "normal";
  els.todoInput.value = "";
  push(todosRef, {
    uid: me.uid,
    name: me.name,
    title: title.slice(0, 200),
    assignee,
    priority,
    done: false,
    createdAt: serverTimestamp(),
  }).catch((err) => {
    console.warn("할 일 추가 실패", err);
    toast("할 일을 추가하지 못했습니다");
  });
});

els.copyRoomBtn.addEventListener("click", async () => {
  const url = roomUrl(room);
  try {
    await navigator.clipboard.writeText(url);
    toast("방 링크를 복사했습니다");
  } catch {
    const ta = document.createElement("textarea");
    ta.value = url;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy");
      toast("방 링크를 복사했습니다");
    } catch {
      toast("복사하지 못했습니다. 주소창의 링크를 복사해 주세요");
    }
    ta.remove();
  }
});

/* 모달 공통 */
function openModal(node) {
  node.hidden = false;
  document.body.style.overflow = "hidden";
  const focusable = node.querySelector("input, select, button");
  if (focusable) setTimeout(() => focusable.focus({ preventScroll: true }), 80);
}

function closeModal(node) {
  node.hidden = true;
  if (![...document.querySelectorAll(".modal")].some((m) => !m.hidden)) {
    document.body.style.overflow = "";
  }
}

document.querySelectorAll(".modal").forEach((modal) => {
  modal.querySelectorAll("[data-close]").forEach((btn) => {
    btn.addEventListener("click", () => closeModal(modal));
  });
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    document.querySelectorAll(".modal").forEach((m) => {
      if (!m.hidden) closeModal(m);
    });
  }
});

/* 프로필 편집 */
let draft = { ...me };

function paintProfileEditor() {
  if (els.profileName.value !== draft.name) els.profileName.value = draft.name;
  els.emojiGrid.querySelectorAll(".emoji-opt").forEach((b) => {
    b.classList.toggle("sel", b.dataset.emoji === draft.emoji);
  });
  els.colorRow.querySelectorAll(".color-opt").forEach((b) => {
    b.classList.toggle("sel", b.dataset.color === draft.color);
  });
  els.statusSeg.querySelectorAll(".seg-opt").forEach((b) => {
    b.classList.toggle("sel", b.dataset.status === draft.status);
  });
  els.previewAvatar.textContent = draft.emoji;
  els.previewAvatar.style.background = tint(draft.color);
  els.previewName.textContent = draft.name || DEFAULT_NAME;
  const st = statusOf(draft.status);
  els.previewStatus.className = "status-pill " + st.cls;
  els.previewStatus.textContent = st.label;
}

for (const emoji of EMOJIS) {
  const b = el("button", { type: "button", class: "emoji-opt", text: emoji, attrs: { "aria-label": emoji } });
  b.dataset.emoji = emoji;
  b.addEventListener("click", () => { draft.emoji = emoji; paintProfileEditor(); });
  els.emojiGrid.appendChild(b);
}

for (const color of COLORS) {
  const b = el("button", { type: "button", class: "color-opt", attrs: { "aria-label": "색 " + color } });
  b.dataset.color = color;
  b.style.background = color;
  b.addEventListener("click", () => { draft.color = color; paintProfileEditor(); });
  els.colorRow.appendChild(b);
}

for (const s of STATUSES) {
  const b = el("button", { type: "button", class: "seg-opt", text: s.label });
  b.dataset.status = s.key;
  b.addEventListener("click", () => { draft.status = s.key; paintProfileEditor(); });
  els.statusSeg.appendChild(b);
}

els.profileName.addEventListener("input", () => {
  draft.name = els.profileName.value;
  paintProfileEditor();
});

els.meBtn.addEventListener("click", () => {
  draft = { ...me };
  paintProfileEditor();
  openModal(els.profileModal);
});

els.profileSave.addEventListener("click", () => {
  const name = (els.profileName.value || "").trim().slice(0, 24) || DEFAULT_NAME;
  me = { ...me, name, emoji: draft.emoji, color: draft.color, status: draft.status };
  saveJSON("pjo.profile", { name: me.name, emoji: me.emoji, color: me.color, status: me.status });
  paintMe();
  pushMe();
  renderMembers();
  closeModal(els.profileModal);
  toast("프로필을 저장했습니다");
});

els.profileName.addEventListener("keydown", (event) => {
  if (event.key === "Enter") { event.preventDefault(); els.profileSave.click(); }
});

/* 안내 · 방 이동 */
els.infoBtn.addEventListener("click", () => {
  els.roomInput.value = room === DEFAULT_ROOM ? "" : room;
  openModal(els.infoModal);
});

els.roomGo.addEventListener("click", () => {
  const next = normalizeRoom(els.roomInput.value || DEFAULT_ROOM);
  if (next === room) { toast("이미 이 방에 있습니다"); return; }
  location.href = roomUrl(next);
});

els.roomInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") { event.preventDefault(); els.roomGo.click(); }
});

/* ── 시작 ─────────────────────────────────────────── */

paintMe();
renderMembers();
renderTodos();
autoGrow(els.msgInput);

setInterval(refreshTimes, 60 * 1000);

if (isFirstVisit) {
  setTimeout(() => {
    draft = { ...me };
    paintProfileEditor();
    openModal(els.profileModal);
    toast("이름과 상태를 정하면 다른 피클에게 보입니다");
  }, 500);
}
