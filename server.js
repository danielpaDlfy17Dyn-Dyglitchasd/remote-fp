const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const PUBLIC_DIR = path.join(__dirname, "public");

const MIME = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
};

const server = http.createServer((req, res) => {
  let file = req.url === "/" ? "pc.html" : req.url.split("?")[0];
  file = file.replace(/^\/+/, "");
  if (file === "") file = "pc.html";

  const candidates = [path.join(PUBLIC_DIR, file), path.join(__dirname, file)];
  let i = 0;
  const tryNext = () => {
    if (i >= candidates.length) {
      res.writeHead(404);
      return res.end("Not found: " + file);
    }
    const full = candidates[i++];
    fs.readFile(full, (err, data) => {
      if (err) return tryNext();
      const ext = path.extname(full);
      res.writeHead(200, { "Content-Type": MIME[ext] || "text/plain" });
      res.end(data);
    });
  };
  tryNext();
});

const wss = new WebSocketServer({ server });

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

// ================= ADATBÁZIS (Upstash Redis - opcionális) =================
const REDIS_URL = process.env.UPSTASH_URL || process.env.UPSTASH_REDIS_REST_URL || "";
const REDIS_TOKEN = process.env.UPSTASH_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || "";
const useRedis = !!(REDIS_URL && REDIS_TOKEN);

async function rdb(command) {
  if (!useRedis) return null;
  const res = await fetch(REDIS_URL, {
    method: "POST",
    headers: { Authorization: "Bearer " + REDIS_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(command),
  });
  const data = await res.json();
  return data.result;
}

// ================= FIÓKOK =================
const accounts = new Map();
const tokens = new Map();
const online = new Map(); // name -> ws

// ================= SZOBÁK =================
const MAX_ROOMS_PER_OWNER = 3;
// code -> { code, pass, type, locked, owner, adminWs, members: Map(name -> {ws, device, category}), inGame }
const rooms = new Map();

function normName(v) {
  return String(v || "").trim().toUpperCase().replace(/[^A-Z0-9_]/g, "");
}

function clientIp(req) {
  const xf = req.headers && req.headers["x-forwarded-for"];
  if (xf) return String(xf).split(",")[0].trim();
  return (req.socket && req.socket.remoteAddress) || "";
}

function isOnline(m) {
  return !!(m.ws && m.ws.readyState === 1);
}

function activePlayer(room) {
  for (const [name, m] of room.members) {
    if (m.device === "phone" && m.category === "Játékosok" && isOnline(m)) return name;
  }
  return null;
}

function roomState(room) {
  const members = [];
  for (const [name, m] of room.members) {
    members.push({ name, device: m.device, category: m.category, online: isOnline(m) });
  }
  return {
    code: room.code, type: room.type, locked: room.locked, inGame: room.inGame,
    members, activePlayer: activePlayer(room),
  };
}

function broadcastRoom(room, type, payload) {
  for (const [, m] of room.members) {
    if (isOnline(m)) send(m.ws, { type, payload });
  }
}

// Dashboard lista küldése a tulajnak
function pushRoomList(ws) {
  if (!ws || !ws.user) return;
  const list = [];
  for (const [, r] of rooms) {
    if (r.owner === ws.user.name) {
      list.push({ code: r.code, type: r.type, locked: r.locked, members: r.members.size });
    }
  }
  send(ws, { type: "my-rooms", payload: { rooms: list } });
}

function pushRoomListToOwner(room) {
  const ows = online.get(room.owner);
  if (ows) pushRoomList(ows);
}

// Telefon automatikus visszatétele a szobájába (örökös tagság)
function autoRejoin(ws) {
  const name = ws.user.name;
  for (const [code, r] of rooms) {
    const m = r.members.get(name);
    if (m) {
      m.ws = ws;
      ws.roomCode = code;
      send(ws, { type: "room-ok", payload: { code, type: r.type } });
      broadcastRoom(r, "room-state", roomState(r));
      return true;
    }
  }
  return false;
}

// Szoba elhagyása adott connection-ből (member ws nullázás, adminWs leválasztás)
function detachFromRoom(room, ws) {
  if (room.adminWs === ws) room.adminWs = null;
  const m = room.members.get(ws.user ? ws.user.name : "");
  if (m && m.ws === ws) m.ws = null;
}

function lanAddresses() {
  const out = [];
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name]) {
      if (iface.family === "IPv4" && !iface.internal) out.push(iface.address);
    }
  }
  return out;
}

// ================= SZOBA-MEGŐRZÉS (Upstash) =================
function roomToJSON(room) {
  const members = [];
  for (const [name, m] of room.members) {
    members.push({ name, device: m.device, category: m.category });
  }
  return JSON.stringify({
    code: room.code, pass: room.pass, type: room.type,
    locked: room.locked, owner: room.owner, adminIp: room.adminIp || "",
    members,
  });
}

function persistRoom(room) {
  if (!useRedis) return;
  rdb(["SET", "room:" + room.code, roomToJSON(room)]).catch(() => {});
}

function unpersistRoom(code) {
  if (!useRedis) return;
  rdb(["DEL", "room:" + code]).catch(() => {});
}

async function loadRooms() {
  if (!useRedis) return;
  try {
    const keys = await rdb(["KEYS", "room:*"]);
    if (!Array.isArray(keys)) return;
    for (const key of keys) {
      const raw = await rdb(["GET", key]);
      if (!raw) continue;
      try {
        const r = JSON.parse(raw);
        const room = {
          code: r.code, pass: r.pass, type: r.type,
          locked: !!r.locked, owner: r.owner,
          adminWs: null, adminIp: r.adminIp || "",
          members: new Map(),
          inGame: false,
        };
        for (const m of (r.members || [])) {
          room.members.set(m.name, { ws: null, device: m.device, category: m.category });
        }
        rooms.set(room.code, room);
      } catch (e) {}
    }
    console.log("Redis: " + rooms.size + " szoba betoltve");
  } catch (e) {}
}

wss.on("connection", (ws, req) => {
  ws.ip = clientIp(req);
  ws.user = null;
  ws.roomCode = null;

  ws.on("message", async (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (data.type) {
      // ---------- DIAGNOSZTIKA ----------
      case "redis-status": {
        let status = { configured: useRedis, ok: false, error: "" };
        if (useRedis) {
          try {
            const res = await rdb(["PING"]);
            status.ok = res === "PONG";
            if (!status.ok) status.error = "Valasz: " + JSON.stringify(res).slice(0, 100);
          } catch (e) {
            status.error = e.message;
          }
        } else {
          status.error = "Nincs beallitva UPSTASH_URL / UPSTASH_TOKEN";
        }
        send(ws, { type: "redis-status", payload: status });
        break;
      }

      // ---------- FIÓK ----------
      case "register": {
        const name = normName(data.payload && data.payload.name);
        const pass = String((data.payload && data.payload.pass) || "");
        if (name.length < 3 || name.length > 15) {
          return send(ws, { type: "register-error", payload: "A név 3–15 karakter legyen (betű, szám, _)." });
        }
        if (pass.length < 3) {
          return send(ws, { type: "register-error", payload: "A jelszó min. 3 karakter legyen." });
        }
        if (accounts.has(name)) {
          return send(ws, { type: "register-error", payload: "Ez a név már foglalt." });
        }
        if (useRedis) {
          try {
            const existing = await rdb(["GET", "account:" + name]);
            if (existing !== null && existing !== undefined) {
              return send(ws, { type: "register-error", payload: "Ez a név már foglalt." });
            }
            await rdb(["SET", "account:" + name, pass]);
          } catch (e) {}
        }
        accounts.set(name, pass);
        const token = crypto.randomUUID();
        tokens.set(token, name);
        if (useRedis) { try { await rdb(["SET", "token:" + token, name]); } catch (e) {} }
        online.set(name, ws);
        ws.user = { name, device: (data.payload && data.payload.device) || "pc" };
        send(ws, { type: "register-ok", payload: { name, token } });
        if (ws.user.device === "phone") autoRejoin(ws);
        else pushRoomList(ws);
        break;
      }

      case "login": {
        const name = normName(data.payload && data.payload.name);
        const pass = String((data.payload && data.payload.pass) || "");
        let stored = accounts.get(name);
        if (stored === undefined && useRedis) {
          try {
            stored = await rdb(["GET", "account:" + name]);
            if (stored) accounts.set(name, stored);
          } catch (e) {}
        }
        if (!stored || stored !== pass) {
          return send(ws, { type: "login-error", payload: "Hibás név vagy jelszó." });
        }
        const token = crypto.randomUUID();
        tokens.set(token, name);
        if (useRedis) { try { await rdb(["SET", "token:" + token, name]); } catch (e) {} }
        online.set(name, ws);
        ws.user = { name, device: (data.payload && data.payload.device) || "pc" };
        send(ws, { type: "login-ok", payload: { name, token } });
        if (ws.user.device === "phone") autoRejoin(ws);
        else pushRoomList(ws);
        break;
      }

      case "auth-token": {
        const token = String((data.payload && data.payload.token) || "");
        let name = tokens.get(token);
        if (!name && useRedis) {
          try {
            name = await rdb(["GET", "token:" + token]);
            if (name) tokens.set(token, name);
          } catch (e) {}
        }
        if (!name) {
          return send(ws, { type: "auth-error" });
        }
        online.set(name, ws);
        ws.user = { name, device: (data.payload && data.payload.device) || "pc" };
        send(ws, { type: "auth-ok", payload: { name } });
        if (ws.user.device === "phone") autoRejoin(ws);
        else pushRoomList(ws);
        break;
      }

      // ---------- SZOBÁK ----------
      case "create-room": {
        if (!ws.user) return send(ws, { type: "room-error", payload: "Előbb jelentkezz be." });
        const code = String((data.payload && data.payload.code) || "").trim().toUpperCase().replace(/[^A-Z]/g, "");
        const pass = String((data.payload && data.payload.pass) || "");
        const type = (data.payload && data.payload.type) === "lan" ? "lan" : "net";
        if (code.length < 5) {
          return send(ws, { type: "room-error", payload: "A kód min. 5 betű legyen (angol abc)." });
        }
        if (!/^[0-9]{3,}$/.test(pass)) {
          return send(ws, { type: "room-error", payload: "A jelszó min. 3 szám legyen, csak számok." });
        }
        if (rooms.has(code)) {
          return send(ws, { type: "room-error", payload: "Ez a kód már foglalt, válassz másikat." });
        }
        let count = 0;
        for (const [, r] of rooms) if (r.owner === ws.user.name) count++;
        if (count >= MAX_ROOMS_PER_OWNER) {
          return send(ws, { type: "room-error", payload: "Max " + MAX_ROOMS_PER_OWNER + " szoba lehet egyszerre. Törölj egyet." });
        }
        const room = {
          code, pass, type,
          locked: false,
          owner: ws.user.name,
          adminWs: ws,
          adminIp: ws.ip,
          members: new Map(),
          inGame: false,
        };
        room.members.set(ws.user.name, { ws, device: ws.user.device, category: "Játékosok" });
        rooms.set(code, room);
        persistRoom(room);
        ws.roomCode = code;
        send(ws, { type: "room-created", payload: { code, type } });
        pushRoomList(ws);
        break;
      }

      // Tulaj belép a saját szobájába (dashboardról)
      case "enter-room": {
        if (!ws.user) return;
        const code = String((data.payload && data.payload.code) || "").toUpperCase();
        const room = rooms.get(code);
        if (!room || room.owner !== ws.user.name) {
          return send(ws, { type: "room-error", payload: "Ez nem a te szobád." });
        }
        // ha eddig máshol voltunk, leválunk
        if (ws.roomCode && ws.roomCode !== code) {
          const old = rooms.get(ws.roomCode);
          if (old) detachFromRoom(old, ws);
        }
        room.adminWs = ws;
        const m = room.members.get(ws.user.name);
        if (m) m.ws = ws;
        ws.roomCode = code;
        send(ws, { type: "room-ok", payload: { code, type: room.type } });
        broadcastRoom(room, "room-state", roomState(room));
        break;
      }

      // Vissza a dashboardra (szoba megmarad); a nem-tulaj tag ténylegesen kilép
      case "leave-room": {
        if (!ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (room) {
          if (room.inGame) {
            room.inGame = false;
            broadcastRoom(room, "game-stopped", { reason: "left" });
          }
          if (room.owner === ws.user.name) {
            // tulaj: csak leválik, a szoba megmarad a dashboardon
            detachFromRoom(room, ws);
          } else {
            // tag: tényleges kilépés (tagság megszűnik)
            room.members.delete(ws.user.name);
            persistRoom(room);
            broadcastRoom(room, "room-state", roomState(room));
            pushRoomListToOwner(room);
          }
        }
        ws.roomCode = null;
        if (ws.user.device === "pc") pushRoomList(ws);
        break;
      }

      // Szoba zárolása (csak tulaj)
      case "lock-room": {
        if (!ws.user) return;
        const code = String((data.payload && data.payload.code) || "").toUpperCase();
        const room = rooms.get(code);
        if (!room || room.owner !== ws.user.name) return;
        room.locked = !!(data.payload && data.payload.locked);
        persistRoom(room);
        broadcastRoom(room, "room-state", roomState(room));
        pushRoomListToOwner(room);
        break;
      }

      // Szoba törlése (csak tulaj)
      case "delete-room": {
        if (!ws.user) return;
        const code = String((data.payload && data.payload.code) || "").toUpperCase();
        const room = rooms.get(code);
        if (!room || room.owner !== ws.user.name) return;
        for (const [, m] of room.members) {
          if (isOnline(m) && m.ws !== ws) send(m.ws, { type: "room-deleted" });
        }
        if (ws.roomCode === code) ws.roomCode = null;
        rooms.delete(code);
        unpersistRoom(code);
        pushRoomList(ws);
        break;
      }

      case "join-room": {
        if (!ws.user) return send(ws, { type: "room-error", payload: "Előbb jelentkezz be." });
        const code = String((data.payload && data.payload.code) || "").trim().toUpperCase().replace(/[^A-Z]/g, "");
        const pass = String((data.payload && data.payload.pass) || "");
        const room = rooms.get(code);
        if (!room) return send(ws, { type: "room-error", payload: "Nincs ilyen szoba." });
        if (room.locked) return send(ws, { type: "room-error", payload: "A szoba zárva van." });
        if (room.pass && room.pass !== pass) return send(ws, { type: "room-error", payload: "Hibás jelszó." });
        // ha máshol voltunk, átmozgatunk
        if (ws.roomCode && ws.roomCode !== code) {
          const old = rooms.get(ws.roomCode);
          if (old) {
            old.members.delete(ws.user.name);
            broadcastRoom(old, "room-state", roomState(old));
            pushRoomListToOwner(old);
          }
        }
        let category = "Nézők";
        if (ws.user.device === "phone" && !activePlayer(room)) category = "Játékosok";
        room.members.set(ws.user.name, { ws, device: ws.user.device, category });
        persistRoom(room);
        ws.roomCode = code;
        send(ws, { type: "room-ok", payload: { code, type: room.type } });
        broadcastRoom(room, "room-state", roomState(room));
        pushRoomListToOwner(room);
        break;
      }

      // Meghívás (csak admin)
      case "invite": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.adminWs !== ws) return send(ws, { type: "invite-error", payload: "Csak a szoba adminja hívhat meg." });
        if (room.locked) return send(ws, { type: "invite-error", payload: "A szoba zárva van." });
        const targetName = normName(data.payload && data.payload.name);
        if (!targetName) return send(ws, { type: "invite-error", payload: "Adj meg nevet." });
        const targetWs = online.get(targetName);
        if (!targetWs || targetWs.readyState !== 1) {
          return send(ws, { type: "invite-error", payload: "Nincs online ilyen játékos." });
        }
        if (room.members.has(targetName)) {
          return send(ws, { type: "invite-error", payload: "Ez a játékos már a szobában van." });
        }
        if (room.type === "lan" && targetWs.ip !== room.adminIp) {
          return send(ws, { type: "invite-error", payload: "Ez a játékos nem a LAN-odon van." });
        }
        send(targetWs, { type: "invite", payload: { roomCode: room.code, from: ws.user.name, type: room.type } });
        send(ws, { type: "invite-sent", payload: { name: targetName } });
        break;
      }

      // Meghívás elfogadása
      case "invite-accept": {
        if (!ws.user) return;
        const code = String((data.payload && data.payload.roomCode) || "").toUpperCase();
        const room = rooms.get(code);
        if (!room) return send(ws, { type: "room-error", payload: "A szoba már nem létezik." });
        if (room.locked) return send(ws, { type: "room-error", payload: "A szoba zárva van." });
        if (room.members.has(ws.user.name)) return;
        let category = "Nézők";
        if (ws.user.device === "phone" && !activePlayer(room)) category = "Játékosok";
        room.members.set(ws.user.name, { ws, device: ws.user.device, category });
        persistRoom(room);
        ws.roomCode = code;
        send(ws, { type: "room-ok", payload: { code, type: room.type } });
        broadcastRoom(room, "room-state", roomState(room));
        pushRoomListToOwner(room);
        break;
      }

      // Kategória váltás (csak admin)
      case "move-member": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.adminWs !== ws) return;
        const name = normName(data.payload && data.payload.name);
        const category = (data.payload && data.payload.category) === "Nézők" ? "Nézők" : "Játékosok";
        const m = room.members.get(name);
        if (!m) return;
        m.category = category;
        persistRoom(room);
        broadcastRoom(room, "room-state", roomState(room));
        break;
      }

      // Kick (csak admin)
      case "kick": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.adminWs !== ws) return send(ws, { type: "invite-error", payload: "Csak a szoba adminja rúghat ki." });
        const name = normName(data.payload && data.payload.name);
        const m = room.members.get(name);
        if (!m) return send(ws, { type: "invite-error", payload: "Nincs ilyen tag a szobában." });
        if (name === ws.user.name) return send(ws, { type: "invite-error", payload: "Magadat nem rúghatod ki." });
        send(m.ws, { type: "kicked" });
        room.members.delete(name);
        persistRoom(room);
        broadcastRoom(room, "room-state", roomState(room));
        pushRoomListToOwner(room);
        break;
      }

      // Játék indítás (csak admin)
      case "start-game": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.adminWs !== ws) return;
        const active = activePlayer(room);
        if (!active) return send(ws, { type: "room-error", payload: "Nincs elérhető játékos a szobában." });
        room.inGame = true;
        broadcastRoom(room, "game-started", { activePlayer: active });
        break;
      }

      // ---------- WEBRTC JELZÉS ----------
      case "signal": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        const active = activePlayer(room);
        let target = null;
        if (room.adminWs === ws) {
          target = active ? room.members.get(active) : null;
          if (target) target = target.ws;
        } else if (active && ws.user.name === active) {
          target = room.adminWs;
        }
        if (target && target !== ws) send(target, { type: "signal", payload: data.payload });
        break;
      }

      // Parancs: admin -> aktív telefon
      case "command": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.adminWs !== ws) return;
        const active = activePlayer(room);
        if (active) {
          const m = room.members.get(active);
          send(m.ws, { type: "command", payload: data.payload });
        }
        break;
      }
    }
  });

  ws.on("close", () => {
    if (ws.user) {
      if (online.get(ws.user.name) === ws) online.delete(ws.user.name);
    }
    // Leválás minden szobáról (a szobák megmaradnak!)
    for (const [code, room] of rooms) {
      let touched = false;
      if (room.adminWs === ws) {
        room.adminWs = null;
        if (room.inGame) {
          room.inGame = false;
          broadcastRoom(room, "game-stopped", { reason: "admin-left" });
        }
        touched = true;
      }
      const m = room.members.get(ws.user ? ws.user.name : "");
      if (m && m.ws === ws) {
        m.ws = null;
        touched = true;
      }
      if (touched) {
        broadcastRoom(room, "room-state", roomState(room));
        pushRoomListToOwner(room);
      }
    }
    ws.roomCode = null;
  });
});

loadRooms();

server.listen(PORT, () => {
  console.log("==========================================");
  console.log("  Interaktív App - szerver fut");
  console.log("==========================================");
  console.log("");
  console.log("  A GEPro megnyitasa:  http://localhost:" + PORT);
  console.log("");
  console.log("  A TELEFONon (ugyanaz a wifi):");
  for (const ip of lanAddresses()) {
    console.log("    http://" + ip + ":" + PORT + "/phone.html");
  }
  console.log("==========================================");
});
