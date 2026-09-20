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

  // Több helyen keressük: public mappában, majd a gyökérben
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

// ================= FIÓKOK =================
// Memóriában tárolva - szerver újraindításnál törlődnek.
const accounts = new Map(); // name -> pass
const tokens = new Map();   // token -> name
const online = new Map();   // name -> ws

// ================= SZOBÁK =================
const rooms = new Map(); // code -> { code, pass, type, admin, members: Map(name->{ws,device,category}), adminIp }

function normName(v) {
  return String(v || "").trim().toUpperCase().replace(/[^A-Z0-9_]/g, "");
}

function clientIp(req) {
  const xf = req.headers && req.headers["x-forwarded-for"];
  if (xf) return String(xf).split(",")[0].trim();
  return (req.socket && req.socket.remoteAddress) || "";
}

function roomState(room) {
  const members = [];
  for (const [name, m] of room.members) {
    members.push({ name, device: m.device, category: m.category });
  }
  const active = activePlayer(room);
  return { code: room.code, type: room.type, members, activePlayer: active };
}

function activePlayer(room) {
  for (const [name, m] of room.members) {
    if (m.device === "phone" && m.category === "Játékosok") return name;
  }
  return null;
}

function broadcastRoom(room, type, payload) {
  for (const [, m] of room.members) send(m.ws, { type, payload });
}

function addMember(room, ws, device) {
  let category = "Nézők";
  if (device === "phone" && !activePlayer(room)) category = "Játékosok";
  room.members.set(ws.user.name, { ws, device, category });
  ws.roomCode = room.code;
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

wss.on("connection", (ws, req) => {
  ws.ip = clientIp(req);
  ws.user = null;
  ws.roomCode = null;

  ws.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (data.type) {
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
        accounts.set(name, pass);
        const token = crypto.randomUUID();
        tokens.set(token, name);
        online.set(name, ws);
        ws.user = { name, device: (data.payload && data.payload.device) || "pc" };
        send(ws, { type: "register-ok", payload: { name, token } });
        break;
      }

      case "login": {
        const name = normName(data.payload && data.payload.name);
        const pass = String((data.payload && data.payload.pass) || "");
        if (!accounts.has(name) || accounts.get(name) !== pass) {
          return send(ws, { type: "login-error", payload: "Hibás név vagy jelszó." });
        }
        const token = crypto.randomUUID();
        tokens.set(token, name);
        online.set(name, ws);
        ws.user = { name, device: (data.payload && data.payload.device) || "pc" };
        send(ws, { type: "login-ok", payload: { name, token } });
        break;
      }

      case "auth-token": {
        const token = String((data.payload && data.payload.token) || "");
        const name = tokens.get(token);
        if (!name) {
          return send(ws, { type: "auth-error" });
        }
        online.set(name, ws);
        ws.user = { name, device: (data.payload && data.payload.device) || "pc" };
        send(ws, { type: "auth-ok", payload: { name } });
        break;
      }

      // ---------- SZOBA ----------
      case "create-room": {
        if (!ws.user) return send(ws, { type: "room-error", payload: "Előbb jelentkezz be." });
        if (ws.roomCode) return send(ws, { type: "room-error", payload: "Már egy szobában vagy." });
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
        const room = {
          code, pass, type,
          admin: ws,
          adminIp: ws.ip,
          members: new Map(),
        };
        room.members.set(ws.user.name, { ws, device: ws.user.device, category: "Játékosok" });
        ws.roomCode = code;
        rooms.set(code, room);
        send(ws, { type: "room-created", payload: { code, type } });
        break;
      }

      case "join-room": {
        if (!ws.user) return send(ws, { type: "room-error", payload: "Előbb jelentkezz be." });
        if (ws.roomCode) return send(ws, { type: "room-error", payload: "Már egy szobában vagy." });
        const code = String((data.payload && data.payload.code) || "").trim().toUpperCase().replace(/[^A-Z]/g, "");
        const pass = String((data.payload && data.payload.pass) || "");
        const room = rooms.get(code);
        if (!room) return send(ws, { type: "room-error", payload: "Nincs ilyen szoba." });
        if (room.pass && room.pass !== pass) return send(ws, { type: "room-error", payload: "Hibás jelszó." });
        addMember(room, ws, ws.user.device);
        send(ws, { type: "room-ok", payload: { code, type: room.type } });
        broadcastRoom(room, "room-state", roomState(room));
        break;
      }

      // Meghívás (csak admin)
      case "invite": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.admin !== ws) return send(ws, { type: "invite-error", payload: "Csak a szoba adminja hívhat meg." });
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
        if (room.members.has(ws.user.name)) return;
        addMember(room, ws, ws.user.device);
        send(ws, { type: "room-ok", payload: { code, type: room.type } });
        broadcastRoom(room, "room-state", roomState(room));
        break;
      }

      // Kategória váltás (csak admin)
      case "move-member": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.admin !== ws) return;
        const name = normName(data.payload && data.payload.name);
        const category = (data.payload && data.payload.category) === "Nézők" ? "Nézők" : "Játékosok";
        const m = room.members.get(name);
        if (!m) return;
        m.category = category;
        broadcastRoom(room, "room-state", roomState(room));
        break;
      }

      // Kick (csak admin)
      case "kick": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.admin !== ws) return send(ws, { type: "invite-error", payload: "Csak a szoba adminja rúghat ki." });
        const name = normName(data.payload && data.payload.name);
        const m = room.members.get(name);
        if (!m) return send(ws, { type: "invite-error", payload: "Nincs ilyen tag a szobában." });
        if (name === ws.user.name) return send(ws, { type: "invite-error", payload: "Magadat nem rúghatod ki." });
        send(m.ws, { type: "kicked" });
        room.members.delete(name);
        broadcastRoom(room, "room-state", roomState(room));
        break;
      }

      // Játék indítás (csak admin)
      case "start-game": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.admin !== ws) return;
        const active = activePlayer(room);
        if (!active) return send(ws, { type: "room-error", payload: "Nincs játékos a szobában (telefon Nézők kategóriában van?)." });
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
        if (ws === room.admin) {
          target = active ? (room.members.get(active) || {}).ws : null;
        } else if (active && ws.user.name === active) {
          target = room.admin;
        }
        if (target && target !== ws) send(target, { type: "signal", payload: data.payload });
        break;
      }

      // Parancs: PC -> aktív telefon
      case "command": {
        if (!ws.user || !ws.roomCode) return;
        const room = rooms.get(ws.roomCode);
        if (!room || room.admin !== ws) return;
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
    if (ws.roomCode) {
      const room = rooms.get(ws.roomCode);
      if (room) {
        if (room.admin === ws) {
          // az admin elment -> szoba törlés
          for (const [, m] of room.members) {
            if (m.ws !== ws) send(m.ws, { type: "peer-left" });
          }
          rooms.delete(ws.roomCode);
        } else {
          room.members.delete(ws.user.name);
          broadcastRoom(room, "room-state", roomState(room));
        }
      }
    }
  });
});

server.listen(PORT, () => {
  console.log("==========================================");
  console.log("  Remote First-Person - szerver fut");
  console.log("==========================================");
  console.log("");
  console.log("  A GEPro megnyitasa:  http://localhost:" + PORT);
  console.log("");
  console.log("  A TELEFONon (ugyanaz a wifi):");
  for (const ip of lanAddresses()) {
    console.log("    http://" + ip + ":" + PORT + "/phone.html");
  }
  console.log("");
  console.log("  Fiókok memóriában tárolódnak (újraindításnál törlődnek).");
  console.log("==========================================");
});
