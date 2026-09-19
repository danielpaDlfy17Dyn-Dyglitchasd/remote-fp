const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
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
  const full = path.join(PUBLIC_DIR, file);

  if (!full.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end("Forbidden");
  }

  fs.readFile(full, (err, data) => {
    if (err) {
      res.writeHead(404);
      return res.end("Not found: " + file);
    }
    const ext = path.extname(full);
    res.writeHead(200, { "Content-Type": MIME[ext] || "text/plain" });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });

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

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

// --- Szobakezelés ---
// A gép létrehoz egy szobát (kód + jelszó), a telefon ezekkel lép be.
const rooms = new Map(); // code (uppercase) -> { pc, phone, pass }

function normCode(v) {
  return String(v || "").trim().toUpperCase().replace(/[^A-Z]/g, "");
}

wss.on("connection", (ws) => {
  ws.roomCode = null;

  ws.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (data.type) {
      // PC: szoba létrehozása
      case "create-room": {
        const code = normCode(data.payload && data.payload.code);
        const pass = String((data.payload && data.payload.pass) || "");
        if (code.length < 5) {
          return send(ws, { type: "room-error", payload: "A kód min. 5 betű legyen (angol abc, csak betűk)." });
        }
        if (!/^[0-9]{3,}$/.test(pass)) {
          return send(ws, { type: "room-error", payload: "A jelszó min. 3 szám legyen, és csak számokat tartalmazzon." });
        }
        if (rooms.has(code)) {
          return send(ws, { type: "room-error", payload: "Ez a kód már foglalt, válassz másikat." });
        }
        rooms.set(code, { pc: ws, phone: null, pass });
        ws.roomCode = code;
        send(ws, { type: "room-created", payload: { code } });
        break;
      }

      // Telefon: belépés
      case "join-room": {
        const code = normCode(data.payload && data.payload.code);
        const pass = String((data.payload && data.payload.pass) || "");
        if (code.length < 5) {
          return send(ws, { type: "room-error", payload: "A kód min. 5 betű legyen (angol abc)." });
        }
        if (!/^[0-9]{3,}$/.test(pass)) {
          return send(ws, { type: "room-error", payload: "A jelszó min. 3 szám legyen, csak számok." });
        }
        const room = rooms.get(code);
        if (!room || !room.pc) {
          return send(ws, { type: "room-error", payload: "Nincs ilyen szoba." });
        }
        if (room.pass && room.pass !== pass) {
          return send(ws, { type: "room-error", payload: "Hibás jelszó." });
        }
        if (room.phone) {
          return send(ws, { type: "room-error", payload: "Ehhez a szobához már csatlakozott egy telefon." });
        }
        room.phone = ws;
        ws.roomCode = code;
        send(ws, { type: "room-ok", payload: { code } });
        send(room.pc, { type: "phone-joined", payload: { code } });
        break;
      }

      // WebRTC jelzés a szobán belül
      case "signal": {
        const code = ws.roomCode;
        if (!code) return;
        const room = rooms.get(code);
        if (!room) return;
        const target = data.payload.to === "pc" ? room.pc : room.phone;
        if (target && target !== ws) send(target, { type: "signal", payload: data.payload });
        break;
      }

      // Parancs: PC -> telefon
      case "command": {
        const code = ws.roomCode;
        if (!code) return;
        const room = rooms.get(code);
        if (room && room.phone) send(room.phone, { type: "command", payload: data.payload });
        break;
      }
    }
  });

  ws.on("close", () => {
    if (ws.roomCode) {
      const room = rooms.get(ws.roomCode);
      if (room) {
        if (room.pc === ws) {
          if (room.phone) send(room.phone, { type: "peer-left" });
          rooms.delete(ws.roomCode);
        } else if (room.phone === ws) {
          room.phone = null;
          if (room.pc) send(room.pc, { type: "phone-left" });
        }
      }
    }
  });
});

server.listen(PORT, () => {
  console.log("==========================================");
  console.log("  Remote First-Person - LAN szerver fut");
  console.log("==========================================");
  console.log("");
  console.log("  A GEPro megnyitasa:  http://localhost:" + PORT);
  console.log("");
  console.log("  A TELEFONon (ugyanaz a wifi):");
  for (const ip of lanAddresses()) {
    console.log("    http://" + ip + ":" + PORT + "/phone.html");
  }
  console.log("");
  console.log("  A gep hozza letre a szobat (kod + jelszo),");
  console.log("  a telefon ezekkel lep be.");
  console.log("==========================================");
});