const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3210;
const ADMIN_PIN = process.env.ADMIN_PIN || "129921";

const DEFAULT_HOUSES = [
  { id: "dollar",  name: "บ้านดอลลาร์",  symbol: "$",  color: "#ef4444", balance: 0 },
  { id: "euro",    name: "บ้านยูโร",     symbol: "€",  color: "#f97316", balance: 0 },
  { id: "yen",     name: "บ้านเยน",      symbol: "¥",  color: "#facc15", balance: 0 },
  { id: "pound",   name: "บ้านปอนด์",    symbol: "£",  color: "#22c55e", balance: 0 },
  { id: "yuan",    name: "บ้านหยวน",     symbol: "元", color: "#f59e0b", balance: 0 },
  { id: "won",     name: "บ้านวอน",      symbol: "₩",  color: "#38bdf8", balance: 0 },
  { id: "rupiah",  name: "บ้านรูเปียห์", symbol: "Rp", color: "#ec4899", balance: 0 },
  { id: "bitcoin", name: "บ้านบิตคอยน์", symbol: "₿",  color: "#a855f7", balance: 0 }
];
// cosmetic fields always come from code, so color changes apply to existing data
const HOUSE_META = Object.fromEntries(DEFAULT_HOUSES.map(h => [h.id, h]));
function withMeta(h) {
  const m = HOUSE_META[h.id];
  return m ? { ...h, name: m.name, symbol: m.symbol, color: m.color } : h;
}

// ---------- storage: Postgres when DATABASE_URL is set, JSON file otherwise ----------
let store;

if (process.env.DATABASE_URL) {
  const { Pool } = require("pg");
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false }
  });

  async function initDb() {
    await pool.query(`CREATE TABLE IF NOT EXISTS houses (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      color TEXT NOT NULL,
      balance BIGINT NOT NULL DEFAULT 0,
      sort INT NOT NULL DEFAULT 0
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS history (
      id SERIAL PRIMARY KEY,
      house_id TEXT NOT NULL,
      house_name TEXT NOT NULL,
      amount BIGINT NOT NULL,
      time TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
    for (let i = 0; i < DEFAULT_HOUSES.length; i++) {
      const h = DEFAULT_HOUSES[i];
      await pool.query(
        `INSERT INTO houses (id, name, symbol, color, balance, sort)
         VALUES ($1, $2, $3, $4, 0, $5) ON CONFLICT (id) DO NOTHING`,
        [h.id, h.name, h.symbol, h.color, i]
      );
    }
  }

  store = {
    ready: initDb(),
    async getHouses() {
      const { rows } = await pool.query("SELECT id, name, symbol, color, balance::int FROM houses ORDER BY sort");
      return rows;
    },
    async getHistory() {
      const { rows } = await pool.query(
        "SELECT house_id AS \"houseId\", house_name AS \"houseName\", amount::int, time FROM history ORDER BY id DESC LIMIT 20"
      );
      return rows;
    },
    async addMoney(houseId, amount) {
      const { rows } = await pool.query(
        "UPDATE houses SET balance = GREATEST(0, balance + $2) WHERE id = $1 RETURNING id, name, symbol, color, balance::int",
        [houseId, amount]
      );
      if (!rows[0]) return null;
      await pool.query(
        "INSERT INTO history (house_id, house_name, amount) VALUES ($1, $2, $3)",
        [rows[0].id, rows[0].name, amount]
      );
      return rows[0];
    }
  };
  console.log("Storage: PostgreSQL");
} else {
  const DATA_FILE = path.join(__dirname, "data.json");
  function loadData() {
    try {
      return JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    } catch {
      const data = { houses: DEFAULT_HOUSES, history: [] };
      saveData(data);
      return data;
    }
  }
  function saveData(data) {
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2), "utf8");
  }

  store = {
    ready: Promise.resolve(),
    async getHouses() { return loadData().houses; },
    async getHistory() { return loadData().history.slice(-20).reverse(); },
    async addMoney(houseId, amount) {
      const data = loadData();
      const house = data.houses.find(h => h.id === houseId);
      if (!house) return null;
      house.balance = Math.max(0, house.balance + amount);
      data.history.push({
        houseId: house.id,
        houseName: house.name,
        amount,
        time: new Date().toISOString()
      });
      saveData(data);
      return house;
    }
  };
  console.log("Storage: JSON file (data.json)");
}

// ---------- app ----------
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Server-Sent Events for realtime updates
const sseClients = new Set();
app.get("/api/stream", (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive"
  });
  res.write("data: connected\n\n");
  sseClients.add(res);
  req.on("close", () => sseClients.delete(res));
});
function broadcast(payload) {
  const data = payload ? JSON.stringify(payload) : "update";
  for (const client of sseClients) client.write(`data: ${data}\n\n`);
}
setInterval(() => {
  for (const client of sseClients) client.write(": ping\n\n");
}, 25000);

app.get("/api/houses", async (req, res) => {
  try {
    const [houses, history] = await Promise.all([store.getHouses(), store.getHistory()]);
    res.json({ houses: houses.map(withMeta), history });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "เกิดข้อผิดพลาดฝั่งเซิร์ฟเวอร์" });
  }
});

app.post("/api/login", (req, res) => {
  if (req.body && req.body.pin === ADMIN_PIN) return res.json({ ok: true });
  res.status(401).json({ ok: false, error: "รหัสผ่านไม่ถูกต้อง" });
});

app.post("/api/add", async (req, res) => {
  const { pin, houseId, houseIds, amount } = req.body || {};
  if (pin !== ADMIN_PIN) return res.status(401).json({ ok: false, error: "ไม่ได้รับอนุญาต" });
  const amt = Number(amount);
  if (!Number.isFinite(amt) || amt === 0) {
    return res.status(400).json({ ok: false, error: "จำนวนเงินไม่ถูกต้อง" });
  }
  const ids = Array.isArray(houseIds) ? houseIds : [houseId];
  if (ids.length === 0 || !ids.every(id => HOUSE_META[id])) {
    return res.status(404).json({ ok: false, error: "ไม่พบบ้านนี้" });
  }
  try {
    const houses = [];
    for (const id of ids) {
      const house = await store.addMoney(id, amt);
      if (house) houses.push(withMeta(house));
    }
    broadcast({
      type: "update",
      amount: amt,
      changes: houses.map(h => ({ name: h.name, symbol: h.symbol, color: h.color }))
    });
    res.json({ ok: true, houses, house: houses[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "เกิดข้อผิดพลาดฝั่งเซิร์ฟเวอร์" });
  }
});

store.ready.then(() => {
  app.listen(PORT, () => console.log(`FinCamp running at http://localhost:${PORT}`));
}).catch(err => {
  console.error("Failed to initialize storage:", err);
  process.exit(1);
});
