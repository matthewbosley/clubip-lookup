const snowflake = require("snowflake-sdk");

function isFullIPv4(s) {
  if (!/^(\d{1,3})(\.\d{1,3}){3}$/.test(s)) return false;
  return s.split(".").every(o => {
    const n = Number(o);
    return Number.isInteger(n) && n >= 0 && n <= 255;
  });
}

function isIPv4Prefix(s) {
  if (!/^(\d{1,3})(\.\d{1,3}){0,2}$/.test(s)) return false;
  return s.split(".").every(o => {
    const n = Number(o);
    return Number.isInteger(n) && n >= 0 && n <= 255;
  });
}

function openConn() {
  return snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
    role: process.env.SNOWFLAKE_ROLE
  });
}

function exec(conn, sqlText, binds) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds,
      complete: (err, _stmt, rows) => {
        if (err) return reject(err);
        resolve(rows || []);
      }
    });
  });
}

module.exports = async function (context, req) {
  const ip = (req.query.ip || "").trim();
  if (!ip) {
    context.res = { status: 400, body: { error: "Missing required query param: ip" } };
    return;
  }

  const isFull = isFullIPv4(ip);
  const isPrefix = !isFull && isIPv4Prefix(ip);
  if (!isFull && !isPrefix) {
    context.res = { status: 400, body: { error: "Enter a full IPv4 (a.b.c.d) or a prefix (a, a.b, a.b.c)." } };
    return;
  }

  const viewFqn = "CLUBIP.PUBLIC.CLUBIP_SEARCH_V";

  const sqlExact = `
    select CLUB_CODE, CLUB_NAME, WAN_IP_EXTRACTED
    from ${viewFqn}
    where WAN_IP_EXTRACTED = ?
    limit 25
  `;

  const sqlPrefix = `
    select CLUB_CODE, CLUB_NAME, WAN_IP_EXTRACTED
    from ${viewFqn}
    where WAN_IP_EXTRACTED like ?
    limit 25
  `;

  const bind = isFull ? ip : `${ip}.%`;
  const conn = openConn();

  try {
    await new Promise((resolve, reject) => conn.connect(err => (err ? reject(err) : resolve())));
    const rows = await exec(conn, isFull ? sqlExact : sqlPrefix, [bind]);

    context.res = {
      status: 200,
      headers: { "content-type": "application/json" },
      body: {
        query: ip,
        mode: isFull ? "exact" : "prefix",
        matches: rows.map(r => ({
          club_code: r.CLUB_CODE,
          club_name: r.CLUB_NAME,
          wan_ip: r.WAN_IP_EXTRACTED
        }))
      }
    };
  } catch (e) {
    context.log.error("Snowflake lookup failed", e);
    context.res = { status: 500, body: { error: "Server error querying Snowflake." } };
  } finally {
    try { conn.destroy(); } catch {}
  }
};
