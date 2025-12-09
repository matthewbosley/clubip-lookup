const snowflake = require("snowflake-sdk");

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

function exec(conn, sqlText, binds = []) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds,
      complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows || []))
    });
  });
}

function isIpv4(s) {
  return /^(\d{1,3})(\.\d{1,3}){3}$/.test(s);
}

module.exports = async function (context, req) {
  const q = (req.query.ip || "").trim();
  if (!q) {
    context.res = { status: 400, body: { error: "Missing required query param: ip" } };
    return;
  }

  // This endpoint is IPv4-only for now (no npm available on your box)
  // If someone enters a prefix (not full ip), we still try.
  let like3 = q;
  let like2 = q;

  if (isIpv4(q)) {
    const parts = q.split(".");
    like3 = `${parts[0]}.${parts[1]}.${parts[2]}.`; // e.g. 4.149.254.
    like2 = `${parts[0]}.${parts[1]}.`;            // e.g. 4.149.
  } else {
    like3 = q.endsWith(".") ? q : (q + ".");
    like2 = q;
  }

  const conn = openConn();
  try {
    await new Promise((resolve, reject) => conn.connect(err => (err ? reject(err) : resolve())));

    // Key piece: split_part(prefix,'/',1) lets us match rows like "4.149.254.68/30"
    // even without CIDR math libraries.
    const rows = await exec(
      conn,
      `
      select name, system_service, region, platform, prefix
      from CLUBIP.PUBLIC.AZURE_SERVICE_TAGS_PREFIXES
      where ip_version = 4
        and (
          split_part(prefix, '/', 1) = ?
          or split_part(prefix, '/', 1) like ? || '%'
          or split_part(prefix, '/', 1) like ? || '%'
        )
      order by name
      limit 50
      `,
      [q, like3, like2]
    );

    context.res = {
      status: 200,
      headers: { "content-type": "application/json" },
      body: {
        query: q,
        note: "Best-effort match (base IP string). Install Node later for exact CIDR containment (IP inside range).",
        matches: rows.map(r => ({
          name: r.NAME,
          system_service: r.SYSTEM_SERVICE,
          region: r.REGION,
          platform: r.PLATFORM,
          prefix: r.PREFIX
        }))
      }
    };
  } catch (e) {
    context.log.error("azurelookup failed", e);
    context.res = { status: 500, body: { error: "Server error querying Snowflake." } };
  } finally {
    try { conn.destroy(); } catch {}
  }
};
