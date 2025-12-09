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

function isFullIpv4(s) {
  if (!/^(\d{1,3})(\.\d{1,3}){3}$/.test(s)) return false;
  return s.split(".").every(o => {
    const n = Number(o);
    return Number.isInteger(n) && n >= 0 && n <= 255;
  });
}

function isIpv4Prefix(s) {
  // allow 1-3 octets: 4  or 4.145 or 4.145.74  (no trailing dot required)
  if (!/^(\d{1,3})(\.\d{1,3}){0,2}$/.test(s)) return false;
  return s.split(".").every(o => {
    const n = Number(o);
    return Number.isInteger(n) && n >= 0 && n <= 255;
  });
}

module.exports = async function (context, req) {
  const q = (req.query.ip || "").trim();
  if (!q) {
    context.res = { status: 400, body: { error: "Missing required query param: ip" } };
    return;
  }

  const conn = openConn();

  try {
    await new Promise((resolve, reject) => conn.connect(err => (err ? reject(err) : resolve())));

    let rows = [];

    if (isFullIpv4(q)) {
      // STRICT: only match prefixes whose base IP equals the searched IP
      rows = await exec(
        conn,
        `
        select name, system_service, region, platform, prefix
        from CLUBIP.PUBLIC.AZURE_SERVICE_TAGS_PREFIXES
        where ip_version = 4
          and split_part(prefix, '/', 1) = ?
        order by system_service, region, prefix
        limit 50
        `,
        [q]
      );
    } else if (isIpv4Prefix(q)) {
      // Prefix search mode (only when user did NOT provide a full IP)
      const like = q.endsWith(".") ? q : (q + ".");
      rows = await exec(
        conn,
        `
        select name, system_service, region, platform, prefix
        from CLUBIP.PUBLIC.AZURE_SERVICE_TAGS_PREFIXES
        where ip_version = 4
          and split_part(prefix, '/', 1) like ? || '%'
        order by system_service, region, prefix
        limit 50
        `,
        [like]
      );
    } else {
      context.res = { status: 400, body: { error: "Enter a valid IPv4 (x.x.x.x) or IPv4 prefix (x, x.x, x.x.x)" } };
      return;
    }

    context.res = {
      status: 200,
      headers: { "content-type": "application/json" },
      body: {
        query: q,
        mode: isFullIpv4(q) ? "exact-base-ip" : "prefix",
        note: "This is strict base-IP matching. True CIDR containment (IP inside range) requires Node/npm or a more complex SQL UDF.",
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
