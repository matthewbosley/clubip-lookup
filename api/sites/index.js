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
      complete: (err, _stmt, rows) => {
        if (err) return reject(err);
        resolve(rows || []);
      }
    });
  });
}

module.exports = async function (context, req) {
  const conn = openConn();
  const viewFqn = "CLUBIP.PUBLIC.CLUBIP_SEARCH_V";

  // Optional: ?q= to filter by club name/code/ip
  const q = (req.query.q || "").trim();

  const sql = q
    ? `
      select CLUB_CODE, CLUB_NAME, WAN_IP_EXTRACTED
      from ${viewFqn}
      where
        (CLUB_CODE ilike '%' || ? || '%')
        or (CLUB_NAME ilike '%' || ? || '%')
        or (WAN_IP_EXTRACTED ilike '%' || ? || '%')
      order by CLUB_CODE asc nulls last, CLUB_NAME asc
      limit 500
    `
    : `
      select CLUB_CODE, CLUB_NAME, WAN_IP_EXTRACTED
      from ${viewFqn}
      order by CLUB_CODE asc nulls last, CLUB_NAME asc
      limit 500
    `;

  const binds = q ? [q, q, q] : [];

  try {
    await new Promise((resolve, reject) => conn.connect(err => (err ? reject(err) : resolve())));
    const rows = await exec(conn, sql, binds);

    context.res = {
      status: 200,
      headers: { "content-type": "application/json" },
      body: {
        count: rows.length,
        sites: rows.map(r => ({
          club_code: r.CLUB_CODE,
          club_name: r.CLUB_NAME,
          wan_ip: r.WAN_IP_EXTRACTED
        }))
      }
    };
  } catch (e) {
    context.log.error("Sites query failed", e);
    context.res = { status: 500, body: { error: "Server error querying Snowflake." } };
  } finally {
    try { conn.destroy(); } catch {}
  }
};
