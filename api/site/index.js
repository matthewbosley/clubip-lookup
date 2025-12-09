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

module.exports = async function (context, req) {
  const ip = (req.query.ip || "").trim();

  if (!ip) {
    context.res = { status: 400, body: { error: "Missing required query param: ip" } };
    return;
  }
  if (!isFullIpv4(ip)) {
    context.res = { status: 400, body: { error: "Provide a full IPv4 address (x.x.x.x) for site lookup." } };
    return;
  }

  const conn = openConn();
  try {
    await new Promise((resolve, reject) => conn.connect(err => (err ? reject(err) : resolve())));

    // Return matched_field + full record
    const rows = await exec(
      conn,
      `
      select
        i.matched_field,
        f.*
      from CLUBIP.PUBLIC.CLUBIP_IP_INDEX_V i
      join CLUBIP.PUBLIC.CLUBIP_FULL_V f
        on f.club_code = i.club_code
      where i.ip_token = ?
      order by i.matched_field
      limit 20
      `,
      [ip]
    );

    context.res = {
      status: 200,
      headers: { "content-type": "application/json" },
      body: {
        query: ip,
        matches: rows.map(r => ({
          matched_field: r.MATCHED_FIELD,
          club_code: r.CLUB_CODE,
          club_name: r.CLUB_NAME_CLEAN,
          club_display: r.CLUB_DISPLAY,

          wan_ip: r.WAN_IP,
          subnet: r.SUBNET,
          gateway_ip: r.GATEWAY_IP,

          internal_lan: r.INTERNAL_LAN,
          mgmnt_wlan: r.MGMNT_WLAN,
          internal_wlan: r.INTERNAL_WLAN,
          public_wifi: r.PUBLIC_WIFI,
          voip: r.VOIP,
          ip_cameras: r.IP_CAMERAS,

          firewall_model: r.FIREWALL_MODEL,
          ap_model: r.AP_MODEL,
          ap_count: r.AP_COUNT,

          remote_url: r.REMOTE_URL,
          dvr: r.DVR,
          unifi_controller: r.UNIFI_CONTROLLER,
          yeastar_pbx: r.YEASTAR_PBX,

          dns: r.DNS,
          isp: r.ISP,
          account: r.ACCOUNT,
          address: r.ADDRESS,
          brand: r.BRAND
        }))
      }
    };
  } catch (e) {
    context.log.error("site lookup failed", e);
    context.res = { status: 500, body: { error: "Server error querying Snowflake." } };
  } finally {
    try { conn.destroy(); } catch {}
  }
};
