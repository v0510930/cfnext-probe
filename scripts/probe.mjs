// CFNext 云端探针：由 GitHub Actions 定时运行
// 作用：在非 Cloudflare 的网络上真实探测候选 IP 能否完成 TLS 握手并返回 colo，
//       把"真正可用"的节点上报给 Worker（POST /ingest），由 Worker 写入面板 KV。
// 说明：地区标签取自榜单源；ms 是本探针到该 IP 的耗时，用于同地区内排序。

import https from 'node:https';

const DOMAIN = 'cf.edge8k.top';
const CONCURRENCY = 60;
const TIMEOUT = 6000;

const SOURCES = [
  { cc: 'HK', cn: '香港', limit: 261 },
  { cc: 'TW', cn: '台湾', limit: 26 },
  { cc: 'KR', cn: '韩国', limit: 67 },
  { cc: 'SG', cn: '新加坡', limit: 250 },
  { cc: 'JP', cn: '日本', limit: 250 },
  { cc: 'US', cn: '美国', limit: 450 },
];
const ALLOW_PORTS = new Set([443, 8443, 2053]);

const INGEST_URL = process.env.INGEST_URL;
const RUN_KEY = process.env.RUN_KEY;

const log = (...a) => console.log(new Date().toISOString().slice(0, 19).replace('T', ' '), ...a);

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function probe(ip, port) {
  return new Promise((resolve) => {
    const started = Date.now();
    let done = false;
    const finish = (v) => { if (!done) { done = true; resolve(v); } };
    let req;
    try {
      req = https.request({
        host: ip,
        port,
        servername: DOMAIN,
        path: '/cdn-cgi/trace',
        method: 'GET',
        headers: { Host: DOMAIN, 'User-Agent': 'cfnext-probe/1.0' },
        timeout: TIMEOUT,
        rejectUnauthorized: false,
        agent: false,
      }, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          if (res.statusCode !== 200) return finish(null);
          const body = Buffer.concat(chunks).toString('utf8');
          const colo = (body.match(/colo=([A-Z0-9]+)/) || [])[1];
          if (!colo) return finish(null);
          finish({ ip, port, colo, ms: Date.now() - started });
        });
      });
      req.on('timeout', () => { req.destroy(); finish(null); });
      req.on('error', () => finish(null));
      req.end();
    } catch {
      finish(null);
    }
  });
}

async function mapLimit(items, limit, fn) {
  const ret = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const idx = i++;
      if (idx >= items.length) break;
      ret[idx] = await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
  return ret;
}

async function main() {
  if (!INGEST_URL || !RUN_KEY) {
    log('缺少环境变量 INGEST_URL / RUN_KEY');
    process.exit(1);
  }

  // 1) 拉取各地区榜单（all.txt 扩池，单地区按 limit 随机抽样控制总耗时）
  const candidates = new Map(); // ip:port -> region
  for (const { cc, cn, limit } of SOURCES) {
    const url = `https://bestcf.pages.dev/random-region/${cc}/all.txt`;
    let text = '';
    try {
      const r = await fetch(url);
      if (!r.ok) { log(`源 ${cc}: HTTP ${r.status}`); continue; }
      text = await r.text();
    } catch (e) {
      log(`源 ${cc}: 拉取失败 ${e.message}`);
      continue;
    }
    const local = [];
    let skipped = 0;
    for (const line of text.split(/\r?\n/)) {
      if (!line.trim()) continue;
      const m = /(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})/.exec(line.split('|')[0]);
      if (!m) continue;
      if (!ALLOW_PORTS.has(parseInt(m[2], 10))) { skipped++; continue; }
      const k = `${m[1]}:${m[2]}`;
      if (candidates.has(k)) continue;
      local.push(k);
    }
    const picked = local.length > limit ? shuffle(local).slice(0, limit) : local;
    for (const k of picked) candidates.set(k, cc);
    log(`源 ${cc}(${cn}): 候选 ${local.length}，抽样 ${picked.length}，跳过非标端口 ${skipped}`);
  }

  const list = [...candidates.entries()].map(([k, cc]) => ({ key: k, region: cc }));
  log(`候选合计 ${list.length}，开始探活（并发 ${CONCURRENCY}）`);

  // 2) 真实探活
  let n = 0;
  const results = (await mapLimit(list, CONCURRENCY, async (c) => {
    const [ip, port] = c.key.split(':');
    const r = await probe(ip, parseInt(port, 10));
    if (++n % 200 === 0) log(`  已测 ${n}/${list.length}`);
    return r ? { ...r, region: c.region, ok: true } : null;
  })).filter(Boolean);

  log(`可用 ${results.length} / ${list.length}`);

  const dist = {};
  for (const r of results) dist[r.region] = (dist[r.region] || 0) + 1;
  log(`可用分布: ${Object.entries(dist).map(([k, v]) => `${k}×${v}`).join(' ')}`);

  if (results.length < 20) {
    log('可用节点过少，本次不上报');
    process.exit(1);
  }

  // 3) 上报给 Worker；Worker 将其保存为境外兜底池
  const url = `${INGEST_URL.replace(/\/+$/, '')}/ingest?key=${encodeURIComponent(RUN_KEY)}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ src: 'gh', items: results }),
  });
  const text = await res.text();
  log(`上报结果 ${res.status}: ${text.slice(0, 500)}`);
  if (!res.ok) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
