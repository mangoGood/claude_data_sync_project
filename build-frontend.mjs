#!/usr/bin/env node
/**
 * 控制台前端构建。
 *
 * 此前这个工程没有任何前端构建：admin-dashboard.js（375 KB）与四个 dashboard 模块
 * 未压缩直接下发，且 SockJS / STOMP / Chart.js 三个库从 cdn.jsdelivr.net 拉——
 * 既是供应链风险（CDN 被投毒等于在控制面执行任意脚本），也让产品无法离线/内网部署。
 *
 * 本脚本做两件事：
 *   1. vendor —— 把三个第三方库从 node_modules 拷到 static/vendor/，版本与原 CDN 一致
 *   2. bundle —— 用 esbuild 压缩五个 dashboard 文件到 static/js/
 *
 * <b>不改变运行时行为</b>：不做模块转换、不改 API、不 tree-shake 全局脚本。
 * 老的 `dashboard-*.js` 是 type="module"，admin-dashboard.js 是普通脚本，
 * 两种加载方式在产物里原样保持——构建只负责"更小"，不负责"更现代"。
 *
 * 用法:
 *   npm run build            构建（vendor + bundle）
 *   npm run vendor           只拷第三方库
 *   npm run build:watch      改动即重建
 */
import * as esbuild from 'esbuild';
import { copyFile, mkdir, readFile, writeFile, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';

const ROOT = path.dirname(new URL(import.meta.url).pathname);
const OUT_JS = path.join(ROOT, 'static', 'js');
const OUT_VENDOR = path.join(ROOT, 'static', 'vendor');

/**
 * 第三方库：源路径必须与原来的 CDN URL 一一对应，版本也要一致。
 * 落本地是为了去掉外部依赖，不是为了升级——升级是另一件事，会改运行时行为。
 */
const VENDOR = [
  { from: 'node_modules/sockjs-client/dist/sockjs.min.js', to: 'sockjs.min.js',
    cdn: 'https://cdn.jsdelivr.net/npm/sockjs-client@1/dist/sockjs.min.js' },
  { from: 'node_modules/stompjs/lib/stomp.min.js', to: 'stomp.min.js',
    cdn: 'https://cdn.jsdelivr.net/npm/stompjs@2.3.3/lib/stomp.min.js' },
  { from: 'node_modules/chart.js/dist/chart.umd.js', to: 'chart.umd.min.js',
    cdn: 'https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js' },
];

/**
 * 待压缩的入口。
 *
 * classic = 普通脚本（全局作用域，互相靠 window 通信）
 * module  = ES module（<script type="module">）
 *
 * 两类<b>分开产出、不合并</b>：admin-dashboard.js 里的函数是靠全局作用域被
 * dashboard-*.js 调到的（escapeHtml 就是），打进一个 bundle 会改变作用域语义。
 */
const ENTRIES = [
  { file: 'dashboard-ssl.js', kind: 'classic' },
  { file: 'admin-dashboard.js', kind: 'classic' },
  { file: 'dashboard-advanced.js', kind: 'module' },
  { file: 'dashboard-subscribe.js', kind: 'module' },
  { file: 'dashboard-dr.js', kind: 'module' },
];

const args = process.argv.slice(2);
const vendorOnly = args.includes('--vendor-only');
const watch = args.includes('--watch');

const fmtKB = (n) => (n / 1024).toFixed(1) + ' KB';

async function sizeOf(p) {
  try { return (await stat(p)).size; } catch { return 0; }
}

async function doVendor() {
  await mkdir(OUT_VENDOR, { recursive: true });
  console.log('第三方库 → static/vendor/');
  for (const v of VENDOR) {
    const src = path.join(ROOT, v.from);
    const dst = path.join(OUT_VENDOR, v.to);
    await copyFile(src, dst);
    const buf = await readFile(dst);
    // 记指纹：将来要核对"本地这份是不是被人动过"
    const sha = createHash('sha256').update(buf).digest('hex').slice(0, 16);
    console.log(`  ${v.to.padEnd(22)} ${fmtKB(buf.length).padStart(10)}  sha256:${sha}`);
  }
  // 留一份来源清单，说明每个文件从哪来、对应原来哪个 CDN URL
  const manifest = VENDOR.map((v) => ({ file: v.to, npm: v.from, replaces: v.cdn }));
  await writeFile(path.join(OUT_VENDOR, 'SOURCES.json'),
    JSON.stringify(manifest, null, 2) + '\n');
}

async function doBundle() {
  await mkdir(OUT_JS, { recursive: true });
  console.log('\n压缩 dashboard → static/js/');
  let before = 0, after = 0;
  for (const e of ENTRIES) {
    const src = path.join(ROOT, e.file);
    const out = path.join(OUT_JS, e.file);
    await esbuild.build({
      entryPoints: [src],
      outfile: out,
      bundle: false,          // 不打包：全局作用域的互相依赖靠 window，打包会改语义
      minify: true,
      sourcemap: true,        // 线上排障要能还原到源码行
      target: ['es2020'],
      format: e.kind === 'module' ? 'esm' : undefined,
      legalComments: 'none',
      charset: 'utf8',        // 不转义中文，否则体积反而变大
      logLevel: 'warning',
    });
    const b = await sizeOf(src), a = await sizeOf(out);
    before += b; after += a;
    console.log(`  ${e.file.padEnd(24)} ${fmtKB(b).padStart(10)} → ${fmtKB(a).padStart(10)}`
      + `  (-${(100 * (b - a) / b).toFixed(0)}%)`);
  }
  console.log(`  ${'合计'.padEnd(22)} ${fmtKB(before).padStart(10)} → ${fmtKB(after).padStart(10)}`
    + `  (-${(100 * (before - after) / before).toFixed(0)}%)`);
}

async function main() {
  await doVendor();
  if (!vendorOnly) {
    await doBundle();
  }
  console.log('\n产物：static/js/ 与 static/vendor/');
  console.log('页面通过 app.frontend.dist=true 切到产物（见 WebMvcConfig）；缺省仍用源文件。');
}

if (watch) {
  const chokidarLess = async () => { await main(); };
  await chokidarLess();
  console.log('\n[watch] 监听 dashboard 源文件…');
  const { watch: fsWatch } = await import('node:fs');
  for (const e of ENTRIES) {
    fsWatch(path.join(ROOT, e.file), { persistent: true }, async () => {
      try { await doBundle(); } catch (err) { console.error(err.message); }
    });
  }
} else {
  await main();
}
