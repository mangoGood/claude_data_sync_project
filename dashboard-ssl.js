/* ============================================================
 * 传输加密（SSL/TLS）配置面板 —— 同步 / 灾备 / 订阅三个向导共用
 *
 * 为什么做成共用模块而不是各写一份：三个向导的第 1 步都是"填连接信息"，
 * 加密档位与证书选择是同一件事。各写一份的结果是三处慢慢长歪——
 * 而"某一类任务的 SSL 选项少了一档"这种差异没人会主动去比对。
 *
 * 用法（每个向导的源/目标各一处）：
 *   HTML:  <div id="cfgSourceSslPanel"></div>
 *   初始化: sslRenderPanel('cfgSource', '源数据库')
 *   读配置: sslGetConfig('cfgSource')   →  { mode, certId }
 *   回填:   sslSetConfig('cfgSource', 'REQUIRED', 'uuid...')
 *
 * 本文件用普通 <script> 加载（非 module），因此函数挂在全局，
 * 与 admin-dashboard.js 的既有风格一致；dashboard-dr.js / dashboard-subscribe.js
 * 是 type="module"，它们通过 window.xxx 调用。
 * ============================================================ */

/**
 * 共享依赖。主脚本把 API 助手放在 `window.__dash` 里（IIFE 封装后它们不在全局作用域，
 * 直接裸用 fetchWithAuth 会 ReferenceError），dashboard-dr.js / dashboard-subscribe.js
 * 也是从这里取。做成函数而不是解构常量：本文件在主脚本之前加载，那时 __dash 还不存在。
 */
function sslDash() {
    return window.__dash || {};
}

function sslApiBase() {
    return sslDash().API_BASE_URL || (window.location.origin + '/api');
}

/**
 * 只带 Authorization 的请求头。
 *
 * <p>不能复用主脚本的 {@code getAuthHeaders()}：它带 {@code Content-Type: application/json}，
 * 而证书上传是 multipart —— 一旦把 Content-Type 写死，浏览器就不会生成 boundary，
 * 后端收到的是一个没有分隔符的 multipart 体，报错还指向"缺少 name 参数"。
 */
function sslAuthOnlyHeaders() {
    return { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') };
}

function sslFetch(path, options) {
    const opts = Object.assign({}, options);
    // 始终显式给 headers：fetchWithAuth 在 headers 缺省时会塞 application/json
    if (!opts.headers) {
        opts.headers = sslAuthOnlyHeaders();
    }
    const d = sslDash();
    if (typeof d.fetchWithAuth === 'function') {
        return d.fetchWithAuth(sslApiBase() + path, opts);
    }
    return fetch(sslApiBase() + path, opts);
}

/** 证书列表缓存。上传后失效重取；null = 尚未加载，[] = 加载过且确实没有。 */
let sslCertCache = null;
/** 上次加载证书列表的失败原因。非空时下拉框要**说出来**，不能装作"没有证书"。 */
let sslCertLoadError = null;

const SSL_MODES = [
    { value: 'DISABLED',        label: '不加密（明文）' },
    { value: 'PREFERRED',       label: '尽力加密（服务端不支持则明文）' },
    { value: 'REQUIRED',        label: '必须加密（不校验证书）' },
    { value: 'VERIFY_CA',       label: '必须加密 + 校验 CA' },
    { value: 'VERIFY_IDENTITY', label: '必须加密 + 校验 CA 与主机名' }
];

/** 需要证书的档位。REQUIRED 及以下只加密不校验，可以不传证书。 */
function sslModeNeedsCert(mode) {
    return mode === 'VERIFY_CA' || mode === 'VERIFY_IDENTITY';
}

/**
 * 加载证书列表。
 *
 * <p>失败时<b>记下原因</b>而不是静默当成空列表——"接口挂了"和"你还没传过证书"在界面上
 * 长得一模一样，但处置完全相反。这一条正是本次开发里真实踩到的：助手函数取不到时
 * 整个下拉框空着，看起来就像没上传过证书。
 */
async function sslLoadCertificates(force) {
    if (sslCertCache && !force) return sslCertCache;
    try {
        const resp = await sslFetch('/certificates', {});
        const result = await resp.json();
        if (result && result.success && Array.isArray(result.data)) {
            sslCertCache = result.data;
            sslCertLoadError = null;
        } else {
            sslCertCache = [];
            sslCertLoadError = (result && result.message) || ('HTTP ' + resp.status);
        }
    } catch (e) {
        console.error('加载证书列表失败', e);
        sslCertCache = [];
        sslCertLoadError = e.message || String(e);
    }
    return sslCertCache;
}

/**
 * 渲染一个加密配置面板到 `${idPrefix}SslPanel`。
 *
 * @param idPrefix 例如 'cfgSource' / 'cfgTarget' / 'drSource' / 'subSource'
 * @param label    展示用名称，例如 '源数据库'
 * @param opts     { kafka: true } → 目标端是 Kafka（订阅任务），文案换成 Kafka 的语汇
 */
function sslRenderPanel(idPrefix, label, opts) {
    const host = document.getElementById(idPrefix + 'SslPanel');
    if (!host) return;
    const isKafka = !!(opts && opts.kafka);

    const modes = isKafka
        ? [{ value: 'DISABLED', label: 'PLAINTEXT（不加密）' },
           { value: 'REQUIRED', label: 'SSL（加密，不校验证书）' },
           { value: 'VERIFY_CA', label: 'SSL + 校验 CA' },
           { value: 'VERIFY_IDENTITY', label: 'SSL + 校验 CA 与主机名' }]
        : SSL_MODES;

    host.innerHTML = `
        <div style="margin-top: 10px; padding: 10px 12px; background: #fafafa; border: 1px solid #f0f0f0; border-radius: 4px;">
            <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 13px; color: #333;">
                <input type="checkbox" id="${idPrefix}SslEnabled" onchange="sslOnToggle('${idPrefix}')">
                <span>启用 SSL/TLS 加密连接</span>
                <span style="font-size: 11px; color: #999;">（不勾选 = 明文，与当前行为一致）</span>
            </label>
            <div id="${idPrefix}SslBody" style="display: none; margin-top: 10px; padding-left: 22px;">
                <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
                    <label style="font-size: 13px; color: #333;">加密档位</label>
                    <select class="form-input" id="${idPrefix}SslMode" style="max-width: 280px; height: 32px;"
                            onchange="sslOnModeChange('${idPrefix}')">
                        ${modes.filter(m => m.value !== 'DISABLED')
                               .map(m => `<option value="${m.value}"${m.value === 'REQUIRED' ? ' selected' : ''}>${m.label}</option>`).join('')}
                    </select>
                    <label style="font-size: 13px; color: #333; margin-left: 8px;">证书</label>
                    <select class="form-input" id="${idPrefix}SslCertId" style="max-width: 240px; height: 32px;"
                            onchange="sslOnCertChange('${idPrefix}')">
                        <option value="">（不使用证书）</option>
                    </select>
                    <button type="button" class="btn-test" onclick="sslOpenUploadModal('${idPrefix}')">上传证书</button>
                </div>
                <div id="${idPrefix}SslHint" style="font-size: 11px; color: #999; margin-top: 6px;"></div>
            </div>
        </div>`;
    // 刻意不在这里拉证书列表：紧随其后的 sslSetConfig() 也要拉一次，两个异步调用会打架
    // ——先发的那个 keepValue 是空，若后完成就会把已回填的选中项清掉。
    // 统一交给 sslSetConfig（回填）与 sslOnToggle（用户勾选）两个入口，它们各自知道该选谁。
}

function sslOnToggle(idPrefix) {
    const on = document.getElementById(idPrefix + 'SslEnabled').checked;
    document.getElementById(idPrefix + 'SslBody').style.display = on ? 'block' : 'none';
    if (on) {
        sslRefreshCertOptions(idPrefix);
    }
    // 默认档位是 REQUIRED 而不是列表里第一项 PREFERRED——PREFERRED 在服务端不支持时会
    // **静默退回明文**，把它当默认值等于让"我开了加密"这句话不可靠。
    // 默认值靠 <option selected> 落实（select 永远有 value，靠 if(!sel.value) 兜不住）。
    sslOnModeChange(idPrefix);
    sslInvalidateConnTest(idPrefix);
}

function sslOnModeChange(idPrefix) {
    const hint = document.getElementById(idPrefix + 'SslHint');
    if (!hint) return;
    const enabled = document.getElementById(idPrefix + 'SslEnabled').checked;
    const mode = document.getElementById(idPrefix + 'SslMode').value;
    const certSel = document.getElementById(idPrefix + 'SslCertId');

    let msg = '';
    if (!enabled) {
        msg = '';
    } else if (mode === 'PREFERRED') {
        msg = '⚠ 服务端不支持 SSL 时会<b>自动退回明文且不报错</b>。需要确保加密请选"必须加密"及以上档位。';
    } else if (mode === 'REQUIRED') {
        msg = '连接必须加密，但不校验服务端证书（可防窃听，不防中间人）。无需上传证书。';
    } else if (mode === 'VERIFY_CA') {
        msg = '校验服务端证书由所选 CA 签发，必须选择证书。';
    } else if (mode === 'VERIFY_IDENTITY') {
        msg = '在校验 CA 之外，还要求证书的 CN/SAN 与所填主机名一致——'
            + '用 IP 连接而证书里只写了域名时会失败，这是预期行为。';
    }

    // Oracle 换 TLS 要换端口：不是加个参数的事，协议从 TCP 变成 TCPS，监听端口通常也变
    const portEl = document.getElementById(idPrefix + 'Port');
    const dbType = sslDbTypeOf(idPrefix);
    if (enabled && dbType === 'oracle' && portEl && portEl.value.trim() === '1521') {
        msg += (msg ? '<br>' : '') + '⚠ Oracle 启用 TLS 需要连 TCPS 监听端口（通常是 <b>2484</b>），当前填的是 1521。';
    }

    if (enabled && sslModeNeedsCert(mode) && certSel && !certSel.value) {
        msg += (msg ? '<br>' : '') + '<span style="color:#fa8c16;">该档位需要选择证书。</span>';
    }
    hint.innerHTML = msg;
    sslInvalidateConnTest(idPrefix);
}

function sslOnCertChange(idPrefix) {
    sslOnModeChange(idPrefix);
}

/** 改了加密配置后，之前那次"测试连接成功"就不作数了——它测的是另一套参数。 */
function sslInvalidateConnTest(idPrefix) {
    const m = idPrefix.match(/^(cfg|dr|sub)(Source|Target)$/);
    if (!m) return;
    try {
        if (m[1] === 'cfg' && typeof cfgOnConnectionFieldChange === 'function') {
            cfgOnConnectionFieldChange(m[2].toLowerCase());
        } else if (m[1] === 'sub' && typeof subOnConnectionFieldChange === 'function') {
            subOnConnectionFieldChange();
        } else if (m[1] === 'dr' && typeof drOnConnectionFieldChange === 'function') {
            drOnConnectionFieldChange(m[2].toLowerCase());
        }
    } catch (e) { /* 向导未打开时忽略 */ }
}

function sslDbTypeOf(idPrefix) {
    try {
        if (idPrefix === 'cfgSource') return typeof cfgSourceType !== 'undefined' ? cfgSourceType : '';
        if (idPrefix === 'cfgTarget') return typeof cfgTargetType !== 'undefined' ? cfgTargetType : '';
        const el = document.getElementById('subscribeSourceType');
        if (idPrefix === 'subSource' && el) return el.value;
    } catch (e) { /* ignore */ }
    return '';
}

async function sslRefreshCertOptions(idPrefix, keepValue) {
    const sel = document.getElementById(idPrefix + 'SslCertId');
    if (!sel) return;
    const current = keepValue !== undefined ? keepValue : sel.value;
    const certs = await sslLoadCertificates();
    if (sslCertLoadError) {
        // 加载失败要说出来。空下拉框看起来就是"还没传过证书"，会让人去重复上传。
        sel.innerHTML = `<option value="">（证书列表加载失败：${sslEscape(sslCertLoadError)}）</option>`;
        return;
    }
    sel.innerHTML = '<option value="">（不使用证书）</option>'
        + certs.map(c => {
            const flags = [];
            if (c.hasClientCert) flags.push('mTLS');
            if (c.expired) flags.push('已过期');
            else if (c.expiringSoon) flags.push('即将到期');
            const suffix = flags.length ? ` [${flags.join(' / ')}]` : '';
            return `<option value="${c.id}">${sslEscape(c.name)}${suffix}</option>`;
        }).join('');
    if (current) sel.value = current;
}

/** 读出面板配置。未启用时返回 DISABLED + 空证书（后端据此清掉引用）。 */
function sslGetConfig(idPrefix) {
    const enabledEl = document.getElementById(idPrefix + 'SslEnabled');
    if (!enabledEl || !enabledEl.checked) {
        return { mode: 'DISABLED', certId: '' };
    }
    return {
        mode: document.getElementById(idPrefix + 'SslMode').value || 'REQUIRED',
        certId: document.getElementById(idPrefix + 'SslCertId').value || ''
    };
}

/** 回填已保存的配置（打开配置页时用）。 */
async function sslSetConfig(idPrefix, mode, certId) {
    const enabledEl = document.getElementById(idPrefix + 'SslEnabled');
    if (!enabledEl) return;
    const on = !!mode && mode !== 'DISABLED';
    enabledEl.checked = on;
    document.getElementById(idPrefix + 'SslBody').style.display = on ? 'block' : 'none';
    if (on) {
        document.getElementById(idPrefix + 'SslMode').value = mode;
        await sslRefreshCertOptions(idPrefix, certId || '');
    }
    sslOnModeChange(idPrefix);
}

/**
 * 提交前校验：VERIFY_* 档位必须选证书。
 * @return 错误信息数组（空 = 通过）
 */
function sslValidate(idPrefix, label) {
    const cfg = sslGetConfig(idPrefix);
    if (sslModeNeedsCert(cfg.mode) && !cfg.certId) {
        return [`${label}选择了「${cfg.mode}」档位，必须选择证书`];
    }
    return [];
}

/* ==================== 上传证书 ==================== */

let sslUploadReturnPrefix = null;

function sslOpenUploadModal(idPrefix) {
    sslUploadReturnPrefix = idPrefix || null;
    let modal = document.getElementById('sslUploadModal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'sslUploadModal';
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-content" style="width: 620px;">
                <div class="modal-header">
                    <h3 class="modal-title">上传 TLS 证书</h3>
                    <span class="modal-close" onclick="sslCloseUploadModal()">&times;</span>
                </div>
                <div class="modal-body">
                    <div style="font-size: 12px; color: #999; margin-bottom: 12px; line-height: 1.6;">
                        证书一律上传 <b>PEM</b> 格式（<code>-----BEGIN CERTIFICATE-----</code> 开头）。
                        平台会自动转换成各数据库驱动各自需要的形态（MySQL/Kafka 要 PKCS12 信任库、
                        PostgreSQL 要 PEM 且私钥须为 PKCS8 DER），无需手工准备多种格式。<br>
                        私钥带口令时请先解密：<code>openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-plain.pem</code>
                    </div>
                    <div class="form-group">
                        <label class="form-label">证书名称 <span style="color:#f5222d;">*</span></label>
                        <input type="text" class="form-input" id="sslUploadName" placeholder="如：生产 MySQL 集群 CA" style="max-width: 400px;">
                    </div>
                    <div class="form-group">
                        <label class="form-label">CA 证书（ca.pem）</label>
                        <input type="file" id="sslUploadCa" accept=".pem,.crt,.cer,.txt">
                        <div style="font-size: 11px; color: #999; margin-top: 4px;">校验服务端证书用。VERIFY_CA / VERIFY_IDENTITY 档位必需。</div>
                    </div>
                    <div class="form-group">
                        <label class="form-label">客户端证书（可选，双向认证 mTLS）</label>
                        <input type="file" id="sslUploadClientCert" accept=".pem,.crt,.cer,.txt">
                    </div>
                    <div class="form-group">
                        <label class="form-label">客户端私钥（可选，与上一项成对提供）</label>
                        <input type="file" id="sslUploadClientKey" accept=".pem,.key,.txt">
                        <div style="font-size: 11px; color: #999; margin-top: 4px;">私钥仅用于建立连接，落库前会加密存放，任何接口都不会回显。</div>
                    </div>
                    <div id="sslUploadStatus" class="connection-status"></div>
                </div>
                <div class="modal-footer">
                    <button class="btn-cancel" onclick="sslCloseUploadModal()">取消</button>
                    <button class="btn-confirm" id="sslUploadBtn" onclick="sslDoUpload()">上传</button>
                </div>
            </div>`;
        document.body.appendChild(modal);
    }
    document.getElementById('sslUploadName').value = '';
    document.getElementById('sslUploadCa').value = '';
    document.getElementById('sslUploadClientCert').value = '';
    document.getElementById('sslUploadClientKey').value = '';
    const st = document.getElementById('sslUploadStatus');
    st.className = 'connection-status';
    st.textContent = '';
    modal.classList.add('show');
    modal.style.display = 'flex';
}

function sslCloseUploadModal() {
    const modal = document.getElementById('sslUploadModal');
    if (modal) {
        modal.classList.remove('show');
        modal.style.display = 'none';
    }
}

async function sslDoUpload() {
    const name = document.getElementById('sslUploadName').value.trim();
    const ca = document.getElementById('sslUploadCa').files[0];
    const clientCert = document.getElementById('sslUploadClientCert').files[0];
    const clientKey = document.getElementById('sslUploadClientKey').files[0];
    const status = document.getElementById('sslUploadStatus');
    const btn = document.getElementById('sslUploadBtn');

    if (!name) {
        status.className = 'connection-status error';
        status.textContent = '✗ 请填写证书名称';
        return;
    }
    if (!ca && !clientCert) {
        status.className = 'connection-status error';
        status.textContent = '✗ 至少要上传 CA 证书或客户端证书';
        return;
    }
    if ((clientCert && !clientKey) || (!clientCert && clientKey)) {
        status.className = 'connection-status error';
        status.textContent = '✗ 客户端证书与私钥必须成对上传';
        return;
    }

    const form = new FormData();
    form.append('name', name);
    if (ca) form.append('caCert', ca);
    if (clientCert) form.append('clientCert', clientCert);
    if (clientKey) form.append('clientKey', clientKey);

    btn.disabled = true;
    status.className = 'connection-status testing';
    status.textContent = '正在上传并解析证书...';
    try {
        // multipart 不能自己设 Content-Type：boundary 必须由浏览器生成。
        // 因此这里不传 headers，让 sslFetch 只补 Authorization。
        const resp = await sslFetch('/certificates', { method: 'POST', body: form });
        const result = await resp.json();
        btn.disabled = false;
        if (result && result.success) {
            status.className = 'connection-status success';
            status.textContent = '✓ 上传成功';
            await sslLoadCertificates(true);
            const newId = result.data && result.data.id;
            if (sslUploadReturnPrefix) {
                await sslRefreshCertOptions(sslUploadReturnPrefix, newId || '');
                sslOnModeChange(sslUploadReturnPrefix);
            }
            setTimeout(sslCloseUploadModal, 600);
        } else {
            status.className = 'connection-status error';
            status.innerHTML = '✗ ' + sslEscape(result && result.message ? result.message : '上传失败')
                .replace(/\n/g, '<br>');
        }
    } catch (e) {
        btn.disabled = false;
        status.className = 'connection-status error';
        status.textContent = '✗ 上传出错: ' + e.message;
    }
}

function sslEscape(s) {
    return String(s === null || s === undefined ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/**
 * 把测连返回的加密状态渲染成一行。
 *
 * <p>这行字是"确实加密了"的<b>唯一凭据</b>：它来自服务端（MySQL 的 Ssl_cipher /
 * PG 的 pg_stat_ssl），不是我们自己的配置回显。配置写了 REQUIRED 不等于连接真的加密了。
 */
function sslRenderTlsBadge(data) {
    if (!data || data.encrypted === null || data.encrypted === undefined) return '';
    if (data.encrypted === false) {
        return '<div style="font-size:11px;color:#fa8c16;margin-top:2px;">🔓 服务端确认：本次连接<b>未加密</b></div>';
    }
    const detail = [data.tlsVersion, data.tlsCipher].filter(Boolean).join(' / ');
    return `<div style="font-size:11px;color:#52c41a;margin-top:2px;">🔒 服务端确认已加密${detail ? '：' + sslEscape(detail) : ''}</div>`;
}
