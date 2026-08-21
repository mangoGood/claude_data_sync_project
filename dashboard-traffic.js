// dashboard-traffic.js —— 流量复制与回放独立 ES module（任务列表 / 创建向导 / 录制文件 / 回放报告）。
//   与 dashboard-dr.js / dashboard-subscribe.js 同一套约定：自有模块作用域；
//   共享依赖经 window.__dash 取得；onclick 与主脚本 switchPage 引用到的函数末尾挂 window。
const __dash = window.__dash;
const { API_BASE_URL, fetchWithAuth, showNotification, escapeHtml, escapeAttr,
        formatDateTime } = __dash;

let trafficCurrentPage = 1;
let trafficPageSize = 10;
let trafficTotalCount = 0;
let trafficAllTasks = [];
let trafficFilterStatus = null;
let trafficFilterTaskType = null;
let trafficKeyword = '';

/** 新建向导里选中的类型；配置弹窗按它切换两套表单。 */
let trafficPendingType = 'TRAFFIC_CAPTURE';
let trafficCurrentTaskId = null;
let trafficCurrentTaskType = null;
let trafficRecordings = [];
/** 回放任务的引擎：由所选录制决定，用户改不了（跨引擎回放是硬拦的）。 */
let trafficReplayEngine = 'mysql';

const trafficStatusMap = {
    'CONFIGURING':       { text: '配置中',   class: 'status-configuring', icon: '⚙' },
    'PENDING':           { text: '启动中',   class: 'status-pending', dot: true },
    'RECEIVED':          { text: '已接收',   class: 'status-pending', dot: true },
    'STARTING':          { text: '启动中',   class: 'status-pending', dot: true },
    'TRAFFIC_CAPTURING': { text: '复制中',   class: 'status-traffic-capturing', dot: true },
    'TRAFFIC_REPLAYING': { text: '回放中',   class: 'status-traffic-replaying', dot: true },
    'RECONNECTING':      { text: '重连中',   class: 'status-paused', dot: true },
    'COMPLETED':         { text: '已完成',   class: 'status-completed', icon: '✓' },
    'FAILED':            { text: '异常',     class: 'status-failed', icon: '✕' },
    'PAUSED':            { text: '已暂停',   class: 'status-paused', icon: '⏸' }
};

const TASK_TYPE_LABEL = {
    'TRAFFIC_CAPTURE': '流量复制',
    'TRAFFIC_REPLAY': '流量回放'
};

// ==================== 列表 ====================

async function fetchTrafficTasks() {
    const params = new URLSearchParams({
        page: trafficCurrentPage,
        pageSize: trafficPageSize,
        sortBy: 'created_at',
        sortDirection: 'DESC'
    });
    if (trafficKeyword) params.append('keyword', trafficKeyword);
    if (trafficFilterStatus) params.append('status', trafficFilterStatus);
    // 没选类型时要把两类都拉回来。后端的 taskType 是等值筛选，只能不传，
    // 拿回全部后在前端筛——否则"流量"页会混进同步/灾备任务。
    if (trafficFilterTaskType) params.append('taskType', trafficFilterTaskType);

    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/workflows?${params}`);
        const data = await res.json();
        if (!data.success) {
            showNotification(data.message || '获取流量任务失败', 'error');
            return;
        }
        let list = (data.data && data.data.list) || [];
        if (!trafficFilterTaskType) {
            list = list.filter(t => t.task_type === 'TRAFFIC_CAPTURE' || t.task_type === 'TRAFFIC_REPLAY');
        }
        trafficAllTasks = list;
        trafficTotalCount = trafficFilterTaskType ? (data.data.total || 0) : list.length;
        renderTrafficTable();
        loadPendingRestores();
    } catch (e) {
        showNotification('获取流量任务失败: ' + e.message, 'error');
    }
}

function renderTrafficTable() {
    const body = document.getElementById('trafficTableBody');
    const empty = document.getElementById('trafficEmptyState');
    if (!body) return;

    if (!trafficAllTasks.length) {
        body.innerHTML = '';
        if (empty) body.appendChild(empty);
        if (empty) empty.style.display = '';
        document.getElementById('trafficPaginationInfo').textContent = '总条数：0';
        return;
    }
    if (empty) empty.style.display = 'none';

    body.innerHTML = trafficAllTasks.map(t => {
        const st = trafficStatusMap[t.status] || { text: t.status, class: '' };
        const isCapture = t.task_type === 'TRAFFIC_CAPTURE';
        const db = isCapture ? (t.source_db_name || '-') : (t.target_db_name || '-');
        const progress = t.status === 'TRAFFIC_CAPTURING' ? '录制中'
            : (t.progress != null ? t.progress + '%' : '-');
        // 回放的"时间轴偏差"由 agent 用 rto_ms 这一栏承载（语义一致：相对计划的滞后）
        const skew = t.rto_ms != null ? formatSkew(t.rto_ms) : '-';
        return `
        <div class="table-row">
            <div class="table-cell col-name">
                <div class="task-name" title="${escapeAttr(t.name || '')}">${escapeHtml(t.name || '')}</div>
                <div class="task-id">${escapeHtml(t.id)}</div>
            </div>
            <div class="table-cell col-status">
                <span class="status-badge ${escapeAttr(st.class)}">${st.dot ? '<span class="status-dot"></span>' : (st.icon || '')} ${escapeHtml(st.text)}</span>
            </div>
            <div class="table-cell col-source">${escapeHtml(TASK_TYPE_LABEL[t.task_type] || t.task_type || '-')}</div>
            <div class="table-cell col-kafka">${escapeHtml(db)}</div>
            <div class="table-cell col-topic">${escapeHtml(progress)}</div>
            <div class="table-cell col-delay">${escapeHtml(skew)}</div>
            <div class="table-cell col-action">${actionsHtml(t)}</div>
        </div>`;
    }).join('');

    const from = (trafficCurrentPage - 1) * trafficPageSize + 1;
    const to = from + trafficAllTasks.length - 1;
    document.getElementById('trafficPaginationInfo').textContent =
        `总条数：${trafficTotalCount}（${from}-${to}）`;
}

function formatSkew(ms) {
    if (ms == null) return '-';
    if (ms < 1000) return ms + ' ms';
    return (ms / 1000).toFixed(1) + ' s';
}

function actionsHtml(t) {
    const id = escapeAttr(t.id);
    const acts = [];
    if (t.status === 'CONFIGURING') {
        acts.push(`<a href="javascript:;" onclick="trafficOpenConfig('${id}')">配置</a>`);
    }
    if (t.status === 'TRAFFIC_CAPTURING' || t.status === 'TRAFFIC_REPLAYING') {
        acts.push(`<a href="javascript:;" onclick="trafficStopTask('${id}')">停止</a>`);
    }
    if (t.task_type === 'TRAFFIC_CAPTURE'
            && ['COMPLETED', 'FAILED', 'PAUSED'].includes(t.status)) {
        acts.push(`<a href="javascript:;" onclick="trafficSyncRecording('${id}')">同步录制</a>`);
    }
    if (t.task_type === 'TRAFFIC_REPLAY' && ['COMPLETED', 'FAILED'].includes(t.status)) {
        acts.push(`<a href="javascript:;" onclick="trafficShowReport('${id}')">回放报告</a>`);
    }
    acts.push(`<a href="javascript:;" onclick="trafficDeleteTask('${id}')" style="color:#ff4d4f;">删除</a>`);
    return acts.join(' | ');
}

function trafficGoToPage(page) {
    if (page < 1) return;
    trafficCurrentPage = page;
    fetchTrafficTasks();
}

function trafficApplyKeywordAndRefresh() {
    const input = document.getElementById('trafficSearchInput');
    trafficKeyword = input ? input.value.trim() : '';
    trafficCurrentPage = 1;
    fetchTrafficTasks();
}

// ==================== 源库开关未还原告警 ====================

/**
 * 这是本功能最大的运维风险的出口：源库 general_log 没关掉会一直写日志表，直到磁盘满。
 * 平时这块是隐藏的，一旦有任务处于"未确认还原"就必须让人看见。
 */
async function loadPendingRestores() {
    const box = document.getElementById('trafficRestoreAlert');
    if (!box) return;
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/pending-source-restores`);
        const data = await res.json();
        const list = (data.data && data.data.list) || [];
        if (!list.length) {
            box.style.display = 'none';
            return;
        }
        box.style.display = '';
        box.innerHTML = '⚠ <b>以下任务的源库语句日志可能仍处于开启状态</b>，'
            + '不关闭会持续写入 mysql.general_log 直到源库磁盘写满，请人工确认：<br>'
            + list.map(x => `· ${escapeHtml(x.taskName || x.taskId)}（原值 general_log=${escapeHtml(String(x.originalGeneralLog))}，`
                + `log_output=${escapeHtml(String(x.originalLogOutput))}）`).join('<br>');
    } catch (e) {
        box.style.display = 'none';
    }
}

// ==================== 引擎 ====================

/**
 * 三种引擎的差异集中在这里。每一项都不是装饰：
 *   * 连接串 scheme 决定后端怎么建 JDBC 连接；
 *   * PG 的连接<b>终生绑定一个库</b>、Oracle 要服务名，所以它们必须填库名，MySQL 不用；
 *   * 捕获会改动的源端状态三家完全不同，告警文案必须跟着换 —— 这是本功能最大的运维风险，
 *     文案含糊等于让用户在不知情的情况下改了自己的生产库。
 */
const TRAFFIC_ENGINES = {
    mysql: {
        label: 'MySQL', scheme: 'mysql', port: '3306', needsDb: false, dbLabel: '数据库',
        warn: '<b>会改动源库参数。</b>捕获期间会把源库的 <code>general_log</code> 打开并把 '
            + '<code>log_output</code> 切到 TABLE，任务结束时自动还原。'
            + '高 QPS 的库上这会带来可观的写开销，请确认后再启动。'
    },
    postgresql: {
        label: 'PostgreSQL', scheme: 'postgresql', port: '5432', needsDb: true, dbLabel: '数据库',
        warn: '<b>会改动源库参数。</b>捕获期间会把 <code>log_statement</code> 打到 all、'
            + '<code>log_destination</code> 切到 jsonlog，任务结束时自动还原。'
            + '<br><b>前置条件：源库的 <code>logging_collector</code> 必须已经是 on</b> —— '
            + '它是 postmaster 参数，只能由 DBA 重启实例才能改；关着的时候语句日志不落文件，我们无处可读。'
            + '<br>另外 PG 会把 <code>PASSWORD \'…\'</code> 明文写进日志，捕获侧会在落盘前脱敏。'
    },
    oracle: {
        label: 'Oracle', scheme: 'oracle', port: '1521', needsDb: true, dbLabel: '服务名 / PDB',
        warn: '<b>会在源库上创建审计策略。</b>捕获期间会建两条统一审计策略（一条 ACTIONS ALL、'
            + '一条针对所选 schema 的对象级 SELECT），任务结束时自动停用并删除。'
            + '<br><b>多表 SELECT（join）只有对象级策略才抓得到</b>，所以请在「库白名单」里填上要录的 schema；'
            + '而且它只覆盖策略创建时<b>已经存在</b>的表。'
            + '<br>审计记录堆在 AUDSYS（默认在 SYSAUX），任务会按已消费位点定期清理。'
    }
};

function trafficEngine(name) {
    return TRAFFIC_ENGINES[name] || TRAFFIC_ENGINES.mysql;
}

/** 源库引擎变化：端口占位、库名可见性、告警文案一起跟上。 */
function onTrafficSrcEngineChange() {
    const eng = trafficEngine(val('trafficSrcEngine'));
    const portEl = document.getElementById('trafficSrcPort');
    portEl.placeholder = eng.port;
    if (!portEl.value || Object.values(TRAFFIC_ENGINES).some(e => e.port === portEl.value)) {
        portEl.value = eng.port;
    }
    document.getElementById('trafficSrcDbGroup').style.display = eng.needsDb ? '' : 'none';
    document.getElementById('trafficSrcDbLabel').textContent = eng.dbLabel;
    document.getElementById('trafficCaptureWarn').innerHTML = eng.warn;
}

// ==================== 创建向导 ====================

function openTrafficCreate() {
    trafficPendingType = 'TRAFFIC_CAPTURE';
    document.getElementById('trafficTaskName').value = '';
    document.querySelectorAll('.traffic-type-card').forEach(el => {
        el.classList.toggle('selected', el.dataset.trafficType === 'TRAFFIC_CAPTURE');
        el.style.borderColor = el.dataset.trafficType === 'TRAFFIC_CAPTURE' ? '#1890ff' : '#e8e8e8';
    });
    document.getElementById('trafficCreateModal').classList.add('show');
}

function closeTrafficCreate() {
    document.getElementById('trafficCreateModal').classList.remove('show');
}

async function trafficCreateConfirm() {
    const name = document.getElementById('trafficTaskName').value.trim();
    if (!name) {
        showNotification('请填写任务名称', 'warning');
        return;
    }
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/workflows`, {
            method: 'POST',
            body: JSON.stringify({
                name, sourceType: 'mysql', targetType: 'mysql', taskType: trafficPendingType
            })
        });
        const data = await res.json();
        if (!data.success) {
            showNotification(data.message || '创建失败', 'error');
            return;
        }
        closeTrafficCreate();
        showNotification('任务创建成功', 'success');
        await fetchTrafficTasks();
        trafficOpenConfig(data.data.id, trafficPendingType);
    } catch (e) {
        showNotification('创建失败: ' + e.message, 'error');
    }
}

// ==================== 配置 ====================

async function trafficOpenConfig(taskId, taskTypeHint) {
    trafficCurrentTaskId = taskId;
    const task = trafficAllTasks.find(t => t.id === taskId);
    trafficCurrentTaskType = taskTypeHint || (task && task.task_type) || 'TRAFFIC_CAPTURE';

    const isCapture = trafficCurrentTaskType === 'TRAFFIC_CAPTURE';
    document.getElementById('trafficConfigTitle').textContent =
        isCapture ? '流量复制配置' : '流量回放配置';
    document.getElementById('trafficCaptureSection').style.display = isCapture ? '' : 'none';
    document.getElementById('trafficReplaySection').style.display = isCapture ? 'none' : '';

    if (isCapture) {
        onTrafficSrcEngineChange();
    } else {
        await loadRecordingOptions();
    }
    document.getElementById('trafficConfigModal').classList.add('show');
}

function closeTrafficConfig() {
    document.getElementById('trafficConfigModal').classList.remove('show');
}

async function loadRecordingOptions() {
    const sel = document.getElementById('trafficRecordingSelect');
    sel.innerHTML = '<option value="">加载中…</option>';
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/recordings?sealedOnly=true`);
        const data = await res.json();
        trafficRecordings = (data.data && data.data.list) || [];
        if (!trafficRecordings.length) {
            sel.innerHTML = '<option value="">（还没有已封口的录制，请先跑一个流量复制任务并停止它）</option>';
            return;
        }
        sel.innerHTML = trafficRecordings.map(r =>
            `<option value="${escapeAttr(r.id)}">[${escapeHtml(trafficEngine(r.engine).label)}] ${escapeHtml(r.name)} — ${r.recordCount} 条 / ${fmtBytes(r.byteSize)} / ${fmtDuration(r.durationMs)}</option>`
        ).join('');
        renderRecordingInfo();
        sel.onchange = renderRecordingInfo;
    } catch (e) {
        sel.innerHTML = '<option value="">加载失败</option>';
    }
}

function renderRecordingInfo() {
    const sel = document.getElementById('trafficRecordingSelect');
    const rec = trafficRecordings.find(r => r.id === sel.value);
    const box = document.getElementById('trafficRecordingInfo');
    if (!rec) { box.textContent = ''; return; }
    const src = rec.source || {};
    const stats = rec.stats || {};

    // 目标库引擎由录制决定：跨引擎回放会被后端硬拦（E3131），
    // 这里直接把表单跟着切过去，免得用户填完一屏才被拒
    trafficReplayEngine = rec.engine || 'mysql';
    const eng = trafficEngine(trafficReplayEngine);
    document.getElementById('trafficTgtEngineLabel').textContent = eng.label;
    const tgtPort = document.getElementById('trafficTgtPort');
    tgtPort.placeholder = eng.port;
    if (!tgtPort.value || Object.values(TRAFFIC_ENGINES).some(e => e.port === tgtPort.value)) {
        tgtPort.value = eng.port;
    }
    document.getElementById('trafficTgtDbGroup').style.display = eng.needsDb ? '' : 'none';
    document.getElementById('trafficTgtDbLabel').textContent = eng.dbLabel;

    let html = `录制窗口：${formatDateTime(rec.t0Wall)} ~ ${formatDateTime(rec.endWall)}　`
        + `源库：${escapeHtml(eng.label)} ${escapeHtml(shortVersion(src.version))}　`
        + `通道：${escapeHtml(rec.captureBackend || '-')}　`
        + `语句：查询 ${stats.select || 0} / DML ${stats.dml || 0} / DDL ${stats.ddl || 0} / DCL ${stats.dcl || 0}`;
    if (rec.gapCount > 0) {
        html += `<br><span class="traffic-gap-badge">时间轴空洞 ${rec.gapCount} 处</span>`
            + ' —— 捕获中断过，那几段时间源库执行的语句已永久丢失，回放时会按原时长静默等待。';
    }
    if (stats.redacted > 0) {
        html += `<br>其中 ${stats.redacted} 条语句的口令已被抹除（数据库自己抹的或捕获侧脱敏），无法回放，会记进报告。`;
    }
    box.innerHTML = html;
}

/** PG/Oracle 的 version() 是一长串，列表里只取前 60 个字符。 */
function shortVersion(v) {
    if (!v) return '?';
    return v.length > 60 ? v.slice(0, 60) + '…' : v;
}

function connString(engine, host, port, user, pass, db) {
    const eng = trafficEngine(engine);
    const tail = eng.needsDb && db ? `/${db}` : '';
    return `${eng.scheme}://${user}:${pass}@${host}:${port}${tail}`;
}

async function trafficSaveAndLaunch() {
    if (!trafficCurrentTaskId) return;
    const isCapture = trafficCurrentTaskType === 'TRAFFIC_CAPTURE';
    const cfgBody = {};
    const engineName = isCapture ? (val('trafficSrcEngine') || 'mysql') : trafficReplayEngine;
    // 复制任务两端都记成源引擎、回放任务两端都记成录制引擎：
    // 流量任务只有一侧是真的，另一侧留着是为了复用既有的 workflows 表与校验
    const wfBody = { sourceType: engineName, targetType: engineName };

    if (isCapture) {
        const host = val('trafficSrcHost'), port = val('trafficSrcPort');
        const user = val('trafficSrcUser'), pass = val('trafficSrcPass');
        const db = val('trafficSrcDb');
        if (!host || !port || !user) { showNotification('请填写完整的源库连接信息', 'warning'); return; }
        if (trafficEngine(engineName).needsDb && !db) {
            showNotification('请填写' + trafficEngine(engineName).dbLabel, 'warning');
            return;
        }
        wfBody.sourceConnection = connString(engineName, host, port, user, pass, db);
        if (db) wfBody.sourceDbName = db;
        wfBody.migrationMode = 'trafficCapture';
        const dbs = val('trafficCaptureDbs');
        if (dbs) wfBody.sourceDbName = dbs.split(',')[0].trim();
        cfgBody.captureDatabases = dbs;
        cfgBody.captureClasses = checkedValues('.traffic-cap-class').join(',');
        cfgBody.captureMaxDurationMs = Math.max(1, num('trafficMaxMinutes', 120)) * 60000;
        cfgBody.captureMaxBytes = Math.max(1, num('trafficMaxGb', 20)) * 1024 * 1024 * 1024;
        cfgBody.captureSampleRate = num('trafficSampleRate', 1);
    } else {
        const host = val('trafficTgtHost'), port = val('trafficTgtPort');
        const user = val('trafficTgtUser'), pass = val('trafficTgtPass');
        const db = val('trafficTgtDb');
        const recId = document.getElementById('trafficRecordingSelect').value;
        if (!recId) { showNotification('请选择要回放的录制文件', 'warning'); return; }
        if (!host || !port || !user) { showNotification('请填写完整的目标库连接信息', 'warning'); return; }
        if (trafficEngine(engineName).needsDb && !db) {
            showNotification('请填写' + trafficEngine(engineName).dbLabel, 'warning');
            return;
        }
        wfBody.targetConnection = connString(engineName, host, port, user, pass, db);
        if (db) wfBody.targetDbName = db;
        wfBody.migrationMode = 'trafficReplay';
        cfgBody.replayRecordingId = recId;
        cfgBody.replaySpeed = num('trafficReplaySpeed', 1);
        cfgBody.replayMaxSessions = num('trafficMaxSessions', 200);
        cfgBody.replayLagPolicy = val('trafficLagPolicy');
        cfgBody.replayGapPolicy = val('trafficGapPolicy');
        cfgBody.replayClasses = checkedValues('.traffic-rep-class').join(',');
        cfgBody.replayAllowDcl = document.getElementById('trafficAllowDcl').checked;
        cfgBody.replayAllowDangerous = document.getElementById('trafficAllowDangerous').checked;
    }

    try {
        let res = await fetchWithAuth(`${API_BASE_URL}/workflows/${trafficCurrentTaskId}/config`, {
            method: 'PUT', body: JSON.stringify(wfBody)
        });
        let data = await res.json();
        if (!data.success) { showNotification(data.message || '保存连接失败', 'error'); return; }

        res = await fetchWithAuth(`${API_BASE_URL}/traffic/config/${trafficCurrentTaskId}`, {
            method: 'PUT', body: JSON.stringify(cfgBody)
        });
        data = await res.json();
        if (!data.success) { showNotification(data.message || '保存流量配置失败', 'error'); return; }

        res = await fetchWithAuth(`${API_BASE_URL}/workflows/${trafficCurrentTaskId}/launch`, { method: 'POST' });
        data = await res.json();
        if (!data.success) { showNotification(data.message || '启动失败', 'error'); return; }

        closeTrafficConfig();
        showNotification('任务已启动', 'success');
        fetchTrafficTasks();
    } catch (e) {
        showNotification('操作失败: ' + e.message, 'error');
    }
}

// ==================== 任务操作 ====================

async function trafficStopTask(taskId) {
    if (!confirm('确定停止该任务？流量复制停止后录制文件会被封口。')) return;
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}/stop`, { method: 'POST' });
        const data = await res.json();
        showNotification(data.message || (data.success ? '已停止' : '停止失败'),
            data.success ? 'success' : 'error');
        // 复制任务停止后录制才封口，顺手把元数据同步回来，用户不必再点一次
        const task = trafficAllTasks.find(t => t.id === taskId);
        if (data.success && task && task.task_type === 'TRAFFIC_CAPTURE') {
            setTimeout(() => trafficSyncRecording(taskId, true), 6000);
        }
        setTimeout(fetchTrafficTasks, 2000);
    } catch (e) {
        showNotification('停止失败: ' + e.message, 'error');
    }
}

async function trafficDeleteTask(taskId) {
    if (!confirm('确定删除该任务？（录制文件不会一并删除，可在「录制文件」里单独管理）')) return;
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}`, { method: 'DELETE' });
        const data = await res.json();
        showNotification(data.message || (data.success ? '已删除' : '删除失败'),
            data.success ? 'success' : 'error');
        fetchTrafficTasks();
    } catch (e) {
        showNotification('删除失败: ' + e.message, 'error');
    }
}

async function trafficSyncRecording(taskId, quiet) {
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/recordings/sync/${taskId}`, { method: 'POST' });
        const data = await res.json();
        if (!quiet) {
            showNotification(data.message || (data.success ? '已同步' : '同步失败'),
                data.success ? 'success' : 'error');
        }
    } catch (e) {
        if (!quiet) showNotification('同步失败: ' + e.message, 'error');
    }
}

// ==================== 录制文件 ====================

async function openTrafficRecordings() {
    const box = document.getElementById('trafficRecordingsList');
    box.innerHTML = '加载中…';
    document.getElementById('trafficRecordingsModal').classList.add('show');
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/recordings`);
        const data = await res.json();
        const list = (data.data && data.data.list) || [];
        if (!list.length) { box.innerHTML = '<div style="color:#999;">暂无录制文件</div>'; return; }
        box.innerHTML = `<table style="width:100%; border-collapse:collapse; font-size:13px;">
            <thead><tr style="background:#fafafa;">
              <th style="text-align:left;padding:8px;">名称</th>
              <th style="text-align:center;padding:8px;">引擎</th>
              <th style="text-align:left;padding:8px;">录制窗口</th>
              <th style="text-align:right;padding:8px;">条数</th>
              <th style="text-align:right;padding:8px;">体积</th>
              <th style="text-align:center;padding:8px;">状态</th>
              <th style="text-align:center;padding:8px;">操作</th>
            </tr></thead><tbody>${list.map(r => `
              <tr style="border-top:1px solid #f0f0f0;">
                <td style="padding:8px;">${escapeHtml(r.name)}
                    ${r.gapCount > 0 ? ` <span class="traffic-gap-badge">空洞 ${r.gapCount}</span>` : ''}</td>
                <td style="padding:8px;text-align:center;">${escapeHtml(trafficEngine(r.engine).label)}
                    <br><span style="color:#999;font-size:11px;">${escapeHtml(r.captureBackend || '-')}</span></td>
                <td style="padding:8px;">${formatDateTime(r.t0Wall)}<br><span style="color:#999;">时长 ${fmtDuration(r.durationMs)}</span></td>
                <td style="padding:8px;text-align:right;">${r.recordCount}</td>
                <td style="padding:8px;text-align:right;">${fmtBytes(r.byteSize)}</td>
                <td style="padding:8px;text-align:center;">${r.sealed ? '<span style="color:#52c41a;">已封口</span>' : '<span style="color:#faad14;">未封口</span>'}</td>
                <td style="padding:8px;text-align:center;">
                    <a href="javascript:;" onclick="trafficDownloadRecording('${escapeAttr(r.id)}')">下载</a> |
                    <a href="javascript:;" onclick="trafficDeleteRecording('${escapeAttr(r.id)}')" style="color:#ff4d4f;">删除</a>
                </td>
              </tr>`).join('')}</tbody></table>`;
    } catch (e) {
        box.innerHTML = '<div style="color:#ff4d4f;">加载失败: ' + escapeHtml(e.message) + '</div>';
    }
}

/**
 * 下载录制。走 fetch 拿 blob 而不是直接 window.open：
 * 下载接口要 Authorization 头，裸链接带不上，会 401。
 */
async function trafficDownloadRecording(id) {
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/recordings/${id}/download`);
        if (!res.ok) {
            showNotification('下载失败（HTTP ' + res.status + '）', 'error');
            return;
        }
        const blob = await res.blob();
        const disp = res.headers.get('Content-Disposition') || '';
        const m = disp.match(/filename="?([^"]+)"?/);
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = m ? m[1] : ('recording-' + id.slice(0, 8) + '.trfz');
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(a.href);
    } catch (e) {
        showNotification('下载失败: ' + e.message, 'error');
    }
}

async function trafficDeleteRecording(id) {
    if (!confirm('确定删除这份录制？删除后引用它的回放任务将无法启动。')) return;
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/recordings/${id}`, { method: 'DELETE' });
        const data = await res.json();
        showNotification(data.message || (data.success ? '已删除' : '删除失败'),
            data.success ? 'success' : 'error');
        openTrafficRecordings();
    } catch (e) {
        showNotification('删除失败: ' + e.message, 'error');
    }
}

// ==================== 回放报告 ====================

async function trafficShowReport(taskId) {
    const box = document.getElementById('trafficReportBody');
    box.innerHTML = '加载中…';
    document.getElementById('trafficReportModal').classList.add('show');
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/replay-report/${taskId}`);
        const data = await res.json();
        if (!data.success) {
            box.innerHTML = '<div style="color:#999;">' + escapeHtml(data.message || '报告尚未生成') + '</div>';
            return;
        }
        const r = data.data || {};
        box.innerHTML = `
          <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:16px;">
            ${statTile('结果', r.outcome || '-')}
            ${statTile('执行成功', r['count.OK'] || '0')}
            ${statTile('回放错误', r['count.REPLAY_ERROR'] || '0')}
            ${statTile('被拦截', r['count.BLOCKED'] || '0')}
          </div>
          <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:16px;">
            ${statTile('时间轴偏差 P50', (r['skew.p50Ms'] || '0') + ' ms')}
            ${statTile('P95', (r['skew.p95Ms'] || '0') + ' ms')}
            ${statTile('P99', (r['skew.p99Ms'] || '0') + ' ms')}
            ${statTile('总耗时', fmtDuration(Number(r.elapsedMs || 0)))}
          </div>
          <div style="font-size:12px; color:#888; margin-bottom:12px;">
            时间轴偏差 = 语句<b>真正开始执行</b>的时刻与录制里计划时刻之差。偏差持续变大说明目标库跟不上源库当时的节奏。
            ${r['count.UNREPLAYABLE_REDACTED'] && r['count.UNREPLAYABLE_REDACTED'] !== '0'
              ? `<br>另有 ${r['count.UNREPLAYABLE_REDACTED']} 条带口令的语句被 MySQL 抹掉，物理上无法回放。` : ''}
            ${r['count.SKIPPED_LATE'] && r['count.SKIPPED_LATE'] !== '0'
              ? `<br>有 ${r['count.SKIPPED_LATE']} 条因落后过多被丢弃（SKIP 档）。` : ''}
          </div>
          ${r.slowestSql ? `<div style="font-size:12px;">最慢语句（${Math.round(Number(r.slowestUs || 0) / 1000)} ms）：
             <pre style="background:#fafafa;padding:8px;border-radius:4px;white-space:pre-wrap;">${escapeHtml(r.slowestSql)}</pre></div>` : ''}
          <div id="trafficReportErrors" style="margin-top:12px;">加载错误明细…</div>`;
        loadReplayErrors(taskId);
    } catch (e) {
        box.innerHTML = '<div style="color:#ff4d4f;">加载失败: ' + escapeHtml(e.message) + '</div>';
    }
}

function statTile(label, value) {
    return `<div style="border:1px solid #f0f0f0;border-radius:6px;padding:10px 12px;">
        <div style="font-size:12px;color:#999;">${escapeHtml(label)}</div>
        <div style="font-size:20px;font-weight:600;margin-top:4px;">${escapeHtml(String(value))}</div></div>`;
}

async function loadReplayErrors(taskId) {
    const box = document.getElementById('trafficReportErrors');
    if (!box) return;
    try {
        const res = await fetchWithAuth(`${API_BASE_URL}/traffic/replay-errors/${taskId}?page=1&pageSize=50`);
        const data = await res.json();
        const recs = (data.data && data.data.records) || [];
        if (!recs.length) { box.innerHTML = '<div style="color:#52c41a;">没有需要关注的语句。</div>'; return; }
        box.innerHTML = `<div style="font-weight:600;margin-bottom:8px;">明细（前 ${recs.length} 条，共 ${data.data.total}）</div>
          <table style="width:100%;border-collapse:collapse;font-size:12px;">
            <thead><tr style="background:#fafafa;">
              <th style="text-align:left;padding:6px;">归宿</th>
              <th style="text-align:left;padding:6px;">语句</th>
              <th style="text-align:left;padding:6px;">说明</th></tr></thead>
            <tbody>${recs.map(x => `<tr style="border-top:1px solid #f0f0f0;">
              <td style="padding:6px;white-space:nowrap;">${escapeHtml(x.outcome || '')}</td>
              <td style="padding:6px;"><code>${escapeHtml((x.q || '').slice(0, 120))}</code></td>
              <td style="padding:6px;color:#888;">${escapeHtml((x.detail || '').slice(0, 120))}</td>
            </tr>`).join('')}</tbody></table>`;
    } catch (e) {
        box.innerHTML = '<div style="color:#999;">错误明细加载失败</div>';
    }
}

// ==================== 小工具 ====================

function val(id) {
    const el = document.getElementById(id);
    return el ? el.value.trim() : '';
}

function num(id, dflt) {
    const v = parseFloat(val(id));
    return isNaN(v) ? dflt : v;
}

function checkedValues(selector) {
    return Array.from(document.querySelectorAll(selector))
        .filter(el => el.checked).map(el => el.value);
}

function fmtBytes(n) {
    n = Number(n || 0);
    if (n < 1024) return n + ' B';
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
    if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MB';
    return (n / 1024 / 1024 / 1024).toFixed(2) + ' GB';
}

function fmtDuration(ms) {
    ms = Number(ms || 0);
    if (ms < 1000) return ms + ' ms';
    const s = Math.round(ms / 1000);
    if (s < 60) return s + ' 秒';
    const m = Math.floor(s / 60);
    if (m < 60) return m + ' 分 ' + (s % 60) + ' 秒';
    return Math.floor(m / 60) + ' 小时 ' + (m % 60) + ' 分';
}

// ==================== 事件绑定 ====================

function bindTrafficEvents() {
    const on = (id, ev, fn) => {
        const el = document.getElementById(id);
        if (el) el.addEventListener(ev, fn);
    };
    on('createTrafficTaskBtn', 'click', openTrafficCreate);
    on('trafficCreateClose', 'click', closeTrafficCreate);
    on('trafficCreateCancel', 'click', closeTrafficCreate);
    on('trafficCreateConfirm', 'click', trafficCreateConfirm);
    on('trafficConfigClose', 'click', closeTrafficConfig);
    on('trafficConfigCancel', 'click', closeTrafficConfig);
    on('trafficConfigSave', 'click', trafficSaveAndLaunch);
    on('trafficSrcEngine', 'change', onTrafficSrcEngineChange);
    on('trafficRecordingsBtn', 'click', openTrafficRecordings);
    on('trafficRecordingsClose', 'click',
        () => document.getElementById('trafficRecordingsModal').classList.remove('show'));
    on('trafficReportClose', 'click',
        () => document.getElementById('trafficReportModal').classList.remove('show'));

    document.querySelectorAll('.traffic-type-card').forEach(card => {
        card.addEventListener('click', () => {
            trafficPendingType = card.dataset.trafficType;
            document.querySelectorAll('.traffic-type-card').forEach(el => {
                const sel = el === card;
                el.classList.toggle('selected', sel);
                el.style.borderColor = sel ? '#1890ff' : '#e8e8e8';
            });
        });
    });

    const search = document.getElementById('trafficSearchInput');
    if (search) {
        search.addEventListener('keydown', e => {
            if (e.key === 'Enter') trafficApplyKeywordAndRefresh();
        });
    }
    const pageSize = document.getElementById('trafficPageSizeSelect');
    if (pageSize) {
        pageSize.addEventListener('change', () => {
            trafficPageSize = parseInt(pageSize.value, 10) || 10;
            trafficCurrentPage = 1;
            fetchTrafficTasks();
        });
    }
    document.querySelectorAll('#trafficStatusDropdown .status-option').forEach(el => {
        el.addEventListener('click', () => {
            trafficFilterStatus = el.dataset.status;
            trafficCurrentPage = 1;
            fetchTrafficTasks();
        });
    });
    document.querySelectorAll('#trafficTaskTypeDropdown .type-option').forEach(el => {
        el.addEventListener('click', () => {
            trafficFilterTaskType = el.dataset.taskType;
            trafficCurrentPage = 1;
            fetchTrafficTasks();
        });
    });
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bindTrafficEvents);
} else {
    bindTrafficEvents();
}

// onclick 与主脚本 switchPage 引用得到的函数挂 window
Object.assign(window, {
    fetchTrafficTasks, trafficGoToPage, trafficApplyKeywordAndRefresh,
    trafficOpenConfig, trafficStopTask, trafficDeleteTask, trafficSyncRecording,
    trafficShowReport, trafficDownloadRecording, trafficDeleteRecording,
    openTrafficRecordings
});
Object.defineProperty(window, 'trafficCurrentPage', {
    get: () => trafficCurrentPage,
    configurable: true
});
__dash.trafficStatusMap = trafficStatusMap;
