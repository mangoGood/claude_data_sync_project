// dashboard-advanced.js —— 高级功能（/api/advanced：调度/告警/慢SQL/诊断/配置版本/克隆/批量
//   + 位点可视化 + 死信裁决）独立 ES module。自有模块作用域，与主脚本及其它特性隔离。
//   共享依赖经主脚本设置的 window.__dash 取得；onclick 引用的函数在末尾显式挂 window。
const __dash = window.__dash;
const { API_BASE_URL, fetchWithAuth, getAuthHeaders, showNotification, escapeHtml, escapeAttr, fetchWorkflows } = __dash;

        // ============ 高级功能（/api/advanced 接入） ============
        const ADV_API = `${API_BASE_URL}/advanced`;
        let advTaskCache = null;          // 任务下拉共用数据源 [{id,name,status}]
        let advAlertEventPageNo = 1;
        let advAlertEventTotalPages = 1;
        let advSlowSqlPageNo = 1;
        let advSlowSqlTotalPages = 1;

        async function advFetchJson(url, options) {
            const resp = await fetchWithAuth(url, options);
            return resp.json();
        }

        function advFmtTime(v) {
            if (!v) return '-';
            return String(v).replace('T', ' ').slice(0, 19);
        }

        // 任务下拉共用数据源（同步任务前 200 个）
        async function advLoadTaskOptions() {
            try {
                const d = await advFetchJson(`${API_BASE_URL}/workflows?page=1&pageSize=200&taskType=SYNC`);
                advTaskCache = (d.success && d.data && d.data.list) ? d.data.list : [];
            } catch (e) {
                advTaskCache = [];
            }
            return advTaskCache;
        }

        function advTaskName(id) {
            if (!id) return '全部任务';
            const t = (advTaskCache || []).find(x => x.id === id);
            return t ? t.name : id.slice(0, 8) + '…';
        }

        function advFillTaskSelect(selectId, withAllOption) {
            const sel = document.getElementById(selectId);
            const prev = sel.value;
            let html = withAllOption ? '<option value="">全部任务</option>' : '<option value="">请选择任务</option>';
            (advTaskCache || []).forEach(t => {
                html += `<option value="${escapeHtml(t.id)}">${escapeHtml(t.name)}</option>`;
            });
            sel.innerHTML = html;
            if (prev) sel.value = prev;
        }

        // ---------- 调度与自动化页 ----------

        async function advLoadAutomationPage() {
            await advLoadTaskOptions();
            advFillTaskSelect('schedTaskSelect', false);
            advFillTaskSelect('retryTaskSelect', false);
            advFillTaskSelect('depUpstreamSelect', false);
            advFillTaskSelect('depDownstreamSelect', false);
            advLoadSchedules();
            advLoadRetryPolicies();
            advLoadDependencies();
        }

        async function advLoadSchedules() {
            const container = document.getElementById('schedList');
            try {
                const d = await advFetchJson(`${ADV_API}/schedules`);
                const list = d.success ? (d.data || []) : [];
                if (list.length === 0) {
                    container.innerHTML = '<div class="adv-empty">暂无调度</div>';
                    return;
                }
                let rows = '';
                list.forEach(s => {
                    const typeLabel = s.scheduleType === 'VALIDATION'
                        ? '数据校验·' + (s.compareType === 'CONTENT' ? '内容对比' : '行数对比')
                        : '全量同步';
                    rows += `<tr>
                        <td>${escapeHtml(s.scheduleName || '-')}</td>
                        <td>${escapeHtml(advTaskName(s.workflowId))}</td>
                        <td>${typeLabel}</td>
                        <td><code>${escapeHtml(s.cronExpression || '')}</code></td>
                        <td><span class="adv-badge ${s.enabled ? '' : 'off'}">${s.enabled ? '启用' : '停用'}</span></td>
                        <td>${advFmtTime(s.lastTriggeredAt)}</td>
                        <td>${advFmtTime(s.nextTriggerAt)}</td>
                        <td>${s.triggerCount || 0}</td>
                        <td>
                            <span class="adv-link" onclick="advToggleSchedule(${s.id}, ${!s.enabled})">${s.enabled ? '停用' : '启用'}</span>
                            &nbsp;<span class="adv-link danger" onclick="advDeleteSchedule(${s.id})">删除</span>
                        </td>
                    </tr>`;
                });
                container.innerHTML = `<table><thead><tr>
                    <th>名称</th><th>任务</th><th>类型</th><th>cron</th><th>状态</th><th>上次触发</th><th>下次触发</th><th>已触发</th><th>操作</th>
                </tr></thead><tbody>${rows}</tbody></table>`;
            } catch (e) {
                container.innerHTML = '<div class="adv-empty">加载失败</div>';
            }
        }

        function advOnSchedTypeChange() {
            const type = document.getElementById('schedTypeSelect').value;
            document.getElementById('schedCompareSelect').style.display = (type === 'VALIDATION') ? '' : 'none';
        }

        async function advCreateSchedule() {
            const workflowId = document.getElementById('schedTaskSelect').value;
            const scheduleName = document.getElementById('schedNameInput').value.trim();
            const cronExpression = document.getElementById('schedCronInput').value.trim();
            const scheduleType = document.getElementById('schedTypeSelect').value;
            const compareType = document.getElementById('schedCompareSelect').value;
            if (!workflowId) { showNotification('请选择任务', 'error'); return; }
            if (!scheduleName) { showNotification('请输入调度名称', 'error'); return; }
            if (!cronExpression) { showNotification('请输入 cron 表达式', 'error'); return; }
            const body = { workflowId, scheduleName, cronExpression, scheduleType };
            if (scheduleType === 'VALIDATION') body.compareType = compareType;
            const d = await advFetchJson(`${ADV_API}/schedules`, {
                method: 'POST', headers: getAuthHeaders(),
                body: JSON.stringify(body)
            });
            if (d.success) {
                showNotification('调度创建成功', 'success');
                document.getElementById('schedNameInput').value = '';
                document.getElementById('schedCronInput').value = '';
                advLoadSchedules();
            } else {
                showNotification(d.message || '创建失败', 'error');
            }
        }

        async function advToggleSchedule(id, enable) {
            const d = await advFetchJson(`${ADV_API}/schedules/${id}`, {
                method: 'PUT', headers: getAuthHeaders(),
                body: JSON.stringify({ enabled: enable })
            });
            if (d.success) { advLoadSchedules(); }
            else { showNotification(d.message || '操作失败', 'error'); }
        }

        async function advDeleteSchedule(id) {
            if (!confirm('确定删除此调度？')) return;
            const d = await advFetchJson(`${ADV_API}/schedules/${id}`, { method: 'DELETE', headers: getAuthHeaders() });
            if (d.success) { showNotification('已删除', 'success'); advLoadSchedules(); }
            else { showNotification(d.message || '删除失败', 'error'); }
        }

        async function advLoadRetryPolicies() {
            const container = document.getElementById('retryList');
            try {
                const d = await advFetchJson(`${ADV_API}/retry-policies`);
                const list = d.success ? (d.data || []) : [];
                if (list.length === 0) {
                    container.innerHTML = '<div class="adv-empty">暂无重试策略</div>';
                    return;
                }
                let rows = '';
                list.forEach(p => {
                    rows += `<tr>
                        <td>${escapeHtml(p.policyName || '-')}</td>
                        <td>${escapeHtml(advTaskName(p.workflowId))}</td>
                        <td>${escapeHtml(p.errorType || 'ALL')}</td>
                        <td>${p.maxRetries}</td>
                        <td>${p.retryIntervalMs} ms</td>
                        <td>${p.backoffStrategy === 'EXPONENTIAL' ? '指数退避' : '固定间隔'}</td>
                        <td><span class="adv-badge ${p.enabled ? '' : 'off'}">${p.enabled ? '启用' : '停用'}</span></td>
                        <td><span class="adv-link danger" onclick="advDeleteRetryPolicy(${p.id})">删除</span></td>
                    </tr>`;
                });
                container.innerHTML = `<table><thead><tr>
                    <th>策略名</th><th>任务</th><th>错误类型</th><th>最大次数</th><th>重试间隔</th><th>退避</th><th>状态</th><th>操作</th>
                </tr></thead><tbody>${rows}</tbody></table>`;
            } catch (e) {
                container.innerHTML = '<div class="adv-empty">加载失败</div>';
            }
        }

        async function advCreateRetryPolicy() {
            const workflowId = document.getElementById('retryTaskSelect').value;
            const policyName = document.getElementById('retryNameInput').value.trim();
            if (!workflowId) { showNotification('请选择任务', 'error'); return; }
            if (!policyName) { showNotification('请输入策略名称', 'error'); return; }
            const d = await advFetchJson(`${ADV_API}/retry-policies`, {
                method: 'POST', headers: getAuthHeaders(),
                body: JSON.stringify({
                    workflowId, policyName,
                    errorType: document.getElementById('retryErrorTypeSelect').value,
                    maxRetries: parseInt(document.getElementById('retryMaxInput').value) || 3,
                    retryIntervalMs: parseInt(document.getElementById('retryIntervalInput').value) || 5000,
                    backoffStrategy: document.getElementById('retryBackoffSelect').value
                })
            });
            if (d.success) {
                showNotification('策略创建成功', 'success');
                document.getElementById('retryNameInput').value = '';
                advLoadRetryPolicies();
            } else {
                showNotification(d.message || '创建失败', 'error');
            }
        }

        async function advDeleteRetryPolicy(id) {
            if (!confirm('确定删除此重试策略？')) return;
            const d = await advFetchJson(`${ADV_API}/retry-policies/${id}`, { method: 'DELETE', headers: getAuthHeaders() });
            if (d.success) { showNotification('已删除', 'success'); advLoadRetryPolicies(); }
            else { showNotification(d.message || '删除失败', 'error'); }
        }

        async function advLoadDependencies() {
            const container = document.getElementById('depList');
            const condLabels = { ON_SUCCESS: '上游成功后', ON_COMPLETION: '上游结束后', ON_FAILURE: '上游失败后' };
            try {
                const d = await advFetchJson(`${ADV_API}/dependencies`);
                const list = d.success ? (d.data || []) : [];
                if (list.length === 0) {
                    container.innerHTML = '<div class="adv-empty">暂无任务依赖</div>';
                    return;
                }
                let rows = '';
                list.forEach(dep => {
                    rows += `<tr>
                        <td>${escapeHtml(advTaskName(dep.upstreamWorkflowId))}</td>
                        <td>→ ${escapeHtml(advTaskName(dep.downstreamWorkflowId))}</td>
                        <td>${condLabels[dep.triggerCondition] || escapeHtml(dep.triggerCondition || '')}</td>
                        <td>${dep.triggerCount || 0}</td>
                        <td>${advFmtTime(dep.lastTriggeredAt)}</td>
                        <td><span class="adv-link danger" onclick="advDeleteDependency(${dep.id})">删除</span></td>
                    </tr>`;
                });
                container.innerHTML = `<table><thead><tr>
                    <th>上游任务</th><th>下游任务</th><th>触发条件</th><th>已触发</th><th>上次触发</th><th>操作</th>
                </tr></thead><tbody>${rows}</tbody></table>`;
            } catch (e) {
                container.innerHTML = '<div class="adv-empty">加载失败</div>';
            }
        }

        async function advCreateDependency() {
            const up = document.getElementById('depUpstreamSelect').value;
            const down = document.getElementById('depDownstreamSelect').value;
            if (!up || !down) { showNotification('请选择上游和下游任务', 'error'); return; }
            if (up === down) { showNotification('上下游不能是同一个任务', 'error'); return; }
            const d = await advFetchJson(`${ADV_API}/dependencies`, {
                method: 'POST', headers: getAuthHeaders(),
                body: JSON.stringify({
                    upstreamWorkflowId: up, downstreamWorkflowId: down,
                    triggerCondition: document.getElementById('depConditionSelect').value
                })
            });
            if (d.success) { showNotification('依赖创建成功', 'success'); advLoadDependencies(); }
            else { showNotification(d.message || '创建失败', 'error'); }
        }

        async function advDeleteDependency(id) {
            if (!confirm('确定删除此依赖？')) return;
            const d = await advFetchJson(`${ADV_API}/dependencies/${id}`, { method: 'DELETE', headers: getAuthHeaders() });
            if (d.success) { showNotification('已删除', 'success'); advLoadDependencies(); }
            else { showNotification(d.message || '删除失败', 'error'); }
        }

        // ---------- 告警管理页 ----------

        const ADV_METRIC_LABELS = {
            RPO_MS: 'RPO(ms)', RTO_MS: 'RTO(ms)', PROCESS_DOWN: '任务失败', SYNC_FAILED: '同步失败'
        };
        const ADV_OP_LABELS = { GT: '>', GTE: '>=', LT: '<', LTE: '<=', EQ: '=' };

        async function advLoadAlertPage() {
            await advLoadTaskOptions();
            advFillTaskSelect('alertTaskSelect', true);
            advOnAlertMetricChange();
            advOnAlertChannelChange();
            advLoadAlertRules();
            advLoadAlertEvents(1);
        }

        function advOnAlertMetricChange() {
            const metric = document.getElementById('alertMetricSelect').value;
            // 布尔型指标（任务失败/同步失败）取值 0/1：默认 > 0 即告警
            if (metric === 'PROCESS_DOWN' || metric === 'SYNC_FAILED') {
                document.getElementById('alertOperatorSelect').value = 'GT';
                document.getElementById('alertThresholdInput').value = '0';
            }
        }

        function advOnAlertChannelChange() {
            const ch = document.getElementById('alertChannelSelect').value;
            document.getElementById('alertWebhookInput').style.display = (ch === 'EMAIL') ? 'none' : '';
            document.getElementById('alertEmailInput').style.display = (ch === 'EMAIL') ? '' : 'none';
        }

        async function advLoadAlertRules() {
            const container = document.getElementById('alertRuleList');
            try {
                const d = await advFetchJson(`${ADV_API}/alert-rules`);
                const list = d.success ? (d.data || []) : [];
                if (list.length === 0) {
                    container.innerHTML = '<div class="adv-empty">暂无告警规则</div>';
                    return;
                }
                let rows = '';
                list.forEach(r => {
                    const cond = `${ADV_METRIC_LABELS[r.metricType] || r.metricType} ${ADV_OP_LABELS[r.operator] || r.operator} ${r.threshold}`;
                    rows += `<tr>
                        <td>${escapeHtml(r.ruleName || '-')}</td>
                        <td>${escapeHtml(advTaskName(r.workflowId))}</td>
                        <td>${escapeHtml(cond)}</td>
                        <td>${escapeHtml(r.notifyChannels || '-')}</td>
                        <td><span class="adv-badge ${r.enabled ? '' : 'off'}">${r.enabled ? '启用' : '停用'}</span></td>
                        <td>${r.triggerCount || 0}</td>
                        <td>${advFmtTime(r.lastTriggeredAt)}</td>
                        <td><span class="adv-link danger" onclick="advDeleteAlertRule(${r.id})">删除</span></td>
                    </tr>`;
                });
                container.innerHTML = `<table><thead><tr>
                    <th>规则名</th><th>任务</th><th>触发条件</th><th>渠道</th><th>状态</th><th>已告警</th><th>最近告警</th><th>操作</th>
                </tr></thead><tbody>${rows}</tbody></table>`;
            } catch (e) {
                container.innerHTML = '<div class="adv-empty">加载失败</div>';
            }
        }

        async function advCreateAlertRule() {
            const ruleName = document.getElementById('alertNameInput').value.trim();
            const threshold = document.getElementById('alertThresholdInput').value.trim();
            const channel = document.getElementById('alertChannelSelect').value;
            const webhookUrl = document.getElementById('alertWebhookInput').value.trim();
            const emails = document.getElementById('alertEmailInput').value.trim();
            if (!ruleName) { showNotification('请输入规则名称', 'error'); return; }
            if (threshold === '' || isNaN(Number(threshold))) { showNotification('请输入数值阈值', 'error'); return; }
            if (channel !== 'EMAIL' && !webhookUrl) { showNotification('请填写 Webhook URL', 'error'); return; }
            if (channel === 'EMAIL' && !emails) { showNotification('请填写收件人', 'error'); return; }
            const d = await advFetchJson(`${ADV_API}/alert-rules`, {
                method: 'POST', headers: getAuthHeaders(),
                body: JSON.stringify({
                    workflowId: document.getElementById('alertTaskSelect').value || null,
                    ruleName,
                    metricType: document.getElementById('alertMetricSelect').value,
                    operator: document.getElementById('alertOperatorSelect').value,
                    threshold: Number(threshold),
                    durationSeconds: 0,
                    notifyChannels: channel,
                    webhookUrl: webhookUrl || null,
                    emailRecipients: emails || null
                })
            });
            if (d.success) {
                showNotification('告警规则创建成功', 'success');
                document.getElementById('alertNameInput').value = '';
                advLoadAlertRules();
            } else {
                showNotification(d.message || '创建失败', 'error');
            }
        }

        async function advDeleteAlertRule(id) {
            if (!confirm('确定删除此告警规则？')) return;
            const d = await advFetchJson(`${ADV_API}/alert-rules/${id}`, { method: 'DELETE', headers: getAuthHeaders() });
            if (d.success) { showNotification('已删除', 'success'); advLoadAlertRules(); }
            else { showNotification(d.message || '删除失败', 'error'); }
        }

        async function advLoadAlertEvents(page) {
            advAlertEventPageNo = Math.max(1, page);
            const container = document.getElementById('alertEventList');
            try {
                const d = await advFetchJson(`${ADV_API}/alert-events?page=${advAlertEventPageNo}&pageSize=20`);
                const data = d.success ? d.data : { list: [], total: 0, totalPages: 1 };
                advAlertEventTotalPages = data.totalPages || 1;
                document.getElementById('alertEventPageInfo').textContent =
                    `总条数：${data.total || 0} | 第 ${advAlertEventPageNo}/${Math.max(1, advAlertEventTotalPages)} 页`;
                const list = data.list || [];
                if (list.length === 0) {
                    container.innerHTML = '<div class="adv-empty">暂无告警事件</div>';
                    return;
                }
                let rows = '';
                list.forEach(ev => {
                    rows += `<tr>
                        <td style="white-space: nowrap;">${advFmtTime(ev.createdAt)}</td>
                        <td>${escapeHtml(ev.ruleName || '-')}</td>
                        <td>${escapeHtml(advTaskName(ev.workflowId))}</td>
                        <td>${ev.metricValue != null ? ev.metricValue : '-'} / ${ev.threshold != null ? ev.threshold : '-'}</td>
                        <td>${escapeHtml(ev.message || '')}</td>
                        <td>${escapeHtml(ev.notifyResult || ev.status || '-')}</td>
                    </tr>`;
                });
                container.innerHTML = `<table><thead><tr>
                    <th>时间</th><th>规则</th><th>任务</th><th>指标值/阈值</th><th>消息</th><th>通知结果</th>
                </tr></thead><tbody>${rows}</tbody></table>`;
            } catch (e) {
                container.innerHTML = '<div class="adv-empty">加载失败</div>';
            }
        }

        function advAlertEventPage(delta) {
            const next = advAlertEventPageNo + delta;
            if (next < 1 || next > advAlertEventTotalPages) return;
            advLoadAlertEvents(next);
        }

        // ---------- 慢SQL检测页 ----------

        async function advLoadSlowSqlPage(page) {
            advSlowSqlPageNo = Math.max(1, page);
            const container = document.getElementById('slowSqlList');
            try {
                const d = await advFetchJson(`${ADV_API}/slow-sql?page=${advSlowSqlPageNo}&pageSize=20`);
                const data = d.success ? d.data : { list: [], total: 0 };
                const total = data.total || 0;
                advSlowSqlTotalPages = Math.max(1, Math.ceil(total / 20));
                document.getElementById('slowSqlPageInfo').textContent =
                    `总条数：${total} | 第 ${advSlowSqlPageNo}/${advSlowSqlTotalPages} 页`;
                const stats = data.stats || {};
                document.getElementById('slowSqlStats').textContent =
                    Object.keys(stats).length > 0
                        ? Object.entries(stats).map(([k, v]) => `${k}: ${v}`).join('　')
                        : '';
                if (advTaskCache === null) await advLoadTaskOptions();
                const list = data.list || [];
                if (list.length === 0) {
                    container.innerHTML = '<div class="adv-empty">暂无慢SQL记录（增量应用超过阈值的 SQL 会被记录在此）</div>';
                    return;
                }
                let rows = '';
                list.forEach(r => {
                    rows += `<tr>
                        <td style="white-space: nowrap;">${advFmtTime(r.createdAt)}</td>
                        <td>${escapeHtml(advTaskName(r.workflowId))}</td>
                        <td>${escapeHtml(r.tableName || '-')}</td>
                        <td>${escapeHtml(r.sqlType || '-')}</td>
                        <td><span class="adv-badge warn">${r.executionTimeMs} ms</span></td>
                        <td class="adv-sql">${escapeHtml((r.sqlText || '').slice(0, 300))}</td>
                    </tr>`;
                });
                container.innerHTML = `<table><thead><tr>
                    <th>时间</th><th>任务</th><th>表</th><th>类型</th><th>耗时</th><th>SQL</th>
                </tr></thead><tbody>${rows}</tbody></table>`;
            } catch (e) {
                container.innerHTML = '<div class="adv-empty">加载失败</div>';
            }
        }

        function advSlowSqlPage(delta) {
            const next = advSlowSqlPageNo + delta;
            if (next < 1 || next > advSlowSqlTotalPages) return;
            advLoadSlowSqlPage(next);
        }

        // ---------- 任务级动作：一键诊断 / 配置版本 / 克隆 ----------

        async function advDiagnoseTask() {
            if (!__dash.detailTaskId) return;
            const modal = document.getElementById('advDiagnoseModal');
            const body = document.getElementById('advDiagnoseBody');
            body.innerHTML = '<div style="text-align:center; padding: 30px; color: #999;">诊断中，请稍候...</div>';
            modal.classList.add('show');
            const d = await advFetchJson(`${ADV_API}/diagnose/${__dash.detailTaskId}`, { method: 'POST', headers: getAuthHeaders() });
            if (!d.success) {
                body.innerHTML = `<div class="adv-empty" style="color:#f5222d;">诊断失败: ${escapeHtml(d.message || '未知错误')}</div>`;
                return;
            }
            const r = d.data;
            document.getElementById('advDiagnoseTitle').textContent = `一键诊断 - ${r.workflowName || ''}`;
            const overallMap = { PASS: ['✓ 全部通过', '#52c41a'], WARNING: ['⚠ 存在警告', '#fa8c16'], FAIL: ['✗ 存在失败项', '#f5222d'] };
            const [overallText, overallColor] = overallMap[r.overall] || [r.overall, '#666'];
            let html = `<div style="margin-bottom: 12px; font-size: 14px; font-weight: 600; color: ${overallColor};">
                ${overallText}（通过 ${r.passed}/${r.total}，失败 ${r.failed}，警告 ${r.warnings}）</div>`;
            (r.checks || []).forEach(c => {
                const cls = c.status === 'PASS' ? 'passed' : (c.status === 'WARNING' ? 'warning' : 'failed');
                const icon = c.status === 'PASS' ? '✓' : (c.status === 'WARNING' ? '⚠' : '✗');
                html += `<div class="check-item ${cls}">
                    <div class="check-icon ${cls}">${icon}</div>
                    <div class="check-content">
                        <div class="check-name">${escapeHtml(c.checkName || '')}</div>
                        <div class="check-message">${escapeHtml(c.message || '')}</div>
                        ${c.detail ? `<div class="check-description">${escapeHtml(c.detail)}</div>` : ''}
                    </div>
                </div>`;
            });
            body.innerHTML = html;
        }

        async function advShowConfigVersions() {
            if (!__dash.detailTaskId) return;
            const modal = document.getElementById('advConfigVersionModal');
            const container = document.getElementById('advConfigVersionList');
            container.innerHTML = '<div class="adv-empty">加载中...</div>';
            modal.classList.add('show');
            const d = await advFetchJson(`${ADV_API}/config-versions/${__dash.detailTaskId}`);
            const list = d.success ? (d.data || []) : [];
            if (list.length === 0) {
                container.innerHTML = '<div class="adv-empty">暂无配置版本（保存任务配置后自动生成快照）</div>';
                return;
            }
            let rows = '';
            list.forEach(v => {
                rows += `<tr>
                    <td>v${v.versionNumber}</td>
                    <td style="white-space: nowrap;">${advFmtTime(v.createdAt)}</td>
                    <td>${escapeHtml(v.changeDescription || '-')}</td>
                    <td>${escapeHtml(v.createdBy || '-')}</td>
                    <td><span class="adv-link" onclick="advRollbackConfig(${v.versionNumber})">回滚到此版本</span></td>
                </tr>`;
            });
            container.innerHTML = `<table><thead><tr>
                <th>版本</th><th>时间</th><th>变更描述</th><th>创建人</th><th>操作</th>
            </tr></thead><tbody>${rows}</tbody></table>`;
        }

        async function advRollbackConfig(versionNumber) {
            if (!__dash.detailTaskId) return;
            if (!confirm(`确定回滚配置到版本 v${versionNumber}？`)) return;
            const d = await advFetchJson(`${ADV_API}/config-versions/${__dash.detailTaskId}/rollback/${versionNumber}`,
                { method: 'POST', headers: getAuthHeaders() });
            if (d.success) {
                showNotification(d.message || '回滚成功', 'success');
                advShowConfigVersions();
                fetchWorkflows();
            } else {
                showNotification(d.message || '回滚失败', 'error');
            }
        }

        async function advCloneTask() {
            if (!__dash.detailTaskId) return;
            const newName = prompt('请输入克隆后的任务名称：');
            if (newName === null) return;
            if (!newName.trim()) { showNotification('任务名称不能为空', 'error'); return; }
            const d = await advFetchJson(`${ADV_API}/clone/${__dash.detailTaskId}`, {
                method: 'POST', headers: getAuthHeaders(),
                body: JSON.stringify({ newName: newName.trim() })
            });
            if (d.success) {
                showNotification(`克隆成功：${d.data && d.data.name ? d.data.name : newName}（配置中）`, 'success');
                document.getElementById('taskDetailModal').classList.remove('show');
                fetchWorkflows();
            } else {
                showNotification(d.message || '克隆失败', 'error');
            }
        }

        // ---------- 失败事件死信裁决 ----------

        // showTaskDetail 时从 error_message 解析出的失败事件 seqno（仅 FAILED+增量失败特征时非空）

        async function dlSkipEventAndRetry() {
            if (!__dash.detailTaskId || __dash.failedSeqno === null) return;
            if (!confirm(`确定跳过失败事件 seqno=${__dash.failedSeqno} 并恢复任务？\n\n该事件将【不会】被应用到目标库，仅记入死信记录；源和目标可能因此产生差异，需人工核对补偿。`)) return;
            try {
                const resp = await fetchWithAuth(`${API_BASE_URL}/workflows/${__dash.detailTaskId}/skip-event`, {
                    method: 'POST', headers: getAuthHeaders(),
                    body: JSON.stringify({ seqno: __dash.failedSeqno })
                });
                const d = await resp.json();
                if (d.success) {
                    showNotification(d.message || '已跳过事件并恢复任务', 'success');
                    document.getElementById('taskDetailModal').classList.remove('show');
                    fetchWorkflows();
                } else {
                    showNotification(d.message || '跳过失败', 'error');
                }
            } catch (e) {
                showNotification('跳过失败事件请求异常', 'error');
            }
        }

        async function dlShowDeadletter() {
            if (!__dash.detailTaskId) return;
            const modal = document.getElementById('dlDeadletterModal');
            const container = document.getElementById('dlDeadletterList');
            container.innerHTML = '<div class="adv-empty">加载中…</div>';
            modal.classList.add('show');
            let d;
            try {
                const resp = await fetchWithAuth(`${API_BASE_URL}/workflows/${__dash.detailTaskId}/deadletter`);
                d = await resp.json();
            } catch (e) {
                container.innerHTML = '<div class="adv-empty">查询失败：请求异常</div>';
                return;
            }
            if (!d || d.success === false) {
                container.innerHTML = `<div class="adv-empty">${escapeHtml((d && d.message) || '查询失败')}</div>`;
                return;
            }
            const records = d.records || [];
            if (records.length === 0) {
                container.innerHTML = '<div class="adv-empty">暂无死信记录</div>';
                return;
            }
            let rows = '';
            records.forEach(r => {
                const stmts = (r.statements || []).map(s => `<div class="adv-sql">${escapeHtml(s)}</div>`).join('');
                rows += `<tr>
                    <td>${r.seqno != null ? r.seqno : '-'}</td>
                    <td style="white-space: nowrap;">${r.ts ? new Date(r.ts).toLocaleString() : '-'}</td>
                    <td>${escapeHtml(r.tableName || '-')}</td>
                    <td>${escapeHtml(r.eventType || '-')}</td>
                    <td style="max-width: 360px;">${stmts || '<span style="color:#999;">（SQL 转换失败，仅存元数据）</span>'}</td>
                </tr>`;
            });
            container.innerHTML = `<table><thead><tr>
                <th>seqno</th><th>跳过时间</th><th>表</th><th>事件类型</th><th>未应用的 SQL</th>
            </tr></thead><tbody>${rows}</tbody></table>`;
        }

        // ---------- 同步位点可视化 ----------

        let _cpTimer = null;

        function cpShowCheckpoint() {
            if (!__dash.detailTaskId) return;
            document.getElementById('cpCheckpointModal').classList.add('show');
            document.getElementById('cpCheckpointBody').innerHTML = '<div class="adv-empty">加载中…</div>';
            cpLoadCheckpoint();
            // 位点是活数据，打开期间定时刷新；关闭/页面不可见时不发请求，关闭即清理
            if (_cpTimer) clearInterval(_cpTimer);
            _cpTimer = setInterval(() => {
                if (document.hidden) return;
                if (!document.getElementById('cpCheckpointModal').classList.contains('show')) return;
                cpLoadCheckpoint();
            }, 5000);
        }

        function cpCloseCheckpoint() {
            document.getElementById('cpCheckpointModal').classList.remove('show');
            if (_cpTimer) { clearInterval(_cpTimer); _cpTimer = null; }
        }

        function cpBadge(status) {
            const map = {
                HEALTHY: ['ok', '健康'], OK: ['ok', '正常'],
                WARNING: ['warn', '警告'], CRITICAL: ['crit', '严重'],
                PARTIAL: ['warn', '部分可用'], NO_DATA: ['na', '无数据'], UNKNOWN: ['na', '未知']
            };
            const [cls, text] = map[status] || ['na', status || '-'];
            return `<span class="cp-badge ${cls}">${escapeHtml(text)}</span>`;
        }

        function cpRow(k, v) {
            return `<div class="cp-kv"><span class="k">${escapeHtml(k)}</span><span class="v">${v === null || v === undefined || v === '' ? '<span style="color:#bbb;">—</span>' : escapeHtml(String(v))}</span></div>`;
        }

        async function cpLoadCheckpoint() {
            if (!__dash.detailTaskId) return;
            const body = document.getElementById('cpCheckpointBody');
            let d;
            try {
                const resp = await fetchWithAuth(`${API_BASE_URL}/workflows/${__dash.detailTaskId}/checkpoint`);
                d = await resp.json();
            } catch (e) {
                body.innerHTML = '<div class="adv-empty">查询失败：请求异常</div>';
                return;
            }
            if (!d || d.success === false) {
                body.innerHTML = `<div class="adv-empty">${escapeHtml((d && d.message) || '查询失败')}</div>`;
                return;
            }

            const binlog = d.binlog || {}, thl = d.thl || {}, ckpt = d.checkpoint || {}, gaps = d.gaps || {};
            const pending = gaps.pending_apply != null ? gaps.pending_apply : gaps.pending_events;

            let html = `<div style="margin-bottom:12px;font-size:13px;">链路状态：${cpBadge(d.linkStatus)}
                <span style="color:#999;font-size:12px;margin-left:12px;">
                    RPO ${d.rpo_ms != null ? d.rpo_ms + ' ms' : '—'} ·
                    RTO ${d.rto_ms != null ? d.rto_ms + ' ms' : '—'}
                </span></div>`;

            // 1. capture 捕获位点
            html += `<div class="cp-stage">
                <div class="cp-stage-title">① 源库捕获（capture）${binlog.available ? '' : cpBadge('NO_DATA')}</div>
                ${cpRow('binlog 文件', binlog.file)}
                ${cpRow('binlog 位点', binlog.position)}
                ${binlog.gtid ? cpRow('GTID 集', binlog.gtid) : ''}
            </div><div class="cp-arrow">↓</div>`;

            // 2. THL 落盘
            html += `<div class="cp-stage">
                <div class="cp-stage-title">② 落盘 THL（extract）${thl.available ? '' : cpBadge('NO_DATA')}</div>
                ${cpRow('最新 seqno', thl.seqno)}
            </div><div class="cp-arrow">↓</div>`;

            // 3. 已应用位点（断点续传起点）
            html += `<div class="cp-stage">
                <div class="cp-stage-title">③ 已应用（increment checkpoint）${ckpt.available ? '' : cpBadge('NO_DATA')}</div>
                ${cpRow('已应用 seqno', ckpt.seqno)}
                ${cpRow('binlog 位点', ckpt.binlog_file ? ckpt.binlog_file + ':' + (ckpt.binlog_position != null ? ckpt.binlog_position : '') : null)}
                ${cpRow('事件 ID', ckpt.event_id)}
                ${cpRow('更新时间', ckpt.updated_at)}
                <div style="font-size:11px;color:#999;margin-top:6px;">断点续传从此位点之后继续（重试/恢复的起点）</div>
            </div>`;

            // 差距
            html += `<div class="cp-stage" style="background:#fff;">
                <div class="cp-stage-title">链路积压</div>
                <div class="cp-kv"><span class="k">待应用事件</span><span class="v">${pending != null ? pending : '—'} ${cpBadge(gaps.pendingApplyStatus)}</span></div>
                <div class="cp-kv"><span class="k">binlog 差距</span><span class="v">${gaps.binlogPositionGap != null ? gaps.binlogPositionGap + ' 字节' : (gaps.binlogFileGap ? gaps.binlogFileGap + ' 个文件' : '—')} ${cpBadge(gaps.binlogGapStatus)}</span></div>
            </div>`;

            html += `<div style="font-size:11px;color:#bbb;text-align:right;">采样时间：${d.timestamp ? new Date(d.timestamp).toLocaleString() : '-'}</div>`;
            body.innerHTML = html;
        }

        // ---------- 批量操作 ----------

        function advToggleBatchSelect(id, checked) {
            if (checked) __dash.batchSelected.add(id); else __dash.batchSelected.delete(id);
            advUpdateBatchToolbar();
        }

        function advToggleBatchAll(checked) {
            document.querySelectorAll('#tableBody .batch-cb').forEach(cb => {
                cb.checked = checked;
                if (checked) __dash.batchSelected.add(cb.dataset.id); else __dash.batchSelected.delete(cb.dataset.id);
            });
            advUpdateBatchToolbar();
        }

        function advClearBatchSelection() {
            __dash.batchSelected.clear();
            document.querySelectorAll('#tableBody .batch-cb').forEach(cb => cb.checked = false);
            const all = document.getElementById('batchSelectAll');
            if (all) all.checked = false;
            advUpdateBatchToolbar();
        }

        function advUpdateBatchToolbar() {
            const bar = document.getElementById('batchToolbar');
            if (!bar) return;
            document.getElementById('batchCount').textContent = __dash.batchSelected.size;
            bar.classList.toggle('show', __dash.batchSelected.size > 0);
        }

        async function advBatchAction(action) {
            if (__dash.batchSelected.size === 0) return;
            const labels = { launch: '启动', stop: '结束', delete: '删除' };
            if (!confirm(`确定批量${labels[action]}选中的 ${__dash.batchSelected.size} 个任务？`)) return;
            const d = await advFetchJson(`${ADV_API}/batch/${action}`, {
                method: 'POST', headers: getAuthHeaders(),
                body: JSON.stringify({ workflowIds: Array.from(__dash.batchSelected) })
            });
            if (d.success) {
                const r = d.data || {};
                const ok = r.successCount != null ? r.successCount : (r.success != null ? r.success : '?');
                const bad = r.failedCount != null ? r.failedCount : (r.failed != null ? r.failed : '?');
                showNotification(`批量${labels[action]}完成：成功 ${ok}，失败 ${bad}`, 'success');
                advClearBatchSelection();
                fetchWorkflows();
            } else {
                showNotification(d.message || '批量操作失败', 'error');
            }
        }

        async function advBatchExport() {
            if (__dash.batchSelected.size === 0) return;
            const d = await advFetchJson(`${ADV_API}/batch/export`, {
                method: 'POST', headers: getAuthHeaders(),
                body: JSON.stringify({ workflowIds: Array.from(__dash.batchSelected) })
            });
            if (!d.success) { showNotification(d.message || '导出失败', 'error'); return; }
            const blob = new Blob([JSON.stringify(d.data, null, 2)], { type: 'application/json' });
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = `sync-tasks-export-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '')}.json`;
            a.click();
            URL.revokeObjectURL(a.href);
            showNotification(`已导出 ${__dash.batchSelected.size} 个任务的配置`, 'success');
        }


// ==== 显式导出：onclick/onchange 内联处理器引用 + 主脚本 switchPage 调用的函数（module 私有→挂 window）====
Object.assign(window, {
    advAlertEventPage, advBatchAction, advBatchExport, advClearBatchSelection, advCloneTask, advCreateAlertRule,
    advCreateDependency, advCreateRetryPolicy, advCreateSchedule, advDeleteAlertRule, advDeleteDependency, advDeleteRetryPolicy,
    advDeleteSchedule, advDiagnoseTask, advLoadAlertEvents, advLoadSlowSqlPage, advOnAlertChannelChange, advOnAlertMetricChange,
    advOnSchedTypeChange, advRollbackConfig, advShowConfigVersions, advSlowSqlPage, advToggleBatchAll, advToggleBatchSelect,
    advToggleSchedule, cpCloseCheckpoint, cpLoadCheckpoint, cpShowCheckpoint, dlShowDeadletter, dlSkipEventAndRetry,
    // 主脚本 switchPage 切到高级页时调用（非 onclick，故单列）
    advLoadAutomationPage, advLoadAlertPage
});
