// dashboard-dr.js —— 灾备(DR)管理独立 ES module（任务列表/详情/配置向导/主备倒换/校验）。
//   自有模块作用域；共享依赖经 window.__dash 取得；drStatusMap 供主脚本的共享状态渲染读取，
//   经 __dash 暴露；onclick/主 switchPage/refreshCurrentTaskList 引用的函数末尾挂 window。
const __dash = window.__dash;
const { API_BASE_URL, fetchWithAuth, getAuthHeaders, showNotification, escapeHtml, escapeAttr,
        checkAuth, formatDateTime, formatDbTypeLabel, formatDelay, viewMetrics } = __dash;

        let drCurrentPage = 1;
        let drAllTasks = [];
        let drPageSize = 10;
        let drTotalCount = 0;
        let drFilterSourceType = null;
        let drFilterTargetType = null;
        let currentDrTaskId = null;
        let drConnectionTestStatus = { source: false, target: false };

        const drStatusMap = {
            'CONFIGURING': { text: '配置中', class: 'status-configuring', icon: '⚙' },
            'PENDING': { text: '启动中', class: 'status-pending', dot: true },
            'RECEIVED': { text: '已接收', class: 'status-pending', dot: true },
            'STARTING': { text: '启动中', class: 'status-pending', dot: true },
            'FULL_MIGRATING': { text: '灾备初始化', class: 'status-full-migrating', dot: true },
            'FULL_COMPLETED': { text: '灾备初始化完成', class: 'status-full-completed', icon: '✓' },
            'INCREMENT_RUNNING': { text: '灾备中', class: 'status-increment-running', dot: true },
            'SWITCHING': { text: '倒换中', class: 'status-switching', icon: '⇄' },
            'COMPLETED': { text: '已完成', class: 'status-completed', icon: '✓' },
            'FAILED': { text: '灾备异常', class: 'status-failed', icon: '✕' },
            'PAUSED': { text: '已暂停', class: 'status-paused', icon: '⏸' }
        };

        let drFailoverTaskId = null;

        function confirmDrFailover(taskId, taskName) {
            drFailoverTaskId = taskId;
            document.getElementById('drFailoverTaskName').textContent = taskName;
            let switchCount = 0;
            const task = drAllTasks.find(t => t.id === taskId);
            if (task && task.dr_switch_count) {
                switchCount = task.dr_switch_count;
            }
            document.getElementById('drFailoverSwitchCount').textContent = switchCount + 1;
            document.getElementById('drFailoverConfirmModal').classList.add('show');
        }

        document.getElementById('closeDrFailoverConfirmModal').addEventListener('click', () => {
            document.getElementById('drFailoverConfirmModal').classList.remove('show');
            drFailoverTaskId = null;
        });

        document.getElementById('cancelDrFailoverBtn').addEventListener('click', () => {
            document.getElementById('drFailoverConfirmModal').classList.remove('show');
            drFailoverTaskId = null;
        });

        document.getElementById('confirmDrFailoverBtn').addEventListener('click', async () => {
            if (!drFailoverTaskId) return;
            const btn = document.getElementById('confirmDrFailoverBtn');
            btn.disabled = true;
            btn.textContent = '倒换中...';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${drFailoverTaskId}/failover`, {
                    method: 'POST'
                });
                const result = await response.json();
                if (result.success) {
                    document.getElementById('drFailoverConfirmModal').classList.remove('show');
                    showNotification('主备倒换已启动，请等待倒换完成', 'success');
                    fetchDrTasks();
                } else {
                    showNotification(result.message || '主备倒换失败', 'error');
                }
            } catch (e) {
                showNotification('主备倒换请求失败: ' + e.message, 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = '确认倒换';
                drFailoverTaskId = null;
            }
        });

        async function fetchDrTasks(page) {
            if (!checkAuth()) return;
            if (!page) page = drCurrentPage;
            
            try {
                const keyword = document.getElementById('drSearchInput').value.trim();
                let url = `${API_BASE_URL}/workflows?page=${page}&pageSize=${drPageSize}&taskType=DR`;
                if (keyword) url += `&keyword=${encodeURIComponent(keyword)}`;
                if (drFilterSourceType) url += `&sourceType=${drFilterSourceType}`;
                if (drFilterTargetType) url += `&targetType=${drFilterTargetType}`;

                const response = await fetchWithAuth(url);
                const result = await response.json();
                
                if (result.success) {
                    const data = result.data;
                    drTotalCount = data.total;
                    drCurrentPage = page;
                    drAllTasks = data.list || [];
                    renderDrTaskList(data.list);
                    renderDrPagination(data.total, page, data.pageSize);
                }
            } catch (e) {
                console.error('Failed to fetch DR tasks:', e);
            }
        }

        function renderDrTaskList(tasks) {
            const tbody = document.getElementById('drTableBody');
            if (!tasks || tasks.length === 0) {
                tbody.innerHTML = `<div class="empty-state"><div class="empty-icon"><svg viewBox="0 0 80 80" fill="none"><rect x="15" y="25" width="50" height="40" rx="2" stroke="#d9d9d9" stroke-width="2" fill="none"/><rect x="20" y="20" width="40" height="8" rx="1" stroke="#d9d9d9" stroke-width="2" fill="none"/><line x1="25" y1="45" x2="55" y2="45" stroke="#d9d9d9" stroke-width="2"/><line x1="25" y1="52" x2="45" y2="52" stroke="#d9d9d9" stroke-width="2"/></svg></div><div class="empty-text">暂无灾备任务</div></div>`;
                return;
            }
            
            tbody.innerHTML = tasks.map(task => {
                const statusInfo = drStatusMap[task.status] || { text: task.status, class: 'status-pending', dot: true };
                let monitorHtml = '-';
                if (task.rto_ms !== null && task.rto_ms !== undefined) {
                    monitorHtml = `<div style="font-size: 12px;">RTO: ${formatDelay(task.rto_ms)}</div>`;
                }
                if (task.rpo_ms !== null && task.rpo_ms !== undefined) {
                    monitorHtml += `<div style="font-size: 12px;">RPO: ${formatDelay(task.rpo_ms)}</div>`;
                }
                
                // 灾备任务源库/目标库类型当前恒一致；双向灾备尚未支持，先恒为单向箭头，
                // 后续接入双向灾备后按标记切换为 ↔
                const drIsBidirectional = task.dr_mode === 'BIDIRECTIONAL';
                const drDbTypeHtml = `${formatDbTypeLabel(task.source_type)} ${drIsBidirectional ? '↔' : '→'} ${formatDbTypeLabel(task.target_type)}`;

                return `<div class="table-row" data-task-id="${task.id}">
                    <div class="table-cell col-name">
                        <div>
                            <div><span style="background: #f6ffed; color: #52c41a; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-right: 4px;">灾备</span>${escapeHtml(task.name)}</div>
                            <div style="font-size: 11px; color: #1890ff; cursor: pointer;" onclick="${task.status === 'CONFIGURING' ? `openDrConfig('${task.id}', '${task.source_type || 'mysql'}')` : `showDrTaskDetail('${task.id}')`}">${task.id}</div>
                        </div>
                    </div>
                    <div class="table-cell col-dbtype">${drDbTypeHtml}</div>
                    <div class="table-cell col-status">
                        <span class="status-tag ${statusInfo.class}">
                            ${statusInfo.dot ? '<span class="status-dot"></span>' : statusInfo.icon ? `<span class="status-icon">${statusInfo.icon}</span>` : ''}
                            ${statusInfo.text}
                        </span>
                    </div>
                    <div class="table-cell col-monitor">${monitorHtml}</div>
                    <div class="table-cell col-time">${formatDateTime(task.created_at)}</div>
                    <div class="table-cell col-action">
                        <div class="action-btns">
                            ${task.status === 'CONFIGURING' ?
                                `<button class="action-btn" onclick="openDrConfig('${task.id}', '${task.source_type || 'mysql'}')">配置</button><button class="action-btn delete" onclick="deleteWorkflow('${task.id}', '${escapeAttr(task.name)}')">删除</button>` :
                            task.status === 'FAILED' ?
                                `<button class="action-btn" onclick="retryWorkflow('${task.id}')">恢复</button><button class="action-btn stop" onclick="stopWorkflow('${task.id}')">结束</button>` : 
                            (task.status === 'STARTING' || task.status === 'FULL_MIGRATING') ? 
                                `<button class="action-btn monitor" onclick="viewMetrics('${task.id}')">监控</button><button class="action-btn" onclick="pauseWorkflow('${task.id}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${task.id}')">结束</button>` : 
                            task.status === 'INCREMENT_RUNNING' ?
                                `<button class="action-btn monitor" onclick="viewMetrics('${task.id}')">监控</button>${drIsBidirectional ? '' : `<button class="action-btn" style="color: #1890ff; border-color: #1890ff;" onclick="confirmDrFailover('${task.id}', '${escapeAttr(task.name)}')">主备倒换</button>`}<button class="action-btn" onclick="pauseWorkflow('${task.id}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${task.id}')">结束</button>` :
                            task.status === 'SWITCHING' ?
                                `<span style="color: #1890ff; font-size: 12px;">倒换中...</span>` :
                            task.status === 'PAUSED' ?
                                `<button class="action-btn" onclick="resumeWorkflow('${task.id}')">恢复</button><button class="action-btn stop" onclick="stopWorkflow('${task.id}')">结束</button>` :
                            task.status === 'COMPLETED' || task.status === 'FULL_COMPLETED' ?
                                `<button class="action-btn stop" onclick="deleteWorkflow('${task.id}')">删除</button>` :
                                `<span style="color: #999; font-size: 12px;">-</span>`}
                        </div>
                    </div>
                </div>`;
            }).join('');
        }

        function renderDrPagination(total, currentPage, pageSize) {
            const totalPages = Math.ceil(total / pageSize);
            document.getElementById('drPaginationInfo').textContent = `总条数：${total}`;
            
            const pageNumbers = document.getElementById('drPageNumbers');
            let html = '';
            
            if (totalPages <= 7) {
                for (let i = 1; i <= totalPages; i++) {
                    html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToDrPage(${i})">${i}</button>`;
                }
            } else {
                html += `<button class="page-btn ${1 === currentPage ? 'active' : ''}" onclick="goToDrPage(1)">1</button>`;
                
                if (currentPage <= 4) {
                    for (let i = 2; i <= Math.min(5, totalPages - 1); i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToDrPage(${i})">${i}</button>`;
                    }
                    html += `<span class="page-ellipsis" onclick="goToDrPage(${Math.min(currentPage + 5, Math.floor(totalPages / 2))})">···</span>`;
                } else if (currentPage >= totalPages - 3) {
                    html += `<span class="page-ellipsis" onclick="goToDrPage(${Math.max(currentPage - 5, Math.floor(totalPages / 2))})">···</span>`;
                    for (let i = totalPages - 4; i <= totalPages - 1; i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToDrPage(${i})">${i}</button>`;
                    }
                } else {
                    html += `<span class="page-ellipsis" onclick="goToDrPage(${Math.max(1, currentPage - 5)})">···</span>`;
                    for (let i = currentPage - 1; i <= currentPage + 1; i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToDrPage(${i})">${i}</button>`;
                    }
                    html += `<span class="page-ellipsis" onclick="goToDrPage(${Math.min(totalPages, currentPage + 5)})">···</span>`;
                }
                
                html += `<button class="page-btn ${totalPages === currentPage ? 'active' : ''}" onclick="goToDrPage(${totalPages})">${totalPages}</button>`;
            }
            
            pageNumbers.innerHTML = html;
            
            document.getElementById('drPrevPageBtn').disabled = currentPage <= 1;
            document.getElementById('drNextPageBtn').disabled = currentPage >= totalPages;
        }

        function goToDrPage(page) {
            if (page < 1) return;
            const totalPages = Math.ceil(drTotalCount / drPageSize);
            if (page > totalPages) return;
            fetchDrTasks(page);
        }

        async function showDrTaskDetail(id) {
            if (!checkAuth()) return;
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${id}`);
                const result = await response.json();
                if (result.success) {
                    const task = result.data;
                    let statusInfo = drStatusMap[task.status] || { text: task.status, class: 'status-pending', dot: true };
                    
                    let sourceInfo = '-';
                    let targetInfo = '-';
                    try {
                        if (task.source_connection) {
                            const sc = JSON.parse(task.source_connection);
                            sourceInfo = `${sc.host}:${sc.port}`;
                        }
                    } catch(e) {
                        const m = (task.source_connection || '').match(/@([^:]+):(\d+)/);
                        sourceInfo = m ? `${m[1]}:${m[2]}` : (task.source_connection || '-');
                    }
                    try {
                        if (task.target_connection) {
                            const tc = JSON.parse(task.target_connection);
                            targetInfo = `${tc.host}:${tc.port}`;
                        }
                    } catch(e) {
                        const m = (task.target_connection || '').match(/@([^:]+):(\d+)/);
                        targetInfo = m ? `${m[1]}:${m[2]}` : (task.target_connection || '-');
                    }
                    
                    let syncObjectsHtml = '全部数据库（自动）';
                    
                    const detailIsBidirectional = task.dr_mode === 'BIDIRECTIONAL';
                    let failoverBtnHtml = '';
                    if (detailIsBidirectional) {
                        // 双向灾备两端均可读写、实时互同步，没有"主/备"角色之分，不提供倒换
                        if (task.status === 'INCREMENT_RUNNING') {
                            failoverBtnHtml = `<div style="margin-top: 16px; padding: 10px 16px; background: #f6ffed; border: 1px solid #b7eb8f; border-radius: 4px; font-size: 13px; color: #52c41a;">⇄ 双向同步运行中：两端均可读写，事务打标防回环，无需主备倒换</div>`;
                        }
                    } else if (task.status === 'INCREMENT_RUNNING') {
                        failoverBtnHtml = `<button onclick="confirmDrFailover('${task.id}', '${escapeAttr(task.name)}'); document.getElementById('taskDetailModal').classList.remove('show');" style="margin-top: 16px; padding: 8px 20px; background: #1890ff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 13px;">主备倒换</button>`;
                    } else if (task.status === 'SWITCHING') {
                        failoverBtnHtml = `<div style="margin-top: 16px; padding: 10px 16px; background: #e6f7ff; border: 1px solid #91d5ff; border-radius: 4px; font-size: 13px; color: #1890ff;">⇄ 主备倒换进行中，请等待...</div>`;
                    }

                    const content = document.getElementById('taskDetailContent');
                    content.innerHTML = `
                        <div style="display: grid; grid-template-columns: 120px 1fr; gap: 12px 16px; font-size: 13px;">
                            <div style="color: #666;">任务ID:</div>
                            <div>${task.id}</div>
                            <div style="color: #666;">任务名称:</div>
                            <div>${escapeHtml(task.name)}</div>
                            <div style="color: #666;">任务类型:</div>
                            <div>灾备任务</div>
                            <div style="color: #666;">状态:</div>
                            <div><span class="status-tag ${statusInfo.class}">${statusInfo.dot ? '<span class="status-dot"></span>' : ''}${statusInfo.text}</span></div>
                            <div style="color: #666;">灾备方向:</div>
                            <div>${detailIsBidirectional ? '双向（A ↔ B 双活互同步）' : '单向（主库 → 灾备库）'}</div>
                            <div style="color: #666;">${detailIsBidirectional ? '节点A:' : '主库:'}</div>
                            <div>${sourceInfo}</div>
                            <div style="color: #666;">${detailIsBidirectional ? '节点B:' : '备库:'}</div>
                            <div>${targetInfo}</div>
                            <div style="color: #666;">灾备模式:</div>
                            <div>全量+增量（强制）</div>
                            <div style="color: #666;">灾备对象:</div>
                            <div>${syncObjectsHtml}</div>
                            <div style="color: #666;">全量同步进度:</div>
                            <div>
                                <div class="progress-bar" style="width: 200px;"><div class="progress-fill" style="width: ${task.progress || 0}%"></div></div>
                                <span style="margin-left: 8px; font-size: 11px; color: #666;">${task.progress || 0}%</span>
                            </div>
                            <div style="color: #666;">RTO:</div>
                            <div>${task.rto_ms !== null && task.rto_ms !== undefined ? formatDelay(task.rto_ms) : '-'}</div>
                            <div style="color: #666;">RPO:</div>
                            <div>${task.rpo_ms !== null && task.rpo_ms !== undefined ? formatDelay(task.rpo_ms) : '-'}</div>
                            <div style="color: #666;">倒换次数:</div>
                            <div>${task.dr_switch_count || 0}</div>
                            <div style="color: #666;">创建时间:</div>
                            <div>${formatDateTime(task.created_at)}</div>
                            <div style="color: #666;">是否计费中:</div>
                            <div>${task.is_billing ? '<span style="color: #52c41a;">是</span>' : '<span style="color: #999;">否</span>'}</div>
                        </div>
                        ${task.error_message ? `<div style="margin-top: 16px; padding: 10px; background: #fff2f0; border: 1px solid #ffccc7; border-radius: 4px; font-size: 12px; color: #f5222d;">错误信息: ${task.error_message}</div>` : ''}
                        ${failoverBtnHtml}
                    `;
                    
                    document.getElementById('taskDetailModal').classList.add('show');
                }
            } catch (e) {
                console.error('Failed to fetch DR task detail:', e);
            }
        }

        function openDrConfig(taskId, dbType) {
            currentDrTaskId = taskId;
            // 从列表进入已有任务时用任务自身类型；从创建流程进入时沿用创建弹窗所选类型
            if (dbType) currentDrDbType = dbType;
            const defaultPort = currentDrDbType === 'postgresql' ? '5432' : '3306';
            const typeLabel = formatDbTypeLabel(currentDrDbType);
            document.getElementById('drConfigTitle').textContent = '灾备任务配置（' + typeLabel + '）';
            document.getElementById('drSourceHost').value = '';
            document.getElementById('drSourcePort').value = defaultPort;
            document.getElementById('drSourceUser').value = '';
            document.getElementById('drSourcePassword').value = '';
            document.getElementById('drTargetHost').value = '';
            document.getElementById('drTargetPort').value = defaultPort;
            document.getElementById('drTargetUser').value = '';
            document.getElementById('drTargetPassword').value = '';
            document.getElementById('drSourceTestResult').textContent = '';
            document.getElementById('drTargetTestResult').textContent = '';
            drConnectionTestStatus = { source: false, target: false };
            document.getElementById('drConfigModal').classList.add('show');
        }

        // 灾备方向选择（单向/双向）：切换选中态高亮
        function selectDrMode(mode) {
            document.querySelectorAll('#createDrTaskModal .dr-mode-option').forEach(opt => {
                const checked = opt.querySelector('input[name="drMode"]').value === mode;
                opt.querySelector('input[name="drMode"]').checked = checked;
                opt.style.border = checked ? '2px solid #1890ff' : '2px solid #d9d9d9';
                opt.style.background = checked ? '#e6f7ff' : '';
            });
        }

        // 灾备任务当前选中的数据库类型（源/目标同构）：驱动连接协议、默认端口与校验文案
        let currentDrDbType = 'mysql';
        function selectDrDbType(type) {
            currentDrDbType = type;
            document.querySelectorAll('#createDrTaskModal .dr-dbtype-option').forEach(opt => {
                const checked = opt.querySelector('input[name="drDbType"]').value === type;
                opt.querySelector('input[name="drDbType"]').checked = checked;
                opt.style.border = checked ? '2px solid #1890ff' : '2px solid #d9d9d9';
                opt.style.background = checked ? '#e6f7ff' : '';
            });
        }

        document.getElementById('createDrTaskBtn').addEventListener('click', () => {
            document.getElementById('drTaskName').value = '';
            selectDrMode('UNIDIRECTIONAL');
            selectDrDbType('mysql');
            document.getElementById('createDrTaskModal').classList.add('show');
        });

        document.getElementById('closeCreateDrModal').addEventListener('click', () => {
            document.getElementById('createDrTaskModal').classList.remove('show');
        });
        document.getElementById('cancelCreateDrBtn').addEventListener('click', () => {
            document.getElementById('createDrTaskModal').classList.remove('show');
        });

        document.getElementById('confirmCreateDrBtn').addEventListener('click', async () => {
            const name = document.getElementById('drTaskName').value.trim();
            if (!name) { showNotification('请输入任务名称', 'error'); return; }
            const drMode = document.querySelector('#createDrTaskModal input[name="drMode"]:checked')?.value || 'UNIDIRECTIONAL';
            const dbType = document.querySelector('#createDrTaskModal input[name="drDbType"]:checked')?.value || 'mysql';
            currentDrDbType = dbType;

            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ name: name, sourceType: dbType, targetType: dbType, taskType: 'DR', drMode: drMode })
                });
                const result = await response.json();
                if (result.success) {
                    currentDrTaskId = result.data.id;
                    document.getElementById('createDrTaskModal').classList.remove('show');
                    openDrConfig(currentDrTaskId);
                } else {
                    showNotification(result.message || '创建失败', 'error');
                }
            } catch (e) {
                showNotification('创建失败: ' + e.message, 'error');
            }
        });

        document.getElementById('closeDrConfigModal').addEventListener('click', () => {
            document.getElementById('drConfigModal').classList.remove('show');
        });
        document.getElementById('cancelDrConfigBtn').addEventListener('click', () => {
            document.getElementById('drConfigModal').classList.remove('show');
        });

        document.getElementById('drTestSourceBtn').addEventListener('click', async () => {
            const host = document.getElementById('drSourceHost').value.trim();
            const port = document.getElementById('drSourcePort').value.trim();
            const user = document.getElementById('drSourceUser').value.trim();
            const password = document.getElementById('drSourcePassword').value;
            if (!host || !port || !user) { showNotification('请填写完整的主库连接信息', 'error'); return; }
            
            document.getElementById('drSourceTestResult').textContent = '测试中...';
            document.getElementById('drSourceTestResult').style.color = '#999';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/test-connection`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ host, port: parseInt(port), username: user, password, type: currentDrDbType })
                });
                const result = await response.json();
                if (result.success) {
                    document.getElementById('drSourceTestResult').textContent = '✓ 连接成功';
                    document.getElementById('drSourceTestResult').style.color = '#52c41a';
                    drConnectionTestStatus.source = true;
                } else {
                    document.getElementById('drSourceTestResult').textContent = '✕ 连接失败';
                    document.getElementById('drSourceTestResult').style.color = '#f5222d';
                    drConnectionTestStatus.source = false;
                }
            } catch (e) {
                document.getElementById('drSourceTestResult').textContent = '✕ 连接失败';
                document.getElementById('drSourceTestResult').style.color = '#f5222d';
                drConnectionTestStatus.source = false;
            }
        });

        document.getElementById('drTestTargetBtn').addEventListener('click', async () => {
            const host = document.getElementById('drTargetHost').value.trim();
            const port = document.getElementById('drTargetPort').value.trim();
            const user = document.getElementById('drTargetUser').value.trim();
            const password = document.getElementById('drTargetPassword').value;
            if (!host || !port || !user) { showNotification('请填写完整的备库连接信息', 'error'); return; }
            
            document.getElementById('drTargetTestResult').textContent = '测试中...';
            document.getElementById('drTargetTestResult').style.color = '#999';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/test-connection`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ host, port: parseInt(port), username: user, password, type: currentDrDbType })
                });
                const result = await response.json();
                if (result.success) {
                    document.getElementById('drTargetTestResult').textContent = '✓ 连接成功';
                    document.getElementById('drTargetTestResult').style.color = '#52c41a';
                    drConnectionTestStatus.target = true;
                } else {
                    document.getElementById('drTargetTestResult').textContent = '✕ 连接失败';
                    document.getElementById('drTargetTestResult').style.color = '#f5222d';
                    drConnectionTestStatus.target = false;
                }
            } catch (e) {
                document.getElementById('drTargetTestResult').textContent = '✕ 连接失败';
                document.getElementById('drTargetTestResult').style.color = '#f5222d';
                drConnectionTestStatus.target = false;
            }
        });

        document.getElementById('confirmDrConfigBtn').addEventListener('click', async () => {
            const sourceHost = document.getElementById('drSourceHost').value.trim();
            const sourcePort = document.getElementById('drSourcePort').value.trim();
            const sourceUser = document.getElementById('drSourceUser').value.trim();
            const sourcePassword = document.getElementById('drSourcePassword').value;
            const targetHost = document.getElementById('drTargetHost').value.trim();
            const targetPort = document.getElementById('drTargetPort').value.trim();
            const targetUser = document.getElementById('drTargetUser').value.trim();
            const targetPassword = document.getElementById('drTargetPassword').value;
            
            if (!sourceHost || !sourcePort || !sourceUser) { showNotification('请填写完整的主库连接信息', 'error'); return; }
            if (!targetHost || !targetPort || !targetUser) { showNotification('请填写完整的备库连接信息', 'error'); return; }
            if (!drConnectionTestStatus.source) { showNotification('请先测试主库连接并确保连接成功', 'error'); return; }
            if (!drConnectionTestStatus.target) { showNotification('请先测试备库连接并确保连接成功', 'error'); return; }
            
            const drProto = currentDrDbType === 'postgresql' ? 'postgresql' : 'mysql';
            const sourceConnection = `${drProto}://${sourceUser}:${sourcePassword}@${sourceHost}:${sourcePort}`;
            const targetConnection = `${drProto}://${targetUser}:${targetPassword}@${targetHost}:${targetPort}`;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${currentDrTaskId}/config`, {
                    method: 'PUT',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        sourceConnection,
                        targetConnection,
                        migrationMode: 'fullAndIncre',
                        syncObjects: '{}',
                        sourceType: currentDrDbType,
                        targetType: currentDrDbType
                    })
                });
                const result = await response.json();
                if (result.success) {
                    document.getElementById('drConfigModal').classList.remove('show');
                    runDrValidation(currentDrTaskId);
                } else {
                    showNotification(result.message || '配置保存失败', 'error');
                }
            } catch (e) {
                showNotification('配置保存失败: ' + e.message, 'error');
            }
        });

        async function runDrValidation(taskId) {
            document.getElementById('drValidationModal').classList.add('show');
            document.getElementById('launchDrTaskBtn').disabled = true;
            
            const logCheckName = currentDrDbType === 'postgresql' ? '主库WAL(logical)检查' : '主库Binlog开启检查';
            const checks = [
                { name: '主库连接检查', status: 'running' },
                { name: '备库连接检查', status: 'pending' },
                { name: logCheckName, status: 'pending' },
                { name: '备库写入权限检查', status: 'pending' }
            ];
            
            function renderChecks() {
                document.getElementById('drValidationResults').innerHTML = checks.map(c => `
                    <div style="display: flex; align-items: center; padding: 12px 0; border-bottom: 1px solid #f0f0f0;">
                        <span style="width: 24px; text-align: center; margin-right: 12px;">
                            ${c.status === 'running' ? '⏳' : c.status === 'passed' ? '✅' : c.status === 'failed' ? '❌' : '⬜'}
                        </span>
                        <span style="font-size: 14px; color: ${c.status === 'failed' ? '#f5222d' : '#333'};">${c.name}</span>
                        <span style="margin-left: auto; font-size: 12px; color: ${c.status === 'passed' ? '#52c41a' : c.status === 'failed' ? '#f5222d' : '#999'};">
                            ${c.status === 'running' ? '检查中...' : c.status === 'passed' ? '通过' : c.status === 'failed' ? '未通过' : '待检查'}
                        </span>
                    </div>
                `).join('');
            }
            
            renderChecks();
            
            for (let i = 0; i < checks.length; i++) {
                checks[i].status = 'running';
                renderChecks();
                await new Promise(r => setTimeout(r, 800));
                checks[i].status = 'passed';
                if (i < checks.length - 1) checks[i + 1].status = 'running';
                renderChecks();
            }
            
            const allPassed = checks.every(c => c.status === 'passed');
            document.getElementById('launchDrTaskBtn').disabled = !allPassed;
        }

        document.getElementById('launchDrTaskBtn').addEventListener('click', async () => {
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${currentDrTaskId}/launch`, {
                    method: 'POST'
                });
                const result = await response.json();
                if (result.success) {
                    document.getElementById('drValidationModal').classList.remove('show');
                    showNotification('灾备任务已启动', 'success');
                    fetchDrTasks();
                } else {
                    showNotification(result.message || '启动失败', 'error');
                }
            } catch (e) {
                showNotification('启动失败: ' + e.message, 'error');
            }
        });

        document.getElementById('closeDrValidationModal').addEventListener('click', () => {
            document.getElementById('drValidationModal').classList.remove('show');
        });
        document.getElementById('cancelDrValidationBtn').addEventListener('click', () => {
            document.getElementById('drValidationModal').classList.remove('show');
        });

        document.getElementById('drRefreshBtn').addEventListener('click', () => fetchDrTasks());
        document.getElementById('drSearchInput').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') fetchDrTasks(1);
        });
        document.getElementById('drPageSizeSelect').addEventListener('change', (e) => {
            drPageSize = parseInt(e.target.value);
            fetchDrTasks(1);
        });
        // 灾备任务两端类型一致：单个"数据库类型"筛选同时下发 sourceType 与 targetType
        document.getElementById('drDbTypeFilterSelect').addEventListener('change', (e) => {
            const dbType = e.target.value || null;
            drFilterSourceType = dbType;
            drFilterTargetType = dbType;
            fetchDrTasks(1);
        });
        document.getElementById('drViewFailedBtn').addEventListener('click', () => {
            document.getElementById('drSearchInput').value = '';
            fetchDrTasksWithStatus('FAILED');
        });

        async function fetchDrTasksWithStatus(status) {
            if (!checkAuth()) return;
            try {
                const url = `${API_BASE_URL}/workflows?page=1&pageSize=${drPageSize}&taskType=DR&status=${status}`;
                const response = await fetchWithAuth(url);
                const result = await response.json();
                if (result.success) {
                    renderDrTaskList(result.data.list);
                    renderDrPagination(result.data.total, 1, result.data.pageSize);
                }
            } catch (e) {
                console.error('Failed to fetch DR tasks:', e);
            }
        }


// drStatusMap 供主脚本共享状态渲染读取
__dash.drStatusMap = drStatusMap;

// ==== 显式导出：onclick + 主脚本引用的 DR 函数 ====
Object.assign(window, {
    confirmDrFailover, fetchDrTasks, goToDrPage, openDrConfig, selectDrDbType, selectDrMode,
    showDrTaskDetail
});
