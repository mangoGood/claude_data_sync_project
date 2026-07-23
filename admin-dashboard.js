"use strict";
// admin-dashboard 主脚本。module 化第 1 阶段：启用严格模式（消除隐式全局、把
// 潜在的"赋值未声明变量"从静默建全局变成显式报错）。第 2 阶段转为 ES module，
// 顶层 state 变模块私有以根治跨特性全局状态污染，onclick 引用的函数经末尾块显式挂 window。
(function () {
// ↑ IIFE 模块封装：顶层声明改为 IIFE 作用域，不再自动挂到 window/全局命名空间；
//   只有下方 Object.assign 显式导出的（被 onclick 等处理器引用的）函数才对外可见。
//   顶层 state（let/var/非导出 const）就此私有化，根治跨特性全局状态污染。
//   仍是经典脚本上下文（非 ES module）：window.x= 导出与裸调用经全局对象回退照常工作，
//   零裸调用破坏风险；多文件 ES module 拆分为后续增量。
        // Global error handler
        window.onerror = function(message, source, lineno, colno, error) {
            console.error('Global error:', message, 'at', source, ':', lineno, ':', colno, error);
            return false;
        };
        
        window.addEventListener('unhandledrejection', function(event) {
            console.error('Unhandled promise rejection:', event.reason);
        });
        
        // API基础URL
        const API_BASE_URL = window.location.origin + '/api';
        
        // 当前页码
        let currentPage = 1;
        let pageSize = 10;
        let totalCount = 0;
        
        let sortField = 'created_at';
        let sortDirection = 'DESC';
        
        let currentStep = 0;
        
        let selectedSourceType = 'mysql';
        let selectedTargetType = 'mysql';
        
        let filterKeyword = null;
        let filterStatus = null;
        let filterSourceType = null;
        let filterTargetType = null;
        let isViewingFailedTasks = false;
        
        // 已选择的同步对象
        let selectedSyncObjects = {};
        
        // 数据库缓存
        let databasesCache = [];
        let tablesCache = {};
        
        // 数据库类型显示名（大小写固定）
        const dbTypeLabelMap = { 'mysql': 'MySQL', 'postgresql': 'PostgreSQL', 'oracle': 'Oracle', 'mongodb': 'MongoDB', 'elasticsearch': 'Elasticsearch' };
        function formatDbTypeLabel(type) {
            return dbTypeLabelMap[(type || '').toLowerCase()] || (type || 'MySQL');
        }

        // 状态映射
        const statusMap = {
            'CONFIGURING': { text: '配置中', class: 'status-configuring', icon: '⚙' },
            'PENDING': { text: '启动中', class: 'status-pending', icon: '' },
            'STARTING': { text: '启动中', class: 'status-starting', icon: '' },
            'FULL_MIGRATING': { text: '全量同步中', class: 'status-full-migrating', dot: true },
            'FULL_COMPLETED': { text: '全量同步完成', class: 'status-full-completed', icon: '✓' },
            'INCREMENT_RUNNING': { text: '增量同步中', class: 'status-increment-running', dot: true },
            'COMPLETED': { text: '已完成', class: 'status-completed', icon: '✓' },
            'FAILED': { text: '失败', class: 'status-failed', icon: '✗' },
            'PAUSED': { text: '已暂停', class: 'status-paused', icon: '' }
        };
        
        // 检查登录状态
        function checkAuth() {
            const token = localStorage.getItem('token');
            if (!token) {
                window.location.href = 'login.html';
                return false;
            }
            try {
                const payload = JSON.parse(atob(token.split('.')[1]));
                if (payload.exp && payload.exp * 1000 < Date.now()) {
                    showTokenExpiredModal();
                    return false;
                }
            } catch (e) {
            }
            return true;
        }
        
        // 获取当前用户信息
        function getCurrentUser() {
            const userStr = localStorage.getItem('user');
            return userStr ? JSON.parse(userStr) : null;
        }
        
        // 显示用户信息
        function displayUserInfo() {
            const user = getCurrentUser();
            if (user) {
                document.querySelectorAll('.username-display').forEach(el => { el.textContent = user.username; });
            }
        }

        // 账号设置弹窗
        function openAccountSettings() {
            const user = getCurrentUser();
            if (!user) return;
            document.getElementById('accountUsernameDisplay').textContent = user.username;
            document.getElementById('accountEmailDisplay').textContent = user.email || '-';
            const oldPasswordInput = document.getElementById('oldPasswordInput');
            const newPasswordInput = document.getElementById('newPasswordInput');
            const confirmPasswordInput = document.getElementById('confirmPasswordInput');
            [oldPasswordInput, newPasswordInput, confirmPasswordInput].forEach(input => {
                input.value = '';
                input.classList.remove('error');
            });
            document.getElementById('accountSettingsModal').classList.add('show');
        }

        function closeAccountSettings() {
            document.getElementById('accountSettingsModal').classList.remove('show');
        }

        document.getElementById('closeAccountSettingsModal').addEventListener('click', closeAccountSettings);
        document.getElementById('cancelAccountSettingsBtn').addEventListener('click', closeAccountSettings);

        document.getElementById('confirmChangePasswordBtn').addEventListener('click', async () => {
            const oldPasswordInput = document.getElementById('oldPasswordInput');
            const newPasswordInput = document.getElementById('newPasswordInput');
            const confirmPasswordInput = document.getElementById('confirmPasswordInput');
            const oldPassword = oldPasswordInput.value;
            const newPassword = newPasswordInput.value;
            const confirmPassword = confirmPasswordInput.value;

            [oldPasswordInput, newPasswordInput, confirmPasswordInput].forEach(input => input.classList.remove('error'));

            let hasError = false;
            if (!oldPassword) { oldPasswordInput.classList.add('error'); hasError = true; }
            if (!newPassword || newPassword.length < 6) { newPasswordInput.classList.add('error'); hasError = true; }
            if (!confirmPassword || confirmPassword !== newPassword) { confirmPasswordInput.classList.add('error'); hasError = true; }
            if (hasError) {
                showNotification('请正确填写密码信息（新密码至少6位，且两次输入一致）', 'error');
                return;
            }

            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/auth/change-password`, {
                    method: 'POST',
                    body: JSON.stringify({ oldPassword, newPassword })
                });
                const result = await response.json();
                if (result.success) {
                    showNotification('密码修改成功，请重新登录', 'success');
                    closeAccountSettings();
                    setTimeout(() => logout(), 1200);
                } else {
                    oldPasswordInput.classList.add('error');
                    showNotification(result.message || '密码修改失败', 'error');
                }
            } catch (e) {
                showNotification('密码修改失败，请稍后重试', 'error');
            }
        });

        // WebSocket 连接
        let stompClient = null;
        
        function connectWebSocket() {
            const user = getCurrentUser();
            if (!user) return;
            
            const token = localStorage.getItem('token') || '';
            const socket = new SockJS(window.location.origin + '/ws?token=' + encodeURIComponent(token));
            stompClient = Stomp.over(socket);
            
            stompClient.connect({}, function(frame) {
                console.log('WebSocket 连接已建立:', frame);
                
                // 订阅全局任务状态更新
                stompClient.subscribe('/topic/task-status', function(message) {
                    console.log('收到全局任务状态更新:', message.body);
                    try {
                        const data = JSON.parse(message.body);
                        handleStatusUpdate(data);
                    } catch (e) {
                        console.error('解析消息失败:', e);
                    }
                });
                
                // 订阅用户专属任务状态更新
                stompClient.subscribe('/user/queue/task-status', function(message) {
                    console.log('收到用户任务状态更新:', message.body);
                    try {
                        const data = JSON.parse(message.body);
                        handleStatusUpdate(data);
                    } catch (e) {
                        console.error('解析消息失败:', e);
                    }
                });
                
            }, function(error) {
                console.error('WebSocket 连接错误:', error);
                // 5秒后重连
                setTimeout(connectWebSocket, 5000);
            });
        }
        
        // 处理状态更新
        function handleStatusUpdate(data) {
            console.log('处理状态更新:', data);
            
            const taskId = data.task_id || data.taskId;
            const row = document.querySelector(`.table-row[data-id="${taskId}"]`);
            if (!row) {
                console.log('未找到对应的任务行:', taskId);
                return;
            }
            
            let statusInfo = statusMap[data.status] || statusMap['PENDING'];
            const migrationMode = data.migration_mode;
            const isFullAndIncre = migrationMode === 'fullAndIncre';
            
            // 全量+增量模式下，FULL_COMPLETED 状态显示为"增量同步中"
            if (data.status === 'FULL_COMPLETED' && isFullAndIncre) {
                statusInfo = { text: '增量同步中', class: 'status-increment-running', dot: true };
            }
            
            const statusCell = row.querySelector('.col-status');
            
            if (statusCell) {
                let statusHtml = `<span class="status-tag ${statusInfo.class}">`;
                if (statusInfo.dot) {
                    statusHtml += `<span class="status-dot"></span>`;
                } else if (statusInfo.icon) {
                    statusHtml += `<span class="status-icon">${statusInfo.icon}</span>`;
                }
                statusHtml += `${statusInfo.text}</span>`;
                
                statusCell.innerHTML = statusHtml;
            }
            
            const actionCell = row.querySelector('.col-action .action-btns');
            if (actionCell) {
                const isStarting = data.status === 'STARTING' || data.status === 'FULL_MIGRATING';
                const isIncrementRunning = data.status === 'INCREMENT_RUNNING';
                const isPaused = data.status === 'PAUSED';
                const isFailed = data.status === 'FAILED';
                const isCompleted = data.status === 'COMPLETED' || data.status === 'FULL_COMPLETED';
                const isConfiguring = data.status === 'CONFIGURING';
                
                const rowDataset = row.dataset;
                const taskName = rowDataset.name || '';
                
                let actionHtml = '';
                if (isConfiguring) {
                    actionHtml = `<button class="action-btn" onclick="openTaskConfig('${taskId}')">配置</button><button class="action-btn delete" onclick="deleteWorkflow('${taskId}', '${taskName.replace(/'/g, "\\'")}')">删除</button>`;
                } else if (isFailed) {
                    actionHtml = `<button class="action-btn" onclick="retryWorkflow('${taskId}')">恢复</button><button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                } else if (isStarting) {
                    actionHtml = `<button class="action-btn monitor" onclick="viewMetrics('${taskId}')">监控</button><button class="action-btn" onclick="pauseWorkflow('${taskId}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                } else if (isIncrementRunning) {
                    actionHtml = `<button class="action-btn monitor" onclick="viewMetrics('${taskId}')">监控</button><button class="action-btn" onclick="pauseWorkflow('${taskId}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                } else if (isPaused) {
                    actionHtml = `<button class="action-btn" onclick="resumeWorkflow('${taskId}')">恢复</button>`;
                } else if (isCompleted) {
                    actionHtml = `<button class="action-btn delete" onclick="deleteWorkflow('${taskId}', '${taskName.replace(/'/g, "\\'")}')">删除</button>`;
                } else {
                    actionHtml = `<button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                }
                actionCell.innerHTML = actionHtml;
            }

            const monitorCell = row.querySelector('.col-monitor');
            if (monitorCell && (data.status === 'INCREMENT_RUNNING' || data.status === 'FULL_COMPLETED')) {
                const rpoMs = data.rpo_ms !== undefined ? data.rpo_ms : data.rpoMs;
                const rtoMs = data.rto_ms !== undefined ? data.rto_ms : data.rtoMs;
                const rpoColor = rpoMs != null && rpoMs < 5000 ? '#52c41a' : rpoMs != null && rpoMs < 30000 ? '#faad14' : rpoMs != null ? '#ff4d4f' : '#999';
                const rtoColor = rtoMs != null && rtoMs < 5000 ? '#52c41a' : rtoMs != null && rtoMs < 30000 ? '#faad14' : rtoMs != null ? '#ff4d4f' : '#999';
                monitorCell.innerHTML = `<div style="font-size: 11px; line-height: 1.6;">
                    <div style="color: ${rpoColor};">RPO: ${rpoMs != null ? formatDelay(rpoMs) : '-'}</div>
                    <div style="color: ${rtoColor};">RTO: ${rtoMs != null ? formatDelay(rtoMs) : '-'}</div>
                </div>`;
            } else if (monitorCell) {
                monitorCell.innerHTML = '<span style="color: #999;">-</span>';
            }

            const drRow = document.querySelector(`#drTableBody .table-row[data-task-id="${taskId}"]`);
            if (drRow) {
                const _drSM = (window.__dash && window.__dash.drStatusMap) || {};
                let drStatusInfo = _drSM[data.status] || _drSM['PENDING'] || { text: data.status, class: 'status-pending' };
                if (data.status === 'FULL_COMPLETED') {
                    drStatusInfo = { text: '灾备中', class: 'status-increment-running', dot: true };
                }
                const drStatusCell = drRow.querySelector('.col-status');
                if (drStatusCell) {
                    let drStatusHtml = `<span class="status-tag ${drStatusInfo.class}">`;
                    if (drStatusInfo.dot) drStatusHtml += '<span class="status-dot"></span>';
                    else if (drStatusInfo.icon) drStatusHtml += `<span class="status-icon">${drStatusInfo.icon}</span>`;
                    drStatusHtml += `${drStatusInfo.text}</span>`;
                    drStatusCell.innerHTML = drStatusHtml;
                }
                const drMonitorCell = drRow.querySelector('.col-monitor');
                if (drMonitorCell && (data.status === 'INCREMENT_RUNNING' || data.status === 'FULL_COMPLETED')) {
                    const rpoMs = data.rpo_ms !== undefined ? data.rpo_ms : data.rpoMs;
                    const rtoMs = data.rto_ms !== undefined ? data.rto_ms : data.rtoMs;
                    let drMonitorHtml = '';
                    if (rtoMs != null) drMonitorHtml += `<div style="font-size: 12px;">RTO: ${formatDelay(rtoMs)}</div>`;
                    if (rpoMs != null) drMonitorHtml += `<div style="font-size: 12px;">RPO: ${formatDelay(rpoMs)}</div>`;
                    drMonitorCell.innerHTML = drMonitorHtml || '-';
                }
                const drActionCell = drRow.querySelector('.col-action .action-btns');
                if (drActionCell) {
                    let drActionHtml = '';
                    if (data.status === 'CONFIGURING') {
                        drActionHtml = `<button class="action-btn" onclick="openDrConfig('${taskId}', '${data.source_type || 'mysql'}')">配置</button>`;
                    } else if (data.status === 'FAILED') {
                        drActionHtml = `<button class="action-btn" onclick="retryWorkflow('${taskId}')">恢复</button><button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                    } else if (data.status === 'STARTING' || data.status === 'FULL_MIGRATING') {
                        drActionHtml = `<button class="action-btn monitor" onclick="viewMetrics('${taskId}')">监控</button><button class="action-btn" onclick="pauseWorkflow('${taskId}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                    } else if (data.status === 'INCREMENT_RUNNING') {
                        drActionHtml = `<button class="action-btn monitor" onclick="viewMetrics('${taskId}')">监控</button><button class="action-btn" onclick="pauseWorkflow('${taskId}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                    } else if (data.status === 'PAUSED') {
                        drActionHtml = `<button class="action-btn" onclick="resumeWorkflow('${taskId}')">恢复</button><button class="action-btn stop" onclick="stopWorkflow('${taskId}')">结束</button>`;
                    } else if (data.status === 'COMPLETED' || data.status === 'FULL_COMPLETED') {
                        drActionHtml = `<button class="action-btn stop" onclick="deleteWorkflow('${taskId}')">删除</button>`;
                    } else {
                        drActionHtml = '<span style="color: #999; font-size: 12px;">-</span>';
                    }
                    drActionCell.innerHTML = drActionHtml;
                }
            }
        }
        
        // 退出登录
        function logout() {
            localStorage.removeItem('token');
            localStorage.removeItem('user');
            window.location.href = 'login.html';
        }

        let _tokenExpiredHandled = false;
        let _tokenExpiredTimer = null;

        function handleTokenExpired() {
            if (_tokenExpiredTimer) {
                clearInterval(_tokenExpiredTimer);
                _tokenExpiredTimer = null;
            }
            localStorage.removeItem('token');
            localStorage.removeItem('user');
            window.location.href = 'login.html?expired=1';
        }

        function showTokenExpiredModal() {
            if (_tokenExpiredHandled) return;
            _tokenExpiredHandled = true;

            const overlay = document.getElementById('tokenExpiredOverlay');
            const countdownEl = document.getElementById('tokenExpiredCountdown');
            overlay.style.display = 'flex';

            let seconds = 5;
            countdownEl.textContent = `${seconds} 秒后自动跳转到登录页`;

            _tokenExpiredTimer = setInterval(() => {
                seconds--;
                if (seconds <= 0) {
                    clearInterval(_tokenExpiredTimer);
                    _tokenExpiredTimer = null;
                    handleTokenExpired();
                } else {
                    countdownEl.textContent = `${seconds} 秒后自动跳转到登录页`;
                }
            }, 1000);
        }

        function getAuthHeaders() {
            const token = localStorage.getItem('token');
            return {
                'Content-Type': 'application/json',
                'Authorization': token ? `Bearer ${token}` : ''
            };
        }

        async function fetchWithAuth(url, options = {}) {
            if (!options.headers) {
                options.headers = getAuthHeaders();
            }
            const response = await fetch(url, options);

            // 401 = token 缺失/过期/失效（后端已统一未认证语义）；403 兜底兼容旧后端
            // （Spring Security 默认对无效 token 返回 403，本应用无角色级授权，403 同样意味着需要重新登录）
            if (response.status === 401 || response.status === 403) {
                showTokenExpiredModal();
                return new Promise(() => {});
            }

            return response;
        }
        
        // 格式化时间（根据用户所在时区显示）
        const ERROR_CODE_MAP = {
            'E1001': { desc: '源数据库连接失败', solution: '请检查源数据库地址、端口、用户名和密码是否正确，确认数据库服务已启动且网络可达' },
            'E1002': { desc: '目标数据库连接失败', solution: '请检查目标数据库地址、端口、用户名和密码是否正确，确认数据库服务已启动且网络可达' },
            'E1003': { desc: '源数据库认证失败', solution: '请检查源数据库用户名和密码是否正确，确认用户有远程登录权限' },
            'E1004': { desc: '目标数据库认证失败', solution: '请检查目标数据库用户名和密码是否正确，确认用户有远程登录权限' },
            'E1005': { desc: '源数据库网络不可达', solution: '请检查源数据库主机地址是否正确，确认网络连通性和防火墙规则' },
            'E1006': { desc: '目标数据库网络不可达', solution: '请检查目标数据库主机地址是否正确，确认网络连通性和防火墙规则' },
            'E2001': { desc: '源数据库Binlog未开启', solution: '请在MySQL配置文件中设置 log_bin=ON 并重启数据库服务' },
            'E2002': { desc: 'Binlog格式不是ROW', solution: '请在MySQL配置文件中设置 binlog_format=ROW 并重启数据库服务' },
            'E2003': { desc: 'Binlog Row Image不是FULL', solution: '请在MySQL配置文件中设置 binlog_row_image=FULL 并重启数据库服务' },
            'E2004': { desc: '源数据库server_id未设置', solution: '请在MySQL配置文件中设置 server_id 为非0值并重启数据库服务' },
            'E2005': { desc: 'Checkpoint初始化失败', solution: '请检查源数据库连接是否正常，确认用户有REPLICATION权限' },
            'E2006': { desc: 'PostgreSQL WAL LSN获取失败', solution: '请检查PostgreSQL连接是否正常，确认用户有replication权限' },
            'E2007': { desc: 'Oracle SCN获取失败', solution: '请检查Oracle连接是否正常，确认用户有SELECT ANY DICTIONARY权限且数据库处于ARCHIVELOG模式' },
            'E2008': { desc: 'Oracle LogMiner会话启动失败', solution: '请确认数据库处于ARCHIVELOG模式，用户具有EXECUTE CATALOG ROLE权限，且redo日志可访问' },
            'E3001': { desc: 'Capture进程启动失败', solution: '请检查Agent日志，确认capture模块JAR包存在且配置正确' },
            'E3002': { desc: 'Capture进程异常退出', solution: '请检查Agent日志，确认源数据库连接正常且binlog/WAL可访问' },
            'E3003': { desc: 'Extract进程启动失败', solution: '请检查Agent日志，确认extract模块JAR包存在且配置正确' },
            'E3004': { desc: '增量同步进程启动失败', solution: '请检查Agent日志，确认increment模块JAR包存在且配置正确' },
            'E3005': { desc: '增量同步进程异常退出', solution: '请检查Agent日志，可能是目标数据库连接中断或SQL执行异常' },
            'E4001': { desc: '全量同步失败', solution: '请检查Agent日志，确认源库和目标库连接正常，表结构和数据无异常' },
            'E4002': { desc: '全量同步超时', solution: '请检查数据量是否过大，考虑分批同步或优化网络带宽' },
            'E4003': { desc: '目标数据库写入失败', solution: '请检查目标数据库磁盘空间、表结构是否与源库一致、是否有写入权限' },
            'E4004': { desc: '主键冲突写入失败', solution: '请检查目标库是否已存在相同主键的数据，可通过恢复任务自动跳过重复数据' },
            'E5001': { desc: '源数据库配置为空', solution: '请检查任务创建时源数据库连接信息是否填写完整' },
            'E5002': { desc: '目标数据库配置为空', solution: '请检查任务创建时目标数据库连接信息是否填写完整' },
            'E5003': { desc: '连接串解析失败', solution: '请检查连接串格式是否正确，正确格式: mysql://user:pass@host:port 或 postgresql://user:pass@host:port 或 oracle://user:pass@host:port/service 或 mongodb://user:pass@host:port 或 elastic://user:pass@host:port' },
            'E9999': { desc: '未知错误', solution: '请查看Agent日志获取详细错误信息，或联系技术支持' }
        };

        function getErrorCodeDescription(code) {
            if (!code) return '未知错误';
            return ERROR_CODE_MAP[code] ? ERROR_CODE_MAP[code].desc : '未知错误 (' + code + ')';
        }

        function getErrorCodeSolution(code) {
            if (!code) return '请查看日志获取详细错误信息';
            return ERROR_CODE_MAP[code] ? ERROR_CODE_MAP[code].solution : '请查看Agent日志获取详细错误信息，或联系技术支持';
        }

        function formatDelay(ms) {
            if (ms == null || ms < 0) return '-';
            if (ms < 1000) return ms + 'ms';
            if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
            if (ms < 3600000) return Math.floor(ms / 60000) + 'm' + Math.floor((ms % 60000) / 1000) + 's';
            return Math.floor(ms / 3600000) + 'h' + Math.floor((ms % 3600000) / 60000) + 'm';
        }

        function formatDateTime(dateString) {
            if (!dateString) return '-';
            
            const date = new Date(dateString);
            
            // 检查日期是否有效
            if (isNaN(date.getTime())) {
                return '-';
            }
            
            // 获取用户所在时区
            const userTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
            
            // 格式化选项 - 使用本地时区
            const options = {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false
            };
            
            // 格式化日期（使用本地时区）
            const formatted = new Intl.DateTimeFormat('zh-CN', options).format(date);
            
            // 获取时区偏移（基于当前日期，因为夏令时可能会影响偏移量）
            const offset = -date.getTimezoneOffset() / 60;
            const offsetStr = offset >= 0 ? `UTC+${offset}` : `UTC${offset}`;
            
            return `${formatted} (${offsetStr})`;
        }
        
        async function fetchWorkflows() {
            if (!checkAuth()) return;
            
            try {
                const user = getCurrentUser();
                const userId = user ? user.id : null;
                let url = `${API_BASE_URL}/workflows?page=${currentPage}&pageSize=${pageSize}${userId ? `&userId=${userId}` : ''}`;
                
                url += `&sortBy=${sortField}&sortDirection=${sortDirection}`;
                url += `&taskType=SYNC`;
                
                if (filterKeyword) {
                    url += `&keyword=${encodeURIComponent(filterKeyword)}`;
                }
                if (filterStatus) {
                    url += `&status=${filterStatus}`;
                }
                if (filterSourceType) {
                    url += `&sourceType=${filterSourceType}`;
                }
                if (filterTargetType) {
                    url += `&targetType=${filterTargetType}`;
                }

                const response = await fetchWithAuth(url);

                const result = await response.json();
                console.log('API 返回结果:', result);
                
                if (result.success) {
                    totalCount = result.data.total;
                    console.log('任务列表:', result.data.list);
                    console.log('总数:', result.data.total);
                    renderTable(result.data.list);
                    updatePagination(result.data.total);
                }
            } catch (error) {
                console.error('获取任务列表失败:', error);
                showNotification('获取任务列表失败', 'error');
            }
        }
        
        async function fetchFailedWorkflows() {
            if (!checkAuth()) return;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/failed`);
                
                const result = await response.json();
                
                if (result.success) {
                    isViewingFailedTasks = true;
                    totalCount = result.data.total;
                    renderTable(result.data.list);
                    updatePagination(result.data.total);
                    
                    document.getElementById('viewFailedTasksBtn').textContent = '查看全部任务';
                    showNotification(`共找到 ${result.data.total} 个异常任务`, 'info');
                }
            } catch (error) {
                console.error('获取异常任务失败:', error);
                showNotification('获取异常任务失败', 'error');
            }
        }
        
        // 处理排序
        function handleSort(field) {
            if (sortField === field) {
                // 点击同一列，切换排序方向
                sortDirection = sortDirection === 'ASC' ? 'DESC' : 'ASC';
            } else {
                // 点击不同列，设置新字段并默认降序
                sortField = field;
                sortDirection = 'DESC';
            }
            
            // 更新排序图标
            updateSortIcons();
            
            // 重新获取数据
            fetchWorkflows();
        }
        
        // 更新排序图标
        function updateSortIcons() {
            const sortableFields = ['name', 'status', 'created_at', 'is_billing'];
            
            sortableFields.forEach(field => {
                const icon = document.getElementById(`sort-icon-${field}`);
                if (icon) {
                    icon.classList.remove('active', 'asc', 'desc');
                    
                    if (sortField === field) {
                        icon.classList.add('active');
                        icon.classList.add(sortDirection.toLowerCase());
                    }
                }
            });
        }
        
        // 渲染表格
        function renderTable(workflows) {
            console.log('renderTable 被调用，workflows:', workflows);
            const tableBody = document.getElementById('tableBody');
            const emptyState = document.getElementById('emptyState');
            
            console.log('tableBody:', tableBody);
            console.log('emptyState:', emptyState);
            
            if (!workflows || workflows.length === 0) {
                console.log('没有数据，显示空状态');
                emptyState.style.display = 'flex';
                const existingRows = tableBody.querySelectorAll('.table-row');
                existingRows.forEach(row => row.remove());
                return;
            }
            
            console.log('有数据，开始渲染');
            emptyState.style.display = 'none';
            
            // 清除已有的表格行
            const existingRows = tableBody.querySelectorAll('.table-row');
            existingRows.forEach(row => row.remove());
            
            // 添加新行
            workflows.forEach(workflow => {
                const row = createTableRow(workflow);
                tableBody.appendChild(row);
            });
        }
        
        // 创建表格行
        function createTableRow(workflow) {
            const row = document.createElement('div');
            row.className = 'table-row';
            row.dataset.id = workflow.id;
            
            let statusInfo = statusMap[workflow.status] || statusMap['pending'];
            const isFullAndIncre = workflow.migration_mode === 'fullAndIncre';
            
            // 全量+增量模式下，FULL_COMPLETED 状态显示为"增量同步中"
            if (workflow.status === 'FULL_COMPLETED' && isFullAndIncre) {
                statusInfo = { text: '增量同步中', class: 'status-increment-running', dot: true };
            }
            
            row.dataset.name = workflow.name;
            
            row.innerHTML = `
                <div class="table-cell col-checkbox">
                    <input type="checkbox" class="batch-cb" data-id="${workflow.id}" ${window.__dash.batchSelected.has(workflow.id) ? 'checked' : ''}
                           onchange="advToggleBatchSelect('${workflow.id}', this.checked)">
                </div>
                <div class="table-cell col-name">
                    <div>
                        <div><span style="background: #f6ffed; color: #52c41a; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-right: 4px;">同步</span>${escapeHtml(workflow.name)}</div>
                        <div style="font-size: 11px; color: #1890ff; cursor: pointer;" onclick="${workflow.status === 'CONFIGURING' ? `openTaskConfig('${workflow.id}')` : `showTaskDetail('${workflow.id}')`}">${workflow.id}</div>
                    </div>
                </div>
                <div class="table-cell col-dbtype">${formatDbTypeLabel(workflow.source_type)}→ ${formatDbTypeLabel(workflow.target_type)}</div>
                <div class="table-cell col-status">
                    <span class="status-tag ${statusInfo.class}">
                        ${statusInfo.dot ? '<span class="status-dot"></span>' : statusInfo.icon ? `<span class="status-icon">${statusInfo.icon}</span>` : ''}
                        ${statusInfo.text}
                    </span>
                </div>
                <div class="table-cell col-created">
                    <div style="font-size: 12px; color: #666;">${formatDateTime(workflow.created_at)}</div>
                </div>
                <div class="table-cell col-monitor">
                    ${workflow.status === 'INCREMENT_RUNNING' || workflow.status === 'FULL_COMPLETED' ?
                        `<div style="font-size: 11px; line-height: 1.6;">
                            <div style="color: ${workflow.rpo_ms != null && workflow.rpo_ms < 5000 ? '#52c41a' : workflow.rpo_ms != null && workflow.rpo_ms < 30000 ? '#faad14' : workflow.rpo_ms != null ? '#ff4d4f' : '#999'};">
                                RPO: ${workflow.rpo_ms != null ? formatDelay(workflow.rpo_ms) : '-'}
                            </div>
                            <div style="color: ${workflow.rto_ms != null && workflow.rto_ms < 5000 ? '#52c41a' : workflow.rto_ms != null && workflow.rto_ms < 30000 ? '#faad14' : workflow.rto_ms != null ? '#ff4d4f' : '#999'};">
                                RTO: ${workflow.rto_ms != null ? formatDelay(workflow.rto_ms) : '-'}
                            </div>
                        </div>` :
                        '<span style="color: #999;">-</span>'}
                </div>
                <div class="table-cell col-billing">
                    ${workflow.is_billing ? '<span style="color: #52c41a;">是</span>' : '<span style="color: #999;">否</span>'}
                </div>
                <div class="table-cell col-action">
                    <div class="action-btns">
                        ${workflow.status === 'CONFIGURING' ?
                            `<button class="action-btn" onclick="openTaskConfig('${workflow.id}')">配置</button><button class="action-btn delete" onclick="deleteWorkflow('${workflow.id}', '${escapeAttr(workflow.name)}')">删除</button>` :
                        workflow.status === 'FAILED' ? 
                            `<button class="action-btn" onclick="retryWorkflow('${workflow.id}')">恢复</button><button class="action-btn stop" onclick="stopWorkflow('${workflow.id}')">结束</button>` : 
                            (workflow.status === 'STARTING' || workflow.status === 'FULL_MIGRATING') ? 
                            `<button class="action-btn monitor" onclick="viewMetrics('${workflow.id}')">监控</button><button class="action-btn" onclick="pauseWorkflow('${workflow.id}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${workflow.id}')">结束</button>` : 
                            workflow.status === 'INCREMENT_RUNNING' ?
                            `<button class="action-btn monitor" onclick="viewMetrics('${workflow.id}')">监控</button><button class="action-btn" onclick="pauseWorkflow('${workflow.id}')">暂停</button><button class="action-btn stop" onclick="stopWorkflow('${workflow.id}')">结束</button>` :
                            workflow.status === 'PAUSED' ?
                            `<button class="action-btn" onclick="resumeWorkflow('${workflow.id}')">恢复</button>` :
                            (workflow.status === 'COMPLETED' || workflow.status === 'FULL_COMPLETED') ? 
                            `<button class="action-btn delete" onclick="deleteWorkflow('${workflow.id}', '${escapeAttr(workflow.name)}')">删除</button>` : 
                            `<button class="action-btn stop" onclick="stopWorkflow('${workflow.id}')">结束</button>`}
                    </div>
                </div>
            `;
            
            return row;
        }
        
        // 更新分页信息
        function updatePagination(total) {
            const totalPages = Math.ceil(total / pageSize);
            
            document.getElementById('paginationInfo').textContent = 
                `总条数：${total} | 第 ${currentPage}/${totalPages} 页`;
            
            // 更新上一页/下一页按钮状态
            document.getElementById('prevPageBtn').disabled = currentPage <= 1;
            document.getElementById('nextPageBtn').disabled = currentPage >= totalPages || totalPages === 0;
            
            // 生成页码按钮
            renderPageNumbers(totalPages);
        }
        
        // 渲染页码按钮
        function renderPageNumbers(totalPages) {
            const pageNumbersContainer = document.getElementById('pageNumbers');
            
            if (totalPages <= 0) {
                pageNumbersContainer.innerHTML = '';
                return;
            }
            
            let html = '';
            
            if (totalPages <= 7) {
                for (let i = 1; i <= totalPages; i++) {
                    html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToPage(${i})">${i}</button>`;
                }
            } else {
                html += `<button class="page-btn ${1 === currentPage ? 'active' : ''}" onclick="goToPage(1)">1</button>`;
                
                if (currentPage <= 4) {
                    for (let i = 2; i <= Math.min(5, totalPages - 1); i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToPage(${i})">${i}</button>`;
                    }
                    html += `<span class="page-ellipsis" onclick="goToPage(${Math.min(currentPage + 5, Math.floor(totalPages / 2))})">···</span>`;
                } else if (currentPage >= totalPages - 3) {
                    html += `<span class="page-ellipsis" onclick="goToPage(${Math.max(currentPage - 5, Math.floor(totalPages / 2))})">···</span>`;
                    for (let i = totalPages - 4; i <= totalPages - 1; i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToPage(${i})">${i}</button>`;
                    }
                } else {
                    html += `<span class="page-ellipsis" onclick="goToPage(${Math.max(1, currentPage - 5)})">···</span>`;
                    for (let i = currentPage - 1; i <= currentPage + 1; i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToPage(${i})">${i}</button>`;
                    }
                    html += `<span class="page-ellipsis" onclick="goToPage(${Math.min(totalPages, currentPage + 5)})">···</span>`;
                }
                
                html += `<button class="page-btn ${totalPages === currentPage ? 'active' : ''}" onclick="goToPage(${totalPages})">${totalPages}</button>`;
            }
            
            pageNumbersContainer.innerHTML = html;
        }
        
        // 跳转到指定页
        function goToPage(page) {
            const totalPages = Math.ceil(totalCount / pageSize);
            
            if (page < 1 || page > totalPages) {
                return;
            }
            
            currentPage = page;
            fetchWorkflows();
        }
        
        // 更改每页显示条数
        function changePageSize(newPageSize) {
            pageSize = parseInt(newPageSize, 10);
            currentPage = 1; // 重置到第一页
            fetchWorkflows();
        }
        
        let isSubmitting = false;
        
        let connectionTestStatus = {
            source: null,
            target: null
        };
        
        function buildConnectionString(type) {
            const host = document.getElementById(type + 'Host').value.trim();
            const port = document.getElementById(type + 'Port').value.trim();
            const username = document.getElementById(type + 'Username').value.trim();
            const password = document.getElementById(type + 'Password').value.trim();
            const dbType = (type === 'source') ? selectedSourceType : selectedTargetType;
            const protocol = dbType === 'postgresql' ? 'postgresql' : (dbType === 'oracle' ? 'oracle' : 'mysql');

            if (!host || !port || !username) return '';

            let connStr = `${protocol}://${username}:${password}@${host}:${port}`;

            if (dbType === 'postgresql' || dbType === 'oracle') {
                let dbNameInput = document.getElementById(type + 'DbName');
                if (!dbNameInput) dbNameInput = document.getElementById(type + 'DbNameInput');
                if (dbNameInput && dbNameInput.value.trim()) {
                    connStr += '/' + dbNameInput.value.trim();
                }
            }

            return connStr;
        }
        
        function buildConnectionStringWithDb(type, dbName) {
            const base = buildConnectionString(type);
            if (!base) return '';
            return `${base}/${dbName}`;
        }
        
        function onConnectionFieldChange(type) {
            connectionTestStatus[type] = null;
            const statusDiv = document.getElementById(type + 'ConnectionStatus');
            statusDiv.className = 'connection-status';
            statusDiv.textContent = '';
            
            const hostInput = document.getElementById(type + 'Host');
            const portInput = document.getElementById(type + 'Port');
            const usernameInput = document.getElementById(type + 'Username');
            const passwordInput = document.getElementById(type + 'Password');
            
            [hostInput, portInput, usernameInput, passwordInput].forEach(input => input.classList.remove('error'));
            
            let dbNameInput = document.getElementById(type + 'DbName');
            if (!dbNameInput) dbNameInput = document.getElementById(type + 'DbNameInput');
            if (dbNameInput) dbNameInput.classList.remove('error');
            
            const host = hostInput.value.trim();
            const port = portInput.value.trim();
            const username = usernameInput.value.trim();
            const password = passwordInput.value.trim();
            
            if (host && !/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?)*)$/.test(host)) {
                hostInput.classList.add('error');
            }
            if (port && !/^\d+$/.test(port)) {
                portInput.classList.add('error');
            }
            if (username && !/^[a-zA-Z0-9_]+$/.test(username)) {
                usernameInput.classList.add('error');
            }
            
            const hiddenInput = document.getElementById(type + 'Connection');
            hiddenInput.value = buildConnectionString(type);
        }
        
        function validateConnectionFields(type) {
            const hostInput = document.getElementById(type + 'Host');
            const portInput = document.getElementById(type + 'Port');
            const usernameInput = document.getElementById(type + 'Username');
            const passwordInput = document.getElementById(type + 'Password');
            
            const host = hostInput.value.trim();
            const port = portInput.value.trim();
            const username = usernameInput.value.trim();
            const password = passwordInput.value.trim();
            
            let errors = [];
            
            [hostInput, portInput, usernameInput, passwordInput].forEach(input => input.classList.remove('error'));
            
            if (!host) {
                errors.push('IP地址');
                hostInput.classList.add('error');
            } else if (!/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?)*)$/.test(host)) {
                errors.push('IP地址格式不正确');
                hostInput.classList.add('error');
            }
            
            if (!port) {
                errors.push('端口号');
                portInput.classList.add('error');
            } else if (!/^\d+$/.test(port) || parseInt(port) < 1 || parseInt(port) > 65535) {
                errors.push('端口号范围1-65535');
                portInput.classList.add('error');
            }
            
            if (!username) {
                errors.push('用户名');
                usernameInput.classList.add('error');
            } else if (!/^[a-zA-Z0-9_]+$/.test(username)) {
                errors.push('用户名格式不正确');
                usernameInput.classList.add('error');
            }
            
            if (!password) {
                errors.push('密码');
                passwordInput.classList.add('error');
            }
            
            const dbType = (type === 'source') ? selectedSourceType : selectedTargetType;
            if (dbType === 'postgresql') {
                let dbNameInput = document.getElementById(type + 'DbName');
                if (!dbNameInput) dbNameInput = document.getElementById(type + 'DbNameInput');
                if (dbNameInput && !dbNameInput.value.trim()) {
                    errors.push('数据库名称(PG必填)');
                    dbNameInput.classList.add('error');
                } else if (dbNameInput) {
                    dbNameInput.classList.remove('error');
                }
            }
            
            return errors;
        }
        
        function testConnection(type) {
            const statusDiv = document.getElementById(type + 'ConnectionStatus');
            const testBtn = document.getElementById('test' + (type === 'source' ? 'Source' : 'Target') + 'Btn');
            
            const errors = validateConnectionFields(type);
            if (errors.length > 0) {
                showNotification('请正确填写: ' + errors.join('、'), 'error');
                return;
            }
            
            const connection = buildConnectionString(type);
            if (!connection) {
                showNotification('请先填写' + (type === 'source' ? '源' : '目标') + '数据库连接信息', 'error');
                return;
            }
            
            const dbType = (type === 'source') ? (selectedSourceType || 'mysql') : (selectedTargetType || 'mysql');
            
            statusDiv.className = 'connection-status testing';
            statusDiv.textContent = '正在测试连接...';
            testBtn.disabled = true;
            
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 20000);
            
            fetchWithAuth(`${API_BASE_URL}/metadata/test-connection`, {
                method: 'POST',
                headers: getAuthHeaders(),
                body: JSON.stringify({ sourceConnection: connection, dbType: dbType }),
                signal: controller.signal
            })
            .then(response => response.json())
            .then(result => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                if (result.success && result.data && result.data.connected) {
                    statusDiv.className = 'connection-status success';
                    statusDiv.textContent = '✓ 连接成功';
                    connectionTestStatus[type] = true;
                } else if (result.success && result.data && !result.data.connected) {
                    statusDiv.className = 'connection-status error';
                    const errorType = result.data.errorType;
                    const errorMsg = result.data.errorMessage || '无法连接到数据库';
                    const suggestion = result.data.suggestion || '';
                    
                    let displayMsg = '';
                    if (errorType === 'AUTH_FAILED') {
                        displayMsg = '✗ 认证失败：用户名或密码错误';
                    } else if (errorType === 'NETWORK_ERROR') {
                        displayMsg = '✗ 网络错误：无法连接到数据库服务器';
                    } else if (errorType === 'DB_TYPE_MISMATCH') {
                        displayMsg = '✗ 类型不匹配：' + errorMsg;
                    } else if (errorType === 'TIMEOUT') {
                        displayMsg = '✗ 连接超时：20秒内未连接到数据库服务器';
                    } else {
                        displayMsg = '✗ 连接失败：' + errorMsg.substring(0, 80);
                    }
                    
                    statusDiv.innerHTML = `<div>${displayMsg}</div>${suggestion ? '<div style="font-size:11px;color:#999;margin-top:2px;">💡 ' + suggestion + '</div>' : ''}`;
                    connectionTestStatus[type] = false;
                } else {
                    statusDiv.className = 'connection-status error';
                    statusDiv.textContent = '✗ 连接失败: ' + (result.message || '无法连接到数据库');
                    connectionTestStatus[type] = false;
                }
            })
            .catch(error => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                statusDiv.className = 'connection-status error';
                if (error.name === 'AbortError') {
                    statusDiv.innerHTML = '<div>✗ 连接超时：20秒内未连接到数据库服务器</div><div style="font-size:11px;color:#999;margin-top:2px;">💡 请检查目标主机是否可达、端口是否开放、防火墙是否放行</div>';
                } else {
                    statusDiv.textContent = '✗ 连接测试出错: ' + error.message;
                }
                connectionTestStatus[type] = false;
            });
        }
        
        // 步骤导航
        function goToStep(step) {
            if (step === 1 && currentStep === 0) {
                if (!selectedSourceType || !selectedTargetType) {
                    showNotification('请选择源数据库和目标数据库类型', 'error');
                    return;
                }
            }
            
            if (step === 2 && currentStep === 1) {
                const errors = validateConnectionFields('source');
                if (errors.length > 0) {
                    showNotification('请正确填写源数据库: ' + errors.join('、'), 'error');
                    return;
                }
                
                if (!connectionTestStatus.source) {
                    showNotification('请先测试源数据库连接', 'error');
                    return;
                }
                
                const sourceConnection = buildConnectionString('source');
                if (selectedSourceType === 'postgresql' || selectedSourceType === 'oracle') {
                    const sourceDbName = document.getElementById('sourceDbName').value.trim();
                    loadSchemas(sourceConnection, sourceDbName);
                    updateTargetDbNameHint();
                } else {
                    loadDatabases(sourceConnection);
                    updateTargetDbNameHint();
                }
            }
            
            if (step === 3 && currentStep === 2) {
                if (Object.keys(selectedSyncObjects).length === 0) {
                    showNotification('请至少选择一个同步对象', 'error');
                    return;
                }
                if (!(selectedSourceType === 'postgresql' && selectedTargetType === 'mysql')) {
                    const targetDbName = document.getElementById('targetDbName').value.trim();
                    if (!targetDbName) {
                        showNotification('请填写目标数据库名称', 'error');
                        return;
                    }
                }
            }
            
            currentStep = step;
            updateStepUI();
        }
        
        function nextStep() {
            if (currentStep === 0) {
                goToStep(1);
            } else if (currentStep === 1) {
                goToStep(2);
            } else if (currentStep === 2) {
                goToStep(3);
            }
        }
        
        function updateStepUI() {
            document.getElementById('step0Nav').className = 'step-item' + (currentStep === 0 ? ' active' : (currentStep > 0 ? ' completed' : ''));
            document.getElementById('step1Nav').className = 'step-item' + (currentStep === 1 ? ' active' : (currentStep > 1 ? ' completed' : ''));
            document.getElementById('step2Nav').className = 'step-item' + (currentStep === 2 ? ' active' : (currentStep > 2 ? ' completed' : ''));
            document.getElementById('step3Nav').className = 'step-item' + (currentStep === 3 ? ' active' : '');
            
            document.getElementById('step0Content').className = 'step-content' + (currentStep === 0 ? ' active' : '');
            document.getElementById('step1Content').className = 'step-content' + (currentStep === 1 ? ' active' : '');
            document.getElementById('step2Content').className = 'step-content' + (currentStep === 2 ? ' active' : '');
            document.getElementById('step3Content').className = 'step-content' + (currentStep === 3 ? ' active' : '');
            
            document.getElementById('nextBtn').style.display = (currentStep >= 0 && currentStep <= 2) ? 'inline-block' : 'none';
            document.getElementById('prevBtn').style.display = (currentStep >= 1 && currentStep <= 3) ? 'inline-block' : 'none';
            document.getElementById('confirmBtn').style.display = currentStep === 3 ? 'inline-block' : 'none';
        }
        
        function selectSourceType(type) {
            selectedSourceType = type;

            document.getElementById('sourceTypeMysql').className = 'db-type-card' + (type === 'mysql' ? ' selected' : '');
            document.getElementById('sourceTypePg').className = 'db-type-card' + (type === 'postgresql' ? ' selected' : '');
            document.getElementById('sourceTypeOracle').className = 'db-type-card' + (type === 'oracle' ? ' selected' : '');
            document.getElementById('sourceTypeMongo').className = 'db-type-card' + (type === 'mongodb' ? ' selected' : '');

            updateTargetTypeCards();
        }

        function selectTargetType(type) {
            const targetCardIds = { 'mysql': 'targetTypeMysql', 'postgresql': 'targetTypePg', 'mongodb': 'targetTypeMongo', 'elasticsearch': 'targetTypeEs' };
            const card = document.getElementById(targetCardIds[type]);
            if (!card || card.classList.contains('disabled')) {
                return;
            }

            selectedTargetType = type;

            // 统一保留各卡的 disabled 状态：互斥限制由 updateTargetTypeCards（随源类型）决定，
            // 点击选中（包括重复点击已选卡）只切换 selected，绝不能清掉 disabled——
            // 否则重复点击会解除互斥，让被禁用的类型重新变为可选
            for (const [t, id] of Object.entries(targetCardIds)) {
                const el = document.getElementById(id);
                const disabled = el.classList.contains('disabled');
                el.className = 'db-type-card' + (t === type ? ' selected' : '') + (disabled ? ' disabled' : '');
            }
        }

        // 类型互斥：MongoDB 只能同 MongoDB 互相同步（副本集到副本集），其余类型不能选 MongoDB 目标；
        // Elasticsearch 只能作为目标且仅 MySQL 源可选（binlog 增量捕获）
        function updateTargetTypeCards() {
            const targetMysqlCard = document.getElementById('targetTypeMysql');
            const targetPgCard = document.getElementById('targetTypePg');
            const targetMongoCard = document.getElementById('targetTypeMongo');
            const targetEsCard = document.getElementById('targetTypeEs');

            if (selectedSourceType === 'mongodb') {
                selectedTargetType = 'mongodb';
                targetMysqlCard.className = 'db-type-card disabled';
                targetPgCard.className = 'db-type-card disabled';
                targetMongoCard.className = 'db-type-card selected';
                targetEsCard.className = 'db-type-card disabled';
                return;
            }

            const esAllowed = selectedSourceType === 'mysql';
            if (selectedTargetType === 'mongodb' || !selectedTargetType
                    || (selectedTargetType === 'elasticsearch' && !esAllowed)) {
                selectedTargetType = 'mysql';
            }
            targetMysqlCard.className = 'db-type-card' + (selectedTargetType === 'mysql' ? ' selected' : '');
            targetPgCard.className = 'db-type-card' + (selectedTargetType === 'postgresql' ? ' selected' : '');
            targetMongoCard.className = 'db-type-card disabled';
            targetEsCard.className = esAllowed
                ? 'db-type-card' + (selectedTargetType === 'elasticsearch' ? ' selected' : '')
                : 'db-type-card disabled';
        }
        
        function updateConnectionPlaceholders() {
            const sourcePort = document.getElementById('sourcePort');
            const targetPort = document.getElementById('targetPort');
            const sourceHost = document.getElementById('sourceHost');
            const targetHost = document.getElementById('targetHost');
            const sourceDbNameRow = document.getElementById('sourceDbNameRow');
            const targetDbNameRow = document.getElementById('targetDbNameRow');
            const sourceDbNameInput = document.getElementById('sourceDbName');

            if (selectedSourceType === 'postgresql') {
                sourcePort.placeholder = '端口号(默认5432)';
                sourceHost.placeholder = 'IP地址';
                if (!sourcePort.value || sourcePort.value === '3306' || sourcePort.value === '1521') sourcePort.value = '5432';
                sourceDbNameRow.style.display = 'block';
                if (sourceDbNameInput) sourceDbNameInput.placeholder = '数据库名称(如 test_db1)';
            } else if (selectedSourceType === 'oracle') {
                sourcePort.placeholder = '端口号(默认1521)';
                sourceHost.placeholder = 'IP地址';
                if (!sourcePort.value || sourcePort.value === '3306' || sourcePort.value === '5432') sourcePort.value = '1521';
                sourceDbNameRow.style.display = 'block';
                if (sourceDbNameInput) sourceDbNameInput.placeholder = '服务名(如 FREEPDB1)';
            } else {
                sourcePort.placeholder = '端口号(默认3306)';
                sourceHost.placeholder = 'IP地址';
                if (!sourcePort.value || sourcePort.value === '5432' || sourcePort.value === '1521') sourcePort.value = '3306';
                sourceDbNameRow.style.display = 'none';
            }

            if (selectedTargetType === 'postgresql') {
                targetPort.placeholder = '端口号(默认5432)';
                targetHost.placeholder = 'IP地址';
                if (!targetPort.value || targetPort.value === '3306') targetPort.value = '5432';
                targetDbNameRow.style.display = 'block';
            } else {
                targetPort.placeholder = '端口号(默认3306)';
                targetHost.placeholder = 'IP地址';
                if (!targetPort.value || targetPort.value === '5432') targetPort.value = '3306';
                targetDbNameRow.style.display = 'none';
            }
        }
        
        let validationPassed = false;
        
        async function runValidation() {
            const sourceConnection = buildConnectionString('source');
            const targetConnection = buildConnectionString('target');
            const modeFull = document.getElementById('modeFull');
            const migrationMode = modeFull.checked ? 'full' : 'fullAndIncre';
            
            const resultDiv = document.getElementById('validationResult');
            const runBtn = document.getElementById('runValidationBtn');
            
            resultDiv.innerHTML = '<div class="validation-loading">正在校验中，请稍候...</div>';
            runBtn.disabled = true;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/validate`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        sourceConnection: sourceConnection,
                        targetConnection: targetConnection,
                        migrationMode: migrationMode,
                        sourceType: selectedSourceType || 'mysql',
                        targetType: selectedTargetType || 'mysql'
                    })
                });
                
                const result = await response.json();
                
                if (result.success && result.data) {
                    renderValidationResult(result.data);
                } else {
                    resultDiv.innerHTML = `<div class="validation-empty" style="color: #f5222d;">校验失败: ${result.message || '未知错误'}</div>`;
                    validationPassed = false;
                }
            } catch (error) {
                console.error('校验失败:', error);
                resultDiv.innerHTML = `<div class="validation-empty" style="color: #f5222d;">校验请求失败: ${error.message}</div>`;
                validationPassed = false;
            } finally {
                runBtn.disabled = false;
            }
        }
        
        function renderValidationResult(data) {
            const resultDiv = document.getElementById('validationResult');
            let html = '';
            
            if (data.checkItems && data.checkItems.length > 0) {
                data.checkItems.forEach(item => {
                    const statusClass = item.passed ? 'passed' : (item.severity === 'warning' ? 'warning' : 'failed');
                    const icon = item.passed ? '✓' : (item.severity === 'warning' ? '⚠' : '✗');
                    
                    html += `
                        <div class="check-item ${statusClass}">
                            <div class="check-icon ${statusClass}">${icon}</div>
                            <div class="check-content">
                                <div class="check-name">${item.name}</div>
                                <div class="check-description">${item.description}</div>
                                <div class="check-message">${item.message}</div>
                            </div>
                        </div>
                    `;
                });
            }
            
            const summaryClass = data.allPassed ? 'passed' : 'failed';
            const summaryText = data.allPassed ? '✓ 所有检查项均已通过，可以创建任务' : '✗ 存在未通过的检查项，请修复后再创建任务';
            html += `<div class="validation-summary ${summaryClass}">${summaryText}</div>`;
            
            resultDiv.innerHTML = html;
            validationPassed = data.allPassed;
        }
        
        // 加载数据库列表
        let schemasCache = [];
        
        function updateTargetDbNameHint() {
            const hintDiv = document.getElementById('targetDbNameHint');
            const targetDbNameInput = document.getElementById('targetDbName');
            const targetDbNameGroup = document.getElementById('targetDbNameGroup');

            if (selectedSourceType === 'postgresql' && selectedTargetType === 'mysql') {
                hintDiv.textContent = 'PostgreSQL的schema对应MySQL的数据库，每个选中的schema将同步到目标MySQL的同名数据库';
                targetDbNameInput.placeholder = '默认使用源schema名称作为目标数据库名';
                targetDbNameGroup.style.display = 'none';
            } else if (selectedSourceType === 'postgresql' && selectedTargetType === 'postgresql') {
                hintDiv.textContent = 'PostgreSQL的schema将同步到目标PostgreSQL的同名schema，需指定目标数据库名称';
                targetDbNameInput.placeholder = '请输入目标PostgreSQL数据库名称（PG三层结构中的db层）';
                targetDbNameGroup.style.display = 'block';
            } else if (selectedSourceType === 'mysql' && selectedTargetType === 'postgresql') {
                hintDiv.textContent = 'MySQL的数据库对应PostgreSQL的schema，需指定同步到目标PostgreSQL的哪个数据库';
                targetDbNameInput.placeholder = '请输入目标PostgreSQL数据库名称（如 myapp_db）';
                targetDbNameGroup.style.display = 'block';
            } else if (selectedSourceType === 'oracle' && selectedTargetType === 'postgresql') {
                hintDiv.textContent = 'Oracle的schema对应PostgreSQL的schema，需指定同步到目标PostgreSQL的哪个数据库';
                targetDbNameInput.placeholder = '请输入目标PostgreSQL数据库名称（如 myapp_db）';
                targetDbNameGroup.style.display = 'block';
            } else {
                hintDiv.textContent = 'MySQL中每个数据库对应一个同步目标库，默认同步到同名数据库';
                targetDbNameInput.placeholder = '留空则使用源数据库名称';
                targetDbNameGroup.style.display = 'none';
            }
        }
        
        async function loadSchemas(connectionStr, database) {
            const sourceList = document.getElementById('sourceObjectsList');
            sourceList.innerHTML = '<div class="empty-selection">加载中...</div>';
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/schemas`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: database })
                });
                
                const result = await response.json();
                
                if (result.success && result.data.schemas) {
                    schemasCache = result.data.schemas;
                    renderSchemas(database);
                } else {
                    sourceList.innerHTML = `<div class="empty-selection" style="color: #f5222d;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                console.error('加载schema列表失败:', error);
                sourceList.innerHTML = '<div class="empty-selection" style="color: #f5222d;">加载失败，请检查连接信息</div>';
            }
        }
        
        function renderSchemas(database) {
            const sourceList = document.getElementById('sourceObjectsList');
            
            if (schemasCache.length === 0) {
                sourceList.innerHTML = '<div class="empty-selection">未找到可访问的schema</div>';
                return;
            }
            
            let html = '';
            schemasCache.forEach(schema => {
                html += `
                    <div class="database-item" data-db="${schema}" data-schema="${schema}" data-pg-database="${database}">
                        <div class="database-header">
                            <input type="checkbox" class="database-checkbox" onclick="selectAllPgTables('${database}', '${schema}', this.checked)">
                            <span class="database-name" onclick="togglePgSchema('${database}', '${schema}')">${schema}</span>
                            <span class="database-expand" id="expand-pg-${schema}" onclick="togglePgSchema('${database}', '${schema}')">▶</span>
                        </div>
                        <div class="table-list" id="tables-pg-${schema}">
                            <div style="padding: 16px; color: #999; font-size: 12px;">加载中...</div>
                        </div>
                    </div>
                `;
            });
            
            sourceList.innerHTML = html;
        }
        
        async function togglePgSchema(database, schema) {
            const tableList = document.getElementById(`tables-pg-${schema}`);
            const expandIcon = document.getElementById(`expand-pg-${schema}`);
            
            if (tableList.classList.contains('show')) {
                tableList.classList.remove('show');
                expandIcon.classList.remove('expanded');
            } else {
                tableList.classList.add('show');
                expandIcon.classList.add('expanded');
                
                if (!tablesCache[`pg-${schema}`]) {
                    await loadPgTables(database, schema);
                }
            }
        }
        
        async function loadPgTables(database, schema) {
            const connectionStr = buildConnectionString('source');
            const tableList = document.getElementById(`tables-pg-${schema}`);
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/tables`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: database, schema: schema })
                });
                
                const result = await response.json();
                
                if (result.success && result.data.tables) {
                    tablesCache[`pg-${schema}`] = result.data.tables;
                    renderPgTables(database, schema);
                } else {
                    tableList.innerHTML = `<div style="padding: 16px; color: #f5222d; font-size: 12px;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                console.error('加载PG表列表失败:', error);
                tableList.innerHTML = '<div style="padding: 16px; color: #f5222d; font-size: 12px;">加载失败</div>';
            }
        }
        
        function renderPgTables(database, schema) {
            const tableList = document.getElementById(`tables-pg-${schema}`);
            const tables = tablesCache[`pg-${schema}`] || [];
            
            if (tables.length === 0) {
                tableList.innerHTML = '<div style="padding: 16px; color: #999; font-size: 12px;">该schema没有表</div>';
                return;
            }
            
            let html = '';
            tables.forEach(table => {
                const isSelected = selectedSyncObjects[schema] && selectedSyncObjects[schema].includes(table.name);
                html += `
                    <div class="table-item">
                        <span class="table-name">${table.name}</span>
                        <span class="table-info">${table.rows} 行</span>
                        <button class="table-add-btn" onclick="addPgTable('${schema}', '${table.name}')" ${isSelected ? 'disabled style="opacity: 0.5;"' : ''}>
                            ${isSelected ? '已选择' : '选择'}
                        </button>
                    </div>
                `;
            });
            
            tableList.innerHTML = html;
        }
        
        function selectAllPgTables(database, schema, checked) {
            const tableList = document.getElementById(`tables-pg-${schema}`);
            const expandIcon = document.getElementById(`expand-pg-${schema}`);

            if (checked) {
                if (tableList && !tableList.classList.contains('show')) {
                    tableList.classList.add('show');
                    expandIcon.classList.add('expanded');
                }
                if (!tablesCache[`pg-${schema}`]) {
                    loadPgTables(database, schema).then(() => {
                        if (!selectedSyncObjects[schema]) {
                            selectedSyncObjects[schema] = [];
                        }
                        tablesCache[`pg-${schema}`].forEach(table => {
                            if (!selectedSyncObjects[schema].includes(table.name)) {
                                selectedSyncObjects[schema].push(table.name);
                            }
                        });
                        renderPgTables(database, schema);
                        renderSelectedObjects();
                    });
                    return;
                }
                if (!selectedSyncObjects[schema]) {
                    selectedSyncObjects[schema] = [];
                }
                tablesCache[`pg-${schema}`].forEach(table => {
                    if (!selectedSyncObjects[schema].includes(table.name)) {
                        selectedSyncObjects[schema].push(table.name);
                    }
                });
            } else {
                delete selectedSyncObjects[schema];
            }
            renderPgTables(database, schema);
            renderSelectedObjects();
        }
        
        function addPgTable(schema, tableName) {
            if (!selectedSyncObjects[schema]) {
                selectedSyncObjects[schema] = [];
            }
            if (!selectedSyncObjects[schema].includes(tableName)) {
                selectedSyncObjects[schema].push(tableName);
            }
            const database = document.getElementById('sourceDbName').value.trim();
            renderPgTables(database, schema);
            renderSelectedObjects();
        }

        async function loadDatabases(connectionStr) {
            const sourceList = document.getElementById('sourceObjectsList');
            sourceList.innerHTML = '<div class="empty-selection">加载中...</div>';
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/databases`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr })
                });
                
                const result = await response.json();
                
                if (result.success && result.data.databases) {
                    databasesCache = result.data.databases;
                    renderDatabases();
                } else {
                    sourceList.innerHTML = `<div class="empty-selection" style="color: #f5222d;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                console.error('加载数据库列表失败:', error);
                sourceList.innerHTML = '<div class="empty-selection" style="color: #f5222d;">加载失败，请检查连接串</div>';
            }
        }
        
        // 渲染数据库列表
        function renderDatabases() {
            const sourceList = document.getElementById('sourceObjectsList');
            
            if (databasesCache.length === 0) {
                sourceList.innerHTML = '<div class="empty-selection">未找到可访问的数据库</div>';
                return;
            }
            
            let html = '';
            databasesCache.forEach(db => {
                html += `
                    <div class="database-item" data-db="${db}">
                        <div class="database-header">
                            <input type="checkbox" class="database-checkbox" onclick="selectAllTables('${db}', this.checked)">
                            <span class="database-name" onclick="toggleDatabase('${db}')">${db}</span>
                            <span class="database-expand" id="expand-${db}" onclick="toggleDatabase('${db}')">▶</span>
                        </div>
                        <div class="table-list" id="tables-${db}">
                            <div style="padding: 16px; color: #999; font-size: 12px;">加载中...</div>
                        </div>
                    </div>
                `;
            });
            
            sourceList.innerHTML = html;
        }
        
        // 展开/折叠数据库
        async function toggleDatabase(db) {
            const tableList = document.getElementById(`tables-${db}`);
            const expandIcon = document.getElementById(`expand-${db}`);
            
            if (tableList.classList.contains('show')) {
                tableList.classList.remove('show');
                expandIcon.classList.remove('expanded');
            } else {
                tableList.classList.add('show');
                expandIcon.classList.add('expanded');
                
                // 加载表列表
                if (!tablesCache[db]) {
                    await loadTables(db);
                }
            }
        }
        
        // 加载表列表
        async function loadTables(db) {
            const connectionStr = buildConnectionStringWithDb('source', db);
            const tableList = document.getElementById(`tables-${db}`);
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/tables`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: db })
                });
                
                const result = await response.json();
                
                if (result.success && result.data.tables) {
                    tablesCache[db] = result.data.tables;
                    renderTables(db);
                } else {
                    tableList.innerHTML = `<div style="padding: 16px; color: #f5222d; font-size: 12px;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                console.error('加载表列表失败:', error);
                tableList.innerHTML = '<div style="padding: 16px; color: #f5222d; font-size: 12px;">加载失败</div>';
            }
        }
        
        // 渲染表列表
        function renderTables(db) {
            const tableList = document.getElementById(`tables-${db}`);
            const tables = tablesCache[db] || [];
            
            if (tables.length === 0) {
                tableList.innerHTML = '<div style="padding: 16px; color: #999; font-size: 12px;">该数据库没有表</div>';
                return;
            }
            
            let html = '';
            tables.forEach(table => {
                const isSelected = selectedSyncObjects[db] && selectedSyncObjects[db].includes(table.name);
                html += `
                    <div class="table-item">
                        <span class="table-name">${table.name}</span>
                        <span class="table-info">${table.rows} 行 | ${table.size}</span>
                        <button class="table-add-btn" onclick="addTable('${db}', '${table.name}')" ${isSelected ? 'disabled style="opacity: 0.5;"' : ''}>
                            ${isSelected ? '已选择' : '选择'}
                        </button>
                    </div>
                `;
            });
            
            tableList.innerHTML = html;
        }
        
        // 选择所有表
        function selectAllTables(db, checked) {
            const tableList = document.getElementById(`tables-${db}`);
            const expandIcon = document.getElementById(`expand-${db}`);

            if (checked) {
                if (tableList && !tableList.classList.contains('show')) {
                    tableList.classList.add('show');
                    expandIcon.classList.add('expanded');
                }
                if (!tablesCache[db]) {
                    loadTables(db).then(() => {
                        if (!selectedSyncObjects[db]) {
                            selectedSyncObjects[db] = [];
                        }
                        tablesCache[db].forEach(table => {
                            if (!selectedSyncObjects[db].includes(table.name)) {
                                selectedSyncObjects[db].push(table.name);
                            }
                        });
                        renderTables(db);
                        renderSelectedObjects();
                    });
                    return;
                }
                if (!selectedSyncObjects[db]) {
                    selectedSyncObjects[db] = [];
                }
                tablesCache[db].forEach(table => {
                    if (!selectedSyncObjects[db].includes(table.name)) {
                        selectedSyncObjects[db].push(table.name);
                    }
                });
            } else {
                delete selectedSyncObjects[db];
            }
            renderTables(db);
            renderSelectedObjects();
        }
        
        // 添加表到选择列表
        function addTable(db, tableName) {
            if (!selectedSyncObjects[db]) {
                selectedSyncObjects[db] = [];
            }
            if (!selectedSyncObjects[db].includes(tableName)) {
                selectedSyncObjects[db].push(tableName);
            }
            renderTables(db);
            renderSelectedObjects();
        }
        
        // 从选择列表移除表
        function removeTable(db, tableName) {
            if (selectedSyncObjects[db]) {
                const index = selectedSyncObjects[db].indexOf(tableName);
                if (index > -1) {
                    selectedSyncObjects[db].splice(index, 1);
                }
                if (selectedSyncObjects[db].length === 0) {
                    delete selectedSyncObjects[db];
                }
            }
            renderSelectedObjects();
            
            if (tablesCache[db]) {
                renderTables(db);
            } else if (tablesCache[`pg-${db}`]) {
                const database = document.getElementById('sourceDbName') ? document.getElementById('sourceDbName').value.trim() : '';
                renderPgTables(database, db);
            }
        }
        
        // 渲染已选择的对象
        function renderSelectedObjects() {
            const selectedList = document.getElementById('selectedObjectsList');
            
            const dbNames = Object.keys(selectedSyncObjects);
            if (dbNames.length === 0) {
                selectedList.innerHTML = '<div class="empty-selection">暂未选择任何对象</div>';
                return;
            }
            
            let html = '';
            const isPgSource = selectedSourceType === 'postgresql';
            dbNames.forEach(db => {
                selectedSyncObjects[db].forEach(tableName => {
                    const label = isPgSource ? `${db} (schema)` : db;
                    html += `
                        <div class="selected-item">
                            <span class="selected-db-name">${label}.</span>
                            <span class="selected-table-name">${tableName}</span>
                            <button class="remove-btn" onclick="removeTable('${db}', '${tableName}')">移除</button>
                        </div>
                    `;
                });
            });
            
            selectedList.innerHTML = html;
        }
        
        // 创建任务
        async function createTask() {
            if (!checkAuth()) return;
            
            if (isSubmitting) {
                showNotification('正在提交中，请稍候...', 'error');
                return;
            }
            
            const name = document.getElementById('taskName').value.trim();
            const sourceConnection = buildConnectionString('source');
            const targetConnection = buildConnectionString('target');
            
            const sourceErrors = validateConnectionFields('source');
            if (sourceErrors.length > 0) {
                showNotification('请正确填写源数据库: ' + sourceErrors.join('、'), 'error');
                return;
            }
            
            const targetErrors = validateConnectionFields('target');
            if (targetErrors.length > 0) {
                showNotification('请正确填写目标数据库: ' + targetErrors.join('、'), 'error');
                return;
            }
            
            if (!connectionTestStatus.source) {
                showNotification('请先测试源数据库连接', 'error');
                goToStep(1);
                return;
            }
            
            if (!connectionTestStatus.target) {
                showNotification('请先测试目标数据库连接', 'error');
                goToStep(1);
                return;
            }
            
            if (!validationPassed) {
                showNotification('请先完成校验检查并确保所有检查项通过', 'error');
                goToStep(3);
                return;
            }
            
            const modeFull = document.getElementById('modeFull');
            const migrationMode = modeFull.checked ? 'full' : 'fullAndIncre';
            
            if (!name) {
                showNotification('请输入任务名称', 'error');
                goToStep(1);
                return;
            }
            
            let syncObjectsJson = null;
            let sourceDbName = null;
            
            if (Object.keys(selectedSyncObjects).length > 0) {
                const syncObjectsData = {};
                Object.keys(selectedSyncObjects).forEach(db => {
                    syncObjectsData[db] = { tables: selectedSyncObjects[db] };
                });
                syncObjectsJson = JSON.stringify(syncObjectsData);

                if (selectedSourceType === 'postgresql' || selectedSourceType === 'oracle') {
                    sourceDbName = document.getElementById('sourceDbName').value.trim();
                } else {
                    const dbNames = Object.keys(selectedSyncObjects);
                    if (dbNames.length === 1) {
                        sourceDbName = dbNames[0];
                    }
                }
            }
            
            let targetDbName = document.getElementById('targetDbName').value.trim();
            if (selectedSourceType === 'postgresql' && selectedTargetType === 'mysql') {
                targetDbName = targetDbName || Object.keys(selectedSyncObjects)[0] || '';
            }
            
            isSubmitting = true;
            const confirmBtn = document.getElementById('confirmBtn');
            confirmBtn.disabled = true;
            confirmBtn.textContent = '创建中...';
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        name,
                        sourceConnection: sourceConnection || 'default-source',
                        targetConnection: targetConnection || 'default-target',
                        migrationMode,
                        syncObjects: syncObjectsJson,
                        sourceDbName,
                        targetDbName: targetDbName,
                        sourceType: selectedSourceType || 'mysql',
                        targetType: selectedTargetType || 'mysql'
                    })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('任务创建成功', 'success');
                    closeModal();
                    clearForm();
                    fetchWorkflows();
                } else {
                    showNotification(result.message || '创建失败', 'error');
                }
            } catch (error) {
                console.error('创建任务失败:', error);
                showNotification('创建任务失败', 'error');
            } finally {
                isSubmitting = false;
                confirmBtn.disabled = false;
                confirmBtn.textContent = '创建任务';
            }
        }
        
        // 暂停工作流
        // pause/stop/resume/delete 等操作后刷新“当前可见”的那个任务列表，
        // 避免在灾备页操作却刷新了实时同步页（DR 页与实时同步页是不同列表）。
        function refreshCurrentTaskList() {
            const drPageEl = document.getElementById('drPage');
            if (drPageEl && drPageEl.style.display !== 'none') {
                fetchDrTasks();
            } else {
                fetchWorkflows();
            }
        }

        window.pauseWorkflow = async function(id) {
            if (!checkAuth()) return;

            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${id}/pause`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });

                const result = await response.json();

                if (result.success) {
                    showNotification('任务已暂停', 'success');
                    refreshCurrentTaskList();
                } else {
                    showNotification(result.message || '暂停失败', 'error');
                }
            } catch (error) {
                console.error('暂停任务失败:', error);
                showNotification('暂停任务失败', 'error');
            }
        }
        
        // 恢复工作流
        window.resumeWorkflow = async function(id) {
            if (!checkAuth()) return;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${id}/resume`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('任务已恢复', 'success');
                    refreshCurrentTaskList();
                } else {
                    showNotification(result.message || '恢复失败', 'error');
                }
            } catch (error) {
                console.error('恢复任务失败:', error);
                showNotification('恢复任务失败', 'error');
            }
        }
        
        // 结束任务
        window.stopWorkflow = async function(id) {
            console.log('stopWorkflow called with id:', id);
            console.log('typeof stopWorkflow:', typeof stopWorkflow);
            console.log('checkAuth result:', checkAuth());
            if (!checkAuth()) return;
            
            console.log('Showing confirm dialog...');
            if (!confirm('确定要结束这个任务吗？结束后任务将标记为已完成。')) {
                console.log('User cancelled');
                return;
            }
            
            console.log('Sending request to:', `${API_BASE_URL}/workflows/${id}/stop`);
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${id}/stop`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('任务已结束', 'success');
                    refreshCurrentTaskList();
                } else {
                    showNotification(result.message || '结束失败', 'error');
                }
            } catch (error) {
                console.error('结束任务失败:', error);
                showNotification('结束任务失败', 'error');
            }
        }
        
        // 显示任务详情
        async function showTaskDetail(id) {
            if (!checkAuth()) return;
            // 供详情页脚的 诊断/配置版本/克隆 按钮取当前任务
            window.__dash.detailTaskId = id;

            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${id}`);
                
                const result = await response.json();
                
                if (result.success) {
                    const task = result.data;
                    let statusInfo = statusMap[task.status] || statusMap['pending'];
                    const isFullAndIncre = task.migration_mode === 'fullAndIncre';

                    // 死信裁决：失败且错误信息带 seqno= 的（增量失败特征）才显示"跳过失败事件并重试"
                    window.__dash.failedSeqno = null;
                    const dlSkipBtn = document.getElementById('dlSkipEventBtn');
                    if (dlSkipBtn) {
                        const seqMatch = /seqno=(\d+)/.exec(task.error_message || '');
                        if (task.status === 'FAILED' && seqMatch) {
                            window.__dash.failedSeqno = parseInt(seqMatch[1], 10);
                            dlSkipBtn.style.display = '';
                        } else {
                            dlSkipBtn.style.display = 'none';
                        }
                    }
                    
                    // 全量+增量模式下，FULL_COMPLETED 状态显示为"增量同步中"
                    if (task.status === 'FULL_COMPLETED' && isFullAndIncre) {
                        statusInfo = { text: '增量同步中', class: 'status-increment-running', dot: true };
                    }
                    
                    // 解析同步对象：区分库级（{"db":{"dbLevel":true}}）与表级（{"db":{"tables":[...]}}）
                    let syncGranularity = '表级';
                    let syncObjectsHtml = '-';
                    if (task.sync_objects) {
                        try {
                            const syncObjects = JSON.parse(task.sync_objects);
                            if (syncObjects && Object.keys(syncObjects).length > 0) {
                                const dbList = [];
                                const tableList = [];
                                for (const [db, dbValue] of Object.entries(syncObjects)) {
                                    // 库名映射展示：entry 的 targetDb（库级/表级均可携带）
                                    const tgtDb = (dbValue && dbValue.targetDb && dbValue.targetDb !== db) ? dbValue.targetDb : null;
                                    if (dbValue && dbValue.dbLevel === true) {
                                        dbList.push(tgtDb
                                            ? `${db} <span style="color:#1890ff;">→ ${tgtDb}</span>（整库）`
                                            : `${db}（整库）`);
                                        continue;
                                    }
                                    let tables = null;
                                    if (Array.isArray(dbValue)) {
                                        tables = dbValue;
                                    } else if (dbValue && dbValue.tables && Array.isArray(dbValue.tables)) {
                                        tables = dbValue.tables;
                                    }
                                    if (tables) {
                                        const mapping = (dbValue && dbValue.tableMapping) || {};
                                        tables.forEach(table => {
                                            const tgtTable = mapping[table];
                                            // 库名/表名映射展示：源库.源表 → 目标库.目标表（无任何映射只显示源）
                                            if (tgtDb || (tgtTable && tgtTable !== table)) {
                                                tableList.push(`${db}.${table} <span style="color:#1890ff;">→ ${tgtDb || db}.${tgtTable || table}</span>`);
                                            } else {
                                                tableList.push(`${db}.${table}`);
                                            }
                                        });
                                    }
                                }
                                if (dbList.length > 0) {
                                    syncGranularity = '库级';
                                    syncObjectsHtml = `<div style="max-height: 120px; overflow-y: auto; border: 1px solid #e8e8e8; border-radius: 4px; padding: 8px;">
                                        ${dbList.map(d => `<div style="font-size: 12px; padding: 2px 0;">${d}</div>`).join('')}
                                    </div>`;
                                } else if (tableList.length > 0) {
                                    syncObjectsHtml = `<div style="max-height: 120px; overflow-y: auto; border: 1px solid #e8e8e8; border-radius: 4px; padding: 8px;">
                                        ${tableList.map(t => `<div style="font-size: 12px; padding: 2px 0;">${t}</div>`).join('')}
                                    </div>`;
                                }
                            }
                        } catch (e) {
                            syncObjectsHtml = task.sync_objects;
                        }
                    }

                    // 列处理配置展示（列过滤/列映射/附加列，mysql→mysql 表级才有）：
                    // 只在确有配置时才渲染该区块，否则整块隐藏。
                    let colProcHtml = '';
                    if (task.sync_objects) {
                        try {
                            const so = JSON.parse(task.sync_objects);
                            const rows = [];
                            const kindLabel = { CREATE_TIME: '创建时间', UPDATE_TIME: '更新时间', CUSTOM: '自定义值' };
                            for (const [db, dv] of Object.entries(so || {})) {
                                if (!dv || typeof dv !== 'object') continue;
                                const cf = dv.columnFilter || {}, cm = dv.columnMapping || {}, ec = dv.extraColumns || {};
                                const tables = new Set([...Object.keys(cf), ...Object.keys(cm), ...Object.keys(ec)]);
                                for (const t of tables) {
                                    const parts = [];
                                    (cf[t] || []).forEach(f => parts.push(`<span style="color:#d46b08;">过滤</span> ${escapeHtml(f.column)} ${escapeHtml(f.op)} ${escapeHtml(String(f.value))} <span style="color:#999;">(命中不同步)</span>`));
                                    Object.entries(cm[t] || {}).forEach(([s, d]) => parts.push(`<span style="color:#1890ff;">列映射</span> ${escapeHtml(s)} → ${escapeHtml(d)}`));
                                    (ec[t] || []).forEach(e => parts.push(`<span style="color:#389e0d;">附加列</span> ${escapeHtml(e.name)} = ${escapeHtml(kindLabel[e.kind] || e.kind)}${e.value ? '：' + escapeHtml(String(e.value)) : ''}`));
                                    if (parts.length) rows.push(`<div style="font-size:12px;padding:4px 0;border-bottom:1px dashed #f0f0f0;"><b>${escapeHtml(db)}.${escapeHtml(t)}</b><div style="margin-top:2px;">${parts.map(p => `<div style="padding:1px 0;">${p}</div>`).join('')}</div></div>`);
                                }
                            }
                            if (rows.length) {
                                colProcHtml = `<div style="max-height:160px;overflow-y:auto;border:1px solid #e8e8e8;border-radius:4px;padding:8px;">${rows.join('')}</div>`;
                            }
                        } catch (e) { /* 忽略解析失败 */ }
                    }

                    // 连接信息只展示类型与地址，不暴露连接串（含账号口令）
                    const extractHostPort = (conn) => {
                        const m = (conn || '').match(/@([^:/@]+):(\d+)/);
                        return m ? `${m[1]}:${m[2]}` : '-';
                    };
                    const sourceTypeLabel = formatDbTypeLabel(task.source_type);
                    const targetTypeLabel = formatDbTypeLabel(task.target_type);
                    const sourceAddr = extractHostPort(task.source_connection);
                    const targetAddr = extractHostPort(task.target_connection);
                    
                    let logsHtml = '';
                    if (task.logs && task.logs.length > 0) {
                        logsHtml = `
                            <div style="margin-top: 20px;">
                                <h4 style="margin-bottom: 12px; font-size: 14px; font-weight: 500;">执行日志</h4>
                                <div style="border: 1px solid #e8e8e8; border-radius: 4px; max-height: 200px; overflow-y: auto;">
                                    ${task.logs.map(log => `
                                        <div style="padding: 8px 12px; border-bottom: 1px solid #f0f0f0;">
                                            <div style="font-size: 12px; color: #999; margin-bottom: 4px;">${formatDateTime(log.created_at)}</div>
                                            <div style="font-size: 13px; color: ${log.level === 'error' ? '#f5222d' : log.level === 'warning' ? '#fa8c16' : '#333'};">
                                                ${log.message}
                                            </div>
                                        </div>
                                    `).join('')}
                                </div>
                            </div>
                        `;
                    }
                    
                    document.getElementById('taskDetailContent').innerHTML = `
                        <div style="display: grid; grid-template-columns: 120px 1fr; gap: 12px 24px;">
                            <div style="font-size: 13px; color: #666;">任务名称:</div>
                            <div style="font-size: 13px;">${escapeHtml(task.name)}</div>
                            
                            <div style="font-size: 13px; color: #666;">任务ID:</div>
                            <div style="font-size: 13px; font-family: monospace;">${task.id}</div>
                            
                            <div style="font-size: 13px; color: #666;">同步模式:</div>
                            <div style="font-size: 13px;">${task.migration_mode === 'fullAndIncre' ? '全量+增量' : '仅全量'}</div>
                            
                            <div style="font-size: 13px; color: #666;">当前状态:</div>
                            <div>
                                <span class="status-tag ${statusInfo.class}">
                                    ${statusInfo.dot ? '<span class="status-dot"></span>' : statusInfo.icon ? `<span class="status-icon">${statusInfo.icon}</span>` : ''}
                                    ${statusInfo.text}
                                </span>
                                ${task.status === 'CONFIGURING' ? `<button class="action-btn" style="margin-left: 8px;" onclick="closeDetailModal(); openTaskConfig('${task.id}')">前往配置</button>` : ''}
                            </div>
                            
                            <div style="font-size: 13px; color: #666;">全量同步进度:</div>
                            <div>
                                <div class="progress-bar" style="width: 200px;">
                                    <div class="progress-fill" style="width: ${task.progress || 0}%"></div>
                                </div>
                                <span style="margin-left: 8px; font-size: 11px; color: #666;">${task.progress || 0}%</span>
                            </div>
                            
                            <div style="font-size: 13px; color: #666;">是否计费中:</div>
                            <div>${task.is_billing ? '<span style="color: #52c41a;">是</span>' : '<span style="color: #999;">否</span>'}</div>
                            
                            <div style="font-size: 13px; color: #666;">库表同步类型:</div>
                            <div style="font-size: 13px;">${syncGranularity}</div>

                            <div style="font-size: 13px; color: #666;">${syncGranularity === '库级' ? '同步库:' : '同步表:'}</div>
                            <div style="font-size: 13px;">${syncObjectsHtml}</div>

                            ${colProcHtml ? `
                            <div style="font-size: 13px; color: #666;">列处理配置:</div>
                            <div style="font-size: 13px;">${colProcHtml}</div>
                            ` : ''}

                            <div style="font-size: 13px; color: #666;">源库类型:</div>
                            <div style="font-size: 13px;">${sourceTypeLabel}</div>

                            <div style="font-size: 13px; color: #666;">源库地址:</div>
                            <div style="font-size: 13px; font-family: monospace;">${sourceAddr}</div>

                            <div style="font-size: 13px; color: #666;">目标库类型:</div>
                            <div style="font-size: 13px;">${targetTypeLabel}</div>

                            <div style="font-size: 13px; color: #666;">目标库地址:</div>
                            <div style="font-size: 13px; font-family: monospace;">${targetAddr}</div>
                            
                            <div style="font-size: 13px; color: #666;">创建时间:</div>
                            <div style="font-size: 13px;">${formatDateTime(task.created_at)}</div>
                            
                            ${task.updated_at ? `
                                <div style="font-size: 13px; color: #666;">更新时间:</div>
                                <div style="font-size: 13px;">${formatDateTime(task.updated_at)}</div>
                            ` : ''}
                            
                            ${task.completed_at ? `
                                <div style="font-size: 13px; color: #666;">完成时间:</div>
                                <div style="font-size: 13px;">${formatDateTime(task.completed_at)}</div>
                            ` : ''}
                            
                            ${task.status === 'FAILED' ? `
                                <div style="font-size: 13px; color: #666;">错误码:</div>
                                <div style="font-size: 13px;">
                                    ${task.error_code ? `<span style="font-weight: 600; background: #fff1f0; color: #f5222d; padding: 2px 8px; border-radius: 4px; font-family: monospace;">${task.error_code}</span>` : '<span style="color: #999;">无</span>'}
                                </div>
                                
                                <div style="font-size: 13px; color: #666;">错误描述:</div>
                                <div style="font-size: 13px; color: #f5222d;">${getErrorCodeDescription(task.error_code)}</div>
                                
                                <div style="font-size: 13px; color: #666;">错误详情:</div>
                                <div style="font-size: 13px; color: #f5222d; background: #fff2f0; padding: 8px 12px; border-radius: 4px; word-break: break-all;">${task.error_message || '无详细信息'}</div>
                                
                                <div style="font-size: 13px; color: #666;">处理建议:</div>
                                <div style="font-size: 13px; color: #1890ff; background: #e6f7ff; padding: 8px 12px; border-radius: 4px;">${getErrorCodeSolution(task.error_code)}</div>
                            ` : ''}
                        </div>
                        ${logsHtml}
                        <div style="margin-top: 20px; border-top: 2px solid #1890ff; padding-top: 16px;">
                            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px;">
                                <div style="font-size: 14px; font-weight: 600; color: #1890ff;">高级监控</div>
                                <button class="btn-test" onclick="downloadDiagnosticsBundle('${task.id}')" title="打包日志尾部+脱敏config+checkpoint+THL尾部，供排障">下载排障包</button>
                            </div>
                            <div class="adv-tabs">
                                <div class="adv-tab active" onclick="switchAdvTab('${task.id}', 'checkpoint')">断点续传</div>
                                <div class="adv-tab" onclick="switchAdvTab('${task.id}', 'latency')">延迟热力图</div>
                                <div class="adv-tab" onclick="switchAdvTab('${task.id}', 'ddl')">在线DDL</div>
                                <div class="adv-tab" onclick="switchAdvTab('${task.id}', 'fanout')">多目标分发</div>
                            </div>
                            <div class="adv-tab-content active" id="advTab-checkpoint">
                                <div class="adv-empty"><div class="adv-empty-icon">○</div>加载中...</div>
                            </div>
                            <div class="adv-tab-content" id="advTab-latency"></div>
                            <div class="adv-tab-content" id="advTab-ddl"></div>
                            <div class="adv-tab-content" id="advTab-fanout"></div>
                        </div>
                    `;
                    
                    _currentDetailTaskId = task.id;
                    _currentDetailLogs = task.logs || [];
                    document.getElementById('taskDetailModal').classList.add('show');
                    loadAdvTab(task.id, 'checkpoint');
                } else {
                    showNotification(result.message || '获取任务详情失败', 'error');
                }
            } catch (error) {
                console.error('获取任务详情失败:', error);
                showNotification('获取任务详情失败', 'error');
            }
        }
        
        // 关闭详情模态框
        function closeDetailModal() {
            stopAdvRefresh();
            document.getElementById('taskDetailModal').classList.remove('show');
        }

        // ============ 高级监控功能 ============
        let _currentDetailTaskId = null;
        let _currentDetailLogs = [];
        let _advRefreshTimer = null;
        let _advCurrentTab = 'checkpoint';

        function switchAdvTab(taskId, tabName) {
            stopAdvRefresh();
            _advCurrentTab = tabName;
            const tabs = document.querySelectorAll('.adv-tab');
            const tabMap = { checkpoint: 0, latency: 1, ddl: 2, fanout: 3 };
            tabs.forEach(t => t.classList.remove('active'));
            if (tabs[tabMap[tabName]]) tabs[tabMap[tabName]].classList.add('active');
            document.querySelectorAll('.adv-tab-content').forEach(c => c.classList.remove('active'));
            document.getElementById('advTab-' + tabName).classList.add('active');
            loadAdvTab(taskId, tabName);
        }

        async function loadAdvTab(taskId, tabName) {
            if (tabName === 'checkpoint') {
                await loadCheckpoint(taskId);
            } else if (tabName === 'latency') {
                await loadLatency(taskId);
            } else if (tabName === 'ddl') {
                renderDdlHistory(_currentDetailLogs);
            } else if (tabName === 'fanout') {
                await loadFanout(taskId);
            }
            startAdvRefresh(taskId, tabName);
        }

        function startAdvRefresh(taskId, tabName) {
            stopAdvRefresh();
            if (tabName === 'ddl') return; // DDL历史无需自动刷新
            _advRefreshTimer = setInterval(async () => {
                if (tabName === 'checkpoint') await loadCheckpoint(taskId);
                else if (tabName === 'latency') await loadLatency(taskId);
                else if (tabName === 'fanout') await loadFanout(taskId);
            }, 10000);
        }

        function stopAdvRefresh() {
            if (_advRefreshTimer) {
                clearInterval(_advRefreshTimer);
                _advRefreshTimer = null;
            }
        }

        // 下载排障压缩包：日志尾部 + 脱敏 config + checkpoint + THL 尾部
        async function downloadDiagnosticsBundle(taskId) {
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/diagnostics/${taskId}`, {
                    headers: { 'Authorization': 'Bearer ' + localStorage.getItem('token') }
                });
                if (!resp.ok) {
                    let msg = '下载排障包失败';
                    try { const err = await resp.json(); msg = err.message || msg; } catch (e) {}
                    showNotification(msg, 'error');
                    return;
                }
                const blob = await resp.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `diagnostics-${taskId}.zip`;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(url);
                showNotification('排障包已下载', 'success');
            } catch (error) {
                console.error('下载排障包失败:', error);
                showNotification('下载排障包失败: ' + error.message, 'error');
            }
        }

        // ---- 断点续传可视化 ----
        async function loadCheckpoint(taskId) {
            const el = document.getElementById('advTab-checkpoint');
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/checkpoint/${taskId}`, {
                    headers: { 'Authorization': 'Bearer ' + localStorage.getItem('token') }
                });
                const data = await resp.json();
                renderCheckpoint(data);
            } catch (e) {
                el.innerHTML = `<div class="adv-empty"><div class="adv-empty-icon">⚠</div>加载失败: ${e.message}<br><span style="font-size:11px;color:#ccc;">请确认 Agent (端口8083) 正在运行</span></div>`;
            }
        }

        function renderCheckpoint(data) {
            const el = document.getElementById('advTab-checkpoint');
            const d = data.data || data;
            if (!d) {
                el.innerHTML = '<div class="adv-empty"><div class="adv-empty-icon">○</div>暂无断点续传数据</div>';
                return;
            }
            const binlog = d.binlog || {};
            const thl = d.thl || {};
            const cp = d.checkpoint || {};
            const gaps = d.gaps || {};
            const rpo = d.rpo_ms;
            const rto = d.rto_ms;

            const binlogStatus = binlog.available ? 'OK' : 'UNKNOWN';
            const thlStatus = thl.available ? 'OK' : 'UNKNOWN';
            const cpStatus = cp.available ? 'OK' : 'UNKNOWN';
            const pendingEvents = gaps.pending_events != null ? gaps.pending_events : '-';
            const binlogGap = gaps.binlog_gap != null ? gaps.binlog_gap : '-';

            el.innerHTML = `
                <div class="adv-metric-grid">
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">Binlog 位点</div>
                        <div class="adv-metric-value" style="font-size:13px;font-family:monospace;">${binlog.file || '-'}</div>
                        <div class="adv-metric-sub">pos: ${binlog.position || '-'}</div>
                        <div style="margin-top:4px;">${advStatusBadge(binlogStatus)}</div>
                    </div>
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">THL Seqno</div>
                        <div class="adv-metric-value">${thl.seqno != null ? thl.seqno : '-'}</div>
                        <div class="adv-metric-sub">${thl.available ? '可用' : '不可用'}</div>
                        <div style="margin-top:4px;">${advStatusBadge(thlStatus)}</div>
                    </div>
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">Checkpoint</div>
                        <div class="adv-metric-value">${cp.seqno != null ? cp.seqno : '-'}</div>
                        <div class="adv-metric-sub">${cp.updated_at || '-'}</div>
                        <div style="margin-top:4px;">${advStatusBadge(cpStatus)}</div>
                    </div>
                </div>
                <div class="adv-metric-grid" style="grid-template-columns: repeat(2, 1fr);">
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">待应用事件数</div>
                        <div class="adv-metric-value ${pendingEvents > 100 ? 'bad' : ''}" style="${pendingEvents > 100 ? 'color:#f5222d;' : ''}">${pendingEvents}</div>
                    </div>
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">Binlog 位点差距</div>
                        <div class="adv-metric-value">${binlogGap}</div>
                    </div>
                </div>
                <div class="adv-metric-grid" style="grid-template-columns: repeat(2, 1fr);">
                    <div class="adv-metric-card" style="background:#e6f7ff;border-color:#91d5ff;">
                        <div class="adv-metric-label">RPO (数据丢失容忍)</div>
                        <div class="adv-metric-value" style="color:#1890ff;">${rpo != null ? rpo + ' ms' : '-'}</div>
                    </div>
                    <div class="adv-metric-card" style="background:#fff7e6;border-color:#ffd591;">
                        <div class="adv-metric-label">RTO (恢复时间)</div>
                        <div class="adv-metric-value" style="color:#fa8c16;">${rto != null ? rto + ' ms' : '-'}</div>
                    </div>
                </div>
                <div class="adv-link-flow">
                    <div class="adv-link-node ${binlog.available ? 'ok' : ''}">capture</div>
                    <span class="adv-link-arrow">→</span>
                    <div class="adv-link-node ${thl.available ? 'ok' : ''}">extract</div>
                    <span class="adv-link-arrow">→</span>
                    <div class="adv-link-node ${cp.available ? 'ok' : ''}">increment</div>
                    <span class="adv-link-arrow">→</span>
                    <div class="adv-link-node ${cp.available ? 'ok' : ''}">checkpoint</div>
                </div>
            `;
        }

        // ---- 同步延迟热力图 ----
        let _latencyData = null;
        let _latencyTrendChart = null;
        let _latencyExpandedTable = null;

        async function loadLatency(taskId) {
            const el = document.getElementById('advTab-latency');
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/table-latency/${taskId}`, {
                    headers: { 'Authorization': 'Bearer ' + localStorage.getItem('token') }
                });
                const data = await resp.json();
                _latencyData = data;
                _latencyExpandedTable = null;
                renderLatency(data);
            } catch (e) {
                el.innerHTML = `<div class="adv-empty"><div class="adv-empty-icon">⚠</div>加载失败: ${e.message}<br><span style="font-size:11px;color:#ccc;">请确认 Agent (端口8083) 正在运行</span></div>`;
            }
        }

        function renderLatency(data) {
            const el = document.getElementById('advTab-latency');
            const d = data.data || data;
            const tables = d.tables || [];
            if (!tables.length) {
                el.innerHTML = '<div class="adv-empty"><div class="adv-empty-icon">○</div>暂无表级延迟数据</div>';
                return;
            }
            const summary = d.summary || {};
            const totalTables = summary.total_tables || tables.length;
            const avgLatency = summary.avg_latency_ms || 0;
            const maxLatency = summary.max_latency_ms || 0;
            const bottleneck = summary.bottleneck_table || '-';

            const rows = tables.map(t => {
                const p50 = t.p50_ms || 0;
                const p95 = t.p95_ms || 0;
                const p99 = t.p99_ms || 0;
                const maxV = t.max_ms || 0;
                return `
                    <tr onclick="toggleLatencyChart('${t.table}')" style="cursor:pointer;">
                        <td style="font-weight:500;">${t.table}</td>
                        <td>${advLatencyCell(p50)}</td>
                        <td>${advLatencyCell(p95)}</td>
                        <td>${advLatencyCell(p99)}</td>
                        <td>${advLatencyCell(maxV)}</td>
                        <td>${advLatencyLevel(p95)}</td>
                    </tr>
                `;
            }).join('');

            el.innerHTML = `
                <div class="adv-metric-grid">
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">总表数</div>
                        <div class="adv-metric-value">${totalTables}</div>
                    </div>
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">平均延迟</div>
                        <div class="adv-metric-value">${avgLatency} ms</div>
                    </div>
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">最大延迟</div>
                        <div class="adv-metric-value" style="color:#f5222d;">${maxLatency} ms</div>
                        <div class="adv-metric-sub">瓶颈表: ${bottleneck}</div>
                    </div>
                </div>
                <table class="adv-heatmap-table">
                    <thead>
                        <tr>
                            <th>表名</th><th>P50</th><th>P95</th><th>P99</th><th>最大</th><th>级别</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
                <div id="latencyChartArea" style="margin-top:12px;display:none;"></div>
            `;
        }

        function toggleLatencyChart(tableName) {
            const area = document.getElementById('latencyChartArea');
            if (_latencyExpandedTable === tableName && area.style.display !== 'none') {
                area.style.display = 'none';
                _latencyExpandedTable = null;
                if (_latencyTrendChart) { _latencyTrendChart.destroy(); _latencyTrendChart = null; }
                return;
            }
            _latencyExpandedTable = tableName;
            area.style.display = 'block';

            const d = (_latencyData && (_latencyData.data || _latencyData)) || {};
            const tables = d.tables || [];
            const table = tables.find(t => t.table === tableName);
            const points = (table && table.heatmap) || [];

            if (_latencyTrendChart) { _latencyTrendChart.destroy(); _latencyTrendChart = null; }

            if (!points.length) {
                area.innerHTML = `<div style="padding:12px;background:#fafafa;border-radius:6px;font-size:12px;color:#666;">表 <b>${tableName}</b> 暂无延迟历史数据点</div>`;
                return;
            }

            area.innerHTML = `
                <div style="padding:8px 12px;background:#fafafa;border-radius:6px;">
                    <div style="font-size:12px;color:#666;margin-bottom:8px;">表 <b>${tableName}</b> 最近 ${points.length} 次应用延迟趋势</div>
                    <div style="height:180px;"><canvas id="latencyTrendCanvas"></canvas></div>
                </div>
            `;

            const labels = points.map(p => new Date(p.ts).toLocaleTimeString('zh-CN', { hour12: false }));
            const values = points.map(p => p.latency);
            const opLabels = points.map(p => p.op || '-');

            const canvas = document.getElementById('latencyTrendCanvas');
            _latencyTrendChart = new Chart(canvas, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: '延迟(ms)',
                        data: values,
                        borderColor: '#1890ff',
                        backgroundColor: '#1890ff20',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 2,
                        pointBackgroundColor: values.map(advLatencyColor),
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: { callbacks: { afterLabel: (ctx) => '操作: ' + (opLabels[ctx.dataIndex] || '-') } }
                    },
                    scales: {
                        x: { display: true, ticks: { maxTicksLimit: 8, font: { size: 10 } }, grid: { color: '#f0f0f0' } },
                        y: { display: true, beginAtZero: true, ticks: { font: { size: 10 } }, grid: { color: '#f0f0f0' } }
                    }
                }
            });
        }

        // ---- 在线DDL变更历史 ----
        function renderDdlHistory(logs) {
            const el = document.getElementById('advTab-ddl');
            const keywords = ['影子表', 'ghost', '_new', '_ghost_', 'DDL', 'ALTER', 'CREATE TABLE', 'RENAME', 'cutover', '影子', '在线DDL'];
            const ddlLogs = (logs || []).filter(log =>
                keywords.some(k => (log.message || '').includes(k))
            ).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

            if (!ddlLogs.length) {
                el.innerHTML = `
                    <div class="adv-empty"><div class="adv-empty-icon">○</div>暂无在线DDL变更记录</div>
                    <div style="font-size:12px;color:#999;padding:0 12px;">
                        <b>说明：</b>系统自动识别 gh-ost/pt-osc 风格的影子表（<code>_tbl_ghost_</code> / <code>_tbl_new</code>），将 CREATE 转换为 ALTER，跳过 RENAME/DML/DROP。
                    </div>
                `;
                return;
            }

            const items = ddlLogs.map(log => {
                const msg = log.message || '';
                let status = '✓ 已应用';
                let statusColor = '#52c41a';
                if (msg.includes('跳过') || msg.includes('SKIP')) {
                    status = '⏸ 已跳过';
                    statusColor = '#faad14';
                } else if (msg.includes('失败') || msg.includes('error')) {
                    status = '✗ 失败';
                    statusColor = '#f5222d';
                }
                return `
                    <div class="adv-timeline-item">
                        <div class="adv-timeline-time">${formatDateTime(log.created_at)} · ${log.level || 'INFO'}</div>
                        <div class="adv-timeline-content">${msg}</div>
                        <div style="margin-top:2px;font-size:12px;color:${statusColor};font-weight:500;">${status}</div>
                    </div>
                `;
            }).join('');

            el.innerHTML = `
                <div style="font-size:12px;color:#666;margin-bottom:12px;">共 ${ddlLogs.length} 条DDL变更记录</div>
                <div class="adv-timeline">${items}</div>
            `;
        }

        // ---- 多目标库分发 ----
        async function loadFanout(taskId) {
            const el = document.getElementById('advTab-fanout');
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/fanout/${taskId}`, {
                    headers: { 'Authorization': 'Bearer ' + localStorage.getItem('token') }
                });
                const data = await resp.json();
                renderFanout(data);
            } catch (e) {
                el.innerHTML = `<div class="adv-empty"><div class="adv-empty-icon">⚠</div>加载失败: ${e.message}<br><span style="font-size:11px;color:#ccc;">请确认 Agent (端口8083) 正在运行</span></div>`;
            }
        }

        function renderFanout(data) {
            const el = document.getElementById('advTab-fanout');
            const d = data.data || data;
            if (!d || (!d.targets && !d.target_count)) {
                el.innerHTML = `
                    <div class="adv-empty"><div class="adv-empty-icon">○</div>该任务未启用多目标分发</div>
                    <div style="font-size:12px;color:#999;padding:0 12px;">在任务配置中开启 fanout 并添加多个目标库即可使用此功能。</div>
                `;
                return;
            }
            const targets = d.targets || [];
            const total = d.total_dispatched || 0;
            const success = d.total_success || 0;
            const failure = d.total_failure || 0;
            const targetCount = d.target_count || targets.length;
            const parallelism = d.parallelism || '-';

            const targetRows = targets.map(t => {
                const tSuccess = t.success_count || 0;
                const tFailure = t.failure_count || 0;
                const tLatency = t.avg_latency_ms || 0;
                const rate = (tSuccess + tFailure) > 0 ? (tSuccess / (tSuccess + tFailure) * 100).toFixed(1) : '100.0';
                const rowColor = tFailure > 0 ? '#fff2f0' : '';
                const level = rate >= 99 ? '🟢' : rate >= 95 ? '🟡' : '🔴';
                return `
                    <tr style="background:${rowColor};">
                        <td style="font-weight:500;">${t.name || t.id || '-'}</td>
                        <td style="font-family:monospace;font-size:11px;">${t.host || '-'}:${t.port || '-'}</td>
                        <td style="color:#52c41a;">${tSuccess}</td>
                        <td style="color:${tFailure > 0 ? '#f5222d' : '#999'};">${tFailure}</td>
                        <td>${tLatency} ms</td>
                        <td>${rate}%</td>
                        <td>${level}</td>
                    </tr>
                `;
            }).join('');

            el.innerHTML = `
                <div class="adv-metric-grid">
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">总分发数</div>
                        <div class="adv-metric-value">${total}</div>
                    </div>
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">成功 / 失败</div>
                        <div class="adv-metric-value"><span style="color:#52c41a;">${success}</span> / <span style="color:#f5222d;">${failure}</span></div>
                    </div>
                    <div class="adv-metric-card">
                        <div class="adv-metric-label">目标数 / 并行度</div>
                        <div class="adv-metric-value">${targetCount} / ${parallelism}</div>
                    </div>
                </div>
                <table class="adv-heatmap-table">
                    <thead>
                        <tr><th>目标库</th><th>主机</th><th>成功</th><th>失败</th><th>平均延迟</th><th>成功率</th><th>状态</th></tr>
                    </thead>
                    <tbody>${targetRows || '<tr><td colspan="7" style="text-align:center;color:#999;">暂无目标库数据</td></tr>'}</tbody>
                </table>
            `;
        }

        // ---- 辅助函数 ----
        function advStatusBadge(status) {
            return `<span class="adv-status-badge adv-status-${status}">${status}</span>`;
        }

        function advLatencyCell(ms) {
            const color = advLatencyColor(ms);
            return `<span class="adv-heatmap-cell" style="background:${color}20;color:${color};">${ms}ms</span>`;
        }

        function advLatencyColor(ms) {
            if (ms < 50) return '#52c41a';
            if (ms < 200) return '#faad14';
            return '#f5222d';
        }

        function advLatencyLevel(p95) {
            if (p95 < 50) return '<span style="color:#52c41a;">正常</span>';
            if (p95 < 200) return '<span style="color:#faad14;">警告</span>';
            return '<span style="color:#f5222d;">严重</span>';
        }
        
        // 删除工作流
        window.deleteWorkflow = async function(id, name) {
            console.log('deleteWorkflow called with id:', id, 'name:', name);
            if (!checkAuth()) return;
            
            if (!confirm(`确定要删除任务【${name || id}】吗？`)) {
                return;
            }
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${id}`, {
                    method: 'DELETE',
                    headers: getAuthHeaders()
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('任务删除成功', 'success');
                    refreshCurrentTaskList();
                } else {
                    showNotification(result.message || '删除失败', 'error');
                }
            } catch (error) {
                console.error('删除任务失败:', error);
                showNotification('删除任务失败', 'error');
            }
        }

        // 重试失败的任务
        window.retryWorkflow = async function(id) {
            console.log('retryWorkflow called with id:', id);
            if (!checkAuth()) return;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${id}/retry`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('任务重试已启动', 'success');
                    fetchWorkflows();
                } else {
                    showNotification(result.message || '重试失败', 'error');
                }
            } catch (error) {
                console.error('重试任务失败:', error);
                showNotification('重试任务失败', 'error');
            }
        }
        
        // 显示通知
        function showNotification(message, type) {
            const notification = document.getElementById('notification');
            notification.textContent = message;
            notification.className = `notification ${type} show`;
            
            setTimeout(() => {
                notification.classList.remove('show');
            }, 3000);
        }
        
        // 打开模态框
        function openModal() {
            document.getElementById('createTaskModal').classList.add('show');
            updateTargetTypeCards();
        }

        function closeModal() {
            document.getElementById('createTaskModal').classList.remove('show');
        }

        function clearForm() {
            document.getElementById('taskNameCreate').value = '';
            selectedSourceType = 'mysql';
            selectedTargetType = 'mysql';
            document.getElementById('sourceTypeMysql').className = 'db-type-card selected';
            document.getElementById('sourceTypePg').className = 'db-type-card';
            document.getElementById('sourceTypeOracle').className = 'db-type-card';
            document.getElementById('sourceTypeMongo').className = 'db-type-card';
            document.getElementById('targetTypeMysql').className = 'db-type-card selected';
            document.getElementById('targetTypePg').className = 'db-type-card';
            document.getElementById('targetTypeMongo').className = 'db-type-card disabled';
            document.getElementById('targetTypeEs').className = 'db-type-card';
        }

        let cfgCurrentStep = 1;
        let cfgWorkflowId = null;
        let cfgSourceType = 'mysql';
        let cfgTargetType = 'mysql';
        let cfgConnectionTestStatus = { source: null, target: null };
        let cfgSelectedSyncObjects = {};
        // 同步粒度：table=表级（现状，选表快照）；database=库级（整库+增量期新对象自动同步）。
        // 库级选中的库在 cfgSelectedSyncObjects 里以哨兵字符串 '__DB_LEVEL__' 表示（表级为表名数组）。
        let cfgSyncGranularity = 'table';
        const DB_LEVEL_SENTINEL = '__DB_LEVEL__';
        // 表名映射（仅表级同步生效）：key = "db.table"，value = 目标表名。
        // 未配置/与源表同名 = 不映射；库级同步（dbLevel）不支持表名映射。
        let cfgTableNameMapping = {};
        // 库名映射：key = 源库名，value = 目标库名（表级/库级同步均支持）。
        // 未配置/与源库同名 = 不映射。取代原"目标数据库名称"输入框（PG 目标的库名走连接区输入）。
        let cfgDbNameMapping = {};
        // 列处理（同引擎 MySQL→MySQL / PG→PG 表级同步）：key 均为 "db.table"
        let cfgColumnFilters = {};   // -> [{column, op, value}]
        let cfgColumnMappings = {};  // -> {源列: 目标列}
        let cfgExtraColumns = {};    // -> [{name, kind, value}]  kind: CREATE_TIME|UPDATE_TIME|CUSTOM
        let cfgColumnsCache = {};    // -> [{name, dataType, columnType, primaryKey, filterable}]
        // 内容对比差异分页游标（openTaskConfig 打开时重置）。原为隐式全局（未声明直接赋值），
        // 经典脚本 sloppy 模式下静默建 window 属性；改用 "use strict"+IIFE 后未声明赋值会 ReferenceError。
        let cfgCompareDiffPage = 0;

        // 列处理是否可用：同引擎任务（mysql→mysql / pg→pg）展示【3.列处理】步骤
        function cfgColProcSupported() {
            return (cfgSourceType === 'mysql' && cfgTargetType === 'mysql')
                || (cfgSourceType === 'postgresql' && cfgTargetType === 'postgresql')
                || (cfgSourceType === 'mongodb' && cfgTargetType === 'mongodb');
        }

        function cfgClearAllColumnProcessing() {
            cfgColumnFilters = {};
            cfgColumnMappings = {};
            cfgExtraColumns = {};
        }

        function cfgClearColumnProcessingForTable(db, tableName) {
            const key = db + '.' + tableName;
            delete cfgColumnFilters[key];
            delete cfgColumnMappings[key];
            delete cfgExtraColumns[key];
        }

        // 目标库名输入变更：空值/与源库同名 = 取消映射
        window.cfgSetDbNameMapping = function(db, value) {
            const v = (value || '').trim();
            if (!v || v === db) {
                delete cfgDbNameMapping[db];
                cfgRenderSelectedObjects();
                return;
            }
            if (!/^[A-Za-z_][A-Za-z0-9_$]*$/.test(v)) {
                showNotification('目标库名仅支持字母、数字、下划线和$，且不能以数字开头', 'error');
                cfgRenderSelectedObjects();
                return;
            }
            cfgDbNameMapping[db] = v;
            cfgRenderSelectedObjects();
        }

        window.cfgSetGranularity = function(mode) {
            if (cfgSyncGranularity === mode) return;
            cfgSyncGranularity = mode;
            cfgSelectedSyncObjects = {};
            cfgTableNameMapping = {};
            cfgDbNameMapping = {};
            cfgClearAllColumnProcessing();
            const radio = document.querySelector(`input[name="cfgSyncGranularity"][value="${mode}"]`);
            if (radio) radio.checked = true;
            cfgRenderDatabases();
            cfgRenderSelectedObjects();
        }

        window.cfgToggleDbLevel = function(db, checked) {
            if (checked) {
                cfgSelectedSyncObjects[db] = DB_LEVEL_SENTINEL;
            } else {
                delete cfgSelectedSyncObjects[db];
                delete cfgDbNameMapping[db];
            }
            cfgClearTableMappingForDb(db);
            cfgRenderDatabases();
            cfgRenderSelectedObjects();
        }

        function cfgClearTableMappingForDb(db) {
            Object.keys(cfgTableNameMapping).forEach(key => {
                if (key.startsWith(db + '.')) delete cfgTableNameMapping[key];
            });
            // 列处理配置随表选择一并清理
            [cfgColumnFilters, cfgColumnMappings, cfgExtraColumns].forEach(store => {
                Object.keys(store).forEach(key => {
                    if (key.startsWith(db + '.')) delete store[key];
                });
            });
        }

        // 目标表名输入变更：空值/与源表同名 = 取消映射；仅允许常规标识符字符，防注入且与引擎改写规则一致
        window.cfgSetTableMapping = function(db, tableName, value) {
            const key = db + '.' + tableName;
            const v = (value || '').trim();
            if (!v || v === tableName) {
                delete cfgTableNameMapping[key];
                cfgRenderSelectedObjects();
                return;
            }
            if (!/^[A-Za-z_][A-Za-z0-9_$]*$/.test(v)) {
                showNotification('目标表名仅支持字母、数字、下划线和$，且不能以数字开头', 'error');
                cfgRenderSelectedObjects();
                return;
            }
            cfgTableNameMapping[key] = v;
            cfgRenderSelectedObjects();
        }

        // ==================== 列处理页面（步骤3，同引擎 mysql→mysql / pg→pg 表级） ====================

        const COLPROC_IDENT_RE = /^[A-Za-z_][A-Za-z0-9_$]*$/;
        // 自定义附加列输入值白名单：与 agent 侧校验一致（值会拼进建表 DEFAULT 字面量）
        const COLPROC_CUSTOM_VALUE_RE = /^[A-Za-z0-9_.\-]{1,128}$/;
        const COLPROC_KIND_LABELS = {
            CREATE_TIME: 'create_time（首次同步时间）',
            UPDATE_TIME: 'update_time（最近更新时间）',
            CUSTOM: '自定义'
        };

        // 渲染列处理页：库级同步显示不支持提示；表级同步填充三个页签的表下拉
        function cfgRenderColProcPage() {
            const notice = document.getElementById('cfgColProcDbLevelNotice');
            const body = document.getElementById('cfgColProcBody');
            if (cfgSyncGranularity === 'database') {
                notice.style.display = 'block';
                body.style.display = 'none';
                return;
            }
            notice.style.display = 'none';
            body.style.display = 'block';

            const tables = [];
            Object.keys(cfgSelectedSyncObjects).forEach(db => {
                if (Array.isArray(cfgSelectedSyncObjects[db])) {
                    cfgSelectedSyncObjects[db].forEach(t => tables.push(db + '.' + t));
                }
            });
            ['cfgFilterTableSelect', 'cfgMappingTableSelect', 'cfgExtraTableSelect'].forEach(id => {
                const select = document.getElementById(id);
                const prev = select.value;
                let options = '<option value="">请选择表</option>';
                tables.forEach(t => {
                    options += `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`;
                });
                select.innerHTML = options;
                if (prev && tables.includes(prev)) select.value = prev;
            });
            cfgOnExtraKindChange();
            cfgRenderFilterList();
            cfgRenderMappingRows();
            cfgRenderExtraList();
        }

        window.cfgSwitchColTab = function(tab) {
            ['filter', 'mapping', 'extra'].forEach(t => {
                const btn = document.getElementById('cfgColTab' + t.charAt(0).toUpperCase() + t.slice(1) + 'Btn');
                const pane = document.getElementById('cfgColPane' + t.charAt(0).toUpperCase() + t.slice(1));
                btn.className = 'colproc-tab' + (t === tab ? ' active' : '');
                pane.className = 'colproc-pane' + (t === tab ? ' active' : '');
            });
        }

        // 加载表的列信息（带缓存）
        async function cfgLoadColumns(db, table) {
            const key = db + '.' + table;
            if (cfgColumnsCache[key]) return cfgColumnsCache[key];
            const connectionStr = cfgBuildConnectionStringWithDb('source', db);
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/columns`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: db, table: table })
                });
                const result = await response.json();
                if (result.success && result.data.columns) {
                    cfgColumnsCache[key] = result.data.columns;
                    return cfgColumnsCache[key];
                }
                showNotification(result.message || '加载列信息失败', 'error');
            } catch (e) {
                showNotification('加载列信息失败', 'error');
            }
            return null;
        }

        window.cfgOnColProcTableChange = async function(tab) {
            if (tab === 'filter') {
                const sel = document.getElementById('cfgFilterTableSelect').value;
                const colSelect = document.getElementById('cfgFilterColumnSelect');
                colSelect.innerHTML = '<option value="">加载中...</option>';
                if (!sel) { colSelect.innerHTML = '<option value="">请先选择表</option>'; return; }
                const dot = sel.indexOf('.');
                const columns = await cfgLoadColumns(sel.substring(0, dot), sel.substring(dot + 1));
                if (!columns) { colSelect.innerHTML = '<option value="">加载失败</option>'; return; }
                const filterable = columns.filter(c => c.filterable);
                if (filterable.length === 0) {
                    colSelect.innerHTML = '<option value="">该表没有可过滤类型的列</option>';
                    return;
                }
                let options = '<option value="">请选择列</option>';
                filterable.forEach(c => {
                    options += `<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)}（${escapeHtml(c.columnType)}）</option>`;
                });
                colSelect.innerHTML = options;
            } else if (tab === 'mapping') {
                const sel = document.getElementById('cfgMappingTableSelect').value;
                if (sel) {
                    const dot = sel.indexOf('.');
                    await cfgLoadColumns(sel.substring(0, dot), sel.substring(dot + 1));
                }
                cfgRenderMappingRows();
            } else {
                const sel = document.getElementById('cfgExtraTableSelect').value;
                if (sel) {
                    const dot = sel.indexOf('.');
                    await cfgLoadColumns(sel.substring(0, dot), sel.substring(dot + 1));
                }
                cfgRenderExtraList();
            }
        }

        // ---------- 列名过滤 ----------

        // 过滤值必须符合列类型：整数/bit/浮点定点/日期时间
        function cfgValidateFilterValue(dataType, value) {
            const t = (dataType || '').toLowerCase();
            if (['tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint', 'year'].includes(t)) {
                return /^-?\d{1,19}$/.test(value) || '该列为整数类型，请输入整数值';
            }
            if (t === 'bit') {
                return /^\d{1,19}$/.test(value) || '该列为 bit 类型，请输入非负整数值（如 0 或 1）';
            }
            if (['decimal', 'numeric', 'float', 'double'].includes(t)) {
                return /^-?\d{1,19}(\.\d{1,10})?$/.test(value) || '该列为数值类型，请输入数值';
            }
            if (t === 'date') {
                return /^\d{4}-\d{2}-\d{2}$/.test(value) || '该列为日期类型，格式：YYYY-MM-DD';
            }
            if (t === 'datetime' || t === 'timestamp') {
                return /^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d{1,3})?)?$/.test(value)
                    || '该列为日期时间类型，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS';
            }
            if (t === 'time') {
                return /^-?\d{1,3}:\d{2}:\d{2}$/.test(value) || '该列为时间类型，格式：HH:MM:SS';
            }
            return '该列类型不支持列过滤（仅支持整数、浮点/定点、日期时间类型）';
        }

        window.cfgAddColumnFilter = function() {
            const tableKey = document.getElementById('cfgFilterTableSelect').value;
            const column = document.getElementById('cfgFilterColumnSelect').value;
            const op = document.getElementById('cfgFilterOpSelect').value;
            const value = document.getElementById('cfgFilterValueInput').value.trim();
            if (!tableKey) { showNotification('请选择表', 'error'); return; }
            if (!column) { showNotification('请选择列', 'error'); return; }
            if (!value) { showNotification('请输入条件值', 'error'); return; }
            const cols = cfgColumnsCache[tableKey] || [];
            const colInfo = cols.find(c => c.name === column);
            const check = cfgValidateFilterValue(colInfo ? colInfo.dataType : '', value);
            if (check !== true) { showNotification(check, 'error'); return; }
            if (!cfgColumnFilters[tableKey]) cfgColumnFilters[tableKey] = [];
            if (cfgColumnFilters[tableKey].some(f => f.column === column && f.op === op && f.value === value)) {
                showNotification('该过滤条件已存在', 'error');
                return;
            }
            cfgColumnFilters[tableKey].push({ column, op, value });
            document.getElementById('cfgFilterValueInput').value = '';
            cfgRenderFilterList();
        }

        window.cfgRemoveColumnFilter = function(tableKey, idx) {
            if (cfgColumnFilters[tableKey]) {
                cfgColumnFilters[tableKey].splice(idx, 1);
                if (cfgColumnFilters[tableKey].length === 0) delete cfgColumnFilters[tableKey];
            }
            cfgRenderFilterList();
        }

        function cfgRenderFilterList() {
            const container = document.getElementById('cfgFilterList');
            const keys = Object.keys(cfgColumnFilters);
            if (keys.length === 0) {
                container.innerHTML = '<div style="color: #999; padding: 8px 0;">暂无过滤条件</div>';
                return;
            }
            let html = '';
            keys.forEach(tableKey => {
                cfgColumnFilters[tableKey].forEach((f, idx) => {
                    html += `
                        <div class="colproc-item">
                            <span>${escapeHtml(tableKey)}：不同步 <b>${escapeHtml(f.column)} ${escapeHtml(f.op)} ${escapeHtml(f.value)}</b> 的行</span>
                            <span class="colproc-item-remove" onclick="cfgRemoveColumnFilter('${escapeHtml(tableKey)}', ${idx})">删除</span>
                        </div>`;
                });
            });
            container.innerHTML = html;
        }

        // ---------- 列名映射 ----------

        window.cfgSetColumnMapping = function(tableKey, srcCol, value) {
            const v = (value || '').trim();
            const mapping = cfgColumnMappings[tableKey] || {};
            if (!v || v === srcCol) {
                delete mapping[srcCol];
                if (Object.keys(mapping).length === 0) delete cfgColumnMappings[tableKey];
                else cfgColumnMappings[tableKey] = mapping;
                cfgRenderMappingRows();
                return;
            }
            if (!COLPROC_IDENT_RE.test(v)) {
                showNotification('目标列名仅支持字母、数字、下划线和$，且不能以数字开头', 'error');
                cfgRenderMappingRows();
                return;
            }
            const cols = (cfgColumnsCache[tableKey] || []).map(c => c.name.toLowerCase());
            const others = Object.entries(mapping).filter(([s]) => s !== srcCol).map(([, t]) => t.toLowerCase());
            if (others.includes(v.toLowerCase())
                || (cols.includes(v.toLowerCase()) && v.toLowerCase() !== srcCol.toLowerCase())) {
                showNotification('目标列名与该表其他列名冲突', 'error');
                cfgRenderMappingRows();
                return;
            }
            mapping[srcCol] = v;
            cfgColumnMappings[tableKey] = mapping;
            cfgRenderMappingRows();
        }

        function cfgRenderMappingRows() {
            const container = document.getElementById('cfgMappingList');
            const tableKey = document.getElementById('cfgMappingTableSelect').value;
            if (!tableKey) {
                // 未选表时汇总展示已配置的映射
                const keys = Object.keys(cfgColumnMappings);
                if (keys.length === 0) {
                    container.innerHTML = '<div style="color: #999; padding: 8px 0;">请选择表后配置列名映射</div>';
                    return;
                }
                let html = '<div style="color: #999; margin-bottom: 6px;">已配置的列名映射（选择表可修改）：</div>';
                keys.forEach(k => {
                    Object.entries(cfgColumnMappings[k]).forEach(([s, t]) => {
                        html += `<div class="colproc-item"><span>${escapeHtml(k)}：${escapeHtml(s)} → <b>${escapeHtml(t)}</b></span></div>`;
                    });
                });
                container.innerHTML = html;
                return;
            }
            const columns = cfgColumnsCache[tableKey];
            if (!columns) {
                container.innerHTML = '<div style="color: #999; padding: 8px 0;">加载列信息中...</div>';
                return;
            }
            const mapping = cfgColumnMappings[tableKey] || {};
            let html = `
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 6px; font-size: 12px; color: #999; padding: 4px 0;">
                    <span>源列名</span><span>类型</span><span>目标列名（留空 = 不改名）</span>
                </div>`;
            columns.forEach(c => {
                const tgt = mapping[c.name] || '';
                html += `
                    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 6px; align-items: center; padding: 3px 0;">
                        <span>${escapeHtml(c.name)}${c.primaryKey ? ' <span style="color:#1890ff;font-size:11px;">PK</span>' : ''}</span>
                        <span style="color: #999;">${escapeHtml(c.columnType)}</span>
                        <input type="text" class="form-input" style="height: 30px; font-size: 12px;"
                               value="${escapeHtml(tgt)}" placeholder="${escapeHtml(c.name)}"
                               onchange="cfgSetColumnMapping('${escapeHtml(tableKey)}', '${escapeHtml(c.name)}', this.value)">
                    </div>`;
            });
            container.innerHTML = html;
        }

        // ---------- 新增附加列 ----------

        window.cfgOnExtraKindChange = function() {
            const kind = document.getElementById('cfgExtraKindSelect').value;
            const nameInput = document.getElementById('cfgExtraNameInput');
            document.getElementById('cfgExtraValueInput').style.display = (kind === 'CUSTOM') ? '' : 'none';
            if (!nameInput.value.trim() || ['create_time', 'update_time'].includes(nameInput.value.trim())) {
                nameInput.value = kind === 'CREATE_TIME' ? 'create_time' : (kind === 'UPDATE_TIME' ? 'update_time' : '');
            }
        }

        window.cfgAddExtraColumn = function() {
            const tableKey = document.getElementById('cfgExtraTableSelect').value;
            const kind = document.getElementById('cfgExtraKindSelect').value;
            const name = document.getElementById('cfgExtraNameInput').value.trim();
            const value = document.getElementById('cfgExtraValueInput').value.trim();
            if (!tableKey) { showNotification('请选择表', 'error'); return; }
            if (!name) { showNotification('请输入附加列名', 'error'); return; }
            if (!COLPROC_IDENT_RE.test(name)) {
                showNotification('列名仅支持字母、数字、下划线和$，且不能以数字开头', 'error');
                return;
            }
            if (kind === 'CUSTOM' && !COLPROC_CUSTOM_VALUE_RE.test(value)) {
                showNotification('自定义输入值仅支持字母、数字及 _ - . （1-128位）', 'error');
                return;
            }
            const lower = name.toLowerCase();
            const cols = (cfgColumnsCache[tableKey] || []).map(c => c.name.toLowerCase());
            const mapped = Object.values(cfgColumnMappings[tableKey] || {}).map(t => t.toLowerCase());
            const extras = (cfgExtraColumns[tableKey] || []).map(e => e.name.toLowerCase());
            if (cols.includes(lower) || mapped.includes(lower) || extras.includes(lower)) {
                showNotification('附加列名与该表已有列名冲突', 'error');
                return;
            }
            if ((cfgExtraColumns[tableKey] || []).some(e => e.kind === kind && kind !== 'CUSTOM')) {
                showNotification('该表已添加过此类型的附加列', 'error');
                return;
            }
            if (!cfgExtraColumns[tableKey]) cfgExtraColumns[tableKey] = [];
            const item = { name, kind };
            if (kind === 'CUSTOM') item.value = value;
            cfgExtraColumns[tableKey].push(item);
            document.getElementById('cfgExtraValueInput').value = '';
            cfgRenderExtraList();
        }

        window.cfgRemoveExtraColumn = function(tableKey, idx) {
            if (cfgExtraColumns[tableKey]) {
                cfgExtraColumns[tableKey].splice(idx, 1);
                if (cfgExtraColumns[tableKey].length === 0) delete cfgExtraColumns[tableKey];
            }
            cfgRenderExtraList();
        }

        function cfgRenderExtraList() {
            const container = document.getElementById('cfgExtraList');
            const keys = Object.keys(cfgExtraColumns);
            if (keys.length === 0) {
                container.innerHTML = '<div style="color: #999; padding: 8px 0;">暂无附加列</div>';
                return;
            }
            let html = '';
            keys.forEach(tableKey => {
                const dot = tableKey.indexOf('.');
                const db = tableKey.substring(0, dot);
                const table = tableKey.substring(dot + 1);
                cfgExtraColumns[tableKey].forEach((e, idx) => {
                    const desc = e.kind === 'CUSTOM'
                        ? `常量 ${escapeHtml(e.value)}@${escapeHtml(db)}@${escapeHtml(table)}`
                        : escapeHtml(COLPROC_KIND_LABELS[e.kind] || e.kind);
                    html += `
                        <div class="colproc-item">
                            <span>${escapeHtml(tableKey)}：<b>${escapeHtml(e.name)}</b>（${desc}）</span>
                            <span class="colproc-item-remove" onclick="cfgRemoveExtraColumn('${escapeHtml(tableKey)}', ${idx})">删除</span>
                        </div>`;
                });
            });
            container.innerHTML = html;
        }

        // ==================== 列处理页面结束 ====================

        let cfgTablesCache = {};
        let cfgDatabasesCache = [];
        let cfgSchemasCache = [];
        let cfgValidationPassed = false;

        async function createTaskFromModal() {
            if (!checkAuth()) return;
            
            const name = document.getElementById('taskNameCreate').value.trim();
            if (!name) {
                showNotification('请输入任务名称', 'error');
                return;
            }
            if (!selectedSourceType || !selectedTargetType) {
                showNotification('请选择源数据库和目标数据库类型', 'error');
                return;
            }
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        name: name,
                        sourceType: selectedSourceType,
                        targetType: selectedTargetType
                    })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('任务创建成功，请继续配置', 'success');
                    closeModal();
                    clearForm();
                    fetchWorkflows();
                    openTaskConfig(result.data.id);
                } else {
                    showNotification(result.message || '创建失败', 'error');
                }
            } catch (error) {
                console.error('创建任务失败:', error);
                showNotification('创建任务失败', 'error');
            }
        }

        async function openTaskConfig(taskId) {
            if (!checkAuth()) return;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}`);
                const result = await response.json();
                
                if (!result.success) {
                    showNotification(result.message || '获取任务信息失败', 'error');
                    return;
                }
                
                const task = result.data;
                if (task.status !== 'CONFIGURING') {
                    showTaskDetail(taskId);
                    return;
                }
                
                cfgWorkflowId = taskId;
                cfgSourceType = task.source_type || 'mysql';
                cfgTargetType = task.target_type || 'mysql';
                cfgCurrentStep = 1;
                cfgConnectionTestStatus = { source: null, target: null };
                cfgSelectedSyncObjects = {};
                cfgTableNameMapping = {};
                cfgTablesCache = {};
                cfgDatabasesCache = [];
                cfgSchemasCache = [];
                cfgValidationPassed = false;
                cfgClearAllColumnProcessing();
                cfgColumnsCache = {};

                // 同步粒度：默认表级；库级同步目前仅支持 MySQL 源（存储程序/触发器/事件复制为 MySQL 实现）
                cfgSyncGranularity = 'table';
                const gTableRadio = document.querySelector('input[name="cfgSyncGranularity"][value="table"]');
                if (gTableRadio) gTableRadio.checked = true;
                const granularityGroup = document.getElementById('cfgGranularityGroup');
                if (granularityGroup) {
                    granularityGroup.style.display = (cfgSourceType === 'mysql' && cfgTargetType === 'mysql') ? '' : 'none';
                }
                
                document.getElementById('configPageTitle').textContent = '任务配置 - ' + task.name;
                
                if (task.source_connection) {
                    cfgParseConnectionString('source', task.source_connection);
                } else {
                    cfgClearConnectionFields('source');
                }
                if (task.target_connection) {
                    cfgParseConnectionString('target', task.target_connection);
                } else {
                    cfgClearConnectionFields('target');
                }
                
                if (task.sync_objects) {
                    try {
                        const syncObjects = JSON.parse(task.sync_objects);
                        cfgSelectedSyncObjects = {};
                        let hasDbLevel = false;
                        for (const [db, dbValue] of Object.entries(syncObjects)) {
                            // 恢复库名映射（entry 的 targetDb，表级/库级均可携带）
                            if (dbValue && dbValue.targetDb && dbValue.targetDb !== db) {
                                cfgDbNameMapping[db] = dbValue.targetDb;
                            }
                            if (dbValue && dbValue.dbLevel === true) {
                                cfgSelectedSyncObjects[db] = DB_LEVEL_SENTINEL;
                                hasDbLevel = true;
                                continue;
                            }
                            let tables = Array.isArray(dbValue) ? dbValue : (dbValue.tables || []);
                            cfgSelectedSyncObjects[db] = tables;
                            // 恢复表名映射（表级 entry 的 tableMapping: {源表: 目标表}）
                            if (dbValue && dbValue.tableMapping && typeof dbValue.tableMapping === 'object') {
                                Object.entries(dbValue.tableMapping).forEach(([src, tgt]) => {
                                    if (tgt && tgt !== src) cfgTableNameMapping[db + '.' + src] = tgt;
                                });
                            }
                            // 恢复列处理配置（表级 entry 的 columnFilter/columnMapping/extraColumns，按表存放）
                            if (dbValue && dbValue.columnFilter && typeof dbValue.columnFilter === 'object') {
                                Object.entries(dbValue.columnFilter).forEach(([t, conds]) => {
                                    if (Array.isArray(conds) && conds.length > 0) cfgColumnFilters[db + '.' + t] = conds;
                                });
                            }
                            if (dbValue && dbValue.columnMapping && typeof dbValue.columnMapping === 'object') {
                                Object.entries(dbValue.columnMapping).forEach(([t, m]) => {
                                    if (m && typeof m === 'object' && Object.keys(m).length > 0) cfgColumnMappings[db + '.' + t] = m;
                                });
                            }
                            if (dbValue && dbValue.extraColumns && typeof dbValue.extraColumns === 'object') {
                                Object.entries(dbValue.extraColumns).forEach(([t, cols]) => {
                                    if (Array.isArray(cols) && cols.length > 0) cfgExtraColumns[db + '.' + t] = cols;
                                });
                            }
                        }
                        // 旧任务兼容：syncObjects 未带 targetDb，但 target_db_name 与唯一源库不同 → 视作库名映射
                        const dbKeys = Object.keys(cfgSelectedSyncObjects);
                        if (Object.keys(cfgDbNameMapping).length === 0 && dbKeys.length === 1
                                && task.target_db_name && task.target_db_name !== dbKeys[0]
                                && cfgSourceType === 'mysql' && cfgTargetType === 'mysql') {
                            cfgDbNameMapping[dbKeys[0]] = task.target_db_name;
                        }
                        cfgSyncGranularity = hasDbLevel ? 'database' : 'table';
                        const gRadio = document.querySelector(`input[name="cfgSyncGranularity"][value="${cfgSyncGranularity}"]`);
                        if (gRadio) gRadio.checked = true;
                    } catch (e) {}
                }
                
                if (task.migration_mode) {
                    if (task.migration_mode === 'fullAndIncre') {
                        document.getElementById('cfgModeFullAndIncre').checked = true;
                    } else {
                        document.getElementById('cfgModeFull').checked = true;
                    }
                }
                
                cfgUpdateDbNameRows();
                cfgUpdateStepUI();
                
                document.getElementById('cfgValidationResult').innerHTML = '<div class="validation-empty">请点击"开始校验"按钮进行数据库同步条件检查</div>';
                
                cfgCompareDiffPage = 0;
                
                document.getElementById('taskConfigModal').classList.add('show');
            } catch (error) {
                console.error('获取任务信息失败:', error);
                showNotification('获取任务信息失败', 'error');
            }
        }

        function cfgProtocolFor(dbType) {
            if (dbType === 'postgresql') return 'postgresql';
            if (dbType === 'oracle') return 'oracle';
            if (dbType === 'mongodb') return 'mongodb';
            if (dbType === 'elasticsearch') return 'elastic';
            return 'mysql';
        }

        function cfgParseConnectionString(type, connStr) {
            const dbType = (type === 'source') ? cfgSourceType : cfgTargetType;
            const protocol = cfgProtocolFor(dbType);
            const regex = new RegExp(protocol + ':\\/\\/([^:]+):([^@]+)@([^:]+):(\\d+)(?:\\/(.+))?', 'i');
            const match = connStr.match(regex);
            if (match) {
                document.getElementById('cfg' + capitalize(type) + 'Username').value = match[1];
                document.getElementById('cfg' + capitalize(type) + 'Password').value = match[2];
                document.getElementById('cfg' + capitalize(type) + 'Host').value = match[3];
                document.getElementById('cfg' + capitalize(type) + 'Port').value = match[4];
                const dbNameInput = document.getElementById('cfg' + capitalize(type) + 'DbNameInput');
                if (match[5] && dbNameInput) {
                    dbNameInput.value = match[5];
                }
            }
        }

        function cfgClearConnectionFields(type) {
            const prefix = 'cfg' + capitalize(type);
            const dbType = (type === 'source') ? cfgSourceType : cfgTargetType;
            const defaultPort = dbType === 'postgresql' ? '5432' : (dbType === 'oracle' ? '1521' : (dbType === 'mongodb' ? '27017' : (dbType === 'elasticsearch' ? '9200' : '3306')));
            document.getElementById(prefix + 'Host').value = '';
            document.getElementById(prefix + 'Port').value = defaultPort;
            document.getElementById(prefix + 'Username').value = '';
            document.getElementById(prefix + 'Password').value = '';
            const dbNameInput = document.getElementById(prefix + 'DbNameInput');
            if (dbNameInput) dbNameInput.value = '';
        }

        function capitalize(str) {
            return str.charAt(0).toUpperCase() + str.slice(1);
        }

        function cfgUpdateDbNameRows() {
            // 连接信息区标题旁的数据库类型徽章：让用户在填写/测试连接时明确当前端点类型
            const srcBadge = document.getElementById('cfgSourceTypeBadge');
            const tgtBadge = document.getElementById('cfgTargetTypeBadge');
            if (srcBadge) srcBadge.textContent = formatDbTypeLabel(cfgSourceType);
            if (tgtBadge) tgtBadge.textContent = formatDbTypeLabel(cfgTargetType);

            const sourceDbRow = document.getElementById('cfgSourceDbNameRow');
            const targetDbRow = document.getElementById('cfgTargetDbNameRow');
            sourceDbRow.style.display = (cfgSourceType === 'postgresql' || cfgSourceType === 'oracle') ? 'block' : 'none';
            targetDbRow.style.display = cfgTargetType === 'postgresql' ? 'block' : 'none';

            // 更新源端数据库名输入框的提示文案
            const sourceDbNameInput = document.getElementById('cfgSourceDbNameInput');
            if (sourceDbNameInput) {
                if (cfgSourceType === 'postgresql') {
                    sourceDbNameInput.placeholder = '数据库名称（PG必填，如 test_db1）';
                } else if (cfgSourceType === 'oracle') {
                    sourceDbNameInput.placeholder = '服务名（Oracle必填，如 FREEPDB1）';
                }
            }
        }

        function cfgBuildConnectionString(type) {
            const prefix = 'cfg' + capitalize(type);
            const host = document.getElementById(prefix + 'Host').value.trim();
            const port = document.getElementById(prefix + 'Port').value.trim();
            const username = document.getElementById(prefix + 'Username').value.trim();
            const password = document.getElementById(prefix + 'Password').value.trim();
            const dbType = (type === 'source') ? cfgSourceType : cfgTargetType;
            const protocol = cfgProtocolFor(dbType);

            if (!host || !port || !username) return '';

            let connStr = `${protocol}://${username}:${password}@${host}:${port}`;

            if (dbType === 'postgresql' || dbType === 'oracle') {
                const dbNameInput = document.getElementById(prefix + 'DbNameInput');
                if (dbNameInput && dbNameInput.value.trim()) {
                    connStr += '/' + dbNameInput.value.trim();
                }
            }

            return connStr;
        }

        function cfgBuildConnectionStringWithDb(type, dbName) {
            const base = cfgBuildConnectionString(type);
            if (!base) return '';
            if (cfgSourceType === 'postgresql' && type === 'source') {
                return base;
            }
            return `${base}/${dbName}`;
        }

        function cfgOnConnectionFieldChange(type) {
            cfgConnectionTestStatus[type] = null;
            const prefix = 'cfg' + capitalize(type);
            const statusDiv = document.getElementById(prefix + 'ConnectionStatus');
            statusDiv.className = 'connection-status';
            statusDiv.textContent = '';
            
            ['Host', 'Port', 'Username', 'Password'].forEach(field => {
                document.getElementById(prefix + field).classList.remove('error');
            });
            const dbNameInput = document.getElementById(prefix + 'DbNameInput');
            if (dbNameInput) dbNameInput.classList.remove('error');
        }

        function cfgValidateConnectionFields(type) {
            const prefix = 'cfg' + capitalize(type);
            const hostInput = document.getElementById(prefix + 'Host');
            const portInput = document.getElementById(prefix + 'Port');
            const usernameInput = document.getElementById(prefix + 'Username');
            const passwordInput = document.getElementById(prefix + 'Password');
            
            const host = hostInput.value.trim();
            const port = portInput.value.trim();
            const username = usernameInput.value.trim();
            const password = passwordInput.value.trim();
            
            let errors = [];
            
            [hostInput, portInput, usernameInput, passwordInput].forEach(input => input.classList.remove('error'));
            
            if (!host) { errors.push('IP地址'); hostInput.classList.add('error'); }
            else if (!/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?)*)$/.test(host)) { errors.push('IP地址格式不正确'); hostInput.classList.add('error'); }
            if (!port) { errors.push('端口号'); portInput.classList.add('error'); }
            else if (!/^\d+$/.test(port) || parseInt(port) < 1 || parseInt(port) > 65535) { errors.push('端口号范围1-65535'); portInput.classList.add('error'); }
            if (!username) { errors.push('用户名'); usernameInput.classList.add('error'); }
            else if (!/^[a-zA-Z0-9_]+$/.test(username)) { errors.push('用户名格式不正确'); usernameInput.classList.add('error'); }
            if (!password) { errors.push('密码'); passwordInput.classList.add('error'); }
            
            const dbType = (type === 'source') ? cfgSourceType : cfgTargetType;
            if (dbType === 'postgresql' || dbType === 'oracle') {
                const dbNameInput = document.getElementById(prefix + 'DbNameInput');
                if (dbNameInput && !dbNameInput.value.trim()) {
                    const label = dbType === 'postgresql' ? '数据库名称(PG必填)' : '服务名(Oracle必填)';
                    errors.push(label);
                    dbNameInput.classList.add('error');
                } else if (dbNameInput) {
                    dbNameInput.classList.remove('error');
                }
            }

            return errors;
        }

        function cfgTestConnection(type) {
            const prefix = 'cfg' + capitalize(type);
            const statusDiv = document.getElementById(prefix + 'ConnectionStatus');
            const testBtn = document.getElementById('cfgTest' + capitalize(type) + 'Btn');
            
            const errors = cfgValidateConnectionFields(type);
            if (errors.length > 0) {
                showNotification('请正确填写: ' + errors.join('、'), 'error');
                return;
            }
            
            const connection = cfgBuildConnectionString(type);
            if (!connection) {
                showNotification('请先填写' + (type === 'source' ? '源' : '目标') + '数据库连接信息', 'error');
                return;
            }
            
            const dbType = (type === 'source') ? cfgSourceType : cfgTargetType;
            
            statusDiv.className = 'connection-status testing';
            statusDiv.textContent = '正在测试连接...';
            testBtn.disabled = true;
            
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 20000);
            
            fetchWithAuth(`${API_BASE_URL}/metadata/test-connection`, {
                method: 'POST',
                headers: getAuthHeaders(),
                body: JSON.stringify({ sourceConnection: connection, dbType: dbType }),
                signal: controller.signal
            })
            .then(response => response.json())
            .then(result => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                if (result.success && result.data && result.data.connected) {
                    statusDiv.className = 'connection-status success';
                    statusDiv.textContent = '✓ 连接成功';
                    cfgConnectionTestStatus[type] = true;
                } else if (result.success && result.data && !result.data.connected) {
                    statusDiv.className = 'connection-status error';
                    const errorType = result.data.errorType;
                    const errorMsg = result.data.errorMessage || '无法连接到数据库';
                    const suggestion = result.data.suggestion || '';
                    let displayMsg = '';
                    if (errorType === 'AUTH_FAILED') displayMsg = '✗ 认证失败：用户名或密码错误';
                    else if (errorType === 'NETWORK_ERROR') displayMsg = '✗ 网络错误：无法连接到数据库服务器';
                    else if (errorType === 'DB_TYPE_MISMATCH') displayMsg = '✗ 类型不匹配：' + errorMsg;
                    else if (errorType === 'TIMEOUT') displayMsg = '✗ 连接超时：20秒内未连接到数据库服务器';
                    // 后端 errorMsg 已是完整描述（如"连接失败：xxx"），直接展示，避免"✗ 连接失败：连接失败："重复前缀
                    else displayMsg = '✗ ' + errorMsg.substring(0, 100);
                    statusDiv.innerHTML = `<div>${displayMsg}</div>${suggestion ? '<div style="font-size:11px;color:#999;margin-top:2px;">💡 ' + suggestion + '</div>' : ''}`;
                    cfgConnectionTestStatus[type] = false;
                } else {
                    statusDiv.className = 'connection-status error';
                    statusDiv.textContent = '✗ 连接失败: ' + (result.message || '无法连接到数据库');
                    cfgConnectionTestStatus[type] = false;
                }
            })
            .catch(error => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                statusDiv.className = 'connection-status error';
                if (error.name === 'AbortError') {
                    statusDiv.innerHTML = '<div>✗ 连接超时：20秒内未连接到数据库服务器</div><div style="font-size:11px;color:#999;margin-top:2px;">💡 请检查目标主机是否可达、端口是否开放、防火墙是否放行</div>';
                } else {
                    statusDiv.textContent = '✗ 连接测试出错: ' + error.message;
                }
                cfgConnectionTestStatus[type] = false;
            });
        }

        // 向导步骤序列：同引擎(mysql→mysql/pg→pg) 为 1连接→2对象→3列处理→4校验；其余库对无列处理步骤（1→2→4）
        function cfgStepSequence() {
            return cfgColProcSupported() ? [1, 2, 3, 4] : [1, 2, 4];
        }

        function cfgGoToStep(step) {
            const seq = cfgStepSequence();
            if (!seq.includes(step)) return;

            if (step === 2 && cfgCurrentStep === 1) {
                const sourceErrors = cfgValidateConnectionFields('source');
                if (sourceErrors.length > 0) {
                    showNotification('请正确填写源数据库: ' + sourceErrors.join('、'), 'error');
                    return;
                }
                if (!cfgConnectionTestStatus.source) {
                    showNotification('请先测试源数据库连接', 'error');
                    return;
                }
                const targetErrors = cfgValidateConnectionFields('target');
                if (targetErrors.length > 0) {
                    showNotification('请正确填写目标数据库: ' + targetErrors.join('、'), 'error');
                    return;
                }
                if (!cfgConnectionTestStatus.target) {
                    showNotification('请先测试目标数据库连接', 'error');
                    return;
                }
                
                cfgSaveConfig().then(() => {
                    const sourceConnection = cfgBuildConnectionString('source');
                    if (cfgSourceType === 'postgresql' || cfgSourceType === 'oracle') {
                        const prefix = 'cfg' + capitalize('source');
                        const dbNameInput = document.getElementById(prefix + 'DbNameInput');
                        const sourceDbName = dbNameInput ? dbNameInput.value.trim() : '';
                        cfgLoadSchemas(sourceConnection, sourceDbName);
                    } else {
                        cfgLoadDatabases(sourceConnection);
                    }
                });
            }

            // 离开步骤 2 向后走（进列处理或校验）：必须已选同步对象，且保存配置
            if (step > 2 && cfgCurrentStep === 2) {
                if (Object.keys(cfgSelectedSyncObjects).length === 0) {
                    showNotification('请至少选择一个同步对象', 'error');
                    return;
                }
                if (cfgTargetType === 'postgresql') {
                    // PG 目标库名取第 1 步目标连接区的数据库名称输入（原步骤 2 的目标数据库名称输入框已移除）
                    const tgtDbInput = document.getElementById('cfgTargetDbNameInput');
                    if (!tgtDbInput || !tgtDbInput.value.trim()) {
                        showNotification('请在连接信息中填写目标PostgreSQL数据库名称', 'error');
                        return;
                    }
                }
                cfgSaveConfig();
            }

            // 离开列处理步骤向后走：把列处理配置持久化进 syncObjects
            if (step === 4 && cfgCurrentStep === 3) {
                cfgSaveConfig();
            }

            cfgCurrentStep = step;
            if (step === 3) {
                cfgRenderColProcPage();
            }
            cfgUpdateStepUI();
        }

        function cfgNextStep() {
            const seq = cfgStepSequence();
            const idx = seq.indexOf(cfgCurrentStep);
            if (idx >= 0 && idx < seq.length - 1) {
                cfgGoToStep(seq[idx + 1]);
            }
        }

        function cfgPrevStep() {
            const seq = cfgStepSequence();
            const idx = seq.indexOf(cfgCurrentStep);
            if (idx > 0) {
                cfgGoToStep(seq[idx - 1]);
            }
        }

        function cfgUpdateStepUI() {
            const colProc = cfgColProcSupported();
            const lastStep = 4;
            document.getElementById('cfgStep1Nav').className = 'step-item' + (cfgCurrentStep === 1 ? ' active' : (cfgCurrentStep > 1 ? ' completed' : ''));
            document.getElementById('cfgStep2Nav').className = 'step-item' + (cfgCurrentStep === 2 ? ' active' : (cfgCurrentStep > 2 ? ' completed' : ''));
            const step3Nav = document.getElementById('cfgStep3Nav');
            step3Nav.style.display = colProc ? '' : 'none';
            step3Nav.className = 'step-item' + (cfgCurrentStep === 3 ? ' active' : (cfgCurrentStep > 3 ? ' completed' : ''));
            document.getElementById('cfgStep4Nav').className = 'step-item' + (cfgCurrentStep === 4 ? ' active' : '');
            // 非 mysql→mysql 无列处理步骤，校验检查按第 3 步展示
            document.getElementById('cfgStep4NavLabel').textContent = colProc ? '4. 校验检查' : '3. 校验检查';

            document.getElementById('cfgStep1Content').className = 'step-content' + (cfgCurrentStep === 1 ? ' active' : '');
            document.getElementById('cfgStep2Content').className = 'step-content' + (cfgCurrentStep === 2 ? ' active' : '');
            document.getElementById('cfgStep3Content').className = 'step-content' + (cfgCurrentStep === 3 ? ' active' : '');
            document.getElementById('cfgStep4Content').className = 'step-content' + (cfgCurrentStep === 4 ? ' active' : '');

            document.getElementById('cfgNextBtn').style.display = (cfgCurrentStep < lastStep) ? 'inline-block' : 'none';
            document.getElementById('cfgPrevBtn').style.display = (cfgCurrentStep > 1) ? 'inline-block' : 'none';
            document.getElementById('cfgLaunchBtn').style.display = cfgCurrentStep === lastStep ? 'inline-block' : 'none';
        }

        async function cfgSaveConfig() {
            const sourceConnection = cfgBuildConnectionString('source');
            const targetConnection = cfgBuildConnectionString('target');
            const modeFull = document.getElementById('cfgModeFull');
            const migrationMode = modeFull.checked ? 'full' : 'fullAndIncre';
            
            let syncObjectsJson = null;
            let sourceDbName = null;
            
            if (Object.keys(cfgSelectedSyncObjects).length > 0) {
                const syncObjectsData = {};
                Object.keys(cfgSelectedSyncObjects).forEach(db => {
                    // 库级同步：{"db":{"dbLevel":true}}，不枚举表（增量期新对象自动纳入同步范围）
                    if (cfgSelectedSyncObjects[db] === DB_LEVEL_SENTINEL) {
                        syncObjectsData[db] = { dbLevel: true };
                    } else {
                        syncObjectsData[db] = { tables: cfgSelectedSyncObjects[db] };
                        // 表名映射（仅表级）：只写与源表名不同的有效映射
                        const mapping = {};
                        cfgSelectedSyncObjects[db].forEach(t => {
                            const tgt = cfgTableNameMapping[db + '.' + t];
                            if (tgt && tgt !== t) mapping[t] = tgt;
                        });
                        if (Object.keys(mapping).length > 0) {
                            syncObjectsData[db].tableMapping = mapping;
                        }
                        // 列处理（仅表级 + mysql→mysql）：columnFilter/columnMapping/extraColumns 按表写入
                        if (cfgColProcSupported()) {
                            const colFilter = {}, colMapping = {}, extraCols = {};
                            cfgSelectedSyncObjects[db].forEach(t => {
                                const key = db + '.' + t;
                                if (Array.isArray(cfgColumnFilters[key]) && cfgColumnFilters[key].length > 0) {
                                    colFilter[t] = cfgColumnFilters[key];
                                }
                                if (cfgColumnMappings[key] && Object.keys(cfgColumnMappings[key]).length > 0) {
                                    colMapping[t] = cfgColumnMappings[key];
                                }
                                if (Array.isArray(cfgExtraColumns[key]) && cfgExtraColumns[key].length > 0) {
                                    extraCols[t] = cfgExtraColumns[key];
                                }
                            });
                            if (Object.keys(colFilter).length > 0) syncObjectsData[db].columnFilter = colFilter;
                            if (Object.keys(colMapping).length > 0) syncObjectsData[db].columnMapping = colMapping;
                            if (Object.keys(extraCols).length > 0) syncObjectsData[db].extraColumns = extraCols;
                        }
                    }
                    // 库名映射：只写与源库名不同的有效映射（表级/库级均支持）
                    const tgtDb = cfgDbNameMapping[db];
                    if (tgtDb && tgtDb !== db) {
                        syncObjectsData[db].targetDb = tgtDb;
                    }
                });
                syncObjectsJson = JSON.stringify(syncObjectsData);

                if (cfgSourceType === 'postgresql' || cfgSourceType === 'oracle') {
                    const prefix = 'cfg' + capitalize('source');
                    const dbNameInput = document.getElementById(prefix + 'DbNameInput');
                    sourceDbName = dbNameInput ? dbNameInput.value.trim() : '';
                } else {
                    const dbNames = Object.keys(cfgSelectedSyncObjects);
                    if (dbNames.length === 1) sourceDbName = dbNames[0];
                }
            }

            // 目标库名派生（原"目标数据库名称"输入框已移除）：
            // - PG 目标：库名是连接层概念，取第 1 步目标连接区的数据库名称输入（PG 必填）
            // - MySQL 目标：取"已选对象"栏的库名映射（未映射 = 与源库同名）
            // - mongo/ES：镜像/索引自动命名，仅用于详情展示
            let targetDbName = '';
            const firstDb = Object.keys(cfgSelectedSyncObjects)[0] || '';
            if (cfgTargetType === 'postgresql') {
                const tgtDbInput = document.getElementById('cfgTargetDbNameInput');
                targetDbName = tgtDbInput ? tgtDbInput.value.trim() : '';
            } else if (cfgSourceType === 'mysql' && cfgTargetType === 'mysql') {
                targetDbName = (sourceDbName && cfgDbNameMapping[sourceDbName]) || sourceDbName || firstDb;
            } else {
                // pg→mysql（同名库）、mongodb 镜像、elasticsearch 索引自动命名
                targetDbName = sourceDbName || firstDb;
            }
            
            try {
                await fetchWithAuth(`${API_BASE_URL}/workflows/${cfgWorkflowId}/config`, {
                    method: 'PUT',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        sourceConnection: sourceConnection || '',
                        targetConnection: targetConnection || '',
                        migrationMode,
                        syncObjects: syncObjectsJson,
                        sourceDbName,
                        targetDbName: targetDbName,
                        sourceType: cfgSourceType,
                        targetType: cfgTargetType
                    })
                });
            } catch (error) {
                console.error('保存配置失败:', error);
            }
        }

        async function cfgRunValidation() {
            const sourceConnection = cfgBuildConnectionString('source');
            const targetConnection = cfgBuildConnectionString('target');
            const modeFull = document.getElementById('cfgModeFull');
            const migrationMode = modeFull.checked ? 'full' : 'fullAndIncre';
            
            const resultDiv = document.getElementById('cfgValidationResult');
            const runBtn = document.getElementById('cfgRunValidationBtn');
            
            resultDiv.innerHTML = '<div class="validation-loading">正在校验中，请稍候...</div>';
            runBtn.disabled = true;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/validate`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        sourceConnection: sourceConnection,
                        targetConnection: targetConnection,
                        migrationMode: migrationMode,
                        sourceType: cfgSourceType,
                        targetType: cfgTargetType
                    })
                });
                
                const result = await response.json();
                
                if (result.success && result.data) {
                    cfgRenderValidationResult(result.data);
                } else {
                    resultDiv.innerHTML = `<div class="validation-empty" style="color: #f5222d;">校验失败: ${result.message || '未知错误'}</div>`;
                    cfgValidationPassed = false;
                }
            } catch (error) {
                console.error('校验失败:', error);
                resultDiv.innerHTML = `<div class="validation-empty" style="color: #f5222d;">校验请求失败: ${error.message}</div>`;
                cfgValidationPassed = false;
            } finally {
                runBtn.disabled = false;
            }
        }

        function cfgRenderValidationResult(data) {
            const resultDiv = document.getElementById('cfgValidationResult');
            let html = '';
            
            if (data.checkItems && data.checkItems.length > 0) {
                data.checkItems.forEach(item => {
                    const statusClass = item.passed ? 'passed' : (item.severity === 'warning' ? 'warning' : 'failed');
                    const icon = item.passed ? '✓' : (item.severity === 'warning' ? '⚠' : '✗');
                    html += `
                        <div class="check-item ${statusClass}">
                            <div class="check-icon ${statusClass}">${icon}</div>
                            <div class="check-content">
                                <div class="check-name">${item.name}</div>
                                <div class="check-description">${item.description}</div>
                                <div class="check-message">${item.message}</div>
                            </div>
                        </div>
                    `;
                });
            }
            
            const summaryClass = data.allPassed ? 'passed' : 'failed';
            const summaryText = data.allPassed ? '✓ 所有检查项均已通过，可以启动任务' : '✗ 存在未通过的检查项，请修复后再启动任务';
            html += `<div class="validation-summary ${summaryClass}">${summaryText}</div>`;
            
            resultDiv.innerHTML = html;
            cfgValidationPassed = data.allPassed;
        }


        // 启动前 schema 预检门禁：返回 true=可继续启动，false=中止。
        // PASS 直接放行；WARNING 提示后由用户确认；FAIL 需明确二次确认强制启动。
        async function schemaPrecheckGate(workflowId) {
            let data;
            try {
                const resp = await fetchWithAuth(`${API_BASE_URL}/advanced/schema-precheck/${workflowId}`, {
                    method: 'POST', headers: getAuthHeaders()
                });
                const j = await resp.json();
                if (!j.success) {
                    // 预检自身出错不应硬卡启动，提示后交由用户决定
                    return confirm('schema 预检未能完成：' + (j.message || '未知错误') + '\n\n仍要启动吗？');
                }
                data = j.data;
            } catch (e) {
                return confirm('schema 预检请求异常，仍要启动吗？');
            }

            const overall = data.overall;
            if (overall === 'PASS') return true;

            const lines = (data.checks || [])
                .filter(c => c.status === 'FAIL' || c.status === 'WARNING')
                .map(c => `${c.status === 'FAIL' ? '✗' : '⚠'} ${c.checkName}：${c.message}${c.detail ? '\n    ' + c.detail : ''}`)
                .join('\n');

            if (overall === 'FAIL') {
                return confirm(`schema 预检发现严重问题（可能导致同步失败）：\n\n${lines}\n\n确定要忽略并强制启动吗？`);
            }
            // WARNING
            return confirm(`schema 预检有警告：\n\n${lines}\n\n确定继续启动吗？`);
        }

        async function launchTask() {
            if (!checkAuth()) return;

            if (!cfgValidationPassed) {
                showNotification('请先完成校验检查并确保所有检查项通过', 'error');
                return;
            }

            try {
                await cfgSaveConfig();

                // 启动前 schema 预检：把结构问题（源表缺失、无主键、列处理引用不存在的列、
                // 目标同名表冲突）挡在启动前。FAIL 需二次确认强制启动，WARNING 提示后可继续。
                if (!await schemaPrecheckGate(cfgWorkflowId)) return;

                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${cfgWorkflowId}/launch`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('任务启动成功', 'success');
                    closeConfigModal();
                    fetchWorkflows();
                } else {
                    showNotification(result.message || '启动失败', 'error');
                }
            } catch (error) {
                console.error('启动任务失败:', error);
                showNotification('启动任务失败', 'error');
            }
        }

        function closeConfigModal() {
            document.getElementById('taskConfigModal').classList.remove('show');
            cfgWorkflowId = null;
        }

        async function cfgLoadDatabases(connectionStr) {
            const sourceList = document.getElementById('cfgSourceObjectsList');
            sourceList.innerHTML = '<div class="empty-selection">加载中...</div>';
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/databases`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr })
                });
                const result = await response.json();
                if (result.success && result.data.databases) {
                    cfgDatabasesCache = result.data.databases;
                    cfgRenderDatabases();
                } else {
                    sourceList.innerHTML = `<div class="empty-selection" style="color: #f5222d;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                sourceList.innerHTML = '<div class="empty-selection" style="color: #f5222d;">加载失败，请检查连接串</div>';
            }
        }

        function cfgRenderDatabases() {
            const sourceList = document.getElementById('cfgSourceObjectsList');
            if (cfgDatabasesCache.length === 0) {
                sourceList.innerHTML = '<div class="empty-selection">未找到可访问的数据库</div>';
                return;
            }
            let html = '';
            if (cfgSyncGranularity === 'database') {
                // 库级：按库勾选整库，不展开表清单（同步范围=库内全部现有+未来新建对象）
                cfgDatabasesCache.forEach(db => {
                    const checked = cfgSelectedSyncObjects[db] === DB_LEVEL_SENTINEL ? 'checked' : '';
                    html += `
                        <div class="database-item" data-db="${db}">
                            <div class="database-header" style="cursor: pointer;" onclick="cfgToggleDbLevel('${db}', !(cfgSelectedSyncObjects['${db}'] === DB_LEVEL_SENTINEL))">
                                <input type="checkbox" class="database-checkbox" ${checked} onclick="event.stopPropagation(); cfgToggleDbLevel('${db}', this.checked)">
                                <span class="database-name">${db}</span>
                                <span style="margin-left: auto; font-size: 11px; color: #999;">整库</span>
                            </div>
                        </div>
                    `;
                });
                sourceList.innerHTML = html;
                return;
            }
            cfgDatabasesCache.forEach(db => {
                html += `
                    <div class="database-item" data-db="${db}">
                        <div class="database-header">
                            <input type="checkbox" class="database-checkbox" onclick="cfgSelectAllTables('${db}', this.checked)">
                            <span class="database-name" onclick="cfgToggleDatabase('${db}')">${db}</span>
                            <span class="database-expand" id="cfg-expand-${db}" onclick="cfgToggleDatabase('${db}')">▶</span>
                        </div>
                        <div class="table-list" id="cfg-tables-${db}">
                            <div style="padding: 16px; color: #999; font-size: 12px;">加载中...</div>
                        </div>
                    </div>
                `;
            });
            sourceList.innerHTML = html;
        }

        async function cfgToggleDatabase(db) {
            const tableList = document.getElementById(`cfg-tables-${db}`);
            const expandIcon = document.getElementById(`cfg-expand-${db}`);
            if (tableList.classList.contains('show')) {
                tableList.classList.remove('show');
                expandIcon.classList.remove('expanded');
            } else {
                tableList.classList.add('show');
                expandIcon.classList.add('expanded');
                if (!cfgTablesCache[db]) {
                    await cfgLoadTables(db);
                }
            }
        }

        async function cfgLoadTables(db) {
            const connectionStr = cfgBuildConnectionStringWithDb('source', db);
            const tableList = document.getElementById(`cfg-tables-${db}`);
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/tables`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: db })
                });
                const result = await response.json();
                if (result.success && result.data.tables) {
                    cfgTablesCache[db] = result.data.tables;
                    cfgRenderTables(db);
                } else {
                    tableList.innerHTML = `<div style="padding: 16px; color: #f5222d; font-size: 12px;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                tableList.innerHTML = '<div style="padding: 16px; color: #f5222d; font-size: 12px;">加载失败</div>';
            }
        }

        function cfgRenderTables(db) {
            const tableList = document.getElementById(`cfg-tables-${db}`);
            const tables = cfgTablesCache[db] || [];
            if (tables.length === 0) {
                tableList.innerHTML = '<div style="padding: 16px; color: #999; font-size: 12px;">该数据库没有表</div>';
                return;
            }
            let html = '';
            tables.forEach(table => {
                const isSelected = Array.isArray(cfgSelectedSyncObjects[db]) && cfgSelectedSyncObjects[db].includes(table.name);
                html += `
                    <div class="table-item">
                        <span class="table-name">${table.name}</span>
                        <span class="table-info">${table.rows} 行 | ${table.size}</span>
                        <button class="table-add-btn" onclick="cfgAddTable('${db}', '${table.name}')" ${isSelected ? 'disabled style="opacity: 0.5;"' : ''}>
                            ${isSelected ? '已选择' : '选择'}
                        </button>
                    </div>
                `;
            });
            tableList.innerHTML = html;
        }

        function cfgSelectAllTables(db, checked) {
            const tableList = document.getElementById(`cfg-tables-${db}`);
            const expandIcon = document.getElementById(`cfg-expand-${db}`);
            if (checked) {
                if (tableList && !tableList.classList.contains('show')) {
                    tableList.classList.add('show');
                    expandIcon.classList.add('expanded');
                }
                if (!cfgTablesCache[db]) {
                    cfgLoadTables(db).then(() => {
                        if (!cfgSelectedSyncObjects[db]) cfgSelectedSyncObjects[db] = [];
                        cfgTablesCache[db].forEach(table => {
                            if (!cfgSelectedSyncObjects[db].includes(table.name)) cfgSelectedSyncObjects[db].push(table.name);
                        });
                        cfgRenderTables(db);
                        cfgRenderSelectedObjects();
                    });
                    return;
                }
                if (!cfgSelectedSyncObjects[db]) cfgSelectedSyncObjects[db] = [];
                cfgTablesCache[db].forEach(table => {
                    if (!cfgSelectedSyncObjects[db].includes(table.name)) cfgSelectedSyncObjects[db].push(table.name);
                });
            } else {
                delete cfgSelectedSyncObjects[db];
                cfgClearTableMappingForDb(db);
            }
            cfgRenderTables(db);
            cfgRenderSelectedObjects();
        }

        function cfgAddTable(db, tableName) {
            if (!cfgSelectedSyncObjects[db]) cfgSelectedSyncObjects[db] = [];
            if (!cfgSelectedSyncObjects[db].includes(tableName)) cfgSelectedSyncObjects[db].push(tableName);
            cfgRenderTables(db);
            cfgRenderSelectedObjects();
        }

        function cfgRemoveTable(db, tableName) {
            if (cfgSelectedSyncObjects[db]) {
                const index = cfgSelectedSyncObjects[db].indexOf(tableName);
                if (index > -1) cfgSelectedSyncObjects[db].splice(index, 1);
                if (cfgSelectedSyncObjects[db].length === 0) delete cfgSelectedSyncObjects[db];
            }
            delete cfgTableNameMapping[db + '.' + tableName];
            cfgClearColumnProcessingForTable(db, tableName);
            cfgRenderSelectedObjects();
            if (cfgTablesCache[db]) cfgRenderTables(db);
            else if (cfgTablesCache[`pg-${db}`]) {
                const prefix = 'cfg' + capitalize('source');
                const dbNameInput = document.getElementById(prefix + 'DbNameInput');
                const database = dbNameInput ? dbNameInput.value.trim() : '';
                cfgRenderPgTables(database, db);
            }
        }

        function cfgRenderSelectedObjects() {
            const selectedList = document.getElementById('cfgSelectedObjectsList');
            const dbNames = Object.keys(cfgSelectedSyncObjects);
            if (dbNames.length === 0) {
                selectedList.innerHTML = '<div class="empty-selection">暂未选择任何对象</div>';
                return;
            }
            let html = '';
            const isPgSource = cfgSourceType === 'postgresql';
            // 库名映射仅 MySQL 源→MySQL 目标有意义（PG 目标库名走连接区；mongo/ES 镜像/索引自动命名）
            const dbMappingEnabled = cfgSourceType === 'mysql' && cfgTargetType === 'mysql';
            dbNames.forEach(db => {
                const mappedDb = cfgDbNameMapping[db] || '';
                // 库头行：库名 + 目标库名映射输入（取代原"目标数据库名称"输入框，按库配置）
                const dbMappingInput = dbMappingEnabled ? `
                            <span style="color: #bbb; flex-shrink: 0;">→</span>
                            <input type="text" class="mapping-name-input" value="${mappedDb}" placeholder="${db}"
                                   title="目标库名（留空 = 与源库同名）"
                                   onchange="cfgSetDbNameMapping('${db}', this.value)"
                                   onclick="event.stopPropagation()">` : '';
                if (cfgSelectedSyncObjects[db] === DB_LEVEL_SENTINEL) {
                    html += `
                        <div class="selected-item" style="background: #fafafa;">
                            <span class="selected-table-name" style="font-weight: 600;">${db}</span>
                            ${dbMappingInput}
                            <button class="remove-btn" onclick="cfgToggleDbLevel('${db}', false)">移除</button>
                        </div>
                        <div class="selected-item">
                            <span class="selected-table-name" style="color: #52c41a; font-size: 12px;">（整库：全部表+存储过程/函数，增量期新对象自动同步）</span>
                        </div>
                    `;
                    return;
                }
                const label = isPgSource ? `${db} (schema)` : db;
                html += `
                    <div class="selected-item" style="background: #fafafa;">
                        <span class="selected-table-name" style="font-weight: 600;">${label}</span>
                        ${dbMappingInput}
                    </div>
                `;
                cfgSelectedSyncObjects[db].forEach(tableName => {
                    const mapped = cfgTableNameMapping[db + '.' + tableName] || '';
                    html += `
                        <div class="selected-item" style="padding-left: 28px;">
                            <span class="selected-table-name">${tableName}</span>
                            <span style="color: #bbb; flex-shrink: 0;">→</span>
                            <input type="text" class="mapping-name-input" value="${mapped}" placeholder="${tableName}"
                                   title="目标表名（留空 = 与源表同名）"
                                   onchange="cfgSetTableMapping('${db}', '${tableName}', this.value)"
                                   onclick="event.stopPropagation()">
                            <button class="remove-btn" onclick="cfgRemoveTable('${db}', '${tableName}')">移除</button>
                        </div>
                    `;
                });
            });
            selectedList.innerHTML = html;
        }

        // 刷新源库对象列表：清空缓存后按当前连接串重新拉取（源库新建了库/表/集合后无需退出向导）
        function cfgRefreshObjects() {
            const sourceConnection = cfgBuildConnectionString('source');
            if (!sourceConnection) {
                showNotification('请先填写源数据库连接信息', 'error');
                return;
            }
            cfgDatabasesCache = [];
            cfgSchemasCache = [];
            cfgTablesCache = {};
            if (cfgSourceType === 'postgresql' || cfgSourceType === 'oracle') {
                const dbNameInput = document.getElementById('cfgSourceDbNameInput');
                cfgLoadSchemas(sourceConnection, dbNameInput ? dbNameInput.value.trim() : '');
            } else {
                cfgLoadDatabases(sourceConnection);
            }
        }

        async function cfgLoadSchemas(connectionStr, database) {
            const sourceList = document.getElementById('cfgSourceObjectsList');
            sourceList.innerHTML = '<div class="empty-selection">加载中...</div>';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/schemas`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: database })
                });
                const result = await response.json();
                if (result.success && result.data.schemas) {
                    cfgSchemasCache = result.data.schemas;
                    cfgRenderSchemas(database);
                } else {
                    sourceList.innerHTML = `<div class="empty-selection" style="color: #f5222d;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                sourceList.innerHTML = '<div class="empty-selection" style="color: #f5222d;">加载失败，请检查连接信息</div>';
            }
        }

        function cfgRenderSchemas(database) {
            const sourceList = document.getElementById('cfgSourceObjectsList');
            if (cfgSchemasCache.length === 0) {
                sourceList.innerHTML = '<div class="empty-selection">未找到可访问的schema</div>';
                return;
            }
            let html = '';
            cfgSchemasCache.forEach(schema => {
                html += `
                    <div class="database-item" data-db="${schema}" data-schema="${schema}" data-pg-database="${database}">
                        <div class="database-header">
                            <input type="checkbox" class="database-checkbox" onclick="cfgSelectAllPgTables('${database}', '${schema}', this.checked)">
                            <span class="database-name" onclick="cfgTogglePgSchema('${database}', '${schema}')">${schema}</span>
                            <span class="database-expand" id="cfg-expand-pg-${schema}" onclick="cfgTogglePgSchema('${database}', '${schema}')">▶</span>
                        </div>
                        <div class="table-list" id="cfg-tables-pg-${schema}">
                            <div style="padding: 16px; color: #999; font-size: 12px;">加载中...</div>
                        </div>
                    </div>
                `;
            });
            sourceList.innerHTML = html;
        }

        async function cfgTogglePgSchema(database, schema) {
            const tableList = document.getElementById(`cfg-tables-pg-${schema}`);
            const expandIcon = document.getElementById(`cfg-expand-pg-${schema}`);
            if (tableList.classList.contains('show')) {
                tableList.classList.remove('show');
                expandIcon.classList.remove('expanded');
            } else {
                tableList.classList.add('show');
                expandIcon.classList.add('expanded');
                if (!cfgTablesCache[`pg-${schema}`]) {
                    await cfgLoadPgTables(database, schema);
                }
            }
        }

        async function cfgLoadPgTables(database, schema) {
            const connectionStr = cfgBuildConnectionString('source');
            const tableList = document.getElementById(`cfg-tables-pg-${schema}`);
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/tables`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: database, schema: schema })
                });
                const result = await response.json();
                if (result.success && result.data.tables) {
                    cfgTablesCache[`pg-${schema}`] = result.data.tables;
                    cfgRenderPgTables(database, schema);
                } else {
                    tableList.innerHTML = `<div style="padding: 16px; color: #f5222d; font-size: 12px;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                tableList.innerHTML = '<div style="padding: 16px; color: #f5222d; font-size: 12px;">加载失败</div>';
            }
        }

        function cfgRenderPgTables(database, schema) {
            const tableList = document.getElementById(`cfg-tables-pg-${schema}`);
            const tables = cfgTablesCache[`pg-${schema}`] || [];
            if (tables.length === 0) {
                tableList.innerHTML = '<div style="padding: 16px; color: #999; font-size: 12px;">该schema没有表</div>';
                return;
            }
            let html = '';
            tables.forEach(table => {
                const isSelected = cfgSelectedSyncObjects[schema] && cfgSelectedSyncObjects[schema].includes(table.name);
                html += `
                    <div class="table-item">
                        <span class="table-name">${table.name}</span>
                        <span class="table-info">${table.rows} 行</span>
                        <button class="table-add-btn" onclick="cfgAddPgTable('${schema}', '${table.name}')" ${isSelected ? 'disabled style="opacity: 0.5;"' : ''}>
                            ${isSelected ? '已选择' : '选择'}
                        </button>
                    </div>
                `;
            });
            tableList.innerHTML = html;
        }

        function cfgSelectAllPgTables(database, schema, checked) {
            const tableList = document.getElementById(`cfg-tables-pg-${schema}`);
            const expandIcon = document.getElementById(`cfg-expand-pg-${schema}`);
            if (checked) {
                if (tableList && !tableList.classList.contains('show')) {
                    tableList.classList.add('show');
                    expandIcon.classList.add('expanded');
                }
                if (!cfgTablesCache[`pg-${schema}`]) {
                    cfgLoadPgTables(database, schema).then(() => {
                        if (!cfgSelectedSyncObjects[schema]) cfgSelectedSyncObjects[schema] = [];
                        cfgTablesCache[`pg-${schema}`].forEach(table => {
                            if (!cfgSelectedSyncObjects[schema].includes(table.name)) cfgSelectedSyncObjects[schema].push(table.name);
                        });
                        cfgRenderPgTables(database, schema);
                        cfgRenderSelectedObjects();
                    });
                    return;
                }
                if (!cfgSelectedSyncObjects[schema]) cfgSelectedSyncObjects[schema] = [];
                cfgTablesCache[`pg-${schema}`].forEach(table => {
                    if (!cfgSelectedSyncObjects[schema].includes(table.name)) cfgSelectedSyncObjects[schema].push(table.name);
                });
            } else {
                delete cfgSelectedSyncObjects[schema];
            }
            cfgRenderPgTables(database, schema);
            cfgRenderSelectedObjects();
        }

        function cfgAddPgTable(schema, tableName) {
            if (!cfgSelectedSyncObjects[schema]) cfgSelectedSyncObjects[schema] = [];
            if (!cfgSelectedSyncObjects[schema].includes(tableName)) cfgSelectedSyncObjects[schema].push(tableName);
            const prefix = 'cfg' + capitalize('source');
            const dbNameInput = document.getElementById(prefix + 'DbNameInput');
            const database = dbNameInput ? dbNameInput.value.trim() : '';
            cfgRenderPgTables(database, schema);
            cfgRenderSelectedObjects();
        }
        
        let validationCurrentPage = 0;
        let validationPageSize = 10;
        let availableWorkflows = [];
        let _validationPollingTimer = null;
        
        let _pendingMetricsTaskId = null;

        function switchPage(page) {
            console.log('switchPage called with:', page);
            // 菜单高亮按 data-page 匹配（原先按 menuItems 索引定位，新增菜单项会整体错位）
            document.querySelectorAll('.menu-item').forEach(item => {
                item.classList.toggle('active', item.dataset.page === page);
            });

            ['syncPage', 'validationPage', 'drPage', 'subscribePage', 'metricsPage',
             'auditPage', 'automationPage', 'alertPage', 'slowsqlPage'].forEach(id => {
                const el = document.getElementById(id);
                if (el) el.style.display = 'none';
            });
            stopMetricsAutoRefresh();

            if (page === 'sync') {
                document.getElementById('syncPage').style.display = 'block';
            } else if (page === 'validation') {
                document.getElementById('validationPage').style.display = 'block';
                fetchValidationTasks();
            } else if (page === 'dr') {
                document.getElementById('drPage').style.display = 'block';
                fetchDrTasks();
            } else if (page === 'subscribe') {
                document.getElementById('subscribePage').style.display = 'block';
                fetchSubscribeTasks();
            } else if (page === 'metrics') {
                document.getElementById('metricsPage').style.display = 'block';
                loadMetricsTaskList().then(() => {
                    if (_pendingMetricsTaskId) {
                        document.getElementById('metricsTaskSelect').value = _pendingMetricsTaskId;
                        const task = _metricsTaskList.find(t => t.id === _pendingMetricsTaskId);
                        if (task) {
                            document.getElementById('metricsTaskSearch').value = _getMetricsTaskLabel(task);
                        }
                        _pendingMetricsTaskId = null;
                        onMetricsTaskChange();
                    }
                });
                if (document.getElementById('metricsAutoRefresh').checked) {
                    startMetricsAutoRefresh();
                }
            } else if (page === 'audit') {
                const auditPage = document.getElementById('auditPage');
                if (auditPage) {
                    auditPage.style.display = 'block';
                    fetchAuditLogs();
                }
            } else if (page === 'automation') {
                document.getElementById('automationPage').style.display = 'block';
                advLoadAutomationPage();
            } else if (page === 'alert') {
                document.getElementById('alertPage').style.display = 'block';
                advLoadAlertPage();
            } else if (page === 'slowsql') {
                document.getElementById('slowsqlPage').style.display = 'block';
                advLoadSlowSqlPage(1);
            }
        }

        function viewMetrics(taskId) {
            _pendingMetricsTaskId = taskId;
            switchPage('metrics');
        }

        // ============ 审计日志 ============
        let _auditCurrentPage = 0;
        const _auditPageSize = 20;

        const _auditActionLabels = {
            CREATE_TASK: '创建任务', UPDATE_CONFIG: '更新配置', LAUNCH_TASK: '启动任务',
            PAUSE_TASK: '暂停任务', RESUME_TASK: '恢复任务', STOP_TASK: '停止任务',
            DELETE_TASK: '删除任务', RETRY_TASK: '重试任务', FAILOVER_TASK: '故障切换',
            LOGIN: '登录', LOGOUT: '登出'
        };

        async function fetchAuditLogs() {
            const action = document.getElementById('auditActionFilter').value;
            const params = new URLSearchParams({
                page: _auditCurrentPage,
                size: _auditPageSize
            });
            if (action) params.append('action', action);

            try {
                const res = await fetch('/api/audit-logs?' + params.toString(), {
                    headers: { 'Authorization': 'Bearer ' + localStorage.getItem('token') }
                });
                const data = await res.json();
                if (data.success) {
                    renderAuditLogs(data.data || []);
                    document.getElementById('auditPageInfo').textContent =
                        `共 ${data.total} 条，第 ${data.page + 1}/${data.totalPages || 1} 页`;
                    document.getElementById('auditPrevBtn').disabled = data.page === 0;
                    document.getElementById('auditNextBtn').disabled = data.page + 1 >= (data.totalPages || 1);
                } else {
                    document.getElementById('auditLogTableBody').innerHTML =
                        `<tr><td colspan="7" style="padding:24px;text-align:center;color:#f5222d;">${data.message || '加载失败'}</td></tr>`;
                }
            } catch (e) {
                document.getElementById('auditLogTableBody').innerHTML =
                    `<tr><td colspan="7" style="padding:24px;text-align:center;color:#f5222d;">加载失败: ${e.message}</td></tr>`;
            }
        }

        function renderAuditLogs(logs) {
            const tbody = document.getElementById('auditLogTableBody');
            if (!logs.length) {
                tbody.innerHTML = `<tr><td colspan="7" style="padding:24px;text-align:center;color:#999;">暂无审计日志</td></tr>`;
                return;
            }
            tbody.innerHTML = logs.map(log => {
                const time = log.createdAt ? new Date(log.createdAt).toLocaleString('zh-CN') : '-';
                const resultBadge = log.result === 'SUCCESS'
                    ? '<span style="color:#52c41a;">成功</span>'
                    : '<span style="color:#f5222d;">失败</span>';
                const details = log.details ? (log.details.length > 80 ? log.details.substring(0, 80) + '...' : log.details) : '-';
                const error = log.errorMessage ? `<div style="color:#f5222d;font-size:12px;">${escapeHtml(log.errorMessage)}</div>` : '';
                return `<tr style="border-bottom:1px solid #f0f0f0;">
                    <td style="padding:12px;">${time}</td>
                    <td style="padding:12px;">${escapeHtml(log.username || '-')}</td>
                    <td style="padding:12px;"><span style="background:#e6f7ff;color:#1890ff;padding:2px 8px;border-radius:4px;font-size:12px;">${_auditActionLabels[log.action] || log.action}</span></td>
                    <td style="padding:12px;">${escapeHtml(log.workflowName || log.workflowId || '-')}</td>
                    <td style="padding:12px;">${resultBadge}</td>
                    <td style="padding:12px;max-width:300px;word-break:break-all;">${escapeHtml(details)}${error}</td>
                    <td style="padding:12px;">${escapeHtml(log.clientIp || '-')}</td>
                </tr>`;
            }).join('');
        }

        function escapeHtml(str) {
            if (!str) return '';
            return String(str).replace(/[&<>"']/g, c => ({
                '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
            })[c]);
        }

        // 用于内联事件属性（onclick="fn('...')"）里的字符串实参：先转义 JS 字符串里的
        // 反斜杠/引号/换行，再 HTML 转义整体——因为该值最终嵌在 HTML 属性值内。
        // 防止任务名含 ' " < 等字符时逃逸出属性或注入脚本。
        function escapeAttr(str) {
            if (str === null || str === undefined) return '';
            const js = String(str)
                .replace(/\\/g, '\\\\')
                .replace(/'/g, "\\'")
                .replace(/"/g, '\\"')
                .replace(/\n/g, '\\n')
                .replace(/\r/g, '\\r');
            return escapeHtml(js);
        }

        function auditPrevPage() {
            if (_auditCurrentPage > 0) {
                _auditCurrentPage--;
                fetchAuditLogs();
            }
        }

        function auditNextPage() {
            _auditCurrentPage++;
            fetchAuditLogs();
        }
        
        function startValidationPolling() {
            stopValidationPolling();
            _validationPollingTimer = setInterval(async () => {
                await fetchValidationTasks();
                const taskList = document.getElementById('validationTaskList');
                const hasRunning = taskList && taskList.querySelector('.status-running');
                if (!hasRunning) {
                    stopValidationPolling();
                }
            }, 3000);
        }
        
        function stopValidationPolling() {
            if (_validationPollingTimer) {
                clearInterval(_validationPollingTimer);
                _validationPollingTimer = null;
            }
        }
        
        async function fetchValidationTasks() {
            if (!checkAuth()) return;
            
            console.log('Fetching validation tasks...');
            console.log('Token:', localStorage.getItem('token'));
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/validation-tasks?page=${validationCurrentPage}&size=${validationPageSize}`);
                
                if (!response.ok) {
                    showNotification(`请求失败: ${response.status}`, 'error');
                    return;
                }
                
                const result = await response.json();
                
                if (result.success) {
                    renderValidationTasks(result.data);
                    const hasRunning = result.data.content && result.data.content.some(t => t.status === 'PENDING' || t.status === 'RUNNING');
                    if (hasRunning && !_validationPollingTimer) {
                        startValidationPolling();
                    }
                } else {
                    showNotification(result.message || '获取对比任务列表失败', 'error');
                }
            } catch (error) {
                console.error('获取对比任务列表失败:', error);
                showNotification('获取对比任务列表失败: ' + error.message, 'error');
            }
        }
        
        function getValidationStatusClass(status, task) {
            if (status === 'COMPLETED' && task) {
                return (task.failedTables || 0) === 0 ? 'status-completed' : 'status-mismatch';
            }
            switch(status) {
                case 'PENDING': return 'status-pending';
                case 'RUNNING': return 'status-running';
                case 'FAILED': return 'status-failed';
                default: return '';
            }
        }
        
        function getValidationStatusText(status, task) {
            if (status === 'COMPLETED' && task) {
                return (task.failedTables || 0) === 0 ? '一致' : '不一致';
            }
            switch(status) {
                case 'PENDING': return '等待中';
                case 'RUNNING': return '对比中';
                case 'FAILED': return '失败';
                default: return status;
            }
        }

        function getCompareTypeText(compareType) {
            switch(compareType) {
                case 'ROW_COUNT': return '行数对比';
                case 'CONTENT': return '内容对比';
                default: return compareType || '行数对比';
            }
        }
        
        function renderValidationTasks(data) {
            const taskList = document.getElementById('validationTaskList');
            
            if (!data.content || data.content.length === 0) {
                taskList.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-icon">
                            <svg viewBox="0 0 80 80" fill="none" xmlns="http://www.w3.org/2000/svg">
                                <rect x="15" y="25" width="50" height="40" rx="2" stroke="#d9d9d9" stroke-width="2" fill="none"/>
                                <rect x="20" y="20" width="40" height="8" rx="1" stroke="#d9d9d9" stroke-width="2" fill="none"/>
                            </svg>
                        </div>
                        <div class="empty-text">暂无对比任务</div>
                    </div>
                `;
                return;
            }
            
            let html = '';
            data.content.forEach(task => {
                const statusClass = getValidationStatusClass(task.status, task);
                const statusText = getValidationStatusText(task.status, task);
                const compareTypeText = getCompareTypeText(task.compareType);
                const taskTypeLabel = task.taskType === 'DR' ? '<span style="background: #fff7e6; color: #fa8c16; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-right: 4px;">灾备</span>' : '<span style="background: #e6f7ff; color: #1890ff; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-right: 4px;">同步</span>';
                let resultText = '-';
                if (task.status === 'RUNNING') {
                    resultText = `${task.passedTables || 0}/${task.totalTables || 0} 表已对比`;
                } else if (task.status === 'COMPLETED') {
                    resultText = `${task.passedTables || 0}/${task.totalTables || 0} 表通过`;
                } else if (task.status === 'FAILED') {
                    resultText = '对比失败';
                }
                
                html += `
                    <div class="table-row">
                        <div class="table-cell col-name">${taskTypeLabel}${escapeHtml(task.name)}</div>
                        <div class="table-cell" style="width: 100px;">
                            <span style="background: ${task.compareType === 'CONTENT' ? '#e6f7ff' : '#f6ffed'}; color: ${task.compareType === 'CONTENT' ? '#1890ff' : '#52c41a'}; padding: 2px 8px; border-radius: 3px; font-size: 12px;">${compareTypeText}</span>
                        </div>
                        <div class="table-cell col-status">
                            <span class="status-badge ${statusClass}">${statusText}</span>
                        </div>
                        <div class="table-cell col-progress">${resultText}</div>
                        <div class="table-cell col-time">${formatDateTime(task.createdAt)}</div>
                        <div class="table-cell col-action">
                            <div class="action-btns">
                                <button class="action-btn" onclick="viewValidationDetail('${task.id}')">详情</button>
                                ${task.status === 'COMPLETED' || task.status === 'FAILED' 
                                    ? `<button class="action-btn delete" onclick="deleteValidationTask('${task.id}')">删除</button>` 
                                    : ''}
                            </div>
                        </div>
                    </div>
                `;
            });
            
            taskList.innerHTML = html;
            renderValidationPagination(data);
        }
        
        function renderValidationPagination(data) {
            const totalElements = data.totalElements || 0;
            const totalPages = data.totalPages || 0;
            const currentPage = data.currentPage || 0;
            
            document.getElementById('validationPaginationInfo').textContent = `总条数：${totalElements}`;
            
            const pageNumbers = document.getElementById('validationPageNumbers');
            let html = '';
            
            if (totalPages <= 7) {
                for (let i = 0; i < totalPages; i++) {
                    html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToValidationPage(${i})">${i + 1}</button>`;
                }
            } else {
                html += `<button class="page-btn ${0 === currentPage ? 'active' : ''}" onclick="goToValidationPage(0)">1</button>`;
                
                if (currentPage <= 3) {
                    for (let i = 1; i <= Math.min(4, totalPages - 2); i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToValidationPage(${i})">${i + 1}</button>`;
                    }
                    html += `<span class="page-ellipsis" onclick="goToValidationPage(${Math.min(currentPage + 5, Math.floor(totalPages / 2))})">···</span>`;
                } else if (currentPage >= totalPages - 4) {
                    html += `<span class="page-ellipsis" onclick="goToValidationPage(${Math.max(currentPage - 5, Math.floor(totalPages / 2))})">···</span>`;
                    for (let i = totalPages - 5; i <= totalPages - 2; i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToValidationPage(${i})">${i + 1}</button>`;
                    }
                } else {
                    html += `<span class="page-ellipsis" onclick="goToValidationPage(${Math.max(0, currentPage - 5)})">···</span>`;
                    for (let i = currentPage - 1; i <= currentPage + 1; i++) {
                        html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToValidationPage(${i})">${i + 1}</button>`;
                    }
                    html += `<span class="page-ellipsis" onclick="goToValidationPage(${Math.min(totalPages - 1, currentPage + 5)})">···</span>`;
                }
                
                html += `<button class="page-btn ${totalPages - 1 === currentPage ? 'active' : ''}" onclick="goToValidationPage(${totalPages - 1})">${totalPages}</button>`;
            }
            
            pageNumbers.innerHTML = html;
            
            document.getElementById('validationPrevPageBtn').disabled = currentPage === 0;
            document.getElementById('validationNextPageBtn').disabled = currentPage >= totalPages - 1;
        }
        
        function goToValidationPage(page) {
            validationCurrentPage = page;
            fetchValidationTasks();
        }
        
        async function openCreateValidationModal() {
            if (!checkAuth()) return;
            
            const select = document.getElementById('validationWorkflowSelect');
            select.innerHTML = '<option value="">加载中...</option>';
            document.getElementById('validationWorkflowInfo').style.display = 'none';
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/validation-tasks/available-workflows`);
                
                const result = await response.json();
                
                if (result.success && result.data) {
                    availableWorkflows = result.data;
                    renderValidationWorkflowOptions();
                } else {
                    select.innerHTML = '<option value="">加载失败</option>';
                }
            } catch (error) {
                console.error('加载可用任务失败:', error);
                select.innerHTML = '<option value="">加载失败</option>';
            }

            document.getElementById('createValidationModal').classList.add('show');
        }

        // 内容对比仅支持同构 SQL 任务（后端 createValidationTask 同样校验）：
        // 源库与目标库同为 MySQL 或同为 PostgreSQL；MongoDB/Elasticsearch 任务只支持行数对比
        function supportsContentCompare(w) {
            const s = (w.sourceType || 'mysql').toLowerCase();
            const t = (w.targetType || 'mysql').toLowerCase();
            return (s === 'mysql' && t === 'mysql')
                || (s === 'postgresql' && t === 'postgresql')
                || (s === 'mongodb' && t === 'mongodb');
        }

        // 按当前对比类型渲染任务下拉：内容对比时隐藏不支持的任务，并展示支持范围提示
        function renderValidationWorkflowOptions() {
            const select = document.getElementById('validationWorkflowSelect');
            const compareType = document.getElementById('validationCompareTypeSelect').value;
            const hint = document.getElementById('contentCompareHint');
            if (hint) hint.style.display = compareType === 'CONTENT' ? 'block' : 'none';

            // 列处理任务（列过滤/列名映射/附加列）：源端与目标端数据不再一一对应，
            // 内容对比与行数对比均不支持，从下拉中隐藏并提示
            const colProcCount = availableWorkflows.filter(w => w.hasColumnProcessing).length;
            const colProcHint = document.getElementById('colProcCompareHint');
            if (colProcHint) colProcHint.style.display = colProcCount > 0 ? 'block' : 'none';
            const comparable = availableWorkflows.filter(w => !w.hasColumnProcessing);

            const list = compareType === 'CONTENT'
                ? comparable.filter(supportsContentCompare)
                : comparable;

            if (list.length === 0) {
                select.innerHTML = compareType === 'CONTENT'
                    ? '<option value="">没有支持内容对比的任务</option>'
                    : '<option value="">没有可对比的任务</option>';
            } else {
                let options = '<option value="">请选择任务</option>';
                list.forEach(w => {
                    const typeLabel = w.taskType === 'DR' ? '[灾备]' : '[同步]';
                    options += `<option value="${escapeHtml(w.id)}">${typeLabel} ${escapeHtml(w.name)}</option>`;
                });
                select.innerHTML = options;
            }
            document.getElementById('validationWorkflowInfo').style.display = 'none';
        }
        
        document.getElementById('validationWorkflowSelect').addEventListener('change', function() {
            const selectedId = this.value;
            const infoDiv = document.getElementById('validationWorkflowInfo');
            
            if (selectedId) {
                const workflow = availableWorkflows.find(w => w.id === selectedId);
                if (workflow) {
                    const typeLabel = workflow.taskType === 'DR' ? '灾备任务' : '同步任务';
                    document.getElementById('validationSelectedName').textContent = workflow.name;
                    document.getElementById('validationSelectedStatus').textContent = typeLabel + ' - ' + (statusMap[workflow.status]?.text || workflow.status);
                    infoDiv.style.display = 'block';
                }
            } else {
                infoDiv.style.display = 'none';
            }
        });
        
        async function createValidationTask() {
            const workflowId = document.getElementById('validationWorkflowSelect').value;
            const compareType = document.getElementById('validationCompareTypeSelect').value;
            
            if (!workflowId) {
                showNotification('请选择任务', 'error');
                return;
            }
            
            const btn = document.getElementById('confirmValidationBtn');
            btn.disabled = true;
            btn.textContent = '创建中...';
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/validation-tasks`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ workflowId, compareType })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    const typeLabel = compareType === 'CONTENT' ? '内容对比' : '行数对比';
                    showNotification(typeLabel + '任务创建成功', 'success');
                    document.getElementById('createValidationModal').classList.remove('show');
                    fetchValidationTasks();
                    startValidationPolling();
                } else {
                    showNotification(result.message || '创建失败', 'error');
                }
            } catch (error) {
                console.error('创建对比任务失败:', error);
                showNotification('创建对比任务失败', 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = '创建';
            }
        }
        
        async function viewValidationDetail(id) {
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/validation-tasks/${id}`);
                
                const result = await response.json();
                
                if (result.success) {
                    const task = result.data;
                    window._currentValidationDetailTask = task;
                    _compareResultData = null;
                    _mismatchTableVisible = 5;
                    _diffVisibleMap = {};
                    _diffPageMap = {};
                    
                    document.getElementById('validationDetailBody').innerHTML = _rebuildDetailHtml();
                    document.getElementById('validationDetailModal').classList.add('show');
                } else {
                    showNotification(result.message || '获取详情失败', 'error');
                }
            } catch (error) {
                console.error('获取对比任务详情失败:', error);
                showNotification('获取详情失败', 'error');
            }
        }
        
        let _compareResultData = null;
        let _compareResultCacheKey = null;
        let _mismatchTableVisible = 5;
        let _diffVisibleMap = {};
        let _diffPageMap = {};

        function renderCompareResult(compareResultStr) {
            try {
                const result = JSON.parse(compareResultStr);
                if (!result.tables || result.tables.length === 0) return '';
                
                const cacheKey = result.sessionId || ('rowCount_' + result.tables.length + '_' + (result.tables[0] ? result.tables[0].sourceTable : ''));
                if (!_compareResultData || _compareResultCacheKey !== cacheKey) {
                    _compareResultData = result;
                    _compareResultCacheKey = cacheKey;
                    _mismatchTableVisible = 5;
                    _diffVisibleMap = {};
                    _diffPageMap = {};
                } else {
                    _compareResultData = result;
                }
                
                return _buildCompareResultHtml();
            } catch (e) {
                return '';
            }
        }

        function _buildCompareResultHtml() {
            const result = _compareResultData;
            const tables = result.tables || [];
            const matchTables = tables.filter(t => t.status === 'MATCH');
            const noPkTables = tables.filter(t => t.status === 'NO_PK');
            const mismatchTables = tables.filter(t => t.status === 'MISMATCH');
            
            let html = '<div style="margin-bottom: 16px;">';
            html += '<div style="font-weight: 500; margin-bottom: 10px;">详细对比结果</div>';
            
            if (matchTables.length > 0) {
                html += `<div style="background: #f6ffed; border: 1px solid #b7eb8f; border-radius: 4px; padding: 8px 12px; margin-bottom: 8px; font-size: 13px;">`;
                html += `<span style="color: #52c41a; font-weight: 500;">✓ ${matchTables.length}个表数据一致</span>`;
                const names = matchTables.map(t => t.sourceTable);
                if (names.length <= 8) {
                    html += `<span style="color: #666; margin-left: 8px;">${names.join('、')}</span>`;
                } else {
                    html += `<span style="color: #666; margin-left: 8px;">${names.slice(0, 8).join('、')} 等${names.length}个表</span>`;
                }
                html += '</div>';
            }
            
            if (noPkTables.length > 0) {
                html += `<div style="background: #fff7e6; border: 1px solid #ffd591; border-radius: 4px; padding: 8px 12px; margin-bottom: 8px; font-size: 13px;">`;
                html += `<span style="color: #fa8c16; font-weight: 500;">⚠ ${noPkTables.length}个表无主键</span>`;
                const names = noPkTables.map(t => t.sourceTable);
                if (names.length <= 8) {
                    html += `<span style="color: #666; margin-left: 8px;">${names.join('、')}</span>`;
                } else {
                    html += `<span style="color: #666; margin-left: 8px;">${names.slice(0, 8).join('、')} 等${names.length}个表</span>`;
                }
                html += '</div>';
            }
            
            if (mismatchTables.length > 0) {
                const visibleCount = Math.min(_mismatchTableVisible, mismatchTables.length);
                for (let i = 0; i < visibleCount; i++) {
                    html += _buildMismatchTableCard(mismatchTables[i], i);
                }
                
                if (mismatchTables.length > visibleCount) {
                    const remaining = mismatchTables.length - visibleCount;
                    html += `<div id="loadMoreTablesBtn" onclick="_loadMoreMismatchTables()" style="text-align: center; padding: 8px; border: 1px dashed #d9d9d9; border-radius: 4px; margin-bottom: 8px; cursor: pointer; color: #1890ff; font-size: 13px;">··· 还有 ${remaining} 个不一致的表，点击查看</div>`;
                }
            }
            
            html += '</div>';
            return html;
        }

        function _buildMismatchTableCard(table, index) {
            const diffCount = table.diffCount || (table.diffs ? table.diffs.length : 0);
            const hasDiffs = table.diffs && table.diffs.length > 0;
            const isExpanded = _diffVisibleMap[index] === true;
            const currentPage = _diffPageMap[index] || 0;
            const pageSize = 10;
            
            let html = `<div style="border: 1px solid #e8e8e8; border-radius: 4px; margin-bottom: 8px; overflow: hidden;">`;
            
            html += `<div style="padding: 10px 12px; background: #fafafa; ${hasDiffs ? 'cursor: pointer;' : ''} display: flex; justify-content: space-between; align-items: center;" ${hasDiffs ? `onclick="_toggleDiffExpand(${index})"` : ''}>`;
            html += `<div style="display: flex; align-items: center; gap: 8px;">`;
            html += `<span style="font-weight: 500; font-size: 13px;">${table.sourceTable}</span>`;
            html += `<span style="background: #fff1f0; color: #f5222d; padding: 1px 6px; border-radius: 3px; font-size: 11px;">不一致</span>`;
            html += `</div>`;
            html += `<div style="display: flex; align-items: center; gap: 12px; font-size: 12px; color: #999;">`;
            html += `<span>源: ${table.sourceRowCount || 0}行</span>`;
            html += `<span>目标: ${table.targetRowCount || 0}行</span>`;
            html += `<span style="color: #f5222d; font-weight: 500;">差异: ${diffCount}行</span>`;
            if (hasDiffs) {
                html += `<span style="color: #1890ff; font-size: 11px;">${isExpanded ? '收起 ▴' : '查看差异 ▾'}</span>`;
            }
            html += `</div>`;
            html += `</div>`;
            
            if (isExpanded && table.diffs && table.diffs.length > 0) {
                const totalPages = Math.ceil(table.diffs.length / pageSize);
                const startIdx = 0;
                const endIdx = Math.min((currentPage + 1) * pageSize, table.diffs.length);
                const visibleDiffs = table.diffs.slice(startIdx, endIdx);
                
                html += `<div style="padding: 8px 12px; border-top: 1px solid #f0f0f0; max-height: 400px; overflow-y: auto;">`;
                
                visibleDiffs.forEach((diff, diffIdx) => {
                    html += _buildDiffRow(diff, diffIdx + startIdx, diffCount);
                });
                
                if (endIdx < table.diffs.length) {
                    const remaining = table.diffs.length - endIdx;
                    html += `<div onclick="_loadMoreDiffs(${index})" style="text-align: center; padding: 6px; margin-top: 4px; cursor: pointer; color: #1890ff; font-size: 12px; border: 1px dashed #d9d9d9; border-radius: 3px;">还有 ${remaining} 行差异，点击加载更多</div>`;
                }
                
                html += '</div>';
            }
            
            html += '</div>';
            return html;
        }

        function _buildDiffRow(diff, globalIdx, totalDiffs) {
            const diffType = diff.diffType || diff.type || 'DIFF';
            const pkValue = diff.primaryKeyValue || (diff.primaryKey ? JSON.stringify(diff.primaryKey) : '');
            const diffFields = diff.diffFields || [];
            
            let sourceData = diff.sourceData;
            let targetData = diff.targetData;
            if (typeof sourceData === 'string') { try { sourceData = JSON.parse(sourceData); } catch(e) {} }
            if (typeof targetData === 'string') { try { targetData = JSON.parse(targetData); } catch(e) {} }
            
            let html = `<div style="margin-bottom: 6px; border: 1px solid #f0f0f0; border-radius: 3px; overflow: hidden; font-size: 12px;">`;
            
            html += `<div style="background: #fff2f0; padding: 4px 8px; display: flex; align-items: center; gap: 8px; border-bottom: 1px solid #ffe7e7;">`;
            html += `<span style="color: #999;">#${globalIdx + 1}</span>`;
            const typeLabel = diffType === 'CONTENT_DIFF' ? '内容差异' : (diffType === 'SOURCE_ONLY' ? '仅源库存在' : (diffType === 'TARGET_ONLY' ? '仅目标库存在' : diffType));
            html += `<span style="background: #f5222d22; color: #f5222d; padding: 0 4px; border-radius: 2px; font-size: 11px;">${typeLabel}</span>`;
            if (pkValue) {
                html += `<span style="color: #333;">主键: ${pkValue}</span>`;
            }
            if (diffFields.length > 0) {
                html += `<span style="color: #fa8c16;">差异字段: ${diffFields.join(', ')}</span>`;
            }
            html += '</div>';
            
            if (diffFields.length > 0 && sourceData && targetData) {
                html += `<table style="width: 100%; border-collapse: collapse;">`;
                html += `<tr style="background: #f6f6f6;"><td style="padding: 2px 8px; font-weight: 500; width: 80px; border-bottom: 1px solid #e8e8e8;">字段</td><td style="padding: 2px 8px; font-weight: 500; border-bottom: 1px solid #e8e8e8; color: #52c41a;">源库</td><td style="padding: 2px 8px; font-weight: 500; border-bottom: 1px solid #e8e8e8; color: #f5222d;">目标库</td></tr>`;
                diffFields.forEach(field => {
                    const sv = sourceData[field] !== undefined ? String(sourceData[field]) : '-';
                    const tv = targetData[field] !== undefined ? String(targetData[field]) : '-';
                    const isDiff = sv !== tv;
                    html += `<tr>`;
                    html += `<td style="padding: 2px 8px; border-bottom: 1px solid #f5f5f5; font-weight: 500; color: #333; width: 80px;">${field}</td>`;
                    html += `<td style="padding: 2px 8px; border-bottom: 1px solid #f5f5f5; color: #52c41a; word-break: break-all;">${_truncateVal(sv)}</td>`;
                    html += `<td style="padding: 2px 8px; border-bottom: 1px solid #f5f5f5; ${isDiff ? 'color: #f5222d; font-weight: 500;' : 'color: #52c41a;'} word-break: break-all;">${_truncateVal(tv)}</td>`;
                    html += '</tr>';
                });
                html += '</table>';
            } else {
                if (sourceData && typeof sourceData === 'object') {
                    html += `<div style="padding: 4px 8px; color: #52c41a; background: #f6ffed; border-bottom: 1px solid #e8e8e8;">源: ${_truncateVal(JSON.stringify(sourceData))}</div>`;
                }
                if (targetData && typeof targetData === 'object') {
                    html += `<div style="padding: 4px 8px; color: #f5222d; background: #fff1f0;">目标: ${_truncateVal(JSON.stringify(targetData))}</div>`;
                }
            }
            
            html += '</div>';
            return html;
        }

        function _truncateVal(val) {
            if (!val) return '-';
            return val.length > 80 ? val.substring(0, 80) + '...' : val;
        }

        function _toggleDiffExpand(tableIdx) {
            if (_diffVisibleMap[tableIdx]) {
                _diffVisibleMap[tableIdx] = false;
                delete _diffPageMap[tableIdx];
            } else {
                _diffVisibleMap[tableIdx] = true;
                _diffPageMap[tableIdx] = 0;
            }
            document.getElementById('validationDetailBody').innerHTML = _rebuildDetailHtml();
        }

        function _loadMoreDiffs(tableIdx) {
            _diffPageMap[tableIdx] = (_diffPageMap[tableIdx] || 0) + 1;
            document.getElementById('validationDetailBody').innerHTML = _rebuildDetailHtml();
        }

        function _loadMoreMismatchTables() {
            _mismatchTableVisible += 5;
            document.getElementById('validationDetailBody').innerHTML = _rebuildDetailHtml();
        }

        function _rebuildDetailHtml() {
            const task = window._currentValidationDetailTask;
            if (!task) return '';
            const statusClass = getValidationStatusClass(task.status, task);
            const statusText = getValidationStatusText(task.status, task);
            
            let detailHtml = `<div style="padding: 8px 0;">`;
            detailHtml += `<div style="margin-bottom: 16px;">`;
            detailHtml += `<div style="font-weight: 500; margin-bottom: 8px;">基本信息</div>`;
            detailHtml += `<div style="font-size: 13px; color: #666; line-height: 1.8;">`;
            detailHtml += `<div>任务名称：${escapeHtml(task.name)}</div>`;
            detailHtml += `<div>对比类型：<span style="background: ${task.compareType === 'CONTENT' ? '#e6f7ff' : '#f6ffed'}; color: ${task.compareType === 'CONTENT' ? '#1890ff' : '#52c41a'}; padding: 2px 8px; border-radius: 3px; font-size: 12px;">${getCompareTypeText(task.compareType)}</span></div>`;
            detailHtml += `<div>关联同步任务：${task.workflowName || '-'}</div>`;
            detailHtml += `<div>状态：<span class="status-badge ${statusClass}">${statusText}</span></div>`;
            detailHtml += `<div>创建时间：${formatDateTime(task.createdAt)}</div>`;
            if (task.startedAt) detailHtml += `<div>开始时间：${formatDateTime(task.startedAt)}</div>`;
            if (task.completedAt) detailHtml += `<div>完成时间：${formatDateTime(task.completedAt)}</div>`;
            detailHtml += `</div></div>`;
            
            detailHtml += `<div style="margin-bottom: 16px;">`;
            detailHtml += `<div style="font-weight: 500; margin-bottom: 8px;">对比结果</div>`;
            detailHtml += `<div style="font-size: 13px; color: #666; line-height: 1.8;">`;
            detailHtml += `<div>总表数：${task.totalTables || '-'}</div>`;
            detailHtml += `<div>通过表数：${task.passedTables || 0}</div>`;
            detailHtml += `<div>失败表数：${task.failedTables || 0}</div>`;
            detailHtml += `<div>总行数：${task.totalRows || 0}</div>`;
            detailHtml += `<div>差异行数：${task.mismatchedRows || 0}</div>`;
            detailHtml += `</div></div>`;

            detailHtml += _buildRepairSectionHtml(task);

            if (task.compareResult) detailHtml += renderCompareResult(task.compareResult);

            if (task.errorMessage) {
                detailHtml += `<div><div style="font-weight: 500; margin-bottom: 8px; color: #f5222d;">错误信息</div>`;
                detailHtml += `<div style="font-size: 13px; color: #f5222d; background: #fff2f0; padding: 8px 12px; border-radius: 4px;">${task.errorMessage}</div></div>`;
            }

            detailHtml += '</div>';
            return detailHtml;
        }

        // ---- 一致性校验闭环：差异修复 ----
        function _buildRepairSectionHtml(task) {
            // MongoDB 内容对比暂不支持一键修复（无 SQL 意义的可执行修复语句），隐藏修复按钮
            const canRepair = task.compareType === 'CONTENT' && task.status === 'COMPLETED'
                && (task.failedTables || 0) > 0 && !task.sourceIsMongo;
            const hasRepaired = task.repairStatus && task.repairStatus !== 'NONE';

            if (!canRepair && !hasRepaired) return '';

            let html = `<div style="margin-bottom: 16px;">`;
            html += `<div style="font-weight: 500; margin-bottom: 8px; display:flex; align-items:center; justify-content:space-between;">`;
            html += `<span>差异修复</span>`;
            if (canRepair) {
                html += `<button class="btn-test" onclick="repairValidationTaskAction('${task.id}')" id="repairBtn-${task.id}">修复差异</button>`;
            }
            html += `</div>`;

            if (hasRepaired) {
                const statusMap = {
                    REPAIRED: { text: '已修复并复核一致', color: '#52c41a', bg: '#f6ffed' },
                    PARTIAL: { text: '部分修复，仍有残留差异', color: '#faad14', bg: '#fff7e6' },
                    NONE: { text: '未修复', color: '#999', bg: '#fafafa' }
                };
                const st = statusMap[task.repairStatus] || { text: task.repairStatus, color: '#999', bg: '#fafafa' };
                html += `<div style="background:${st.bg}; border-radius:4px; padding:8px 12px; margin-bottom:8px; font-size:13px;">`;
                html += `<span style="color:${st.color}; font-weight:500;">${st.text}</span>`;
                if (task.repairedAt) html += `<span style="color:#999; margin-left:8px; font-size:12px;">修复时间: ${formatDateTime(task.repairedAt)}</span>`;
                html += `</div>`;

                if (task.repairSummary) {
                    try {
                        const summary = JSON.parse(task.repairSummary);
                        const tables = summary.tables || [];
                        if (tables.length) {
                            html += `<table style="width:100%; border-collapse:collapse; font-size:12px;">`;
                            html += `<tr style="background:#f6f6f6;"><td style="padding:4px 8px;font-weight:500;">表</td><td style="padding:4px 8px;font-weight:500;">状态</td><td style="padding:4px 8px;font-weight:500;">插入</td><td style="padding:4px 8px;font-weight:500;">更新</td><td style="padding:4px 8px;font-weight:500;">删除</td><td style="padding:4px 8px;font-weight:500;">错误</td><td style="padding:4px 8px;font-weight:500;">复核结果</td></tr>`;
                            tables.forEach(t => {
                                const verified = t.verifiedRowCountMatch === true;
                                html += `<tr>`;
                                html += `<td style="padding:4px 8px;border-bottom:1px solid #f5f5f5;">${t.table}</td>`;
                                html += `<td style="padding:4px 8px;border-bottom:1px solid #f5f5f5;">${t.status || '-'}</td>`;
                                html += `<td style="padding:4px 8px;border-bottom:1px solid #f5f5f5;">${t.inserted ?? '-'}</td>`;
                                html += `<td style="padding:4px 8px;border-bottom:1px solid #f5f5f5;">${t.updated ?? '-'}</td>`;
                                html += `<td style="padding:4px 8px;border-bottom:1px solid #f5f5f5;">${t.deleted ?? '-'}</td>`;
                                html += `<td style="padding:4px 8px;border-bottom:1px solid #f5f5f5; ${(t.errors||0) > 0 ? 'color:#f5222d;' : ''}">${t.errors ?? '-'}</td>`;
                                html += `<td style="padding:4px 8px;border-bottom:1px solid #f5f5f5; ${verified ? 'color:#52c41a;' : 'color:#f5222d;'}">${t.status && t.status.startsWith('SKIPPED') ? '-' : (verified ? '✓ 行数一致' : '✗ 仍有差异')}</td>`;
                                html += `</tr>`;
                            });
                            html += `</table>`;
                        }
                    } catch (e) { /* ignore malformed summary */ }
                }
            }

            html += `</div>`;
            return html;
        }

        async function repairValidationTaskAction(id) {
            if (!confirm('修复将以源库当前数据为准，向目标库写入/更新/删除对应行以消除差异，且不可撤销。确定要继续吗？')) {
                return;
            }
            const btn = document.getElementById(`repairBtn-${id}`);
            if (btn) { btn.disabled = true; btn.textContent = '修复中...'; }
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/validation-tasks/${id}/repair`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                const result = await response.json();
                if (result.success) {
                    showNotification('差异修复完成', 'success');
                    await viewValidationDetail(id);
                    fetchValidationTasks();
                } else {
                    showNotification(result.message || '修复失败', 'error');
                    if (btn) { btn.disabled = false; btn.textContent = '修复差异'; }
                }
            } catch (error) {
                console.error('修复差异失败:', error);
                showNotification('修复失败', 'error');
                if (btn) { btn.disabled = false; btn.textContent = '修复差异'; }
            }
        }

        async function deleteValidationTask(id) {
            if (!confirm('确定要删除这个对比任务吗？')) return;
            
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/validation-tasks/${id}`, {
                    method: 'DELETE',
                    headers: getAuthHeaders()
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification('对比任务已删除', 'success');
                    fetchValidationTasks();
                } else {
                    showNotification(result.message || '删除失败', 'error');
                }
            } catch (error) {
                console.error('删除对比任务失败:', error);
                showNotification('删除失败', 'error');
            }
        }
        
        // 事件监听
        document.getElementById('createTaskBtn').addEventListener('click', openModal);
        document.getElementById('closeModal').addEventListener('click', closeModal);
        document.getElementById('cancelBtn').addEventListener('click', closeModal);
        document.getElementById('closeConfigModal').addEventListener('click', closeConfigModal);
        // 刷新按钮的点击处理在 DOMContentLoaded 里按当前视图（正常/异常任务）分派，
        // 此处不再重复绑定——重复绑定会让异常任务视图下的刷新被普通列表数据覆盖。

        // 分页控件事件
        document.getElementById('pageSizeSelect').addEventListener('change', (e) => {
            changePageSize(e.target.value);
        });
        
        // 详情模态框事件
        document.getElementById('closeDetailModal').addEventListener('click', closeDetailModal);
        document.getElementById('closeDetailBtn').addEventListener('click', closeDetailModal);
        
        // 点击模态框外部关闭
        document.getElementById('createTaskModal').addEventListener('click', (e) => {
            if (e.target.id === 'createTaskModal') {
                closeModal();
            }
        });
        
        document.getElementById('taskDetailModal').addEventListener('click', (e) => {
            if (e.target.id === 'taskDetailModal') {
                closeDetailModal();
            }
        });
        
        document.getElementById('closeValidationModal').addEventListener('click', () => {
            document.getElementById('createValidationModal').classList.remove('show');
        });
        document.getElementById('cancelValidationBtn').addEventListener('click', () => {
            document.getElementById('createValidationModal').classList.remove('show');
        });
        
        document.getElementById('validationPageSizeSelect').addEventListener('change', (e) => {
            validationPageSize = parseInt(e.target.value);
            validationCurrentPage = 0;
            fetchValidationTasks();
        });
        
        document.getElementById('closeValidationDetailModal').addEventListener('click', () => {
            document.getElementById('validationDetailModal').classList.remove('show');
        });
        document.getElementById('closeValidationDetailBtn').addEventListener('click', () => {
            document.getElementById('validationDetailModal').classList.remove('show');
        });
        
        document.getElementById('createValidationModal').addEventListener('click', (e) => {
            if (e.target.id === 'createValidationModal') {
                document.getElementById('createValidationModal').classList.remove('show');
            }
        });
        
        document.getElementById('validationDetailModal').addEventListener('click', (e) => {
            if (e.target.id === 'validationDetailModal') {
                document.getElementById('validationDetailModal').classList.remove('show');
            }
        });
        
        // 页面加载时获取任务列表。
        // 筛选交互：查询框只承载任务名称/ID（回车或点刷新按钮生效）；
        // 状态/源库类型/目标库类型经下拉选择后以小标签形式展示在查询框下方，标签右上角红✕移除。
        function initFilterDropdown() {
            const searchBox = document.getElementById('searchBox');
            const searchInput = document.getElementById('searchInput');
            const filterDropdown = document.getElementById('filterDropdown');
            const statusDropdown = document.getElementById('statusDropdown');
            const sourceTypeDropdown = document.getElementById('sourceTypeDropdown');
            const targetTypeFilterDropdown = document.getElementById('targetTypeFilterDropdown');

            const closeAllTypeDropdowns = () => {
                statusDropdown.classList.remove('show');
                sourceTypeDropdown.classList.remove('show');
                targetTypeFilterDropdown.classList.remove('show');
            };

            // 聚焦/点击均展开条件下拉——查询框里已输入关键字时也保持可选，
            // 保证"名称/ID + 状态/库类型"可叠加筛选（输入过程不收起，回车确认时才收起）
            searchInput.addEventListener('focus', () => {
                filterDropdown.classList.add('show');
            });

            searchInput.addEventListener('click', (e) => {
                e.stopPropagation();
                filterDropdown.classList.add('show');
            });

            document.addEventListener('click', (e) => {
                if (!e.target.closest('.search-box-wrapper')) {
                    filterDropdown.classList.remove('show');
                    closeAllTypeDropdowns();
                }
            });

            // 绑定范围收窄到本页自己的下拉容器：document 级全局选择器会把订阅页的
            // 同类选项一并绑上本页 handler，造成跨页筛选状态污染（点订阅页"状态"
            // 会同时设置同步页的 filterStatus 并加标签）
            filterDropdown.querySelectorAll('.filter-option').forEach(option => {
                option.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const filterType = option.dataset.filter;

                    filterDropdown.classList.remove('show');

                    if (filterType === 'status') {
                        closeAllTypeDropdowns();
                        statusDropdown.classList.add('show');
                    } else if (filterType === 'sourceType') {
                        closeAllTypeDropdowns();
                        sourceTypeDropdown.classList.add('show');
                    } else if (filterType === 'targetType') {
                        closeAllTypeDropdowns();
                        targetTypeFilterDropdown.classList.add('show');
                    }
                });
            });

            statusDropdown.querySelectorAll('.status-option').forEach(option => {
                option.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const status = option.dataset.status;
                    const statusText = option.textContent;

                    filterStatus = status;
                    addFilterTag('状态', statusText, 'status');
                    statusDropdown.classList.remove('show');

                    currentPage = 1;
                    fetchWorkflows();
                });
            });

            sourceTypeDropdown.querySelectorAll('.type-option').forEach(option => {
                option.addEventListener('click', (e) => {
                    e.stopPropagation();
                    filterSourceType = option.dataset.sourceType;
                    addFilterTag('源库类型', formatDbTypeLabel(filterSourceType), 'sourceType');
                    sourceTypeDropdown.classList.remove('show');

                    currentPage = 1;
                    fetchWorkflows();
                });
            });

            targetTypeFilterDropdown.querySelectorAll('.type-option').forEach(option => {
                option.addEventListener('click', (e) => {
                    e.stopPropagation();
                    filterTargetType = option.dataset.targetType;
                    addFilterTag('目标库类型', formatDbTypeLabel(filterTargetType), 'targetType');
                    targetTypeFilterDropdown.classList.remove('show');

                    currentPage = 1;
                    fetchWorkflows();
                });
            });

            searchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    applyKeywordFromSearchInput();
                    filterDropdown.classList.remove('show');
                    currentPage = 1;
                    fetchWorkflows();
                } else if (e.key === 'Escape') {
                    filterDropdown.classList.remove('show');
                }
            });
        }

        // 关键字直接取自查询框（回车与刷新按钮共用）：空 = 不按名称/ID 过滤
        function applyKeywordFromSearchInput() {
            const value = document.getElementById('searchInput').value.trim();
            filterKeyword = value || null;
            updateClearFiltersVisibility();
        }
        
        function addFilterTag(label, value, type) {
            const filterTags = document.getElementById('filterTags');
            // 同类型条件去重：重复添加时替换旧 tag（否则旧 tag 删不掉且状态不一致）
            const existing = filterTags.querySelector(`[data-type="${type}"]`);
            if (existing) {
                existing.remove();
            }
            const tag = document.createElement('span');
            tag.className = 'filter-tag';
            tag.dataset.type = type;
            tag.innerHTML = `${label}：${value}<span class="tag-remove" onclick="removeFilterTag('${type}')">×</span>`;
            filterTags.appendChild(tag);
            updateClearFiltersVisibility();
        }

        function removeFilterTag(type) {
            const filterTags = document.getElementById('filterTags');
            // 同类型可能残留多个（历史数据），全部移除
            filterTags.querySelectorAll(`[data-type="${type}"]`).forEach(t => t.remove());

            if (type === 'status') {
                filterStatus = null;
            } else if (type === 'sourceType') {
                filterSourceType = null;
            } else if (type === 'targetType') {
                filterTargetType = null;
            }
            updateClearFiltersVisibility();

            currentPage = 1;
            fetchWorkflows();
        }

        function updateClearFiltersVisibility() {
            const btn = document.getElementById('clearFiltersBtn');
            if (!btn) return;
            const hasTags = document.getElementById('filterTags').children.length > 0;
            const hasKeyword = !!document.getElementById('searchInput').value.trim();
            btn.style.display = (hasTags || hasKeyword) ? 'inline' : 'none';
        }

        // "清空"按钮：还原所有筛选条件并重查列表
        window.clearAllFiltersAndRefetch = function(e) {
            if (e) e.stopPropagation();
            clearAllFilters();
            currentPage = 1;
            fetchWorkflows();
        }

        function clearAllFilters() {
            filterKeyword = null;
            filterStatus = null;
            filterSourceType = null;
            filterTargetType = null;
            isViewingFailedTasks = false;

            const filterTags = document.getElementById('filterTags');
            filterTags.innerHTML = '';

            document.getElementById('searchInput').value = '';
            updateClearFiltersVisibility();

            document.getElementById('viewFailedTasksBtn').textContent = '查看异常任务';
        }
        
        document.addEventListener('DOMContentLoaded', () => {
            if (checkAuth()) {
                displayUserInfo();
                updateSortIcons();
                fetchWorkflows();
                connectWebSocket();
                initFilterDropdown();
                initMetricsTaskSearch();

                setInterval(() => {
                    const token = localStorage.getItem('token');
                    if (token) {
                        try {
                            const payload = JSON.parse(atob(token.split('.')[1]));
                            if (payload.exp && payload.exp * 1000 < Date.now()) {
                                showTokenExpiredModal();
                            }
                        } catch (e) {
                        }
                    }
                }, 30000);
            }
            
            document.getElementById('viewFailedTasksBtn').addEventListener('click', () => {
                if (isViewingFailedTasks) {
                    clearAllFilters();
                    fetchWorkflows();
                } else {
                    clearAllFilters();
                    fetchFailedWorkflows();
                }
            });
            
            document.getElementById('refreshBtn').addEventListener('click', () => {
                // 刷新按钮同时生效查询框中的名称/ID 关键字
                applyKeywordFromSearchInput();
                if (isViewingFailedTasks) {
                    fetchFailedWorkflows();
                } else {
                    currentPage = 1;
                    fetchWorkflows();
                }
            });
            
            setInterval(() => {
                // 仅在浏览器标签可见且当前处于"实时同步管理"页时轮询——
                // 停留在灾备/订阅等其他页面或标签切后台时不再每 5 秒空刷同步列表
                if (document.hidden) return;
                const syncPage = document.getElementById('syncPage');
                if (!syncPage || syncPage.style.display === 'none') return;
                if (isViewingFailedTasks) {
                    fetchFailedWorkflows();
                } else {
                    fetchWorkflows();
                }
            }, 5000);
            
            // Debug: verify functions are accessible
            console.log('Functions check:');
            console.log('stopWorkflow:', typeof window.stopWorkflow, typeof stopWorkflow);
            console.log('deleteWorkflow:', typeof window.deleteWorkflow, typeof deleteWorkflow);
            console.log('pauseWorkflow:', typeof window.pauseWorkflow, typeof pauseWorkflow);
            console.log('resumeWorkflow:', typeof window.resumeWorkflow, typeof resumeWorkflow);
        });

        let _metricsAutoRefreshTimer = null;
        let _metricsCharts = {};
        let _metricsHistory = {
            captureRate: [],
            e2eLatency: [],
            queueDepth: [],
            checkpointLag: [],
            timestamps: []
        };
        const METRICS_HISTORY_MAX = 360;

        let _metricsTaskList = [];

        const _metricsTaskTypeMap = {
            'SYNC': '同步',
            'SUBSCRIBE': '订阅',
            'DR': '容灾'
        };

        function _getMetricsTaskLabel(task) {
            const typeLabel = _metricsTaskTypeMap[task.taskType] || task.taskType || '同步';
            const name = task.name || task.taskName || task.id || '';
            return `[${typeLabel}] ${name}`;
        }

        function _renderMetricsTaskDropdown(filter) {
            const dropdown = document.getElementById('metricsTaskDropdown');
            const keyword = (filter || '').toLowerCase().trim();
            let filtered = _metricsTaskList;
            if (keyword) {
                filtered = _metricsTaskList.filter(t => {
                    const label = _getMetricsTaskLabel(t).toLowerCase();
                    const id = (t.id || '').toLowerCase();
                    return label.includes(keyword) || id.includes(keyword);
                });
            } else {
                filtered = _metricsTaskList.slice(0, 5);
            }
            if (filtered.length === 0) {
                dropdown.innerHTML = '<div style="padding: 10px 12px; color: #999; font-size: 13px;">无匹配任务</div>';
            } else {
                dropdown.innerHTML = filtered.map(t => {
                    const typeLabel = _metricsTaskTypeMap[t.taskType] || t.taskType || '同步';
                    const typeColor = t.taskType === 'SUBSCRIBE' ? '#52c41a' : t.taskType === 'DR' ? '#722ed1' : '#1890ff';
                    return `<div class="metrics-task-option" data-id="${t.id}" style="padding: 8px 12px; cursor: pointer; display: flex; align-items: center; gap: 8px; font-size: 13px;">
                        <span style="background: ${typeColor}15; color: ${typeColor}; padding: 1px 6px; border-radius: 3px; font-size: 11px; white-space: nowrap;">${typeLabel}</span>
                        <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${t.name || t.id}</span>
                        <span style="color: #999; font-size: 11px; margin-left: auto; flex-shrink: 0;">${t.status || ''}</span>
                    </div>`;
                }).join('');
            }
            dropdown.style.display = 'block';

            dropdown.querySelectorAll('.metrics-task-option').forEach(opt => {
                opt.addEventListener('mousedown', function(e) {
                    e.preventDefault();
                    const taskId = this.dataset.id;
                    const task = _metricsTaskList.find(t => t.id === taskId);
                    document.getElementById('metricsTaskSelect').value = taskId;
                    document.getElementById('metricsTaskSearch').value = task ? _getMetricsTaskLabel(task) : taskId;
                    dropdown.style.display = 'none';
                    onMetricsTaskChange();
                });
                opt.addEventListener('mouseenter', function() {
                    this.style.backgroundColor = '#f5f5f5';
                });
                opt.addEventListener('mouseleave', function() {
                    this.style.backgroundColor = '';
                });
            });
        }

        async function loadMetricsTaskList() {
            const currentVal = document.getElementById('metricsTaskSelect').value;
            _metricsTaskList = [];
            const taskMap = new Map();

            // 从 Admin API 获取所有任务
            try {
                const resp = await fetchWithAuth(`${API_BASE_URL}/workflows?page=1&pageSize=200`);
                if (resp && resp.ok) {
                    const data = await resp.json();
                    const tasks = data.data?.list || data.records || data.data || data.items || [];
                    (Array.isArray(tasks) ? tasks : []).forEach(task => {
                        const tid = task.id || task.taskId;
                        if (tid && !taskMap.has(tid)) {
                            taskMap.set(tid, {
                                id: tid,
                                name: task.name || task.taskName || tid,
                                taskType: task.task_type || task.taskType || 'SYNC',
                                status: task.status || '',
                                createdAt: task.created_at || task.createdAt || null
                            });
                        }
                    });
                }
            } catch (e) {
                console.warn('Admin API not available for task list');
            }

            // 从 Agent status API 获取运行中的任务
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/agent/status`);
                if (resp.ok) {
                    const data = await resp.json();
                    if (data.tasks && Array.isArray(data.tasks)) {
                        data.tasks.forEach(task => {
                            if (!taskMap.has(task.taskId)) {
                                taskMap.set(task.taskId, {
                                    id: task.taskId,
                                    name: task.taskId,
                                    taskType: task.taskType || 'SYNC',
                                    status: task.status || 'RUNNING'
                                });
                            }
                        });
                    }
                }
            } catch (e) {
                console.warn('Agent status API not available for task list');
            }

            // 从 Metrics API 获取有监控数据的任务
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/metrics`);
                if (resp.ok) {
                    const data = await resp.json();
                    if (data.tasks && Array.isArray(data.tasks)) {
                        data.tasks.forEach(task => {
                            const tid = task.taskId;
                            if (tid && !taskMap.has(tid)) {
                                taskMap.set(tid, {
                                    id: tid,
                                    name: tid,
                                    taskType: task.taskType || 'SYNC',
                                    status: 'RUNNING'
                                });
                            }
                        });
                    }
                }
            } catch (e) {
                console.warn('Metrics API not available for task list');
            }

            // 只保留有监控数据的任务（运行中的任务）
            _metricsTaskList = Array.from(taskMap.values()).filter(t => {
                const s = (t.status || '').toUpperCase();
                return s === 'INCREMENT_RUNNING' || s === 'SUBSCRIBE_RUNNING' || s === 'FULL_MIGRATING' || s === 'RUNNING' || s === 'STARTING' || s === 'PENDING';
            });

            // 恢复之前选中的值
            if (currentVal) {
                document.getElementById('metricsTaskSelect').value = currentVal;
                const task = _metricsTaskList.find(t => t.id === currentVal);
                if (task) {
                    document.getElementById('metricsTaskSearch').value = _getMetricsTaskLabel(task);
                }
            }
        }

        function onMetricsTaskChange() {
            const taskId = document.getElementById('metricsTaskSelect').value;
            if (taskId) {
                document.getElementById('metricsEmpty').style.display = 'none';
                document.getElementById('metricsContent').style.display = 'block';
                _metricsHistory = { captureRate: [], e2eLatency: [], queueDepth: [], checkpointLag: [], timestamps: [] };
                loadMetricsHistory(taskId);
                refreshMetrics();
            } else {
                document.getElementById('metricsEmpty').style.display = 'block';
                document.getElementById('metricsContent').style.display = 'none';
            }
        }

        function initMetricsTaskSearch() {
            const searchInput = document.getElementById('metricsTaskSearch');
            const dropdown = document.getElementById('metricsTaskDropdown');

            searchInput.addEventListener('focus', function() {
                if (_metricsTaskList.length === 0) {
                    loadMetricsTaskList().then(() => _renderMetricsTaskDropdown(this.value));
                } else {
                    _renderMetricsTaskDropdown(this.value);
                }
            });

            searchInput.addEventListener('input', function() {
                _renderMetricsTaskDropdown(this.value);
            });

            searchInput.addEventListener('blur', function() {
                setTimeout(() => { dropdown.style.display = 'none'; }, 200);
            });

            searchInput.addEventListener('keydown', function(e) {
                if (e.key === 'Escape') {
                    dropdown.style.display = 'none';
                    this.blur();
                }
            });
        }

        // agent HTTP 地址：不再硬编码 localhost。默认取"当前页面主机名 + agent 端口"，
        // 使跨机部署（前端与 agent 同主机不同端口的常见形态）自动可用；
        // 若 agent 独立部署在别处，可在 URL 上带 ?agentBase=http://host:8083 覆盖，
        // 或部署时把这里改成后端反代地址（如 window.location.origin + '/agent'）。
        const AGENT_BASE_URL = (function() {
            try {
                const override = new URLSearchParams(window.location.search).get('agentBase');
                if (override) return override.replace(/\/$/, '');
            } catch (e) {}
            const host = window.location.hostname || 'localhost';
            return `${window.location.protocol}//${host}:8083`;
        })();

        function onMetricsTimeRangeChange() {
            const taskId = document.getElementById('metricsTaskSelect').value;
            if (taskId) loadMetricsHistory(taskId);
        }

        /** 按时间范围构造历史查询：任务启动以来按任务 createdAt 起算，聚合到 ~300 个点以内 */
        function _buildMetricsHistoryQuery(taskId) {
            const range = document.getElementById('metricsTimeRange')?.value || '1h';
            if (range === '24h') {
                return 'last=24h&interval=300000';
            }
            if (range === 'all') {
                const task = _metricsTaskList.find(t => t.id === taskId);
                const created = task && task.createdAt ? new Date(task.createdAt).getTime() : NaN;
                if (!isNaN(created) && created > 0) {
                    const spanMs = Math.max(Date.now() - created, 60000);
                    // 落到 30s 的整数倍桶，全程压缩到 ≤300 个点，响应体和渲染量都可控
                    const interval = Math.max(30000, Math.ceil(spanMs / 300 / 30000) * 30000);
                    return `start=${created}&interval=${interval}`;
                }
                // 拿不到创建时间：退化为持久化保留期上限（7天），30分钟桶
                return 'last=168h&interval=1800000';
            }
            return 'last=1h&interval=30000';
        }

        async function loadMetricsHistory(taskId) {
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/metrics/${taskId}/history?${_buildMetricsHistoryQuery(taskId)}`);
                if (resp.ok) {
                    const data = await resp.json();
                    if (data.success && data.metrics && data.metrics.length > 0) {
                        _metricsHistory = { captureRate: [], e2eLatency: [], queueDepth: [], checkpointLag: [], timestamps: [] };
                        for (const point of data.metrics) {
                            const date = new Date(point.ts);
                            _metricsHistory.timestamps.push(date.toLocaleTimeString('zh-CN', { hour12: false }));
                            _metricsHistory.captureRate.push(point.captureRate || 0);
                            _metricsHistory.e2eLatency.push(point.e2eLatency || 0);
                            _metricsHistory.queueDepth.push((point.captureQueueDepth || 0) + (point.extractQueueDepth || 0) + (point.applyQueueDepth || 0));
                            _metricsHistory.checkpointLag.push(point.checkpointLag || 0);
                        }
                        drawSparkline('sparkCaptureRate', _metricsHistory.captureRate, '#52c41a');
                        drawSparkline('sparkE2eLatency', _metricsHistory.e2eLatency, '#1890ff');
                        drawSparkline('sparkQueueDepth', _metricsHistory.queueDepth, '#fa8c16');
                        drawSparkline('sparkCheckpointLag', _metricsHistory.checkpointLag, '#f5222d');
                        updateChart('chartCaptureRate', 'captureRate', '事件/sec', _metricsHistory.timestamps, _metricsHistory.captureRate, '#52c41a');
                        updateChart('chartE2eLatency', 'e2eLatency', '延迟(ms)', _metricsHistory.timestamps, _metricsHistory.e2eLatency, '#1890ff');
                        updateChart('chartQueueDepth', 'queueDepth', '队列深度', _metricsHistory.timestamps, _metricsHistory.queueDepth, '#fa8c16');
                        updateChart('chartCheckpointLag', 'checkpointLag', '滞后(sec)', _metricsHistory.timestamps, _metricsHistory.checkpointLag, '#f5222d');
                    }
                }
            } catch (e) {
                console.warn('Failed to load metrics history:', e.message);
            }
        }

        async function refreshMetrics() {
            const taskId = document.getElementById('metricsTaskSelect').value;
            if (!taskId) return;
            try {
                const resp = await fetch(`${AGENT_BASE_URL}/api/metrics/${taskId}`);
                if (resp.ok) {
                    const data = await resp.json();
                    renderMetrics(data);
                } else if (resp.status === 404) {
                    renderMetrics({
                        captureRate: 0, e2eLatency: 0, queueDepth: 0, checkpointLag: 0,
                        captureQueueDepth: 0, extractQueueDepth: 0, applyQueueDepth: 0,
                        processes: []
                    });
                } else {
                    clearMetricsDisplay();
                }
            } catch (e) {
                console.warn('Metrics API not available:', e.message);
                clearMetricsDisplay();
            }
        }

        function clearMetricsDisplay() {
            ['metricCaptureRate', 'metricE2eLatency', 'metricQueueDepth', 'metricCheckpointLag', 'metricProcessHealth'].forEach(id => {
                const el = document.getElementById(id);
                if (el) { el.textContent = '--'; el.className = 'metric-card-value'; }
            });
        }

        function renderMetrics(data) {
            const captureRate = data.captureRate || 0;
            const e2eLatency = data.e2eLatency || 0;
            const queueDepth = data.queueDepth || 0;
            const checkpointLag = data.checkpointLag || 0;
            const processes = data.processes || [];

            updateMetricCard('metricCaptureRate', captureRate, v => v > 0 ? 'good' : 'bad');
            updateMetricCard('metricE2eLatency', e2eLatency, v => v < 500 ? 'good' : v < 2000 ? 'warn' : 'bad');
            updateMetricCard('metricQueueDepth', queueDepth, v => v < 1000 ? 'good' : v < 5000 ? 'warn' : 'bad');
            updateMetricCard('metricCheckpointLag', checkpointLag, v => v < 10 ? 'good' : v < 60 ? 'warn' : 'bad');

            const healthyCount = processes.filter(p => p.state === 'RUNNING').length;
            const totalCount = processes.length || 1;
            document.getElementById('metricProcessHealth').textContent = `${healthyCount}/${totalCount}`;
            document.getElementById('metricProcessHealth').className = 'metric-card-value ' + (healthyCount === totalCount ? 'good' : healthyCount > 0 ? 'warn' : 'bad');

            const now = new Date().toLocaleTimeString('zh-CN', { hour12: false });
            _metricsHistory.captureRate.push(captureRate);
            _metricsHistory.e2eLatency.push(e2eLatency);
            _metricsHistory.queueDepth.push(queueDepth);
            _metricsHistory.checkpointLag.push(checkpointLag);
            _metricsHistory.timestamps.push(now);

            if (_metricsHistory.timestamps.length > METRICS_HISTORY_MAX) {
                _metricsHistory.captureRate.shift();
                _metricsHistory.e2eLatency.shift();
                _metricsHistory.queueDepth.shift();
                _metricsHistory.checkpointLag.shift();
                _metricsHistory.timestamps.shift();
            }

            drawSparkline('sparkCaptureRate', _metricsHistory.captureRate, '#52c41a');
            drawSparkline('sparkE2eLatency', _metricsHistory.e2eLatency, '#1890ff');
            drawSparkline('sparkQueueDepth', _metricsHistory.queueDepth, '#fa8c16');
            drawSparkline('sparkCheckpointLag', _metricsHistory.checkpointLag, '#f5222d');

            updateChart('chartCaptureRate', 'captureRate', '事件/sec', _metricsHistory.timestamps, _metricsHistory.captureRate, '#52c41a');
            updateChart('chartE2eLatency', 'e2eLatency', '延迟(ms)', _metricsHistory.timestamps, _metricsHistory.e2eLatency, '#1890ff');
            updateChart('chartQueueDepth', 'queueDepth', '队列深度', _metricsHistory.timestamps, _metricsHistory.queueDepth, '#fa8c16');
            updateChart('chartCheckpointLag', 'checkpointLag', '滞后(sec)', _metricsHistory.timestamps, _metricsHistory.checkpointLag, '#f5222d');

            renderProcessStatus(processes);

            if (_chartModalInstance && _currentModalKey) {
                renderModalChart(_currentModalKey, _currentModalUnit, _currentModalColor);
            }
        }

        function updateMetricCard(elementId, value, colorFn) {
            const el = document.getElementById(elementId);
            el.textContent = typeof value === 'number' ? (Number.isInteger(value) ? value : value.toFixed(1)) : value;
            el.className = 'metric-card-value ' + colorFn(value);
        }

        function drawSparkline(canvasId, data, color) {
            const canvas = document.getElementById(canvasId);
            if (!canvas || data.length < 2) return;
            const ctx = canvas.getContext('2d');
            const w = canvas.width, h = canvas.height;
            ctx.clearRect(0, 0, w, h);
            const min = Math.min(...data), max = Math.max(...data);
            const range = max - min || 1;
            ctx.beginPath();
            ctx.strokeStyle = color;
            ctx.lineWidth = 1.5;
            data.forEach((v, i) => {
                const x = (i / (data.length - 1)) * w;
                const y = h - ((v - min) / range) * (h - 4) - 2;
                if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
            });
            ctx.stroke();
        }

        // 数据签名缓存：轮询间隔内若无新指标（历史序列未变），跳过 Chart.js update 省主线程开销
        const _chartDataSig = {};
        function updateChart(canvasId, key, label, labels, data, color) {
            const canvas = document.getElementById(canvasId);
            if (!canvas) return;
            const sig = data.length + '|' + (data.length ? data[0] + ':' + data[data.length - 1] : '') + '|' + labels[labels.length - 1];
            if (_metricsCharts[key]) {
                if (_chartDataSig[key] === sig) return; // 数据无变化，免重绘
                _chartDataSig[key] = sig;
                _metricsCharts[key].data.labels = labels;
                _metricsCharts[key].data.datasets[0].data = data;
                _metricsCharts[key].update('none');
                return;
            }
            _chartDataSig[key] = sig;
            _metricsCharts[key] = new Chart(canvas, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: label,
                        data: data,
                        borderColor: color,
                        backgroundColor: color + '20',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { display: true, ticks: { maxTicksLimit: 8, font: { size: 10 } }, grid: { color: '#f0f0f0' } },
                        y: { display: true, beginAtZero: true, ticks: { font: { size: 10 } }, grid: { color: '#f0f0f0' } }
                    }
                }
            });
        }

        function renderProcessStatus(processes) {
            const grid = document.getElementById('processStatusGrid');
            if (!grid || !processes.length) return;
            grid.innerHTML = processes.map(p => {
                const stateClass = p.state === 'RUNNING' ? 'running' : p.state === 'STARTING' ? 'starting' : 'stopped';
                const stateText = p.state === 'RUNNING' ? '运行中' : p.state === 'STARTING' ? '启动中' : '已停止';
                const cbState = p.circuitBreakerState || 'CLOSED';
                const cbLabel = cbState === 'OPEN' ? '<span style="color:#f5222d;font-size:11px;">断路器开启</span>' : '';
                return `<div class="process-status-item">
                    <div class="process-name">${p.name}</div>
                    <div class="process-state ${stateClass}">${stateText}</div>
                    <div class="process-meta">PID: ${p.pid || '-'} | 运行: ${p.uptime || '-'}</div>
                    <div class="process-meta">重试: ${p.retryCount || 0}次 ${cbLabel}</div>
                </div>`;
            }).join('');
        }

        function startMetricsAutoRefresh() {
            stopMetricsAutoRefresh();
            _metricsAutoRefreshTimer = setInterval(() => {
                if (document.getElementById('metricsPage').style.display !== 'none' && document.getElementById('metricsTaskSelect').value) {
                    refreshMetrics();
                }
            }, 5000);
        }

        function stopMetricsAutoRefresh() {
            if (_metricsAutoRefreshTimer) {
                clearInterval(_metricsAutoRefreshTimer);
                _metricsAutoRefreshTimer = null;
            }
        }

        function toggleMetricsAutoRefresh() {
            if (document.getElementById('metricsAutoRefresh').checked) {
                startMetricsAutoRefresh();
            } else {
                stopMetricsAutoRefresh();
            }
        }

        let _chartModalInstance = null;
        let _chartModalChart = null;
        let _chartModalGrain = '5s';

        function openChartModal(key, title, unit, color) {
            if (_chartModalInstance) return;
            const overlay = document.createElement('div');
            overlay.className = 'chart-modal-overlay';
            overlay.id = 'chartModalOverlay';
            overlay.innerHTML = `
                <div class="chart-modal-panel">
                    <div class="chart-modal-header">
                        <div class="chart-modal-title">${title}</div>
                        <div class="chart-modal-controls">
                            <button class="time-grain-btn ${_chartModalGrain === '5s' ? 'active' : ''}" onclick="setChartGrain('5s')">5秒</button>
                            <button class="time-grain-btn ${_chartModalGrain === '30s' ? 'active' : ''}" onclick="setChartGrain('30s')">30秒</button>
                            <button class="time-grain-btn ${_chartModalGrain === '1m' ? 'active' : ''}" onclick="setChartGrain('1m')">1分钟</button>
                            <button class="time-grain-btn ${_chartModalGrain === '5m' ? 'active' : ''}" onclick="setChartGrain('5m')">5分钟</button>
                            <div class="chart-modal-close" onclick="closeChartModal()">&times;</div>
                        </div>
                    </div>
                    <div class="chart-modal-body">
                        <canvas id="chartModalCanvas"></canvas>
                    </div>
                </div>
            `;
            overlay.addEventListener('click', function(e) {
                if (e.target === overlay) closeChartModal();
            });
            document.body.appendChild(overlay);
            _chartModalInstance = overlay;
            document.addEventListener('keydown', _chartModalEscHandler);
            _chartModalChart = null;
            renderModalChart(key, unit, color);
        }

        function _chartModalEscHandler(e) {
            if (e.key === 'Escape') closeChartModal();
        }

        function closeChartModal() {
            if (_chartModalChart) {
                _chartModalChart.destroy();
                _chartModalChart = null;
            }
            if (_chartModalInstance) {
                _chartModalInstance.remove();
                _chartModalInstance = null;
            }
            document.removeEventListener('keydown', _chartModalEscHandler);
        }

        function setChartGrain(grain) {
            _chartModalGrain = grain;
            const btns = document.querySelectorAll('.time-grain-btn');
            btns.forEach(b => {
                b.classList.toggle('active', b.textContent.trim() === { '5s': '5秒', '30s': '30秒', '1m': '1分钟', '5m': '5分钟' }[grain]);
            });
            renderModalChart(_currentModalKey, _currentModalUnit, _currentModalColor);
        }

        let _currentModalKey = '';
        let _currentModalUnit = '';
        let _currentModalColor = '';

        function renderModalChart(key, unit, color) {
            _currentModalKey = key;
            _currentModalUnit = unit;
            _currentModalColor = color;

            const canvas = document.getElementById('chartModalCanvas');
            if (!canvas) return;

            const rawData = _metricsHistory[key] || [];
            const rawTimestamps = _metricsHistory.timestamps || [];

            let labels, data;
            const grainMap = { '5s': 1, '30s': 6, '1m': 12, '5m': 60 };
            const step = grainMap[_chartModalGrain] || 1;

            if (step <= 1) {
                labels = rawTimestamps;
                data = rawData;
            } else {
                labels = [];
                data = [];
                for (let i = 0; i < rawData.length; i += step) {
                    const slice = rawData.slice(i, i + step);
                    const avg = slice.reduce((a, b) => a + b, 0) / slice.length;
                    data.push(Math.round(avg * 10) / 10);
                    const tSlice = rawTimestamps.slice(i, i + step);
                    labels.push(tSlice[Math.floor(tSlice.length / 2)] || tSlice[0] || '');
                }
            }

            if (_chartModalChart) {
                _chartModalChart.data.labels = labels;
                _chartModalChart.data.datasets[0].data = data;
                _chartModalChart.data.datasets[0].label = unit;
                _chartModalChart.update();
                return;
            }

            _chartModalChart = new Chart(canvas, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: unit,
                        data: data,
                        borderColor: color,
                        backgroundColor: color + '15',
                        fill: true,
                        tension: 0.3,
                        pointRadius: step <= 1 ? 2 : 3,
                        pointHoverRadius: 5,
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: { duration: 300 },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            mode: 'index',
                            intersect: false,
                            backgroundColor: 'rgba(0,0,0,0.75)',
                            titleFont: { size: 12 },
                            bodyFont: { size: 12 },
                            padding: 10,
                            cornerRadius: 6
                        }
                    },
                    scales: {
                        x: {
                            display: true,
                            ticks: { maxTicksLimit: 15, font: { size: 11 } },
                            grid: { color: '#f0f0f0' }
                        },
                        y: {
                            display: true,
                            beginAtZero: true,
                            ticks: { font: { size: 11 } },
                            grid: { color: '#f0f0f0' },
                            title: { display: true, text: unit, font: { size: 12 } }
                        }
                    },
                    interaction: {
                        mode: 'nearest',
                        axis: 'x',
                        intersect: false
                    }
                }
            });
        }

        // ============================================================
        // 表头列宽拖拽调整（实时同步/灾备任务/数据订阅三个列表页，互不影响）
        // 用列级 CSS 规则（作用域限定在各自 page 容器下）覆盖默认 flex 比例，
        // 而不是给具体行/表头节点加内联样式：因为表格行会被整体重新渲染（innerHTML），
        // 内联样式会在下一次刷新时丢失；CSS 规则则对之后新渲染出的行天然生效。
        // ============================================================
        const COL_RESIZE_MIN_WIDTH = 60;
        let _colResizeState = null;

        function _getColClass(el) {
            return Array.from(el.classList).find(c => c.startsWith('col-') && c !== 'col-resizer');
        }

        function _applyColumnWidths(pageId, widths) {
            let styleTag = document.getElementById('colwidth-style-' + pageId);
            if (!styleTag) {
                styleTag = document.createElement('style');
                styleTag.id = 'colwidth-style-' + pageId;
                document.head.appendChild(styleTag);
            }
            let css = '';
            Object.keys(widths).forEach(colClass => {
                css += `#${pageId} .${colClass} { flex: 0 0 ${widths[colClass]}px !important; width: ${widths[colClass]}px !important; }\n`;
            });
            styleTag.textContent = css;
        }

        function initResizableColumns(pageId, storageKey) {
            const header = document.querySelector(`#${pageId} .table-header`);
            if (!header) return;

            let savedWidths = {};
            try { savedWidths = JSON.parse(localStorage.getItem(storageKey) || '{}'); } catch (e) { savedWidths = {}; }
            if (Object.keys(savedWidths).length > 0) {
                _applyColumnWidths(pageId, savedWidths);
            }

            Array.from(header.children).forEach(cell => {
                const colClass = _getColClass(cell);
                if (!colClass || colClass === 'col-checkbox') return;
                if (cell.querySelector(':scope > .col-resizer')) return;

                const resizer = document.createElement('div');
                resizer.className = 'col-resizer';
                resizer.title = '拖动调整列宽';
                resizer.addEventListener('click', e => e.stopPropagation());
                resizer.addEventListener('mousedown', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    let widths = {};
                    try { widths = JSON.parse(localStorage.getItem(storageKey) || '{}'); } catch (e2) { widths = {}; }
                    _colResizeState = {
                        pageId, storageKey, colClass, widths,
                        startX: e.clientX,
                        startWidth: cell.getBoundingClientRect().width
                    };
                    resizer.classList.add('resizing');
                    document.body.classList.add('col-resizing');
                });
                cell.appendChild(resizer);
            });
        }

        document.addEventListener('mousemove', (e) => {
            if (!_colResizeState) return;
            const state = _colResizeState;
            const newWidth = Math.max(COL_RESIZE_MIN_WIDTH, Math.round(state.startWidth + (e.clientX - state.startX)));
            state.widths[state.colClass] = newWidth;
            _applyColumnWidths(state.pageId, state.widths);
        });

        document.addEventListener('mouseup', () => {
            if (!_colResizeState) return;
            localStorage.setItem(_colResizeState.storageKey, JSON.stringify(_colResizeState.widths));
            document.querySelectorAll('.col-resizer.resizing').forEach(r => r.classList.remove('resizing'));
            document.body.classList.remove('col-resizing');
            _colResizeState = null;
        });

        initResizableColumns('syncPage', 'colWidths_sync');
        initResizableColumns('drPage', 'colWidths_dr');
        initResizableColumns('subscribePage', 'colWidths_subscribe');

    // ==== 跨模块共享总线：供 dashboard-advanced.js 等独立模块取共享依赖/状态 ====
    window.__dash = {
        API_BASE_URL, fetchWithAuth, getAuthHeaders, showNotification, escapeHtml, escapeAttr, fetchWorkflows,
        checkAuth, formatDateTime, formatDbTypeLabel, formatDelay, viewMetrics,
        batchSelected: new Set(),
        detailTaskId: null, failedSeqno: null,
    };

    // ==== 显式导出：onclick/onchange 等内联处理器引用的函数（IIFE 内私有→挂 window）====
    Object.assign(window, {
    _loadMoreDiffs, _loadMoreMismatchTables, _toggleDiffExpand, addPgTable, addTable, auditNextPage,
    auditPrevPage, cfgAddPgTable, cfgAddTable, cfgGoToStep, cfgNextStep, cfgOnConnectionFieldChange,
    cfgPrevStep, cfgRefreshObjects, cfgRemoveTable, cfgRunValidation, cfgSelectAllPgTables, cfgSelectAllTables,
    cfgTestConnection, cfgToggleDatabase, cfgTogglePgSchema, closeChartModal, closeConfigModal, closeDetailModal,
    createTaskFromModal, createValidationTask, deleteValidationTask, downloadDiagnosticsBundle, escapeAttr, escapeHtml,
    fetchAuditLogs, goToPage, goToValidationPage, handleSort, handleTokenExpired, launchTask,
    logout, onMetricsTimeRangeChange, openAccountSettings, openChartModal, openCreateValidationModal, openTaskConfig,
    refreshMetrics, removeFilterTag, removeTable, renderValidationWorkflowOptions, repairValidationTaskAction, selectAllPgTables,
    selectAllTables, selectSourceType, selectTargetType, setChartGrain, showTaskDetail, switchAdvTab,
    switchPage, toggleDatabase, toggleLatencyChart, toggleMetricsAutoRefresh, togglePgSchema, viewMetrics,
    viewValidationDetail
    });
})();
