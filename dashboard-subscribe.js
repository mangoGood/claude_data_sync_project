// dashboard-subscribe.js —— 数据订阅管理独立 ES module（订阅任务管理/配置/对象选择/校验/创建）。
//   自有模块作用域，与主脚本及其它特性隔离；共享依赖经 window.__dash 取得，onclick/主 switchPage
//   引用的函数末尾显式挂 window。
const __dash = window.__dash;
const { API_BASE_URL, fetchWithAuth, getAuthHeaders, showNotification, escapeHtml, escapeAttr,
        checkAuth, formatDbTypeLabel, formatDelay, viewMetrics } = __dash;

        // ========== 订阅任务管理 ==========
        let subscribePageSize = 10;
        let subscribeCurrentPage = 1;
        let subscribeFilterKeyword = null;
        let subscribeFilterStatus = null;
        let subscribeFilterSourceType = null;

        const subscribeStatusMap = {
            'CONFIGURING': { text: '配置中', class: 'status-configuring', icon: '⚙' },
            'PENDING': { text: '启动中', class: 'status-pending', dot: true },
            'RECEIVED': { text: '已接收', class: 'status-pending', dot: true },
            'STARTING': { text: '启动中', class: 'status-pending', dot: true },
            'SUBSCRIBE_RUNNING': { text: '订阅中', class: 'status-increment-running', dot: true },
            'PAUSED': { text: '已暂停', class: 'status-paused', icon: '⏸' },
            'COMPLETED': { text: '已完成', class: 'status-full-completed', icon: '✓' },
            'FAILED': { text: '异常', class: 'status-failed', icon: '✕' }
        };

        let subscribeTotalCount = 0;

        async function fetchSubscribeTasks(page = 1) {
            subscribeCurrentPage = page;
            let url = `${API_BASE_URL}/workflows?page=${page}&pageSize=${subscribePageSize}&taskType=SUBSCRIBE`;
            if (subscribeFilterStatus) url += `&status=${subscribeFilterStatus}`;
            if (subscribeFilterKeyword) url += `&keyword=${encodeURIComponent(subscribeFilterKeyword)}`;
            if (subscribeFilterSourceType) url += `&sourceType=${subscribeFilterSourceType}`;

            try {
                const response = await fetchWithAuth(url);
                const data = await response.json();
                if (data.success) {
                    subscribeTotalCount = data.data.total || 0;
                    renderSubscribeTaskList(data.data.list || []);
                    updateSubscribePagination(subscribeTotalCount);
                }
            } catch (e) {
                console.error('Failed to fetch subscribe tasks:', e);
            }
        }

        function renderSubscribeTaskList(tasks) {
            const tableBody = document.getElementById('subscribeTableBody');
            const emptyState = document.getElementById('subscribeEmptyState');

            if (!tasks || tasks.length === 0) {
                emptyState.style.display = 'flex';
                const existingRows = tableBody.querySelectorAll('.table-row');
                existingRows.forEach(row => row.remove());
                return;
            }

            emptyState.style.display = 'none';
            const existingRows = tableBody.querySelectorAll('.table-row');
            existingRows.forEach(row => row.remove());

            tasks.forEach(task => {
                const row = createSubscribeTableRow(task);
                tableBody.appendChild(row);
            });
        }

        function createSubscribeTableRow(task) {
            const row = document.createElement('div');
            row.className = 'table-row';
            row.dataset.id = task.id;

            const statusInfo = subscribeStatusMap[task.status] || { text: task.status, class: 'status-configuring', icon: '?' };
            const statusHtml = statusInfo.dot ? `<span class="status-dot"></span>` : statusInfo.icon ? `<span class="status-icon">${statusInfo.icon}</span>` : '';
            const kafkaDisplay = task.kafka_bootstrap_servers ? task.kafka_bootstrap_servers.substring(0, 30) : '-';
            const topicStrategyMap = { 'TABLE': '按表分区', 'TASK': '按任务', 'GLOBAL': '全局' };
            const topicStrategyText = topicStrategyMap[task.kafka_topic_strategy] || task.kafka_topic_strategy || '-';
            const sourceTypeMap = { 'mysql': 'MySQL', 'postgresql': 'PostgreSQL', 'oracle': 'Oracle' };
            const sourceTypeText = sourceTypeMap[task.source_type] || task.source_type || 'MySQL';

            row.innerHTML = `
                <div class="table-cell col-checkbox">
                    <div class="checkbox"></div>
                </div>
                <div class="table-cell col-name">
                    <div>
                        <div><span style="background: #f6ffed; color: #52c41a; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-right: 4px;">订阅</span>${escapeHtml(task.name)}</div>
                        <div style="font-size: 11px; color: #1890ff; cursor: pointer;" onclick="${task.status === 'CONFIGURING' ? `openSubscribeConfig('${task.id}', '${task.source_type || 'mysql'}', '${escapeAttr(task.name)}')` : `showSubscribeDetail('${task.id}')`}">${task.id}</div>
                    </div>
                </div>
                <div class="table-cell col-status">
                    <span class="status-tag ${statusInfo.class}">
                        ${statusHtml}
                        ${statusInfo.text}
                    </span>
                </div>
                <div class="table-cell col-source">
                    <span style="font-size: 12px; color: #666;">${sourceTypeText}</span>
                </div>
                <div class="table-cell col-kafka">
                    <span style="font-size: 12px; color: #666;" title="${task.kafka_bootstrap_servers || ''}">${kafkaDisplay}</span>
                </div>
                <div class="table-cell col-topic">
                    <span style="font-size: 12px; color: #666;">${topicStrategyText}</span>
                </div>
                <div class="table-cell col-delay">
                    ${task.status === 'SUBSCRIBE_RUNNING' ?
                        `<div style="font-size: 11px; line-height: 1.6;">
                            <div style="color: ${task.rpo_ms != null && task.rpo_ms < 5000 ? '#52c41a' : task.rpo_ms != null && task.rpo_ms < 30000 ? '#faad14' : task.rpo_ms != null ? '#ff4d4f' : '#999'};">
                                RPO: ${task.rpo_ms != null ? formatDelay(task.rpo_ms) : '-'}
                            </div>
                            <div style="color: ${task.rto_ms != null && task.rto_ms < 5000 ? '#52c41a' : task.rto_ms != null && task.rto_ms < 30000 ? '#faad14' : task.rto_ms != null ? '#ff4d4f' : '#999'};">
                                RTO: ${task.rto_ms != null ? formatDelay(task.rto_ms) : '-'}
                            </div>
                        </div>` :
                        '<span style="color: #999;">-</span>'}
                </div>
                <div class="table-cell col-action">
                    <div class="action-btns">
                        ${task.status === 'CONFIGURING' ?
                            `<button class="action-btn" onclick="openSubscribeConfig('${task.id}', '${task.source_type || 'mysql'}', '${escapeAttr(task.name)}')">配置</button><button class="action-btn" onclick="launchSubscribeTask('${task.id}')">启动</button><button class="action-btn delete" onclick="deleteSubscribeTask('${task.id}')">删除</button>` :
                        task.status === 'SUBSCRIBE_RUNNING' ?
                            `<button class="action-btn monitor" onclick="viewMetrics('${task.id}')">监控</button><button class="action-btn" onclick="pauseSubscribeTask('${task.id}')">暂停</button><button class="action-btn stop" onclick="stopSubscribeTask('${task.id}')">停止</button>` :
                        task.status === 'PAUSED' ?
                            `<button class="action-btn" onclick="resumeSubscribeTask('${task.id}')">恢复</button>` :
                        task.status === 'FAILED' ?
                            `<button class="action-btn" onclick="retrySubscribeTask('${task.id}')">重试</button><button class="action-btn stop" onclick="stopSubscribeTask('${task.id}')">结束</button>` :
                        task.status === 'COMPLETED' ?
                            `<button class="action-btn delete" onclick="deleteSubscribeTask('${task.id}')">删除</button>` :
                            `<button class="action-btn stop" onclick="stopSubscribeTask('${task.id}')">结束</button>`}
                    </div>
                </div>
            `;

            return row;
        }

        function updateSubscribePagination(total) {
            const totalPages = Math.ceil(total / subscribePageSize);

            document.getElementById('subscribePaginationInfo').textContent =
                `总条数：${total} | 第 ${subscribeCurrentPage}/${totalPages || 1} 页`;

            document.getElementById('subscribePrevPageBtn').disabled = subscribeCurrentPage <= 1;
            document.getElementById('subscribeNextPageBtn').disabled = subscribeCurrentPage >= totalPages || totalPages === 0;

            renderSubscribePageNumbers(totalPages);
        }

        function renderSubscribePageNumbers(totalPages) {
            const container = document.getElementById('subscribePageNumbers');
            if (totalPages <= 0) { container.innerHTML = ''; return; }

            let html = '';
            const maxVisible = 5;
            let startPage = Math.max(1, subscribeCurrentPage - Math.floor(maxVisible / 2));
            let endPage = Math.min(totalPages, startPage + maxVisible - 1);
            if (endPage - startPage < maxVisible - 1) {
                startPage = Math.max(1, endPage - maxVisible + 1);
            }

            if (startPage > 1) {
                html += `<button class="page-btn" onclick="subscribeGoToPage(1)">1</button>`;
                if (startPage > 2) html += `<span class="page-ellipsis">...</span>`;
            }

            for (let i = startPage; i <= endPage; i++) {
                html += `<button class="page-btn ${i === subscribeCurrentPage ? 'active' : ''}" onclick="subscribeGoToPage(${i})">${i}</button>`;
            }

            if (endPage < totalPages) {
                if (endPage < totalPages - 1) html += `<span class="page-ellipsis">...</span>`;
                html += `<button class="page-btn" onclick="subscribeGoToPage(${totalPages})">${totalPages}</button>`;
            }

            container.innerHTML = html;
        }

        function subscribeGoToPage(page) {
            const totalPages = Math.ceil(subscribeTotalCount / subscribePageSize);
            if (page < 1 || page > totalPages) return;
            fetchSubscribeTasks(page);
        }

        // ========== 订阅任务配置状态 ==========
        let subCurrentStep = 1;
        let subSourceTested = false;
        let subKafkaTested = false;
        let subSelectedSyncObjects = {};
        let subDatabasesCache = [];
        let subSchemasCache = [];
        let subTablesCache = {};
        let subValidationPassed = false;

        let subEntrySelectedSourceType = 'mysql';

        function openSubscribeModal() {
            subEntrySelectedSourceType = 'mysql';
            document.getElementById('subscribeEntryTaskName').value = '';
            document.getElementById('subEntryTypeMysql').classList.add('selected');
            document.getElementById('subEntryTypePg').classList.remove('selected');
            document.getElementById('subEntryTypeOracle').classList.remove('selected');
            document.getElementById('createSubscribeEntryModal').classList.add('show');
        }

        function subEntrySelectSourceType(type) {
            subEntrySelectedSourceType = type;
            document.getElementById('subEntryTypeMysql').classList.toggle('selected', type === 'mysql');
            document.getElementById('subEntryTypePg').classList.toggle('selected', type === 'postgresql');
            document.getElementById('subEntryTypeOracle').classList.toggle('selected', type === 'oracle');
        }

        function closeSubscribeEntryModal() {
            document.getElementById('createSubscribeEntryModal').classList.remove('show');
        }

        async function confirmCreateSubscribeEntry() {
            if (!checkAuth()) return;
            const name = document.getElementById('subscribeEntryTaskName').value.trim();
            if (!name) { showNotification('请输入任务名称', 'error'); return; }

            const btn = document.getElementById('confirmSubscribeEntryBtn');
            btn.disabled = true;
            btn.textContent = '创建中...';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        name: name,
                        sourceType: subEntrySelectedSourceType,
                        targetType: subEntrySelectedSourceType,
                        taskType: 'SUBSCRIBE'
                    })
                });
                const result = await response.json();
                if (result.success) {
                    closeSubscribeEntryModal();
                    showNotification('任务创建成功，请继续配置', 'success');
                    fetchSubscribeTasks();
                    openSubscribeConfig(result.data.id, subEntrySelectedSourceType, name);
                } else {
                    showNotification(result.message || '创建失败', 'error');
                }
            } catch (e) {
                showNotification('创建失败: ' + e.message, 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = '创建任务';
            }
        }

        function openSubscribeConfig(taskId, sourceType, taskName) {
            subCurrentStep = 1;
            subSourceTested = false;
            subKafkaTested = false;
            subSelectedSyncObjects = {};
            subDatabasesCache = [];
            subSchemasCache = [];
            subTablesCache = {};
            subValidationPassed = false;

            document.getElementById('subscribeConfigTaskId').value = taskId;
            document.getElementById('subscribeSourceType').value = sourceType || 'mysql';
            document.getElementById('subscribeConfigTitle').textContent = '订阅任务配置' + (taskName ? ' - ' + taskName : '');

            const typeLabelMap = { mysql: 'MySQL', postgresql: 'PostgreSQL', oracle: 'Oracle' };
            document.getElementById('subSourceTypeDisplay').textContent = typeLabelMap[sourceType] || 'MySQL';

            // 根据源库类型初始化端口和数据库名输入行
            const dbNameRow = document.getElementById('subSourceDbNameRow');
            const dbNameInput = document.getElementById('subSourceDbNameInput');
            const hintDiv = dbNameRow.querySelector('div');
            if (sourceType === 'postgresql') {
                document.getElementById('subSourcePort').value = '5432';
                dbNameRow.style.display = 'block';
                dbNameInput.placeholder = '数据库名称（PG必填，如 test_db1）';
                if (hintDiv) hintDiv.textContent = 'PostgreSQL 为三层结构(db-schema-table)，请指定要订阅的数据库';
            } else if (sourceType === 'oracle') {
                document.getElementById('subSourcePort').value = '1521';
                dbNameRow.style.display = 'block';
                dbNameInput.placeholder = '服务名/SID（如 FREEPDB1、ORCL）';
                if (hintDiv) hintDiv.textContent = 'Oracle 请指定服务名或 SID，用于建立连接和 LogMiner 会话';
            } else {
                document.getElementById('subSourcePort').value = '3306';
                dbNameRow.style.display = 'none';
            }

            document.getElementById('subSourceHost').value = '';
            document.getElementById('subSourceUsername').value = '';
            document.getElementById('subSourcePassword').value = '';
            document.getElementById('subSourceDbNameInput').value = '';
            document.getElementById('subscribeKafkaServers').value = '';
            document.getElementById('subscribeTopicStrategy').value = 'TABLE';
            document.getElementById('subscribeTopicPrefix').value = 'cdc';
            document.getElementById('subscribeFormat').value = 'DEBEZIUM_JSON';
            document.getElementById('subSourceConnectionStatus').className = 'connection-status';
            document.getElementById('subSourceConnectionStatus').textContent = '';
            document.getElementById('subKafkaConnectionStatus').className = 'connection-status';
            document.getElementById('subKafkaConnectionStatus').textContent = '';
            document.getElementById('subSourceObjectsList').innerHTML = '<div class="empty-selection">请先填写源数据库连接并点击"下一步"</div>';
            document.getElementById('subSelectedObjectsList').innerHTML = '<div class="empty-selection">暂未选择任何对象</div>';
            document.getElementById('subValidationResult').innerHTML = '<div class="validation-empty">请点击"开始校验"按钮进行数据订阅条件检查</div>';

            subUpdateStepUI();
            document.getElementById('createSubscribeTaskModal').classList.add('show');

            // 尝试加载已保存的配置（编辑场景）
            loadSubscribeConfigIfExists(taskId);
        }

        async function loadSubscribeConfigIfExists(taskId) {
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}`);
                const result = await response.json();
                if (!result.success || !result.data) return;
                const task = result.data;
                if (task.source_connection) {
                    subParseConnectionString(task.source_connection);
                }
                if (task.kafka_bootstrap_servers) {
                    document.getElementById('subscribeKafkaServers').value = task.kafka_bootstrap_servers;
                }
                if (task.kafka_topic_strategy) {
                    document.getElementById('subscribeTopicStrategy').value = task.kafka_topic_strategy;
                }
                if (task.kafka_topic_prefix) {
                    document.getElementById('subscribeTopicPrefix').value = task.kafka_topic_prefix;
                }
                if (task.subscribe_format) {
                    document.getElementById('subscribeFormat').value = task.subscribe_format;
                }
                if (task.sync_objects) {
                    try {
                        const syncObjects = JSON.parse(task.sync_objects);
                        subSelectedSyncObjects = {};
                        for (const [db, dbValue] of Object.entries(syncObjects)) {
                            let tables = Array.isArray(dbValue) ? dbValue : (dbValue.tables || []);
                            subSelectedSyncObjects[db] = tables;
                        }
                        subRenderSelectedObjects();
                    } catch (e) {}
                }
            } catch (e) {
                console.error('加载订阅任务配置失败:', e);
            }
        }

        function subParseConnectionString(connStr) {
            if (!connStr) return;
            const match = connStr.match(/^(?:mysql|postgresql|oracle):\/\/([^:]+):([^@]+)@([^:]+):(\d+)(?:\/(.*))?$/);
            if (!match) return;
            document.getElementById('subSourceUsername').value = match[1];
            document.getElementById('subSourcePassword').value = match[2];
            document.getElementById('subSourceHost').value = match[3];
            document.getElementById('subSourcePort').value = match[4];
            if (match[5]) {
                document.getElementById('subSourceDbNameInput').value = match[5];
                const sourceType = document.getElementById('subscribeSourceType').value;
                if (sourceType === 'postgresql' || sourceType === 'oracle') {
                    document.getElementById('subSourceDbNameRow').style.display = 'block';
                }
            }
        }

        // 注：原代码此处有一份重复的 subRenderSelectedObjects 定义（无移除按钮版），
        // 经典脚本里函数声明后者覆盖前者、前者为死代码；module 严格词法作用域不容重复声明，
        // 故删除此死定义，保留下方带"移除"按钮的有效版本。

        function closeSubscribeModal() {
            document.getElementById('createSubscribeTaskModal').classList.remove('show');
        }

        function subOnSourceTypeChange() {
            const sourceType = document.getElementById('subscribeSourceType').value;
            const dbNameInput = document.getElementById('subSourceDbNameInput');
            const dbNameRow = document.getElementById('subSourceDbNameRow');
            const hintDiv = dbNameRow.querySelector('div');
            if (sourceType === 'postgresql') {
                document.getElementById('subSourcePort').value = '5432';
                dbNameRow.style.display = 'block';
                dbNameInput.placeholder = '数据库名称（PG必填，如 test_db1）';
                hintDiv.textContent = 'PostgreSQL 为三层结构(db-schema-table)，请指定要订阅的数据库';
            } else if (sourceType === 'oracle') {
                document.getElementById('subSourcePort').value = '1521';
                dbNameRow.style.display = 'block';
                dbNameInput.placeholder = '服务名/SID（如 ORCL、ORCLPDB1）';
                hintDiv.textContent = 'Oracle 请指定服务名或 SID，用于建立连接和 LogMiner 会话';
            } else {
                document.getElementById('subSourcePort').value = '3306';
                dbNameRow.style.display = 'none';
            }
            subSourceTested = false;
            document.getElementById('subSourceConnectionStatus').className = 'connection-status';
            document.getElementById('subSourceConnectionStatus').textContent = '';
        }

        function subOnConnectionFieldChange() {
            subSourceTested = false;
            subKafkaTested = false;
        }

        function subBuildSourceConnectionString() {
            const sourceType = document.getElementById('subscribeSourceType').value;
            const host = document.getElementById('subSourceHost').value.trim();
            const port = document.getElementById('subSourcePort').value.trim();
            const username = document.getElementById('subSourceUsername').value.trim();
            const password = document.getElementById('subSourcePassword').value.trim();
            if (!host || !port || !username) return null;
            let protocol;
            if (sourceType === 'postgresql') {
                protocol = 'postgresql';
            } else if (sourceType === 'oracle') {
                protocol = 'oracle';
            } else {
                protocol = 'mysql';
            }
            let conn = `${protocol}://${username}:${password}@${host}:${port}`;
            const dbName = document.getElementById('subSourceDbNameInput').value.trim();
            if (dbName) conn += `/${dbName}`;
            return conn;
        }

        function subValidateSourceFields() {
            const errors = [];
            if (!document.getElementById('subSourceHost').value.trim()) errors.push('源库IP地址');
            if (!document.getElementById('subSourcePort').value.trim()) errors.push('源库端口号');
            if (!document.getElementById('subSourceUsername').value.trim()) errors.push('源库用户名');
            const sourceType = document.getElementById('subscribeSourceType').value;
            if (sourceType === 'postgresql' && !document.getElementById('subSourceDbNameInput').value.trim()) {
                errors.push('PG数据库名称');
            }
            if (sourceType === 'oracle' && !document.getElementById('subSourceDbNameInput').value.trim()) {
                errors.push('Oracle服务名/SID');
            }
            return errors;
        }

        function subTestSourceConnection() {
            const errors = subValidateSourceFields();
            if (errors.length > 0) {
                showNotification('请正确填写: ' + errors.join('、'), 'error');
                return;
            }
            const connection = subBuildSourceConnectionString();
            if (!connection) { showNotification('请先填写源数据库连接信息', 'error'); return; }

            const sourceType = document.getElementById('subscribeSourceType').value;
            const statusDiv = document.getElementById('subSourceConnectionStatus');
            const testBtn = document.getElementById('subTestSourceBtn');

            statusDiv.className = 'connection-status testing';
            statusDiv.textContent = '正在测试连接...';
            testBtn.disabled = true;

            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 20000);

            fetchWithAuth(`${API_BASE_URL}/metadata/test-connection`, {
                method: 'POST',
                headers: getAuthHeaders(),
                body: JSON.stringify({ sourceConnection: connection, dbType: sourceType }),
                signal: controller.signal
            })
            .then(response => response.json())
            .then(result => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                if (result.success && result.data && result.data.connected) {
                    statusDiv.className = 'connection-status success';
                    statusDiv.textContent = '✓ 连接成功';
                    subSourceTested = true;
                } else {
                    statusDiv.className = 'connection-status error';
                    const errMsg = (result.data && result.data.errorMessage) ? result.data.errorMessage : (result.message || '无法连接到数据库');
                    const suggestion = (result.data && result.data.suggestion) ? result.data.suggestion : '';
                    statusDiv.innerHTML = `<div>✗ 连接失败：${errMsg.substring(0, 80)}</div>${suggestion ? '<div style="font-size:11px;color:#999;margin-top:2px;">💡 ' + suggestion + '</div>' : ''}`;
                    subSourceTested = false;
                }
            })
            .catch(error => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                statusDiv.className = 'connection-status error';
                if (error.name === 'AbortError') {
                    statusDiv.innerHTML = '<div>✗ 连接超时</div><div style="font-size:11px;color:#999;margin-top:2px;">💡 请检查目标主机是否可达、端口是否开放</div>';
                } else {
                    statusDiv.textContent = '✗ 连接测试出错: ' + error.message;
                }
                subSourceTested = false;
            });
        }

        function subTestKafkaConnection() {
            const kafkaServers = document.getElementById('subscribeKafkaServers').value.trim();
            if (!kafkaServers) { showNotification('请输入Kafka地址', 'error'); return; }

            const statusDiv = document.getElementById('subKafkaConnectionStatus');
            const testBtn = document.getElementById('subTestKafkaBtn');

            statusDiv.className = 'connection-status testing';
            statusDiv.textContent = '正在测试Kafka连接...';
            testBtn.disabled = true;

            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 15000);

            fetchWithAuth(`${API_BASE_URL}/metadata/test-kafka`, {
                method: 'POST',
                headers: getAuthHeaders(),
                body: JSON.stringify({ bootstrapServers: kafkaServers }),
                signal: controller.signal
            })
            .then(response => response.json())
            .then(result => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                if (result.success && result.data && result.data.connected) {
                    const brokerCount = result.data.brokerCount || '?';
                    const topicCount = result.data.topicCount || 0;
                    statusDiv.className = 'connection-status success';
                    statusDiv.textContent = `✓ 连接成功，${brokerCount}个Broker，${topicCount}个Topic`;
                    subKafkaTested = true;
                } else {
                    statusDiv.className = 'connection-status error';
                    const data = result.data || {};
                    const errMsg = data.errorMessage || result.message || '无法连接到Kafka';
                    const suggestion = data.suggestion || '';
                    statusDiv.innerHTML = `<div>✗ 连接失败：${errMsg.substring(0, 80)}</div>${suggestion ? '<div style="font-size:11px;color:#999;margin-top:2px;">💡 ' + suggestion + '</div>' : ''}`;
                    subKafkaTested = false;
                }
            })
            .catch(error => {
                clearTimeout(timeoutId);
                testBtn.disabled = false;
                statusDiv.className = 'connection-status error';
                if (error.name === 'AbortError') {
                    statusDiv.innerHTML = '<div>✗ 连接超时</div><div style="font-size:11px;color:#999;margin-top:2px;">💡 请检查Kafka服务是否启动、端口是否开放</div>';
                } else {
                    statusDiv.textContent = '✗ 连接测试出错: ' + error.message;
                }
                subKafkaTested = false;
            });
        }

        function subGoToStep(step) {
            if (step < 1 || step > 3) return;

            if (step === 2 && subCurrentStep === 1) {
                const sourceErrors = subValidateSourceFields();
                if (sourceErrors.length > 0) { showNotification('请正确填写源数据库: ' + sourceErrors.join('、'), 'error'); return; }
                if (!subSourceTested) { showNotification('请先测试源数据库连接', 'error'); return; }
                const kafkaServers = document.getElementById('subscribeKafkaServers').value.trim();
                if (!kafkaServers) { showNotification('请输入Kafka地址', 'error'); return; }
                if (!subKafkaTested) { showNotification('请先测试Kafka连接', 'error'); return; }

                const sourceConnection = subBuildSourceConnectionString();
                const sourceType = document.getElementById('subscribeSourceType').value;
                if (sourceType === 'postgresql' || sourceType === 'oracle') {
                    const dbName = document.getElementById('subSourceDbNameInput').value.trim();
                    subLoadSchemas(sourceConnection, dbName);
                } else {
                    subLoadDatabases(sourceConnection);
                }
            }

            if (step === 3 && subCurrentStep === 2) {
                if (Object.keys(subSelectedSyncObjects).length === 0) {
                    showNotification('请至少选择一个订阅对象', 'error');
                    return;
                }
            }

            subCurrentStep = step;
            subUpdateStepUI();
        }

        function subNextStep() {
            subGoToStep(subCurrentStep + 1);
        }

        function subUpdateStepUI() {
            document.getElementById('subStep1Nav').className = 'step-item' + (subCurrentStep === 1 ? ' active' : (subCurrentStep > 1 ? ' completed' : ''));
            document.getElementById('subStep2Nav').className = 'step-item' + (subCurrentStep === 2 ? ' active' : (subCurrentStep > 2 ? ' completed' : ''));
            document.getElementById('subStep3Nav').className = 'step-item' + (subCurrentStep === 3 ? ' active' : '');

            document.getElementById('subStep1Content').className = 'step-content' + (subCurrentStep === 1 ? ' active' : '');
            document.getElementById('subStep2Content').className = 'step-content' + (subCurrentStep === 2 ? ' active' : '');
            document.getElementById('subStep3Content').className = 'step-content' + (subCurrentStep === 3 ? ' active' : '');

            document.getElementById('subNextBtn').style.display = (subCurrentStep >= 1 && subCurrentStep <= 2) ? 'inline-block' : 'none';
            document.getElementById('subPrevBtn').style.display = (subCurrentStep >= 2 && subCurrentStep <= 3) ? 'inline-block' : 'none';
            document.getElementById('subCreateBtn').style.display = subCurrentStep === 3 ? 'inline-block' : 'none';
        }

        // ========== 订阅对象选择 ==========
        async function subLoadDatabases(connectionStr) {
            const sourceList = document.getElementById('subSourceObjectsList');
            sourceList.innerHTML = '<div class="empty-selection">加载中...</div>';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/databases`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr })
                });
                const result = await response.json();
                if (result.success && result.data.databases) {
                    subDatabasesCache = result.data.databases;
                    subRenderDatabases();
                } else {
                    sourceList.innerHTML = `<div class="empty-selection" style="color: #f5222d;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                sourceList.innerHTML = '<div class="empty-selection" style="color: #f5222d;">加载失败，请检查连接串</div>';
            }
        }

        function subRenderDatabases() {
            const sourceList = document.getElementById('subSourceObjectsList');
            if (subDatabasesCache.length === 0) {
                sourceList.innerHTML = '<div class="empty-selection">未找到可访问的数据库</div>';
                return;
            }
            let html = '';
            subDatabasesCache.forEach(db => {
                html += `
                    <div class="database-item" data-db="${db}">
                        <div class="database-header">
                            <input type="checkbox" class="database-checkbox" onclick="subSelectAllTables('${db}', this.checked)">
                            <span class="database-name" onclick="subToggleDatabase('${db}')">${db}</span>
                            <span class="database-expand" id="sub-expand-${db}" onclick="subToggleDatabase('${db}')">▶</span>
                        </div>
                        <div class="table-list" id="sub-tables-${db}">
                            <div style="padding: 16px; color: #999; font-size: 12px;">加载中...</div>
                        </div>
                    </div>
                `;
            });
            sourceList.innerHTML = html;
        }

        async function subToggleDatabase(db) {
            const tableList = document.getElementById(`sub-tables-${db}`);
            const expandIcon = document.getElementById(`sub-expand-${db}`);
            if (tableList.classList.contains('show')) {
                tableList.classList.remove('show');
                expandIcon.classList.remove('expanded');
            } else {
                tableList.classList.add('show');
                expandIcon.classList.add('expanded');
                if (!subTablesCache[db]) {
                    await subLoadTables(db);
                }
            }
        }

        async function subLoadTables(db) {
            const sourceType = document.getElementById('subscribeSourceType').value;
            let protocol;
            if (sourceType === 'postgresql') {
                protocol = 'postgresql';
            } else if (sourceType === 'oracle') {
                protocol = 'oracle';
            } else {
                protocol = 'mysql';
            }
            const host = document.getElementById('subSourceHost').value.trim();
            const port = document.getElementById('subSourcePort').value.trim();
            const username = document.getElementById('subSourceUsername').value.trim();
            const password = document.getElementById('subSourcePassword').value.trim();
            const dbName = (sourceType === 'postgresql' || sourceType === 'oracle') ? document.getElementById('subSourceDbNameInput').value.trim() : db;
            let connectionStr = `${protocol}://${username}:${password}@${host}:${port}/${dbName}`;

            const tableList = document.getElementById(`sub-tables-${db}`);
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/tables`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: dbName, schema: db })
                });
                const result = await response.json();
                if (result.success && result.data.tables) {
                    subTablesCache[db] = result.data.tables;
                    subRenderTables(db);
                } else {
                    tableList.innerHTML = `<div style="padding: 16px; color: #f5222d; font-size: 12px;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                tableList.innerHTML = '<div style="padding: 16px; color: #f5222d; font-size: 12px;">加载失败</div>';
            }
        }

        function subRenderTables(db) {
            const tableList = document.getElementById(`sub-tables-${db}`);
            const tables = subTablesCache[db] || [];
            if (tables.length === 0) {
                tableList.innerHTML = '<div style="padding: 16px; color: #999; font-size: 12px;">该数据库没有表</div>';
                return;
            }
            let html = '';
            tables.forEach(table => {
                const isSelected = subSelectedSyncObjects[db] && subSelectedSyncObjects[db].includes(table.name);
                html += `
                    <div class="table-item">
                        <span class="table-name">${table.name}</span>
                        <span class="table-info">${table.rows} 行 | ${table.size}</span>
                        <button class="table-add-btn" onclick="subAddTable('${db}', '${table.name}')" ${isSelected ? 'disabled style="opacity: 0.5;"' : ''}>
                            ${isSelected ? '已选择' : '选择'}
                        </button>
                    </div>
                `;
            });
            tableList.innerHTML = html;
        }

        function subSelectAllTables(db, checked) {
            const tableList = document.getElementById(`sub-tables-${db}`);
            const expandIcon = document.getElementById(`sub-expand-${db}`);
            if (checked) {
                if (tableList && !tableList.classList.contains('show')) {
                    tableList.classList.add('show');
                    expandIcon.classList.add('expanded');
                }
                if (!subTablesCache[db]) {
                    subLoadTables(db).then(() => {
                        if (!subSelectedSyncObjects[db]) subSelectedSyncObjects[db] = [];
                        subTablesCache[db].forEach(table => {
                            if (!subSelectedSyncObjects[db].includes(table.name)) subSelectedSyncObjects[db].push(table.name);
                        });
                        subRenderTables(db);
                        subRenderSelectedObjects();
                    });
                    return;
                }
                if (!subSelectedSyncObjects[db]) subSelectedSyncObjects[db] = [];
                subTablesCache[db].forEach(table => {
                    if (!subSelectedSyncObjects[db].includes(table.name)) subSelectedSyncObjects[db].push(table.name);
                });
            } else {
                delete subSelectedSyncObjects[db];
            }
            subRenderTables(db);
            subRenderSelectedObjects();
        }

        function subAddTable(db, tableName) {
            if (!subSelectedSyncObjects[db]) subSelectedSyncObjects[db] = [];
            if (!subSelectedSyncObjects[db].includes(tableName)) subSelectedSyncObjects[db].push(tableName);
            subRenderTables(db);
            subRenderSelectedObjects();
        }

        function subRemoveTable(db, tableName) {
            if (subSelectedSyncObjects[db]) {
                const index = subSelectedSyncObjects[db].indexOf(tableName);
                if (index > -1) subSelectedSyncObjects[db].splice(index, 1);
                if (subSelectedSyncObjects[db].length === 0) delete subSelectedSyncObjects[db];
            }
            subRenderSelectedObjects();
            if (subTablesCache[db]) subRenderTables(db);
        }

        function subRenderSelectedObjects() {
            const selectedList = document.getElementById('subSelectedObjectsList');
            const dbNames = Object.keys(subSelectedSyncObjects);
            if (dbNames.length === 0) {
                selectedList.innerHTML = '<div class="empty-selection">暂未选择任何对象</div>';
                return;
            }
            let html = '';
            dbNames.forEach(db => {
                subSelectedSyncObjects[db].forEach(tableName => {
                    html += `
                        <div class="selected-item">
                            <span class="selected-db-name">${db}.</span>
                            <span class="selected-table-name">${tableName}</span>
                            <button class="remove-btn" onclick="subRemoveTable('${db}', '${tableName}')">移除</button>
                        </div>
                    `;
                });
            });
            selectedList.innerHTML = html;
        }

        async function subLoadSchemas(connectionStr, database) {
            const sourceList = document.getElementById('subSourceObjectsList');
            sourceList.innerHTML = '<div class="empty-selection">加载中...</div>';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/schemas`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({ sourceConnection: connectionStr, database: database })
                });
                const result = await response.json();
                if (result.success && result.data.schemas) {
                    subSchemasCache = result.data.schemas;
                    subRenderSchemas();
                } else {
                    sourceList.innerHTML = `<div class="empty-selection" style="color: #f5222d;">${result.message || '加载失败'}</div>`;
                }
            } catch (error) {
                sourceList.innerHTML = '<div class="empty-selection" style="color: #f5222d;">加载失败，请检查连接信息</div>';
            }
        }

        function subRenderSchemas() {
            const sourceList = document.getElementById('subSourceObjectsList');
            if (subSchemasCache.length === 0) {
                sourceList.innerHTML = '<div class="empty-selection">未找到可访问的Schema</div>';
                return;
            }
            let html = '';
            subSchemasCache.forEach(schema => {
                html += `
                    <div class="database-item" data-db="${schema}">
                        <div class="database-header">
                            <input type="checkbox" class="database-checkbox" onclick="subSelectAllTables('${schema}', this.checked)">
                            <span class="database-name" onclick="subToggleDatabase('${schema}')">${schema} (schema)</span>
                            <span class="database-expand" id="sub-expand-${schema}" onclick="subToggleDatabase('${schema}')">▶</span>
                        </div>
                        <div class="table-list" id="sub-tables-${schema}">
                            <div style="padding: 16px; color: #999; font-size: 12px;">加载中...</div>
                        </div>
                    </div>
                `;
            });
            sourceList.innerHTML = html;
        }

        // ========== 订阅校验 ==========
        async function subRunValidation() {
            const sourceConnection = subBuildSourceConnectionString();
            const kafkaServers = document.getElementById('subscribeKafkaServers').value.trim();
            const sourceType = document.getElementById('subscribeSourceType').value;

            const resultDiv = document.getElementById('subValidationResult');
            const runBtn = document.getElementById('subRunValidationBtn');

            resultDiv.innerHTML = '<div class="validation-loading">正在校验中，请稍候...</div>';
            runBtn.disabled = true;

            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/metadata/validate-subscribe`, {
                    method: 'POST',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        sourceConnection: sourceConnection,
                        kafkaBootstrapServers: kafkaServers,
                        sourceType: sourceType
                    })
                });

                const result = await response.json();

                if (result.success && result.data) {
                    subRenderValidationResult(result.data);
                } else {
                    resultDiv.innerHTML = `<div class="validation-empty" style="color: #f5222d;">校验失败: ${result.message || '未知错误'}</div>`;
                    subValidationPassed = false;
                }
            } catch (error) {
                console.error('校验失败:', error);
                resultDiv.innerHTML = `<div class="validation-empty" style="color: #f5222d;">校验请求失败: ${error.message}</div>`;
                subValidationPassed = false;
            } finally {
                runBtn.disabled = false;
            }
        }

        function subRenderValidationResult(data) {
            const resultDiv = document.getElementById('subValidationResult');
            let html = '';

            const checkItems = data.checkItems || [];
            if (checkItems.length > 0) {
                checkItems.forEach(item => {
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

            const allPassed = data.allPassed !== undefined ? data.allPassed : false;
            const summaryClass = allPassed ? 'passed' : 'failed';
            const summaryText = allPassed ? '✓ 所有检查项均已通过，可以创建订阅任务' : '✗ 存在未通过的检查项，请修复后再创建任务';
            html += `<div class="validation-summary ${summaryClass}">${summaryText}</div>`;

            resultDiv.innerHTML = html;
            subValidationPassed = allPassed;
        }

        // ========== 创建订阅任务 ==========
        async function saveSubscribeConfig() {
            if (!subValidationPassed) {
                showNotification('请先通过校验检查', 'error');
                return;
            }

            const taskId = document.getElementById('subscribeConfigTaskId').value;
            if (!taskId) {
                showNotification('任务ID缺失，请重新进入配置', 'error');
                return;
            }

            const sourceType = document.getElementById('subscribeSourceType').value;
            const sourceConnection = subBuildSourceConnectionString();
            const kafkaServers = document.getElementById('subscribeKafkaServers').value.trim();
            const topicStrategy = document.getElementById('subscribeTopicStrategy').value;
            const topicPrefix = document.getElementById('subscribeTopicPrefix').value.trim();
            const format = document.getElementById('subscribeFormat').value;

            let syncObjectsJson = null;
            let sourceDbName = null;
            if (Object.keys(subSelectedSyncObjects).length > 0) {
                const syncObjectsData = {};
                Object.keys(subSelectedSyncObjects).forEach(db => {
                    syncObjectsData[db] = { tables: subSelectedSyncObjects[db] };
                });
                syncObjectsJson = JSON.stringify(syncObjectsData);

                if (sourceType === 'postgresql' || sourceType === 'oracle') {
                    sourceDbName = document.getElementById('subSourceDbNameInput').value.trim();
                } else {
                    const dbNames = Object.keys(subSelectedSyncObjects);
                    if (dbNames.length === 1) sourceDbName = dbNames[0];
                }
            }

            const btn = document.getElementById('subCreateBtn');
            btn.disabled = true;
            btn.textContent = '保存中...';
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}/config`, {
                    method: 'PUT',
                    headers: getAuthHeaders(),
                    body: JSON.stringify({
                        sourceConnection,
                        sourceType,
                        targetType: sourceType,
                        migrationMode: 'subscribe',
                        syncObjects: syncObjectsJson,
                        sourceDbName,
                        kafkaBootstrapServers: kafkaServers,
                        kafkaTopicPrefix: topicPrefix,
                        kafkaTopicStrategy: topicStrategy,
                        subscribeFormat: format
                    })
                });
                const data = await response.json();
                if (!data.success) { showNotification(data.message, 'error'); return; }

                closeSubscribeModal();
                showNotification('配置已保存，可点击「启动」运行任务', 'success');
                fetchSubscribeTasks();
            } catch (e) {
                showNotification('保存失败: ' + e.message, 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = '保存配置';
            }
        }

        async function launchSubscribeTask(taskId) {
            // 启动前校验是否已完成配置
            try {
                const checkResp = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}`);
                const checkData = await checkResp.json();
                if (checkData.success && checkData.data) {
                    const t = checkData.data;
                    if (!t.source_connection || !t.kafka_bootstrap_servers || !t.sync_objects) {
                        showNotification('任务尚未完成配置，请先点击「配置」填写连接信息和订阅对象', 'error');
                        return;
                    }
                }
            } catch (e) { /* 忽略校验失败，交由后端 launch 兜底 */ }

            if (!confirm('确定要启动此订阅任务吗？')) return;
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}/launch`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                const data = await response.json();
                if (data.success) {
                    showNotification('订阅任务已启动', 'success');
                    fetchSubscribeTasks();
                } else {
                    showNotification(data.message, 'error');
                }
            } catch (e) {
                showNotification('启动失败: ' + e.message, 'error');
            }
        }

        async function pauseSubscribeTask(taskId) {
            if (!confirm('确定要暂停此订阅任务吗？')) return;
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}/pause`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                const data = await response.json();
                if (data.success) {
                    showNotification('订阅任务已暂停', 'success');
                    fetchSubscribeTasks();
                } else {
                    showNotification(data.message, 'error');
                }
            } catch (e) {
                showNotification('暂停失败: ' + e.message, 'error');
            }
        }

        async function resumeSubscribeTask(taskId) {
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}/resume`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                const data = await response.json();
                if (data.success) {
                    showNotification('订阅任务已恢复', 'success');
                    fetchSubscribeTasks();
                } else {
                    showNotification(data.message, 'error');
                }
            } catch (e) {
                showNotification('恢复失败: ' + e.message, 'error');
            }
        }

        async function stopSubscribeTask(taskId) {
            if (!confirm('确定要停止此订阅任务吗？停止后需要重新启动。')) return;
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}/stop`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                const data = await response.json();
                if (data.success) {
                    showNotification('订阅任务已停止', 'success');
                    fetchSubscribeTasks();
                } else {
                    showNotification(data.message, 'error');
                }
            } catch (e) {
                showNotification('停止失败: ' + e.message, 'error');
            }
        }

        async function retrySubscribeTask(taskId) {
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}/retry`, {
                    method: 'POST',
                    headers: getAuthHeaders()
                });
                const data = await response.json();
                if (data.success) {
                    showNotification('订阅任务重试已启动', 'success');
                    fetchSubscribeTasks();
                } else {
                    showNotification(data.message, 'error');
                }
            } catch (e) {
                showNotification('重试失败: ' + e.message, 'error');
            }
        }

        async function deleteSubscribeTask(taskId) {
            if (!confirm('确定要删除此订阅任务吗？')) return;
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}`, {
                    method: 'DELETE',
                    headers: getAuthHeaders()
                });
                const data = await response.json();
                if (data.success) {
                    showNotification('订阅任务已删除', 'success');
                    fetchSubscribeTasks();
                } else {
                    showNotification(data.message, 'error');
                }
            } catch (e) {
                showNotification('删除失败: ' + e.message, 'error');
            }
        }

        async function showSubscribeDetail(taskId) {
            try {
                const response = await fetchWithAuth(`${API_BASE_URL}/workflows/${taskId}`, {
                    headers: getAuthHeaders()
                });
                const data = await response.json();
                if (!data.success) { showNotification(data.message, 'error'); return; }

                const task = data.data;
                const statusInfo = subscribeStatusMap[task.status] || { text: task.status };
                const topicStrategyMap = { 'TABLE': '按表分区', 'TASK': '按任务', 'GLOBAL': '全局' };

                let html = `
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                        <div><div style="font-size: 13px; color: #666;">任务名称:</div><div style="font-weight: 500;">${escapeHtml(task.name)}</div></div>
                        <div><div style="font-size: 13px; color: #666;">状态:</div><div><span class="status-badge ${statusInfo.class || ''}">${statusInfo.text}</span></div></div>
                        <div><div style="font-size: 13px; color: #666;">源库类型:</div><div>${task.source_type || 'mysql'}</div></div>
                        <div><div style="font-size: 13px; color: #666;">源库连接:</div><div style="word-break: break-all; font-size: 12px;">${task.source_connection || '-'}</div></div>
                        <div><div style="font-size: 13px; color: #666;">Kafka地址:</div><div style="word-break: break-all; font-size: 12px;">${task.kafka_bootstrap_servers || '-'}</div></div>
                        <div><div style="font-size: 13px; color: #666;">Topic策略:</div><div>${topicStrategyMap[task.kafka_topic_strategy] || task.kafka_topic_strategy || '-'}</div></div>
                        <div><div style="font-size: 13px; color: #666;">Topic前缀:</div><div>${task.kafka_topic_prefix || 'cdc'}</div></div>
                        <div><div style="font-size: 13px; color: #666;">消息格式:</div><div>${task.subscribe_format || 'DEBEZIUM_JSON'}</div></div>
                        <div><div style="font-size: 13px; color: #666;">延迟(RTO):</div><div>${task.rto_ms != null ? task.rto_ms + 'ms' : '-'}</div></div>
                        <div><div style="font-size: 13px; color: #666;">创建时间:</div><div>${task.created_at || '-'}</div></div>
                    </div>
                `;

                if (task.error_message) {
                    html += `<div style="margin-top: 16px; padding: 12px; background: #fff2f0; border: 1px solid #ffccc7; border-radius: 4px;">
                        <div style="font-size: 13px; color: #cf1322; font-weight: 500;">错误信息</div>
                        <div style="font-size: 12px; color: #666; margin-top: 4px;">${task.error_message}</div>
                    </div>`;
                }

                document.getElementById('subscribeDetailContent').innerHTML = html;
                document.getElementById('subscribeDetailModal').classList.add('show');
            } catch (e) {
                showNotification('获取详情失败: ' + e.message, 'error');
            }
        }

        function closeSubscribeDetailModal() {
            document.getElementById('subscribeDetailModal').classList.remove('show');
        }

        document.getElementById('createSubscribeTaskBtn').addEventListener('click', openSubscribeModal);
        document.getElementById('closeSubscribeEntryModal').addEventListener('click', closeSubscribeEntryModal);
        document.getElementById('cancelSubscribeEntryBtn').addEventListener('click', closeSubscribeEntryModal);
        document.getElementById('createSubscribeEntryModal').addEventListener('click', (e) => {
            if (e.target.id === 'createSubscribeEntryModal') closeSubscribeEntryModal();
        });

        // 订阅任务筛选交互：查询框只承载任务名称/ID（回车或点刷新生效）；
        // 状态/源库类型经下拉选择后以小标签展示在查询框下方（订阅目标固定 Kafka，无目标库类型筛选）。
        (function initSubscribeFilter() {
            const searchInput = document.getElementById('subscribeSearchInput');
            const filterDropdown = document.getElementById('subscribeFilterDropdown');
            const statusDropdown = document.getElementById('subscribeStatusDropdown');
            const sourceTypeDropdown = document.getElementById('subscribeSourceTypeDropdown');

            const closeAllTypeDropdowns = () => {
                statusDropdown.classList.remove('show');
                sourceTypeDropdown.classList.remove('show');
            };

            // 聚焦/点击均展开条件下拉——查询框里已输入关键字时也保持可选（与实时同步页一致）
            searchInput.addEventListener('focus', () => {
                filterDropdown.classList.add('show');
            });

            searchInput.addEventListener('click', (e) => {
                e.stopPropagation();
                filterDropdown.classList.add('show');
            });

            document.addEventListener('click', (e) => {
                if (!e.target.closest('#subscribeSearchBox') && !e.target.closest('#subscribeFilterDropdown')
                        && !e.target.closest('#subscribeStatusDropdown') && !e.target.closest('#subscribeSourceTypeDropdown')) {
                    filterDropdown.classList.remove('show');
                    closeAllTypeDropdowns();
                }
            });

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
                    }
                });
            });

            statusDropdown.querySelectorAll('.status-option').forEach(option => {
                option.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const status = option.dataset.status;
                    const statusText = option.textContent;

                    subscribeFilterStatus = status;
                    addSubscribeFilterTag('状态', statusText, 'status');
                    statusDropdown.classList.remove('show');

                    fetchSubscribeTasks(1);
                });
            });

            sourceTypeDropdown.querySelectorAll('.type-option').forEach(option => {
                option.addEventListener('click', (e) => {
                    e.stopPropagation();
                    subscribeFilterSourceType = option.dataset.sourceType;
                    addSubscribeFilterTag('源库类型', formatDbTypeLabel(subscribeFilterSourceType), 'sourceType');
                    sourceTypeDropdown.classList.remove('show');

                    fetchSubscribeTasks(1);
                });
            });

            searchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    filterDropdown.classList.remove('show');
                    subscribeApplyKeywordAndRefresh();
                } else if (e.key === 'Escape') {
                    filterDropdown.classList.remove('show');
                }
            });
        })();

        // 关键字直接取自查询框（回车与刷新按钮共用）：空 = 不按名称/ID 过滤
        function subscribeApplyKeywordAndRefresh() {
            const value = document.getElementById('subscribeSearchInput').value.trim();
            subscribeFilterKeyword = value || null;
            fetchSubscribeTasks(1);
        }

        function addSubscribeFilterTag(label, value, type) {
            const filterTags = document.getElementById('subscribeFilterTags');
            // 同类型条件去重：重复添加时替换旧 tag
            const existing = filterTags.querySelector(`[data-type="${type}"]`);
            if (existing) {
                existing.remove();
            }
            const tag = document.createElement('span');
            tag.className = 'filter-tag';
            tag.dataset.type = type;
            tag.innerHTML = `${label}：${value}<span class="tag-remove" onclick="removeSubscribeFilterTag('${type}')">×</span>`;
            filterTags.appendChild(tag);
        }

        function removeSubscribeFilterTag(type) {
            const filterTags = document.getElementById('subscribeFilterTags');
            filterTags.querySelectorAll(`[data-type="${type}"]`).forEach(t => t.remove());

            if (type === 'status') {
                subscribeFilterStatus = null;
            } else if (type === 'sourceType') {
                subscribeFilterSourceType = null;
            }

            fetchSubscribeTasks(1);
        }

        document.getElementById('subscribePageSizeSelect').addEventListener('change', (e) => {
            subscribePageSize = parseInt(e.target.value);
            subscribeCurrentPage = 1;
            fetchSubscribeTasks(1);
        });

        function debounce(fn, delay) {
            let timer;
            return function(...args) {
                clearTimeout(timer);
                timer = setTimeout(() => fn.apply(this, args), delay);
            };
        }


// ==== 显式导出：onclick + 主脚本(switchPage 等)引用的订阅函数（module 私有→挂 window）====
Object.assign(window, {
    closeSubscribeDetailModal, closeSubscribeModal, confirmCreateSubscribeEntry, deleteSubscribeTask, fetchSubscribeTasks, launchSubscribeTask,
    openSubscribeConfig, pauseSubscribeTask, removeSubscribeFilterTag, resumeSubscribeTask, retrySubscribeTask, saveSubscribeConfig,
    showSubscribeDetail, stopSubscribeTask, subAddTable, subEntrySelectSourceType, subGoToStep, subNextStep,
    subOnConnectionFieldChange, subRemoveTable, subRunValidation, subSelectAllTables, subTestKafkaConnection, subTestSourceConnection,
    subToggleDatabase, subscribeApplyKeywordAndRefresh, subscribeGoToPage
});
