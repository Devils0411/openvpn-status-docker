// График ЦП и ОЗУ
let cpuChart = null;
let cpuInterval = null;
const LIVE_UPDATE_MS = 5000;

// ГРАФИК OPENVPN LIVE (Трафик клиентов)
let ovpnChart = null;
let ovpnInterval = null;
let ovpnCurrentPeriod = 'live'; // Текущий выбранный период
const OVPN_LIVE_UPDATE_MS = 5000; // Обновление каждые 5 секунд
const interfaceAliasMap = {}; // Хранилище соответствия: имя -> алиас

// Форматирование скорости (бит/с → Кбит/с → Мбит/с)
function formatSpeed(bps) {
    if (bps >= 1_000_000) {
        return `${(bps / 1_000_000).toFixed(2)} Мбит/с`;
    } else if (bps >= 1_000) {
        return `${(bps / 1_000).toFixed(2)} Кбит/с`;
    } else {
        return `${bps.toFixed(0)} бит/с`;
    }
}

// Форматирование для оси Y (только число + единица)
function formatSpeedAxis(value) {
    if (value >= 1_000_000) {
        return `${(value / 1_000_000).toFixed(1)} Мбит/с`;
    } else if (value >= 1_000) {
        return `${(value / 1_000).toFixed(1)} Кбит/с`;
    } else {
        return `${value.toFixed(0)} бит/с`;
    }
}

function themeColors() {
    const isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    return {
        cpuBorder: isDark ? 'rgba(255,99,132,1)' : 'rgba(220,53,69,1)',
        cpuFill: isDark ? 'rgba(255,99,132,0.12)' : 'rgba(220,53,69,0.08)',
        ramBorder: isDark ? 'rgba(100,181,246,1)' : 'rgba(54,162,235,1)',
        ramFill: isDark ? 'rgba(100,181,246,0.12)' : 'rgba(54,162,235,0.08)',
        text: isDark ? '#ddd' : '#222',
        grid: isDark ? '#333' : '#eee'
    };
}

function initCpuChart() {
    const ctx = document.getElementById('cpuChart').getContext('2d');
    const colors = themeColors();

    if (cpuChart) {
        cpuChart.destroy();
    }

    cpuChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'ЦП %',
                    data: [],
                    borderColor: colors.cpuBorder,
                    backgroundColor: colors.cpuFill,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                    yAxisID: 'y'
                },
                {
                    label: 'ОЗУ %',
                    data: [],
                    borderColor: colors.ramBorder,
                    backgroundColor: colors.ramFill,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                    yAxisID: 'y'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { 
                    position: 'bottom',
                    labels: {
                        color: colors.text
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function (ctx) {
                            return `${ctx.dataset.label}: ${ctx.parsed.y}%`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    title: { 
                        display: true, 
                        text: 'Процент',
                        color: colors.text
                    },
                    grid: { color: colors.grid },
                    ticks: {
                        color: colors.text
                    }
                },
                x: {
                    title: { 
                        display: true, 
                        text: 'Время',
                        color: colors.text
                    },
                    grid: { color: colors.grid },
                    ticks: {
                        color: colors.text
                    }
                }
            }
        }
    });
}

function updateCpuChart(period = 'live') {
    if (!cpuChart) return;
    
    const basePath = window.basePath || '';
    fetch(`${basePath}/api/cpu?period=${period}`)
        .then(r => {
            if (!r.ok) throw new Error('Network response was not ok');
            return r.json();
        })
        .then(data => {
            if (data.error) { console.error(data.error); return; }
            const labels = data.utc_labels.map(ts => {
                const d = new Date(ts);
                if (period === 'live' ) {
                    return d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                } else if (period === 'hour') {
                    return d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
                } else if (period === 'day') {
                    return d.getHours().toString().padStart(2, '0') + ':00';
                } else {
                    return d.toLocaleDateString('ru-RU');
                }
            });

            const cpu = data.cpu_percent || [];
            const ram = data.ram_percent || [];

            cpuChart.data.labels = labels;
            cpuChart.data.datasets[0].data = cpu;
            cpuChart.data.datasets[1].data = ram;

            // Динамический цвет CPU: если последняя точка > 80% -> красная
            const latestCpu = cpu.length ? cpu[cpu.length - 1] : 0;
            const colors = themeColors();
            if (latestCpu > 80) {
                cpuChart.data.datasets[0].borderColor = 'rgba(220,20,60,1)';
                cpuChart.data.datasets[0].backgroundColor = 'rgba(220,20,60,0.12)';
            } else {
                cpuChart.data.datasets[0].borderColor = colors.cpuBorder;
                cpuChart.data.datasets[0].backgroundColor = colors.cpuFill;
            }

            cpuChart.update('none');
        })
        .catch(err => console.error('Ошибка при загрузке CPU данных:', err));

    // автообновление для live
    if (period === 'live') {
        if (cpuInterval) clearTimeout(cpuInterval);
        cpuInterval = setTimeout(() => updateCpuChart('live'), LIVE_UPDATE_MS);
    }
}

function toggleCpuChartVisibility() {
    const chartContainer = document.getElementById('cpuChartContainer');
    const toggleBtn = document.getElementById('toggleCpuChartBtn');
    if (!chartContainer || !toggleBtn) return;
    const icon = toggleBtn.querySelector('i');
    if (!icon) return;
    const isVisible = chartContainer.style.display === 'block';

    if (isVisible) {
        chartContainer.style.display = 'none';
        toggleBtn.classList.remove('active', 'btn-primary');
        toggleBtn.classList.add('btn-outline-secondary');
        toggleBtn.setAttribute('title', 'Показать график');
        icon.classList.remove('bi-graph-down');
        icon.classList.add('bi-graph-up');
        localStorage.setItem('cpuChartVisible', 'false');

        // Останавливаем автообновление при скрытии
        if (cpuInterval) {
            clearTimeout(cpuInterval);
            cpuInterval = null;
        }
        
        // Уничтожаем график при скрытии
        if (cpuChart) {
            cpuChart.destroy();
            cpuChart = null;
        }
    } else {
        chartContainer.style.display = 'block';
        toggleBtn.classList.add('active', 'btn-primary');
        toggleBtn.classList.remove('btn-outline-secondary');
        toggleBtn.setAttribute('title', 'Скрыть график');
        icon.classList.remove('bi-graph-up');
        icon.classList.add('bi-graph-down');
        localStorage.setItem('cpuChartVisible', 'true');

        setTimeout(() => {
            initCpuChart();
            // Запускаем обновление при показе
            const activePeriod = document.querySelector('.cpu-period.active');
            updateCpuChart(activePeriod ? activePeriod.dataset.period : 'live');
        }, 10);
    }
}

// Системная информация


function formatDiskGb(n) {
    if (n == null || Number.isNaN(Number(n))) return '—';
    const x = Number(n);
    return Math.abs(x - Math.round(x)) < 1e-6 ? String(Math.round(x)) : x.toFixed(1);
}

function formatCpuPercent(p) {
    if (p == null || Number.isNaN(Number(p))) return '—';
    const x = Number(p);
    return Math.abs(x - Math.round(x)) < 0.05 ? String(Math.round(x)) : x.toFixed(1);
}

function setUtilMeter(fillEl, pct) {
    if (!fillEl) return;
    const raw = Number(pct);
    const v = Number.isFinite(raw) ? Math.max(0, Math.min(100, raw)) : 0;
    fillEl.style.width = `${v}%`;
    fillEl.classList.remove('dash-meter__fill--ok', 'dash-meter__fill--warn', 'dash-meter__fill--crit');
    if (v < 50) fillEl.classList.add('dash-meter__fill--ok');
    else if (v <= 80) fillEl.classList.add('dash-meter__fill--warn');
    else fillEl.classList.add('dash-meter__fill--crit');
    const track = fillEl.closest('.dash-meter');
    if (track) track.setAttribute('aria-valuenow', String(Math.round(v)));
}

function vpnInactiveSummaryPhrase(count) {
    if (count <= 0) return { text: 'Активны', allActive: true };
    const n = count;
    const mod10 = n % 10;
    const mod100 = n % 100;
    let phrase;
    if (mod10 === 1 && mod100 !== 11) {
        phrase = `${n} из 6 не активен`;
    } else if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) {
        phrase = `${n} из 6 не активны`;
    } else {
        phrase = `${n} из 6 не активны`;
    }
    return { text: phrase, allActive: false };
}

function escapeHtml(text) {
    const d = document.createElement('div');
    d.textContent = text == null ? '' : String(text);
    return d.innerHTML;
}

async function updateSystemInfo() {
    try {
        let basePath = window.basePath || '';
        if (!basePath) {
            const path = window.location.pathname;
            if (path.includes('/status')) {
                basePath = '/status';
            }
        }
        const response = await fetch(basePath + '/api/system_info');
        const data = await response.json();

        // Обновляем только динамические данные (память, диск, загрузка сети)
        const memoryElement = document.getElementById('memory_used');
        const diskUsedDetail = document.getElementById('disk_used');
        const diskTotalDetail = document.getElementById('disk_total');
        const diskUsedKpi = document.getElementById('admin-kpi-disk-used');
        const diskTotalKpi = document.getElementById('admin-kpi-disk-total');
        const networkElement = document.getElementById('network_load');

        if (memoryElement && memoryElement.textContent !== String(data.memory_used)) {
            memoryElement.textContent = data.memory_used;
        }

        const du = formatDiskGb(data.disk_used);
        const dt = formatDiskGb(data.disk_total);
        if (diskUsedDetail && diskUsedDetail.textContent !== du) diskUsedDetail.textContent = du;
        if (diskTotalDetail && diskTotalDetail.textContent !== dt) diskTotalDetail.textContent = dt;
        if (diskUsedKpi && diskUsedKpi.textContent !== du) diskUsedKpi.textContent = du;
        if (diskTotalKpi && diskTotalKpi.textContent !== dt) diskTotalKpi.textContent = dt;

        let networkHtml = '';
        for (const [iface, stats] of Object.entries(data.network_load)) {
            const ifaceAlias = getInterfaceAlias(iface);
            networkHtml += `<p><b>${ifaceAlias}</b>: Передача: ${stats.sent_speed} Мбит/с, Прием: ${stats.recv_speed} Мбит/с</p>`;
        }

        if (networkElement && networkElement.innerHTML !== networkHtml) networkElement.innerHTML = networkHtml;

        // Обновление KPI
        const elOvpn = document.getElementById('admin-stat-ovpn');
        const elWg = document.getElementById('admin-stat-wg');
        const elKpiCpu = document.getElementById('admin-kpi-cpu');
        const elKpiUptime = document.getElementById('admin-kpi-uptime');
        const openvpn = data.vpn_clients?.OpenVPN ?? 0;
        const wireguard = data.vpn_clients?.WireGuard ?? 0;

        if (elOvpn && elOvpn.textContent !== String(openvpn)) elOvpn.textContent = String(openvpn);
        if (elWg && elWg.textContent !== String(wireguard)) elWg.textContent = String(wireguard);
        
        const cpuStr = formatCpuPercent(data.cpu_load);
        if (elKpiCpu && elKpiCpu.textContent !== cpuStr) elKpiCpu.textContent = cpuStr;
        
        const cpuBar = document.getElementById('cpu_bar');
        const rawCpuNum = Number(data.cpu_load);
        setUtilMeter(cpuBar, Number.isFinite(rawCpuNum) ? rawCpuNum : 0);

        const elRamPct = document.getElementById('admin-kpi-ram-pct');
        const elMemUsed = document.getElementById('admin-kpi-mem-used');
        const elMemTotal = document.getElementById('admin-kpi-mem-total');
        
        let ramPct = data.memory_percent;
        if (ramPct == null && data.memory_total > 0) {
            ramPct = Math.round((100 * Number(data.memory_used)) / Number(data.memory_total) * 10) / 10;
        }
        const ramPctStr = ramPct == null ? '—' : formatCpuPercent(ramPct);
        
        if (elRamPct && elRamPct.textContent !== ramPctStr) elRamPct.textContent = ramPctStr;
        if (elMemUsed && elMemUsed.textContent !== String(data.memory_used)) elMemUsed.textContent = data.memory_used;
        if (elMemTotal && data.memory_total != null && elMemTotal.textContent !== String(data.memory_total)) {
            elMemTotal.textContent = data.memory_total;
        }
        
        const ramBar = document.getElementById('ram_bar');
        setUtilMeter(ramBar, ramPct == null || !Number.isFinite(Number(ramPct)) ? 0 : Number(ramPct));
        
        if (elKpiUptime && elKpiUptime.textContent !== data.uptime) {
            elKpiUptime.textContent = data.uptime;
            elKpiUptime.setAttribute('title', data.uptime);
        }

        const elCpuCoresKpi = document.getElementById('admin-kpi-cpu-cores');
        const nCores = data.cpu_cores;
        if (elCpuCoresKpi && nCores != null && elCpuCoresKpi.textContent !== String(nCores)) {
            elCpuCoresKpi.textContent = String(nCores);
        }

        const elHostOs = document.getElementById('dash-host-os');
        if (elHostOs && data.os_label != null && elHostOs.textContent !== String(data.os_label)) {
            elHostOs.textContent = data.os_label;
        }

    } catch (error) {
        console.error('Ошибка при загрузке данных:', error);
    }
}

// График vnstat
let selectedIfaceName = null;  // Реальное имя для API-запросов (требует vnstat)
let selectedIfaceAlias = null; // Алиас для отображения в UI
let selectedPeriod = 'day';
let bwChartInstance = null;

// Функция для получения алиаса интерфейса
function getInterfaceAlias(ifaceName) {
    if (!interfaceAliasMap || !ifaceName) return ifaceName;
    return interfaceAliasMap[ifaceName] || ifaceName;
}

// Загрузка маппинга интерфейсов
async function loadInterfaceAliases() {
    try {
        const basePath = window.basePath || '';
        const res = await fetch(basePath + '/api/interfaces');
        const data = await res.json();
        
        // Очищаем и заполняем маппинг
        Object.keys(interfaceAliasMap).forEach(key => delete interfaceAliasMap[key]);
        
        data.interfaces.forEach(iface => {
            const name = iface.name;
            const alias = iface.alias || name;
            interfaceAliasMap[name] = alias;
        });
    } catch (e) {
        console.error('Ошибка при загрузке алиасов интерфейсов:', e);
    }
}

async function loadInterfaces() {
    try {
        const basePath = window.basePath || '';
        const res = await fetch(basePath + '/api/interfaces');
        const data = await res.json();
        const container = document.getElementById('interface-filters');
        container.innerHTML = '';
        // Очищаем глобальную карту перед новой загрузкой
        Object.keys(interfaceAliasMap).forEach(key => delete interfaceAliasMap[key]);

        const defaultIfaces = ['eth0', 'enp3s0', 'ens33', 'wlan0'];
        let selectedByDefault = null;

        data.interfaces.forEach(iface => {
            const name = iface.name;
            const alias = iface.alias || name; // Если алиас не задан, fallback на имя
            interfaceAliasMap[name] = alias;

            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'btn btn-outline-secondary iface';
            btn.dataset.name = name;
            btn.textContent = alias; // Отображаем алиас, а не имя
            btn.addEventListener('click', () => selectIface(name, btn));
            container.appendChild(btn);

            if (!selectedByDefault && defaultIfaces.includes(name)) {
                selectedByDefault = { name, btn };
            }
        });

        if (!selectedByDefault && data.interfaces.length > 0) {
            selectedByDefault = { name: data.interfaces[0].name, btn: container.children[0] };
        }

        if (selectedByDefault) {
            selectIface(selectedByDefault.name, selectedByDefault.btn);
        }
    } catch (e) {
        console.error('Ошибка при загрузке интерфейсов:', e);
    }
}

function selectIface(name, btn) {
    document.querySelectorAll('.iface').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    selectedIfaceName = name;
    selectedIfaceAlias = interfaceAliasMap[name] || name;

    // Обновляем заголовок над графиком
    const ifaceTitleEl = document.getElementById('bwIface');
    if (ifaceTitleEl) ifaceTitleEl.textContent = selectedIfaceAlias;

    updateGraph();
}

function getThemeColors() {
    const isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    return {
        rx: {
            border: isDark ? 'rgba(100, 181, 246, 1)' : 'rgba(54, 162, 235, 1)',
            fill: isDark ? 'rgba(100, 181, 246, 0.25)' : 'rgba(54, 162, 235, 0.2)'
        },
        tx: {
            border: isDark ? 'rgba(255, 138, 128, 1)' : 'rgba(255, 99, 132, 1)',
            fill: isDark ? 'rgba(255, 138, 128, 0.25)' : 'rgba(255, 99, 132, 0.2)'
        },
        grid: isDark ? '#333' : '#ddd',
        text: isDark ? '#ccc' : '#333'
    };
}

async function updateGraph() {
    if (!selectedIfaceName) return;
    const bwBox = document.getElementById('bwChartContainer');
    if (!bwBox || bwBox.style.display !== 'block') return;

    try {
        const basePath = window.basePath || '';
        const res = await fetch(`${basePath}/api/bw?iface=${selectedIfaceName}&period=${selectedPeriod}`);
        const data = await res.json();
        if (!data) return;

        const rawLabels = (data.utc_labels && data.utc_labels.length) ? data.utc_labels : (data.labels || []);
        const xAxisTitle = (selectedPeriod === 'hour' || selectedPeriod === 'day') ? 'Время' : 'Дата';

        // Преобразование UTC меток в локальное время
        const labels = rawLabels.map(lab => {
            const d = new Date(lab);
            if (isNaN(d.getTime())) {
                console.warn('Invalid UTC label:', lab);
                return lab;
            }
            if (selectedPeriod === 'hour' || selectedPeriod === 'day') {
                return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            } else {
                return d.toLocaleDateString([], { day: '2-digit', month: '2-digit' });
            }
        });

        const ctx = document.getElementById("bwChart").getContext("2d");
        const colors = getThemeColors();

        const datasets = [
            {
                label: "Принято",
                data: data.rx_mbps,
                fill: true,
                borderColor: colors.rx.border,
                backgroundColor: colors.rx.fill,
                tension: 0.2,
                pointRadius: 2
            },
            {
                label: "Передано",
                data: data.tx_mbps,
                fill: true,
                borderColor: colors.tx.border,
                backgroundColor: colors.tx.fill,
                tension: 0.2,
                pointRadius: 2
            }
        ];

        if (bwChartInstance) {
            bwChartInstance.data.labels = labels;
            bwChartInstance.data.datasets = datasets;
            bwChartInstance.options.scales.x.title.text = xAxisTitle;
            bwChartInstance.options.scales.x.title.color = colors.text;
            bwChartInstance.options.scales.y.title.color = colors.text;
            bwChartInstance.options.scales.x.ticks.color = colors.text;
            bwChartInstance.options.scales.y.ticks.color = colors.text;
            bwChartInstance.options.scales.x.grid.color = colors.grid;
            bwChartInstance.options.scales.y.grid.color = colors.grid;
            bwChartInstance.options.plugins.legend.labels.color = colors.text;
            bwChartInstance.update();
        } else {
            bwChartInstance = new Chart(ctx, {
                type: "line",
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    interaction: { mode: "index", intersect: false },
                    scales: {
                        y: {
                            title: { 
                                display: true, 
                                text: "Мбит/с",
                                color: colors.text
                            },
                            beginAtZero: true,
                            grid: { color: colors.grid },
                            ticks: { color: colors.text }
                        },
                        x: {
                            title: { 
                                display: true, 
                                text: xAxisTitle,
                                color: colors.text
                            },
                            grid: { color: colors.grid },
                            ticks: { color: colors.text }
                        }
                    },
                    plugins: {
                        legend: { 
                            position: "bottom", 
                            labels: { 
                                color: colors.text,
                                usePointStyle: false 
                            } 
                        }
                    }
                }
            });
        }
    } catch (e) {
        console.error("Ошибка при обновлении графика bw:", e);
    }
}


// Загрузка месячного трафика для ВСЕХ интерфейсов
async function loadAllMonthlyTraffic() {
    try {
        const basePath = window.basePath || '';
        const res = await fetch(`${basePath}/api/bw/monthly_traffic`);
        const data = await res.json();
        
        if (data.error) {
            console.error("Ошибка API:", data.error);
            return;
        }
        
        // Обновляем RX (мес) для каждого интерфейса
        document.querySelectorAll('.monthly-rx').forEach(el => {
            const iface = el.dataset.iface;
            if (data[iface]) {
                el.textContent = data[iface].rx_human || '0 B';
            } else {
                el.textContent = '0 B';
            }
        });
        
        // Обновляем TX (мес) для каждого интерфейса
        document.querySelectorAll('.monthly-tx').forEach(el => {
            const iface = el.dataset.iface;
            if (data[iface]) {
                el.textContent = data[iface].tx_human || '0 B';
            } else {
                el.textContent = '0 B';
            }
        });      
    } catch (e) {
        console.error("Ошибка при загрузке месячного трафика:", e);
    }
}

function toggleChartVisibility() {
    const chartContainer = document.getElementById('bwChartContainer');
    const toggleChartBtn = document.getElementById('toggleChartBtn');
    if (!chartContainer || !toggleChartBtn) return;
    const icon = toggleChartBtn.querySelector('i');
    if (!icon) return;
    const isVisible = chartContainer.style.display === 'block';

    if (isVisible) {
        chartContainer.style.display = 'none';
        toggleChartBtn.classList.remove('active', 'btn-primary');
        toggleChartBtn.classList.add('btn-outline-secondary');
        toggleChartBtn.setAttribute('title', 'Показать график');
        icon.classList.remove('bi-graph-down');
        icon.classList.add('bi-graph-up');
        localStorage.setItem('chartVisible', 'false');
        
        // Уничтожаем график при скрытии
        if (bwChartInstance) {
            bwChartInstance.destroy();
            bwChartInstance = null;
        }
    } else {
        chartContainer.style.display = 'block';
        toggleChartBtn.classList.add('active', 'btn-primary');
        toggleChartBtn.classList.remove('btn-outline-secondary');
        toggleChartBtn.setAttribute('title', 'Скрыть график');
        icon.classList.remove('bi-graph-up');
        icon.classList.add('bi-graph-down');
        localStorage.setItem('chartVisible', 'true');
        
        setTimeout(() => {
            updateGraph();
        }, 10);
    }
}

// Инициализация после загрузки страницы
document.addEventListener('DOMContentLoaded', () => {
    // 🔹 Загружаем алиасы интерфейсов ПЕРВЫМ делом
    loadInterfaceAliases();
    // Восстановление состояния CPU графика
    const cpuChartContainer = document.getElementById('cpuChartContainer');
    const toggleCpuChartBtn = document.getElementById('toggleCpuChartBtn');
    const cpuSavedState = localStorage.getItem('cpuChartVisible') === 'true';
    // Восстановление состояния графика OpenVPN
    const ovpnChartContainer = document.getElementById('ovpnChartContainer');
    const toggleOvpnChartBtn = document.getElementById('toggleOvpnChartBtn');
    const ovpnSavedState = localStorage.getItem('ovpnChartVisible') === 'true';


    if (cpuSavedState && toggleCpuChartBtn && cpuChartContainer) {
        cpuChartContainer.style.display = 'block';
        toggleCpuChartBtn.classList.add('active', 'btn-primary');
        toggleCpuChartBtn.classList.remove('btn-outline-secondary');
        toggleCpuChartBtn.setAttribute('title', 'Скрыть график');
        const cpuIcon = toggleCpuChartBtn.querySelector('i');
        cpuIcon.classList.remove('bi-graph-up');
        cpuIcon.classList.add('bi-graph-down');
    }

    // Инициализация CPU графика только если он видим
    if (cpuChartContainer && cpuChartContainer.style.display === 'block') {

        setTimeout(() => {
            initCpuChart();
            updateCpuChart('live');
        }, 100);
    }

    // Обработчики для кнопок периода CPU
    document.querySelectorAll('.cpu-period').forEach(btn => {
        btn.addEventListener('click', function () {
            document.querySelectorAll('.cpu-period').forEach(b => b.classList.remove('active'));
            this.classList.add('active');

            if (cpuInterval) clearTimeout(cpuInterval);
            updateCpuChart(this.dataset.period);
        });
    });

    // Обработчик для кнопки переключения видимости CPU графика
    if (toggleCpuChartBtn) {
        toggleCpuChartBtn.addEventListener('click', toggleCpuChartVisibility);
    }

    // Восстановление состояния BW графика
    const toggleChartBtn = document.getElementById('toggleChartBtn');
    if (toggleChartBtn) {
        toggleChartBtn.addEventListener('click', toggleChartVisibility);
        
        const savedState = localStorage.getItem('chartVisible') === 'true';
        const chartContainer = document.getElementById('bwChartContainer');
        const icon = toggleChartBtn.querySelector('i');
        
        if (savedState && chartContainer) {
            chartContainer.style.display = 'block';
            toggleChartBtn.classList.add('active', 'btn-primary');
            toggleChartBtn.classList.remove('btn-outline-secondary');
            toggleChartBtn.setAttribute('title', 'Скрыть график');
            icon.classList.remove('bi-graph-up');
            icon.classList.add('bi-graph-down');
            
            setTimeout(() => {
                loadInterfaces();
            }, 100);
        } else {
            // Если график скрыт, всё равно загружаем интерфейсы
            loadInterfaces();
        }
    } else {
        loadInterfaces();
    }

    // Обработчики для кнопок периода BW графика
    document.querySelectorAll('.period').forEach(b => {
        b.addEventListener('click', e => {
            document.querySelectorAll('.period').forEach(p => p.classList.remove('active'));
            e.currentTarget.classList.add('active');
            selectedPeriod = e.currentTarget.dataset.period;
            updateGraph();
        });

        if (b.dataset.period === selectedPeriod) {
            b.classList.add('active');
        }
    });

    // Обработчики для кнопок периода OpenVPN
    document.querySelectorAll('.ovpn-period').forEach(btn => {
        btn.addEventListener('click', function () {
            // Убираем active со всех кнопок
            document.querySelectorAll('.ovpn-period').forEach(b => {
                b.classList.remove('active', 'btn-primary');
                b.classList.add('btn-outline-secondary');
            });
            
            // Добавляем active на нажатую кнопку
            this.classList.remove('btn-outline-secondary');
            this.classList.add('active', 'btn-primary');
            
            // Очищаем интервал и обновляем график
            if (ovpnInterval) clearTimeout(ovpnInterval);
            updateOvpnChart(this.dataset.period);
        });
    });

    if (ovpnSavedState && toggleOvpnChartBtn && ovpnChartContainer) {
        ovpnChartContainer.style.display = 'block';
        toggleOvpnChartBtn.classList.add('active', 'btn-primary');
        toggleOvpnChartBtn.classList.remove('btn-outline-secondary');
        const ovpnIcon = toggleOvpnChartBtn.querySelector('i');
        if (ovpnIcon) {
            ovpnIcon.classList.remove('bi-graph-up');
            ovpnIcon.classList.add('bi-graph-down');
        }
    }

    if (ovpnChartContainer && ovpnChartContainer.style.display !== 'none') {
        setTimeout(() => {
            loadOvpnClients();
            initOvpnChart();
            updateOvpnChart(ovpnCurrentPeriod || 'live');
        }, 100);
    } else {
        setTimeout(() => {
            loadOvpnClients();
        }, 100);
    }

    if (toggleOvpnChartBtn) {
        toggleOvpnChartBtn.addEventListener('click', toggleOvpnChartVisibility);
    }

    const ovpnClientSelect = document.getElementById('ovpn-client-select');
    if (ovpnClientSelect) {
        ovpnClientSelect.addEventListener('change', function() {
            if (this.value) {
                if (ovpnChartContainer) ovpnChartContainer.style.display = 'block';
                if (!ovpnChart) initOvpnChart();
                // Используем текущий выбранный период
                updateOvpnChart(ovpnCurrentPeriod || 'live');
            } else {
                if (ovpnChartContainer) ovpnChartContainer.style.display = 'none';
            }
        });
    }

    // Обновляем стиль при переключении темы
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
        const cpuChartEl = document.getElementById('cpuChartContainer');
        if (cpuChart && cpuChartEl && cpuChartEl.style.display === 'block') {
            cpuChart.destroy();
            initCpuChart();
            const active = document.querySelector('.cpu-period.active');
            updateCpuChart(active ? active.dataset.period : 'live');
        }
        
        if (bwChartInstance) {
            bwChartInstance.destroy();
            bwChartInstance = null;
            updateGraph();
        }

        if (ovpnChart && document.getElementById('ovpnChartContainer').style.display === 'block') {
            ovpnChart.destroy();
            initOvpnChart();
            updateOvpnChart(ovpnCurrentPeriod || 'live');
        }
    });

    updateSystemInfo();
    setInterval(updateSystemInfo, 5000);
    // ЗАГРУЗКА МЕСЯЧНОГО ТРАФИКА ПО ВСЕМ ИНТЕРФЕЙСАМ
    setTimeout(() => {
        loadAllMonthlyTraffic();
    }, 250);
});

// Инициализация графика OpenVPN
function initOvpnChart() {
    const ctx = document.getElementById('ovpnSpeedChart');
    if (!ctx) return;

    const colors = themeColors();

    if (ovpnChart) {
        ovpnChart.destroy();
    }

    ovpnChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Отправлено (TX)',
                    data: [],
                    borderColor: 'rgba(255, 99, 132, 1)',
                    backgroundColor: 'rgba(255, 99, 132, 0.12)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                    yAxisID: 'y'
                },
                {
                    label: 'Получено (RX)',
                    data: [],
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.12)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                    yAxisID: 'y'
                }

            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { 
                    position: 'bottom',
                    labels: { color: colors.text }
                },
                tooltip: {
                    callbacks: {
                        label: function (ctx) {
                            const speed = ctx.parsed.y;
			    return `${ctx.dataset.label}: ${formatSpeed(ctx.parsed.y)}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { 
                        display: true, 
                        text: 'Скорость',
                        color: colors.text
                    },
                    grid: { color: colors.grid },
                    ticks: {
                        color: colors.text,
                        callback: function(value) {
                            return formatSpeedAxis(value);
                        }
                    }
                },
                x: {
                    title: { 
                        display: true, 
                        text: 'Время',
                        color: colors.text
                    },
                    grid: { color: colors.grid },
                    ticks: {
                        color: colors.text
                    }
                }
            }
        }
    });
}

// Обновление данных графика OpenVPN
function updateOvpnChart(period = 'live') {
    if (!ovpnChart) return;
    ovpnCurrentPeriod = period;
    const client = document.getElementById('ovpn-client-select')?.value;

    if (!client) {
        const container = document.getElementById('ovpnChartContainer');
        if (container) container.style.display = 'none';
        return;
    }

    const container = document.getElementById('ovpnChartContainer');
    if (container) container.style.display = 'block';

    const basePath = window.basePath || '';

    // Для live используем старый endpoint, для истории - новый
    const url = period === 'live' 
        ? `${basePath}/api/ovpn/live_chart?client=${encodeURIComponent(client)}`
        : `${basePath}/api/ovpn/speed_stats?client=${encodeURIComponent(client)}&period=${period}`;

    fetch(url)
        .then(r => {
            if (!r.ok) throw new Error('Network response was not ok');
            return r.json();
        })
        .then(data => {
            if (data.error || !data.labels) { 
                console.error(data.error); 
                return; 
            }

            // Преобразуем метки времени в читаемый формат
            let labels = [];
            if (period === 'live') {
                // Для live: преобразуем UTC время в локальное
                labels = data.labels.map(ts => {
                    const d = new Date(ts);
                    if (isNaN(d.getTime())) {
                        console.error('Invalid date:', ts);
                        return ts;
                    }
                    return d.toLocaleTimeString('ru-RU', { 
                        hour: '2-digit', 
                        minute: '2-digit', 
                        second: '2-digit' 
                    });
                });
            } else {
                // Для исторических данных (hour, day, week)
                labels = data.labels.map((label) => {
                    try {
                        const d = new Date(label);
                        if (isNaN(d.getTime())) {
                            return label;
                        }
                        
                        if (period === 'hour') {
                            return d.toLocaleTimeString('ru-RU', { 
                                hour: '2-digit', 
                                minute: '2-digit' 
                            });
                        } else if (period === 'day') {
                            return d.toLocaleTimeString('ru-RU', { 
                                hour: '2-digit', 
                                minute: '2-digit' 
                            });
                        } else if (period === 'week') {
                            const days = ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб'];
                            const dayName = days[d.getDay()];
                            const dateStr = d.toLocaleDateString('ru-RU', { 
                                day: '2-digit', 
                                month: '2-digit' 
                            });
                            return `${dayName} ${dateStr}`;
                        }
                    } catch (e) {
                        console.error('Ошибка преобразования даты:', label, e);
                    }
                    return label;
                });
            }

            // Получаем данные скорости (из speed_stats приходят сразу rx_speed/tx_speed)
            const rxData = data.rx_speed || [];
            const txData = data.tx_speed || [];

            ovpnChart.data.labels = labels;
            ovpnChart.data.datasets[0].data = rxData;
            ovpnChart.data.datasets[1].data = txData;

            // Обновляем заголовок оси Y в зависимости от периода
            const yAxisTitle = period === 'live' ? 'Скорость (бит/с)' : 'Средняя скорость (бит/с)';
            ovpnChart.options.scales.y.title.text = yAxisTitle;

            ovpnChart.update('none');
        })
        .catch(err => console.error('Ошибка при загрузке OpenVPN данных:', err));

    // Автообновление только для live режима
    if (ovpnInterval) clearTimeout(ovpnInterval);

    if (period === 'live') {
        ovpnInterval = setTimeout(() => updateOvpnChart('live'), OVPN_LIVE_UPDATE_MS);
    }
}

// Загрузка списка клиентов OpenVPN
async function loadOvpnClients() {
    const basePath = window.basePath || '';
    try {
        const response = await fetch(`${basePath}/api/ovpn/chart/clients`);
        const clients = await response.json();
        const select = document.getElementById('ovpn-client-select');

        if (!select) return;

        select.innerHTML = '<option value="">Выберите клиента</option>';
        clients.forEach(client => {
            const option = document.createElement('option');
            option.value = client;
            option.textContent = client;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Ошибка загрузки клиентов OpenVPN:', error);
    }
}

// Переключение видимости графика OpenVPN
function toggleOvpnChartVisibility() {
    const chartContainer = document.getElementById('ovpnChartContainer');
    const toggleBtn = document.getElementById('toggleOvpnChartBtn');
    if (!chartContainer || !toggleBtn) return;

    const icon = toggleBtn.querySelector('i');
    const isVisible = chartContainer.style.display === 'block';

    if (isVisible) {
        chartContainer.style.display = 'none';
        toggleBtn.classList.remove('active', 'btn-primary');
        toggleBtn.classList.add('btn-outline-secondary');
        toggleBtn.setAttribute('title', 'Показать график OpenVPN');
        if (icon) {
            icon.classList.remove('bi-graph-down');
            icon.classList.add('bi-graph-up');
        }
        localStorage.setItem('ovpnChartVisible', 'false');

        if (ovpnInterval) {
            clearTimeout(ovpnInterval);
            ovpnInterval = null;
        }
        
        if (ovpnChart) {
            ovpnChart.destroy();
            ovpnChart = null;
        }
    } else {
        chartContainer.style.display = 'block';
        toggleBtn.classList.add('active', 'btn-primary');
        toggleBtn.classList.remove('btn-outline-secondary');
        toggleBtn.setAttribute('title', 'Скрыть график OpenVPN');
        if (icon) {
            icon.classList.remove('bi-graph-up');
            icon.classList.add('bi-graph-down');
        }
        localStorage.setItem('ovpnChartVisible', 'true');

        setTimeout(() => {
            initOvpnChart();
            // Восстанавливаем последний выбранный период
            updateOvpnChart(ovpnCurrentPeriod || 'live');
        }, 10);
    }
}