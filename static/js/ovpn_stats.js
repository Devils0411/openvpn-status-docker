let clientChart = null;
let selectedClient = null;
let selectedChartPeriod = 'day';
let selectedChartDate = null;
const OVPN_DATE_KEY = 'ovpnStats.selectedDate';
const OVPN_CHART_VISIBLE_KEY = 'ovpnStats.chartVisible';

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

function humanizeBytes(bytes) {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0;
    let val = bytes;
    while (val >= 1024 && i < units.length - 1) {
        val /= 1024;
        i++;
    }
    return val.toFixed(2) + ' ' + units[i];
}

function formatLabel(dateStr, period) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) {
        console.warn('Invalid date label:', dateStr);
        return dateStr;
    }
    if (period === 'day' || period === 'hour') {
        return d.toLocaleTimeString('ru-RU', { 
            hour: '2-digit', 
            minute: '2-digit',
            hour12: false 
        });
    }
    return d.toLocaleDateString('ru-RU', { 
        day: '2-digit', 
        month: '2-digit',
        year: 'numeric'
    });
}

async function updateClientChart() {
    if (!selectedClient) return;
    
    const basePath = window.basePath || '';
    let url = `${basePath}/api/ovpn/client_chart?client=${encodeURIComponent(selectedClient)}&period=${selectedChartPeriod}`;

    if ((selectedChartPeriod === 'day' || selectedChartPeriod === 'hour') && selectedChartDate) {
        url += `&date=${selectedChartDate}`;
    }

    try {
        const res = await fetch(url);
        const data = await res.json();
        if (data.error) {
            console.error(data.error);
            return;
        }

        const rawLabels = (data.labels && data.labels.length) ? data.labels : [];
        const labels = rawLabels.map(lab => formatLabel(lab, selectedChartPeriod));

        const colors = getThemeColors();
        const xAxisTitle = (selectedChartPeriod === 'day' || selectedChartPeriod === 'hour') ? 'Время' : 'Дата';

        const datasets = [
            {
                label: 'Получено',
                data: data.rx_bytes,
                fill: true,
                borderColor: colors.rx.border,
                backgroundColor: colors.rx.fill,
                tension: 0.2,
                pointRadius: selectedChartPeriod === 'day' ? 1 : 2
            },
            {
                label: 'Передано',
                data: data.tx_bytes,
                fill: true,
                borderColor: colors.tx.border,
                backgroundColor: colors.tx.fill,
                tension: 0.2,
                pointRadius: selectedChartPeriod === 'day' ? 1 : 2
            }
        ];

        const ctx = document.getElementById('clientChart');
        if (!ctx) return;

        if (clientChart) {
            clientChart.data.labels = labels;
            clientChart.data.datasets = datasets;
            clientChart.options.scales.x.title.text = xAxisTitle;
            clientChart.options.scales.x.title.color = colors.text;
            clientChart.options.scales.y.title.color = colors.text;
            clientChart.options.scales.x.ticks.color = colors.text;
            clientChart.options.scales.y.ticks.color = colors.text;
            clientChart.options.scales.x.grid.color = colors.grid;
            clientChart.options.scales.y.grid.color = colors.grid;
            clientChart.options.plugins.legend.labels.color = colors.text;
            clientChart.update();
        } else {
            clientChart = new Chart(ctx.getContext('2d'), {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    scales: {
                        y: {
                            title: { display: true, text: 'Трафик', color: colors.text },
                            beginAtZero: true,
                            grid: { color: colors.grid },
                            ticks: {
                                color: colors.text,
                                callback: function (value) { return humanizeBytes(value); }
                            }
                        },
                        x: {
                            title: { display: true, text: xAxisTitle, color: colors.text },
                            grid: { color: colors.grid },
                            ticks: { 
                                color: colors.text,
                                maxTicksLimit: selectedChartPeriod === 'day' ? 24 : undefined
                            }
                        }
                    },
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: { color: colors.text, usePointStyle: false }
                        },
                        tooltip: {
                            callbacks: {
                                label: function (ctx) {
                                    return `${ctx.dataset.label}: ${humanizeBytes(ctx.parsed.y)}`;
                                }
                            }
                        }
                    }
                }
            });
        }
    } catch (e) {
        console.error('Ошибка при загрузке графика клиента:', e);
    }
}

function selectClient(clientName) {
    const container = document.getElementById('clientChartContainer');
    const nameEl = document.getElementById('chartClientName');
    if (!container || !nameEl) return;

    // Если график скрыт, показываем его при выборе клиента
    if (container.style.display === 'none') {
        toggleClientChartVisibility(true);
    }

    if (selectedClient === clientName) {
        selectedClient = null;
        container.style.display = 'none';
        document.querySelectorAll('.client-table tbody tr').forEach(r => r.classList.remove('table-active'));
        if (clientChart) {
            clientChart.destroy();
            clientChart = null;
        }
        return;
    }

    selectedClient = clientName;
    nameEl.textContent = clientName;
    container.style.display = 'block';

    document.querySelectorAll('.client-table tbody tr').forEach(r => {
        r.classList.toggle('table-active', r.dataset.client === clientName);
    });

    if (clientChart) {
        clientChart.destroy();
        clientChart = null;
    }

    setTimeout(() => {
        updateClientChart();
        container.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }, 10);
}

function toggleClientChartVisibility(forceShow = null) {
    const chartContainer = document.getElementById('clientChartContainer');
    const toggleBtn = document.getElementById('toggleClientChartBtn');
    if (!chartContainer || !toggleBtn) return;
    
    const icon = toggleBtn.querySelector('i');
    const isVisible = chartContainer.style.display !== 'none';
    const shouldShow = forceShow !== null ? forceShow : !isVisible;
    
    if (shouldShow) {
        chartContainer.style.display = 'block';
        toggleBtn.classList.add('active', 'btn-primary');
        toggleBtn.classList.remove('btn-outline-secondary');
        toggleBtn.setAttribute('title', 'Скрыть график');
        if (icon) {
            icon.classList.remove('bi-graph-up');
            icon.classList.add('bi-graph-down');
        }
        localStorage.setItem(OVPN_CHART_VISIBLE_KEY, 'true');
        
        setTimeout(() => {
            if (clientChart) {
                clientChart.resize();
            } else if (selectedClient) {
                updateClientChart();
            }
        }, 10);
    } else {
        chartContainer.style.display = 'none';
        toggleBtn.classList.remove('active', 'btn-primary');
        toggleBtn.classList.add('btn-outline-secondary');
        toggleBtn.setAttribute('title', 'Показать график');
        if (icon) {
            icon.classList.remove('bi-graph-down');
            icon.classList.add('bi-graph-up');
        }
        localStorage.setItem(OVPN_CHART_VISIBLE_KEY, 'false');
        
        if (clientChart) {
            clientChart.destroy();
            clientChart = null;
        }
    }
}

function updateDatePickerVisibility() {
    const datePickerContainer = document.getElementById('datePickerContainer');
    if (datePickerContainer) {
        datePickerContainer.style.display = (selectedChartPeriod === 'day' || selectedChartPeriod === 'hour') ? 'block' : 'none';
    }
}

function setDefaultDate() {
    const dateInput = document.getElementById('chartDate');
    if (dateInput && !selectedChartDate) {
        const today = new Date().toISOString().split('T')[0];
        dateInput.value = today;
        selectedChartDate = today;
        try { localStorage.setItem(OVPN_DATE_KEY, today); } catch (e) {}
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Обработка времени подключения
    document.querySelectorAll('.connection-time[data-utc]').forEach(cell => {
        const utcDateStr = cell.dataset.utc;
        if (utcDateStr) {
            const utcDate = new Date(utcDateStr);
            if (!isNaN(utcDate.getTime())) {
                cell.textContent = utcDate.toLocaleString('ru-RU', {
                    year: 'numeric',
                    month: '2-digit',
                    day: '2-digit',
                    hour: '2-digit',
                    minute: '2-digit'
                });
            } else {
                cell.textContent = 'Нет данных';
            }
        } else {
            cell.textContent = 'Нет данных';
        }
    });
    
    // Фильтр клиентов
    const clientFilter = document.getElementById('clientFilter');
    if (clientFilter) {
        clientFilter.addEventListener('input', function () {
            const filterValue = clientFilter.value.toLowerCase();
            document.querySelectorAll('.client-table tbody tr').forEach(row => {
                const clientName = row.querySelector('.client-name').textContent.toLowerCase();
                row.style.display = clientName.includes(filterValue) ? '' : 'none';
            });
        });
    }

    // Клик по строке таблицы
    document.querySelectorAll('.client-table tbody tr[data-client]').forEach(row => {
        row.addEventListener('click', () => {
            selectClient(row.dataset.client);
        });
    });

    // Инициализация периода
    const activePeriodBtn = document.querySelector('.chart-period.active');
    if (activePeriodBtn && activePeriodBtn.dataset.period) {
        selectedChartPeriod = activePeriodBtn.dataset.period;
    }

    // Обработчики кнопок периода
    document.querySelectorAll('.chart-period').forEach(btn => {
        btn.addEventListener('click', function (e) {
            selectedChartPeriod = this.dataset.period || selectedChartPeriod;
            updateDatePickerVisibility();
            if (selectedChartPeriod === 'day' || selectedChartPeriod === 'hour') {
                setDefaultDate();
            }
        });
    });

    // ✅ Обработчик кнопки переключения видимости графика
    const toggleBtn = document.getElementById('toggleClientChartBtn');
    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => toggleClientChartVisibility());
        
        // ✅ График скрыт по умолчанию при загрузке страницы
        const chartContainer = document.getElementById('clientChartContainer');
        if (chartContainer) {
            chartContainer.style.display = 'none';
        }
        toggleBtn.classList.remove('active', 'btn-primary');
        toggleBtn.classList.add('btn-outline-secondary');
        toggleBtn.setAttribute('title', 'Показать график');
        const icon = toggleBtn.querySelector('i');
        if (icon) {
            icon.classList.remove('bi-graph-down');
            icon.classList.add('bi-graph-up');
        }
    }

    // ✅ Убрано восстановление последнего клиента (чтобы график не открывался сам)
    // Восстанавливаем только выбранную дату
    try {
        const savedDate = localStorage.getItem(OVPN_DATE_KEY);
        if (savedDate) {
            selectedChartDate = savedDate;
            const dateInput = document.getElementById('chartDate');
            if (dateInput) {
                dateInput.value = savedDate;
            }
        }
    } catch (e) {
        console.warn('Не удалось восстановить дату:', e);
    }


    // Инициализация
    updateDatePickerVisibility();
    setDefaultDate();

    // Смена темы
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
        if (clientChart) {
            clientChart.destroy();
            clientChart = null;
            updateClientChart();
        }
    });
});