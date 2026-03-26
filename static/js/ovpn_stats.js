let clientChart = null;
let selectedClient = null;
let selectedChartPeriod = 'month';
let selectedChartDate = null;
const OVPN_DATE_KEY = 'ovpnStats.selectedDate';
const OVPN_PERIOD_KEY = 'ovpnStats.selectedPeriod';
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
    if (period === 'day') {
        return d.toLocaleTimeString('ru-RU', {
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        });
    } else if (period === 'year') {
        // ✅ Для года показываем месяц
        return d.toLocaleDateString('ru-RU', {
            month: 'short',
            year: 'numeric'
        }).replace(/\./g, '');
    }
    
    return d.toLocaleDateString('ru-RU', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric'
    });
}

// ✅ Генерация опций для выбора месяца
function generateMonthOptions() {
    const monthNames = [
        'январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
        'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь'
    ];
    const select = document.getElementById('chartMonth');
    if (!select) return;

    // Очищаем существующие опции
    select.innerHTML = '';

    const now = new Date();
    const currentYear = now.getFullYear();

    // Генерируем месяцы за последние 12 месяцев + текущий
    for (let i = 5; i >= 0; i--) {
        const date = new Date(currentYear, now.getMonth() - i, 1);
        const year = date.getFullYear();
        const month = date.getMonth();
        const value = `${year}-${String(month + 1).padStart(2, '0')}`;
        const label = `${monthNames[month]} ${year} г.`;
        
        const option = document.createElement('option');
        option.value = value;
        option.textContent = label;
        select.appendChild(option);
    }
}

async function updateClientChart() {
    if (!selectedClient) return;
    const basePath = window.basePath || '';
    let url = `${basePath}/api/ovpn/client_chart?client=${encodeURIComponent(selectedClient)}&period=${selectedChartPeriod}`;
    
    // ✅ Добавляем дату для day и month
    if ((selectedChartPeriod === 'day' || selectedChartPeriod === 'month') && selectedChartDate) {
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

        const dateDisplay = document.getElementById('chartDateDisplay');
        if (dateDisplay) {
            if (selectedChartPeriod === 'day' && selectedChartDate) {
                const dateObj = new Date(selectedChartDate);
                dateDisplay.textContent = dateObj.toLocaleDateString('ru-RU', {
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric'
                });
            } else if (selectedChartPeriod === 'month' && selectedChartDate) {
                // ✅ Отображаем выбранный месяц, а не текущий
                const [year, month] = selectedChartDate.split('-');
                const monthNames = [
                    'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                    'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
                ];
                dateDisplay.textContent = `${monthNames[parseInt(month) - 1]} ${year} г.`;
            } else if (selectedChartPeriod === 'year') {
                const now = new Date();
                dateDisplay.textContent = now.getFullYear().toString();
            } else {
                dateDisplay.textContent = '';
            }
        }

        const colors = getThemeColors();
        const xAxisTitle = (selectedChartPeriod === 'day') ? 'Время' : 'Дата';

        const datasets = [
            {
                label: 'Передано',
                data: data.rx_bytes,
                fill: true,
                borderColor: colors.rx.border,
                backgroundColor: colors.rx.fill,
                tension: 0.2,
                pointRadius: selectedChartPeriod === 'day' ? 1 : 2
            },
            {
                label: 'Получено',
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
       				maxTicksLimit: selectedChartPeriod === 'year' ? 12 : (selectedChartPeriod === 'day' ? 24 : undefined),
        			autoSkip: true,
        			maxRotation: 45,
        			minRotation: 0
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

// ✅ Обновление видимости селекторов даты/месяца
function updateDatePickerVisibility() {
    const datePickerContainer = document.getElementById('datePickerContainer');
    const dateInput = document.getElementById('chartDate');
    const monthSelect = document.getElementById('chartMonth');
    const dateLabel = document.getElementById('dateLabel');
    const monthLabel = document.getElementById('monthLabel');
    
    if (datePickerContainer) {
        datePickerContainer.style.display = (selectedChartPeriod === 'day' || selectedChartPeriod === 'month') ? 'block' : 'none';
    }

    if (dateInput && monthSelect && dateLabel && monthLabel) {
        if (selectedChartPeriod === 'day') {
            dateInput.style.display = 'inline-block';
            dateLabel.style.display = 'inline';
            monthSelect.style.display = 'none';
            monthLabel.style.display = 'none';
        } else if (selectedChartPeriod === 'month') {
            dateInput.style.display = 'none';
            dateLabel.style.display = 'none';
            monthSelect.style.display = 'inline-block';
            monthLabel.style.display = 'inline';
            // ✅ Генерируем опции только один раз при показе
            if (monthSelect.options.length === 0) {
                generateMonthOptions();
            }
        } else {
            dateInput.style.display = 'none';
            monthSelect.style.display = 'none';
            dateLabel.style.display = 'none';
            monthLabel.style.display = 'none';
        }
    }
}

// ✅ Установка даты/месяца по умолчанию (только если нет сохранённого значения)
function setDefaultDate() {
    const dateInput = document.getElementById('chartDate');
    const monthSelect = document.getElementById('chartMonth');
    const today = new Date();
    
    // ✅ НЕ переопределяем, если дата уже восстановлена из localStorage
    if (selectedChartPeriod === 'day') {
        if (dateInput && !selectedChartDate) {
            const todayStr = today.toISOString().split('T')[0];
            dateInput.value = todayStr;
            selectedChartDate = todayStr;
        }
    } else if (selectedChartPeriod === 'month') {
        if (monthSelect && !selectedChartDate) {
            const monthStr = today.toISOString().slice(0, 7); // YYYY-MM
            if (monthSelect.options.length === 0) {
                generateMonthOptions();
            }
            monthSelect.value = monthStr;
            selectedChartDate = monthStr;
        }
    }
}

// ✅ Обновление статистики при изменении даты/месяца
function updateStatsOnDateChange() {
    const dateInput = document.getElementById('chartDate');
    const monthSelect = document.getElementById('chartMonth');
    let newDate = null;
    
    if (selectedChartPeriod === 'day' && dateInput) {
        newDate = dateInput.value;
    } else if (selectedChartPeriod === 'month' && monthSelect) {
        newDate = monthSelect.value;
    }
    
    if (!newDate || newDate === selectedChartDate) return;
    
    selectedChartDate = newDate;
    
    // ✅ Сохраняем дату И период в localStorage
    try { 
        localStorage.setItem(OVPN_DATE_KEY, newDate);
        localStorage.setItem(OVPN_PERIOD_KEY, selectedChartPeriod);
    } catch (e) {}
    
    // ✅ Обновляем график клиента (если выбран)
    if (selectedClient) {
        updateClientChart();
    }
    
    // ✅ Обновляем страницу статистики (перезагрузка с новой датой)
    const url = new URL(window.location.href);
    url.searchParams.set('date', newDate);
    url.searchParams.set('period', selectedChartPeriod);
    window.history.pushState({}, '', url);
    
    // Показываем индикатор загрузки
    const tableContainer = document.querySelector('.client-table');
    if (tableContainer) {
        tableContainer.style.opacity = '0.5';
        tableContainer.style.pointerEvents = 'none';
    }
    
    // Перезагружаем страницу для обновления таблицы
    setTimeout(() => {
        window.location.reload();
    }, 300);
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
    
    // ✅ 1. Сначала восстанавливаем период из URL или localStorage
    const urlParams = new URLSearchParams(window.location.search);
    const urlPeriod = urlParams.get('period');
    
    if (urlPeriod && ['day', 'month', 'year'].includes(urlPeriod)) {
        selectedChartPeriod = urlPeriod;
    } else {
        try {
            const savedPeriod = localStorage.getItem(OVPN_PERIOD_KEY);
            if (savedPeriod && ['day', 'month', 'year'].includes(savedPeriod)) {
                selectedChartPeriod = savedPeriod;
            }
        } catch (e) {}
        
        if (!selectedChartPeriod) {
            const activePeriodBtn = document.querySelector('.chart-period.active');
            if (activePeriodBtn && activePeriodBtn.dataset.period) {
                selectedChartPeriod = activePeriodBtn.dataset.period;
            } else {
                selectedChartPeriod = 'month';
            }
        }
    }
    
    // ✅ 2. Обработчики кнопок периода
    document.querySelectorAll('.chart-period').forEach(btn => {
        btn.addEventListener('click', function (e) {
            const period = this.dataset.period;
            if (period) {
                selectedChartPeriod = period;
                try { localStorage.setItem(OVPN_PERIOD_KEY, period); } catch (e) {}
            }
            updateDatePickerVisibility();
            if (selectedChartPeriod === 'day' || selectedChartPeriod === 'month') {
                setDefaultDate();
            }
        });
    });
    
    // Обработчик кнопки переключения видимости графика
    const toggleBtn = document.getElementById('toggleClientChartBtn');
    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => toggleClientChartVisibility());
        
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
    
    // ✅ 3. Восстанавливаем сохранённую дату ИЗ localStorage (ПОСЛЕ периода)
    try {
        const savedDate = localStorage.getItem(OVPN_DATE_KEY);
        const savedPeriod = localStorage.getItem(OVPN_PERIOD_KEY);
        
        // ✅ Используем сохранённый период, если он есть
        if (savedPeriod && ['day', 'month', 'year'].includes(savedPeriod)) {
            selectedChartPeriod = savedPeriod;
        }
        
        if (savedDate && (selectedChartPeriod === 'day' || selectedChartPeriod === 'month')) {
            selectedChartDate = savedDate;
        }
    } catch (e) {
        console.warn('Не удалось восстановить дату:', e);
    }
    
    // ✅ 4. Вызываем updateDatePickerVisibility ПЕРЕД установкой значений
    updateDatePickerVisibility();
    
    // ✅ 5. Теперь устанавливаем значения в input/select
    if (selectedChartDate) {
        const dateInput = document.getElementById('chartDate');
        const monthSelect = document.getElementById('chartMonth');
        
        if (selectedChartPeriod === 'day' && dateInput) {
            dateInput.value = selectedChartDate;
        } else if (selectedChartPeriod === 'month' && monthSelect) {
            if (monthSelect.options.length === 0) {
                generateMonthOptions();
            }
            monthSelect.value = selectedChartDate;
        }
    }
    
    // Обработчик изменения даты
    const dateInput = document.getElementById('chartDate');
    if (dateInput) {
        dateInput.addEventListener('change', updateStatsOnDateChange);
    }
    
    // Обработчик изменения месяца
    const monthSelect = document.getElementById('chartMonth');
    if (monthSelect) {
        monthSelect.addEventListener('change', updateStatsOnDateChange);
    }
    
    // Смена темы
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
        if (clientChart) {
            clientChart.destroy();
            clientChart = null;
            updateClientChart();
        }
    });
});