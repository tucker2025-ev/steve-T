/**
 * EV Charger Transaction Management
 */

const API_URL = "http://192.168.29.230:9081/api/Charger/getAll/trans";
const API_KEY = "Tucker";
let dataTable;

async function fetchData() {
    try {
        const response = await fetch(API_URL, {
            headers: { 'STEVE-API-KEY': API_KEY }
        });
        const allData = await response.json();

        updateStats(allData);
        populateFilters(allData);
        renderTable(allData);
    } catch (error) {
        console.error("Fetch error:", error);
    }
}

function updateStats(data) {
    document.getElementById('totalTransactions').textContent = data.length;
    document.getElementById('activeTransactions').textContent = data.filter(t => !t.stopTimestamp).length;
    document.getElementById('inactiveTransactions').textContent = data.filter(t => t.stopTimestamp).length;
}

function populateFilters(data) {
    const tagSelect = document.getElementById('ocppTagFilter');
    const stationSelect = document.getElementById('chargerFilter');

    // Save current selection to prevent losing it on re-render
    const currentTag = tagSelect.value;
    const currentStation = stationSelect.value;

    const tags = [...new Set(data.map(t => t.ocppIdTag))].filter(Boolean).sort();
    const stations = [...new Set(data.map(t => t.chargeBoxId))].filter(Boolean).sort();

    tagSelect.innerHTML = '<option value="">All Tags</option>';
    stationSelect.innerHTML = '<option value="">All Stations</option>';

    tags.forEach(tag => {
        const selected = tag === currentTag ? 'selected' : '';
        tagSelect.innerHTML += `<option value="${tag}" ${selected}>${tag}</option>`;
    });

    stations.forEach(id => {
        const selected = id === currentStation ? 'selected' : '';
        stationSelect.innerHTML += `<option value="${id}" ${selected}>${id}</option>`;
    });
}

function renderTable(data) {
    if ($.fn.DataTable.isDataTable('#transactionTable')) {
        dataTable.destroy();
    }

    const tbody = document.getElementById('tableBody');
    tbody.innerHTML = '';

    data.forEach(t => {
        const isActive = !t.stopTimestamp;
        const statusText = isActive ? 'Active' : 'Finished';

        const row = `
            <tr class="data-row ${isActive ? 'active-row' : 'inactive-row'}">
                <td class="fw-bold">${t.id}</td>
                <td>
                    <div class="station-code small">${t.chargeBoxId}</div>
                    <div class="text-muted" style="font-size: 0.7rem;">Conn: ${t.connectorId}</div>
                </td>
                <td><span class="badge bg-light text-dark border">${t.ocppIdTag || 'N/A'}</span></td>
                <td>${t.startValue}</td>
                <td class="${isActive ? 'text-primary fw-bold' : ''}">${t.stopValue || '---'}</td>
                <td class="small">${t.startTimestamp}</td>
                <td class="small text-muted">${t.stopTimestamp || 'Ongoing'}</td>
                <td class="small">${t.stopReason || '---'}</td>
                <td>
                    <span class="status-pill ${isActive ? 'pill-active' : 'pill-completed'}">
                        <i class="fas ${isActive ? 'fa-bolt fa-beat' : 'fa-check-circle'} me-1"></i>
                        ${statusText}
                    </span>
                </td>
            </tr>`;
        tbody.innerHTML += row;
    });

    // Initialize DataTable with "Active" as the default search filter
    dataTable = $('#transactionTable').DataTable({
        order: [[0, 'desc']],
        pageLength: 10,
        dom: 'lrtip',
        searchCols: [
            null, null, null, null, null, null, null, null, { "search": "Active" }
        ],
        language: {
            search: "_INPUT_",
            searchPlaceholder: "Search ID..."
        }
    });
}

// Event Listeners
document.addEventListener('DOMContentLoaded', () => {
    fetchData();

    document.getElementById('statusFilterSelect').addEventListener('change', function() {
        dataTable.column(8).search(this.value).draw();
    });

    document.getElementById('ocppTagFilter').addEventListener('change', function() {
        dataTable.column(2).search(this.value).draw();
    });

    document.getElementById('chargerFilter').addEventListener('change', function() {
        dataTable.column(1).search(this.value).draw();
    });

    document.getElementById('idSearchInput').addEventListener('keyup', function() {
        dataTable.column(0).search(this.value).draw();
    });
});