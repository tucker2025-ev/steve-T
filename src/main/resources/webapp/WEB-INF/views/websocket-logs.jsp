<%--

    SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
    Copyright (C) 2013-2026 SteVe Community Team
    All Rights Reserved.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

--%>

<%--

    SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
    Copyright (C) 2013-2025 SteVe Community Team
    All Rights Reserved.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://www.springframework.org/tags" prefix="spring" %>

<%@ include file="00-context.jsp" %>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WebSocket Logs</title>
    <%-- The updated styles provide a modern, themed, and user-friendly interface. --%>
    <style>
        :root {
            --primary-color: #E4022D;
            --primary-text-color: #FFFFFF;
            --secondary-bg-color: #f8f9fa;
            --border-color: #dee2e6;
            --text-color: #212529;
            --table-hover-bg: #f1f1f1;
            --animation-highlight-bg: #fff3cd;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            background-color: var(--secondary-bg-color);
            color: var(--text-color);
            line-height: 1.5;
        }

        .header {
            background-color: var(--primary-color);
            color: var(--primary-text-color);
            padding: 20px 40px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .header h2 {
            margin: 0;
            font-size: 1.75rem;
        }

        .main-content {
            padding: 20px 40px;
        }

        .controls-container {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            display: flex;
            gap: 20px;
            align-items: flex-end; /* Aligns items to the bottom */
            flex-wrap: wrap;
        }

        .control-group {
            display: flex;
            flex-direction: column;
            flex-grow: 1; /* Allows filter input to take more space */
        }

        .control-group:first-child {
            flex-grow: 0;
        }

        label {
            font-weight: 600;
            margin-bottom: 5px;
            font-size: 0.9em;
            color: #495057;
        }

        input[type="date"],
        input[type="text"] {
            padding: 10px;
            border: 1px solid var(--border-color);
            border-radius: 5px;
            font-size: 1rem;
            transition: border-color 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
            box-sizing: border-box;
            width: 100%;
        }

        input[type="date"]:focus,
        input[type="text"]:focus {
            outline: none;
            border-color: var(--primary-color);
            box-shadow: 0 0 0 3px rgba(228, 2, 45, 0.25);
        }

        button[type="submit"] {
            padding: 10px 20px;
            background-color: var(--primary-color);
            color: var(--primary-text-color);
            border: none;
            border-radius: 5px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: background-color 0.2s ease-in-out;
        }

        button[type="submit"]:hover {
            background-color: #c30024; /* Darker shade of primary color */
        }

        .table-container {
            margin-top: 25px;
            overflow-x: auto; /* Makes table horizontally scrollable on small screens */
            background-color: #fff;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
            min-width: 1200px; /* Ensures columns don't get too cramped */
        }

        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
            vertical-align: top;
        }

        /* Specific column widths for better layout */
        th:nth-child(1), td:nth-child(1) { width: 12%; } /* Time */
        th:nth-child(2), td:nth-child(2) { width: 8%; }  /* ChargeBox ID */
        th:nth-child(3), td:nth-child(3) { width: 20%; } /* Session ID */
        th:nth-child(4), td:nth-child(4) { width: 8%; }  /* Transaction ID */
        th:nth-child(5), td:nth-child(5) { width: 10%; } /* Event */
        th:nth-child(6), td:nth-child(6) { width: 30%; } /* Payload */
        th:nth-child(7), td:nth-child(7) { width: 12%; } /* Direction */


        /* Improved Payload styling for readability */
        td:nth-child(6) {
            font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
            font-size: 0.85em;
            white-space: pre-wrap; /* Preserves formatting of JSON */
            word-break: break-all;
        }

        thead th {
            background-color: #f1f3f5;
            font-weight: 600;
            position: sticky; /* Keeps header visible on vertical scroll */
            top: 0;
            z-index: 10;
        }

        tbody tr:nth-child(odd) {
            background-color: #ffffff;
        }

        tbody tr:nth-child(even) {
            background-color: var(--secondary-bg-color);
        }

        tbody tr:hover {
            background-color: var(--table-hover-bg);
        }

        /* Animation for when new data is inserted */
        @keyframes highlight-new-row {
            from { background-color: var(--animation-highlight-bg); }
            to { background-color: inherit; }
        }

        .new-row {
            animation: highlight-new-row 2s ease-out;
        }

        .no-logs-message {
            margin-top: 25px;
            padding: 20px;
            background-color: #fff8e1;
            border: 1px solid #ffecb3;
            border-radius: 8px;
            text-align: center;
            color: #6d4c41;
        }
    </style>
    <script type="text/javascript" src="${ctxPath}/static/js/jquery-2.0.3.min.js"></script>
</head>
<body>

<div class="header">
    <h2>WebSocket Logs</h2>
</div>

<div class="main-content">
    <form action="${ctxPath}/manager/websocket-logs" method="get" class="controls-container">
        <div class="control-group">
            <label for="logDate">Select Date</label>
            <input type="date" id="logDate" name="date" value="${selectedDate}" max="${today}" required>
        </div>

        <div class="control-group">
            <label for="filterInput">Filter Logs (by any column)</label>
            <input type="text" id="filterInput" placeholder="Type to filter anything...">
        </div>

        <button type="submit">Fetch Logs</button>
    </form>

    <div class="table-container">
        <table border="0">
            <thead>
            <tr>
                <th>Time</th>
                <th>ChargeBox ID</th>
                <th>Session ID</th>
                <th>Transaction ID</th>
                <th>Event</th>
                <th>Payload</th>
                <th>Direction</th>
            </tr>
            </thead>
            <tbody id="log-table-body">
            <c:forEach var="log" items="${logs}">
                <tr>
                    <td>${log.time}</td>
                    <td>${log.chargeBoxId}</td>
                    <td>${log.sessionId}</td>
                    <td><c:if test="${log.transactionId > 0}">${log.transactionId}</c:if></td>
                    <td>${log.event}</td>
                    <td>${log.payload}</td>
                    <td>${log.direction}</td>
                </tr>
            </c:forEach>
            </tbody>
        </table>
        <c:if test="${empty logs}">
            <p class="no-logs-message">No logs found for the selected date.</p>
        </c:if>
    </div>
</div>

<script type="text/javascript">
    $(document).ready(function() {

        const logTableBody = $('#log-table-body');
        const filterInput = $('#filterInput');
        const logDateInput = $('#logDate');

        // --- Live Filter Functionality ---
        function filterTable() {
            const filterText = filterInput.val().toLowerCase();
            let visibleRows = 0;
            logTableBody.find('tr').each(function() {
                const rowText = $(this).text().toLowerCase();
                if (rowText.indexOf(filterText) === -1) {
                    $(this).hide();
                } else {
                    $(this).show();
                    visibleRows++;
                }
            });
            // You can add a message here if no rows are visible after filtering
        }

        // Trigger filtering on keyup
        filterInput.on('keyup', filterTable);

        // --- Auto-Refresh Functionality ---
        let lastFirstRowHtml = logTableBody.find('tr:first').html();

        function fetchLogs() {
            const selectedDate = logDateInput.val();
            // Do not refresh if a filter is active to prevent user disruption
            if (filterInput.val() !== '') {
                return;
            }

            $.ajax({
                url: '${ctxPath}/manager/websocket-logs',
                method: 'GET',
                data: { date: selectedDate, ajax: true }, // Added ajax param to potentially get only data in the future
                success: function (html) {
                    const newTableBody = $(html).find('#log-table-body');
                    const newFirstRowHtml = newTableBody.find('tr:first').html();

                    if (newFirstRowHtml && newFirstRowHtml !== lastFirstRowHtml) {
                        lastFirstRowHtml = newFirstRowHtml;
                        logTableBody.html(newTableBody.html());

                        // Add the highlight animation to the newest row
logTableBody.find('tr:first').addClass('new-row');

                        setTimeout(function() {
logTableBody.find('.new-row').removeClass('new-row');
                        }, 2000); // Duration of the animation
                    }
                },
                error: function() {
                    console.error("Failed to fetch logs.");
                }
            });
        }

        // Fetch logs every 5 seconds (more reasonable to reduce flicker and load)
        setInterval(fetchLogs, 5000);

        // Apply filter on initial page load if there's any pre-filled text
        if (filterInput.val()) {
            filterTable();
        }
    });
</script>

</body>
</html>

