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
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>

<html>
<head>
    <title>ChargePoint Status</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background: #f9f9f9;
        }
        .container {
            width: 90%;
            margin: 40px auto;
            padding: 20px;
            background: #fff;
            border-radius: 10px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        }
        h2 {
            color: #333;
            margin-bottom: 10px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 30px;
        }
        th, td {
            padding: 10px 14px;
            border: 1px solid #ccc;
        }
        th {
            background-color: #eee;
        }
        .no-data {
            color: #999;
            padding: 10px 0;
        }
    </style>
</head>
<body>
<div class="container">

    <h2>Online Charge Points (${onlineCount})</h2>
    <c:if test="${empty onlineChargePointList}">
        <div class="no-data">No online charge points found.</div>
    </c:if>
    <c:if test="${not empty onlineChargePointList}">
        <table>
            <thead>
                <tr>
                    <th>ChargeBox ID</th>
                    <th>OCPP Version</th>
                    <th>Connected Since</th>
                </tr>
            </thead>
            <tbody>
                <c:forEach var="cp" items="${onlineChargePointList}">
                    <tr>
                        <td><a href="${ctxPath}/manager/chargepoints/details/${cp.chargeBoxPk}">${cp.chargeBoxId}</a></td>
                        <td>${cp.version}</td>
                        <td>${cp.connectedSince}</td>
                    </tr>
                </c:forEach>
            </tbody>
        </table>
    </c:if>

    <h2>Offline Charge Points (${offlineCount})</h2>
    <c:if test="${empty offlineChargePointList}">
        <div class="no-data">No offline charge points found.</div>
    </c:if>
    <c:if test="${not empty offlineChargePointList}">
        <table>
            <thead>
                <tr>
                    <th>ChargeBox ID</th>
                    <th>OCPP Version</th>
                    <th>Connected Since</th>
                </tr>
            </thead>
            <tbody>
                <c:forEach var="cp" items="${offlineChargePointList}">
                    <tr>
                        <td><a href="${ctxPath}/manager/chargepoints/details/${cp.chargeBoxPk}">${cp.chargeBoxId}</a></td>
                        <td>${cp.version}</td>
                        <td>${cp.connectedSince}</td>
                    </tr>
                </c:forEach>
            </tbody>
        </table>
    </c:if>

</div>
</body>
</html>
