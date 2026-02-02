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
<%@ taglib prefix="form" uri="http://www.springframework.org/tags/form" %>

<form:form action="${ctxPath}/manager/operations/${opVersion}/RemoteStartTransaction"
           modelAttribute="params">

    <!-- ================= Charge Points ================= -->
    <section>
        <span>Charge Points with OCPP ${opVersion}</span>
    </section>

    <!-- ⚠️ DO NOT CHANGE THIS INCLUDE -->
    <%@ include file="../00-cp-single.jsp" %>

    <!-- ================= Parameters ================= -->
    <section>
        <span>Parameters</span>
    </section>

    <table class="userInput">
        <tr>
            <td>Connector ID:</td>
            <td>
                <form:select path="connectorId" disabled="true"/>
            </td>
        </tr>

        <tr>
            <td>OCPP ID Tag:</td>
            <td>
                <form:select path="idTag">
                    <form:options items="${idTagList}" />
                </form:select>
            </td>
        </tr>

        <tr>
            <td></td>
            <td>
                <div class="submit-button">
                    <input type="submit" value="Perform"/>
                </div>
            </td>
        </tr>
    </table>

</form:form>

<!-- ================= SEARCH UI (STEVE SAFE) ================= -->

<link rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/css/select2.min.css"/>

<script src="https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/js/select2.min.js"></script>

<script>
    $(document).ready(function () {

        /* 🔍 Search for ChargePoint (rendered by 00-cp-single.jsp) */
        $('select[name*="charge"]').select2({
            placeholder: "🔍 Search Charge Point...",
            width: 'resolve'
        });

        /* 🔍 Search for IdTag */
        $('select[name="idTag"]').select2({
            placeholder: "🔍 Search IdTag...",
            width: 'resolve'
        });

    });
</script>
