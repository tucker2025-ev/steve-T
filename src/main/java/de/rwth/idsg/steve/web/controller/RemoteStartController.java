/*
 * SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
 * Copyright (C) 2013-2026 SteVe Community Team
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package de.rwth.idsg.steve.web.controller;

import de.rwth.idsg.steve.web.dto.RemoteStartMessage;
import de.rwth.idsg.steve.web.dto.RemoteStartResponse;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static jooq.steve.db.Tables.CHARGER_SERVER;
import static jooq.steve.db.Tables.TRANSACTION_START;
import static jooq.steve.db.tables.Connector.CONNECTOR;

@RestController
@RequestMapping("/api/charger")
public class RemoteStartController {

    @Autowired
    private ChargerServerStausController controller;
    @Autowired
    private DSLContext dslContext;


    @GetMapping("/start")
    public RemoteStartResponse startCharging(
            @RequestParam String idtag,
            @RequestParam String conqr
    ) {

        String chargerBox = dslContext.select(CHARGER_SERVER.CHARGER_BOX_ID)
                .from(CHARGER_SERVER)
                .where(CHARGER_SERVER.CHARGER_QR_CODE.eq(conqr))
                .fetchOneInto(String.class);


        controller.sendRemoteStart(chargerBox, idtag, 1);
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Integer transactionId = getTransactionId(chargerBox, 1);

        if (transactionId != null) {
            RemoteStartMessage remoteStartMessage = new RemoteStartMessage();
            remoteStartMessage.setCharger_type("SOCKET15");
            remoteStartMessage.setCharger_id(chargerBox);
            remoteStartMessage.setCon_no("1");
            remoteStartMessage.setStatus_notification("Preparing");
            remoteStartMessage.setStatus("true");
            remoteStartMessage.setWalletbit(String.valueOf(transactionId));
            remoteStartMessage.setCharger_status("1");

            RemoteStartResponse response = new RemoteStartResponse();
            response.setStatus("true");
            response.setMessage(remoteStartMessage);

            return response;
        }

        return null;
    }

    private Integer getTransactionId(final String chargeBoxId, final Integer connectorId) {

        Integer connectorPk = dslContext.select(CONNECTOR.CONNECTOR_PK)
                .from(CONNECTOR)
                .where(CONNECTOR.CHARGE_BOX_ID.eq(chargeBoxId))
                .and(CONNECTOR.CONNECTOR_ID.eq(connectorId))
                .fetchOne(CONNECTOR.CONNECTOR_PK);

        return dslContext
                .select(TRANSACTION_START.TRANSACTION_PK)
                .from(TRANSACTION_START)
                .where(TRANSACTION_START.CONNECTOR_PK.eq(connectorPk))
                .orderBy(TRANSACTION_START.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Integer.class);
    }

}
