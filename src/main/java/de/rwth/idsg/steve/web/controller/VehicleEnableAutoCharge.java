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

import de.rwth.idsg.steve.service.ExtractMac;
import de.rwth.idsg.steve.service.ManuallyStopTransaction;
import de.rwth.idsg.steve.service.ResponseDTO;
import de.rwth.idsg.steve.web.dto.AutoChargeResponse;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static jooq.steve.db.Tables.VEHICLE;
import static jooq.steve.db.Tables.TRANSACTION_START;
import static jooq.steve.db.Tables.CONNECTOR;

@RestController
@RequestMapping("/api")
public class VehicleEnableAutoCharge {

    @Autowired
    private DSLContext dslContext;

    @Autowired
    private ExtractMac extractMac;
    @Autowired
    private ManuallyStopTransaction stopTransaction;

    @GetMapping(value = "/isEnable")
    public ResponseDTO isEnableAutoCharge(@RequestParam(value = "txId", required = true) Integer transactionId,
                                          @RequestParam(value = "idtag", required = true) String idTag) {

        String vid = extractMac.transactionVidMap.get(transactionId);

        boolean exists = dslContext.fetchExists(
                dslContext.selectOne()
                        .from(VEHICLE)
                        .where(VEHICLE.ID_TAG.eq(idTag))
                        .and(VEHICLE.VID_NUMBER.eq(vid))

                        .and(VEHICLE.IS_ENABLE.eq(false)));

        ResponseDTO response = new ResponseDTO();
        response.setMessage(exists);

        if (exists) {
            AutoChargeResponse responseData = new AutoChargeResponse();
            responseData.setTransactionId(transactionId);
            responseData.setIdTag(idTag);
            responseData.setVid(vid);
            response.setData(responseData);
        } else {
            response.setData(null);
        }

        return response;
    }


    @GetMapping(value = "/isEnable/stop")
    public ResponseDTO isEnableAutoChargeNew(@RequestParam(value = "txId", required = true) Integer transactionId,
                                             @RequestParam(value = "idtag", required = true) String idTag) {

        String vid = extractMac.transactionVidMap.get(transactionId);

        boolean exists = dslContext.fetchExists(
                dslContext.selectOne()
                        .from(VEHICLE)
                        .where(VEHICLE.ID_TAG.eq(idTag))
                        .and(VEHICLE.VID_NUMBER.eq(vid))

                        .and(VEHICLE.IS_ENABLE.eq(false)));

        ResponseDTO response = new ResponseDTO();
        response.setMessage(exists);

        if (exists) {
            AutoChargeResponse responseData = new AutoChargeResponse();
            responseData.setTransactionId(transactionId);
            responseData.setIdTag(idTag);
            responseData.setVid(vid);
            response.setData(responseData);
        } else {
            response.setData(null);
        }
        stopTransaction.manuallyStopTransaction(retrieveChargeBoxId(transactionId),transactionId,"AutoChargeEnableStop");
        return response;
    }

    private String retrieveChargeBoxId(Integer txId) {

        return dslContext
                .select(CONNECTOR.CHARGE_BOX_ID)
                .from(TRANSACTION_START)
                .join(CONNECTOR)
                .on(TRANSACTION_START.CONNECTOR_PK.eq(CONNECTOR.CONNECTOR_PK))
                .where(TRANSACTION_START.TRANSACTION_PK.eq(txId))
                .fetchOptional(CONNECTOR.CHARGE_BOX_ID)
                .orElse(null);
    }


}
