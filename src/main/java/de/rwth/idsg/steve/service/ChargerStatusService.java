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
package de.rwth.idsg.steve.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static jooq.steve.db.Tables.CONNECTOR;
import static jooq.steve.db2.Tables.CHARGER_CONNECTOR_STATUS_LOG;
import static jooq.steve.db2.Tables.CHARGER_STATUS;

@Slf4j
@Service
public class ChargerStatusService {

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondaryContext;

    @Autowired
    private DSLContext dslContext;

    @Autowired
    private RestTemplate restTemplate;
    private static final String HEADING = "Charger Alert";
    private static final String API_URL = "http://15.207.37.132/new/send_notification.php";

    public void sendNotification(String title, String message) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("idtag", "tucker");
            requestBody.put("title", title);
            requestBody.put("message", message);
            requestBody.put("payload", "");

            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(requestBody);
            HttpEntity<String> request = new HttpEntity<>(json, headers);

            ResponseEntity<String> response =
                    restTemplate.postForEntity(API_URL, request, String.class);

        } catch (Exception e) {
            log.error("SendNotification Error Occur : ");
        }
    }


    public void sendNotification(final String chargeBoxId, final Integer connectorId, final String status, final String errorCode, final String errorInfo, final String vendorErrorCode) {

        Set<String> normalStatuses = new HashSet<>(
                Arrays.asList("Available", "Preparing", "Charging", "Finishing", "Reserved", "Low Wallet")
        );

        Set<String> normalErrorCodes = new HashSet<>(
                Arrays.asList("NoError")
        );

        if (!normalStatuses.contains(status) || !normalErrorCodes.contains(errorCode)) {
            secondaryContext.update(CHARGER_STATUS)
                    .set(CHARGER_STATUS.CONNECTOR_LAST_STATUS, status)
                    .set(CHARGER_STATUS.IS_CONNECTOR_ERROR, true)
                    .set(CHARGER_STATUS.CONNECTOR_STATUS_TIMESTAMP, DateTime.now())
                    .where(CHARGER_STATUS.CHARGE_POINT.eq(chargeBoxId)
                            .and(CHARGER_STATUS.CONNECTOR_ID.eq(connectorId)))
                    .execute();


            String content = String.format(
                    "ChargeBox: %s | Connector: %d | Status: %s | Error: %s | Info: %s | Vendor Code: %s",
                    chargeBoxId, connectorId, status, errorCode, errorInfo, vendorErrorCode
            );
            sendNotification(HEADING, content);

            secondaryContext.insertInto(CHARGER_CONNECTOR_STATUS_LOG)
                    .set(CHARGER_CONNECTOR_STATUS_LOG.CHARGER_ID, chargeBoxId)
                    .set(CHARGER_CONNECTOR_STATUS_LOG.CONNECTOR_ID, connectorId)
                    .set(CHARGER_CONNECTOR_STATUS_LOG.STATUS, status)
                    .set(CHARGER_CONNECTOR_STATUS_LOG.ERROR_CODE, errorCode)
                    .set(CHARGER_CONNECTOR_STATUS_LOG.ERROR_INFO, errorInfo)
                    .set(CHARGER_CONNECTOR_STATUS_LOG.VENDOR_ERROR_CODE, vendorErrorCode)
                    .set(CHARGER_CONNECTOR_STATUS_LOG.ERROR_OCCUR_TIMESTAMP, DateTime.now())
                    .set(CHARGER_CONNECTOR_STATUS_LOG.IS_CONNECTOR_ERROR, true)
                    .execute();


        } else {
            Boolean isConnectorError = secondaryContext
                    .select(CHARGER_STATUS.IS_CONNECTOR_ERROR)
                    .from(CHARGER_STATUS)
                    .where(CHARGER_STATUS.CHARGE_POINT.eq(chargeBoxId))
                    .and(CHARGER_STATUS.CONNECTOR_ID.eq(connectorId))
                    .fetchOne(CHARGER_STATUS.IS_CONNECTOR_ERROR);
            boolean connectorError = Boolean.TRUE.equals(isConnectorError);

            if (connectorError && normalStatuses.contains(status)) {
                String content = String.format(
                        "ChargeBox: %s | Connector: %d | Status: %s",
                        chargeBoxId, connectorId, "Connector Error Resolved"
                );
                sendNotification(HEADING, content);

                secondaryContext.update(CHARGER_STATUS)
                        .set(CHARGER_STATUS.CONNECTOR_LAST_STATUS, status)
                        .set(CHARGER_STATUS.IS_CONNECTOR_ERROR, false)
                        .set(CHARGER_STATUS.CONNECTOR_STATUS_TIMESTAMP, DateTime.now())
                        .set(CHARGER_STATUS.CONNECTOR_ERROR_RESOLVED_TIMESTAMP, DateTime.now())
                        .where(CHARGER_STATUS.CHARGE_POINT.eq(chargeBoxId)
                                .and(CHARGER_STATUS.CONNECTOR_ID.eq(connectorId)))
                        .execute();

                secondaryContext.update(CHARGER_CONNECTOR_STATUS_LOG)
                        .set(CHARGER_CONNECTOR_STATUS_LOG.IS_CONNECTOR_ERROR, false)
                        .set(CHARGER_CONNECTOR_STATUS_LOG.ERROR_RESOLVED_TIMESTAMP, DateTime.now())
                        .where(CHARGER_CONNECTOR_STATUS_LOG.ERROR_OCCUR_TIMESTAMP.eq(
                                DSL.select(CHARGER_CONNECTOR_STATUS_LOG.ERROR_OCCUR_TIMESTAMP)
                                        .from(CHARGER_CONNECTOR_STATUS_LOG)
                                        .where(CHARGER_CONNECTOR_STATUS_LOG.CHARGER_ID.eq(chargeBoxId))
                                        .and(CHARGER_CONNECTOR_STATUS_LOG.CONNECTOR_ID.eq(connectorId))
                                        .orderBy(CHARGER_CONNECTOR_STATUS_LOG.ERROR_OCCUR_TIMESTAMP.desc())
                                        .limit(1)
                        ))
                        .execute();


            }

            if (!connectorError) {
                secondaryContext.update(CHARGER_STATUS)
                        .set(CHARGER_STATUS.CONNECTOR_LAST_STATUS, status)
                        .set(CHARGER_STATUS.IS_CONNECTOR_ERROR, false)
                        .set(CHARGER_STATUS.CONNECTOR_STATUS_TIMESTAMP, DateTime.now())
                        .where(CHARGER_STATUS.CHARGE_POINT.eq(chargeBoxId)
                                .and(CHARGER_STATUS.CONNECTOR_ID.eq(connectorId)))
                        .execute();
            }
        }
    }

    private void insertChargerStatus(final String chargeBoxId, final Integer connectorId, final boolean isOnline) {
        boolean exists = secondaryContext.fetchExists(
                secondaryContext.selectOne()
                        .from(CHARGER_STATUS)
                        .where(CHARGER_STATUS.CHARGE_POINT.eq(chargeBoxId))
                        .and(CHARGER_STATUS.CONNECTOR_ID.eq(connectorId))
        );

        if (exists) {
            if (isOnline) {
                String content = String.format(
                        "ChargeBox: %s | Connector: %d | Status: %s",
                        chargeBoxId, connectorId, "Is Online"
                );
                sendNotification(HEADING, content);
                secondaryContext.update(CHARGER_STATUS)
                        .set(CHARGER_STATUS.CHARGER_ONLINE_TIMESTAMP, DateTime.now())
                        .set(CHARGER_STATUS.IS_ONLINE, true)
                        .where(CHARGER_STATUS.CHARGE_POINT.eq(chargeBoxId)
                                .and(CHARGER_STATUS.CONNECTOR_ID.eq(connectorId)))
                        .execute();
            } else {
                String content = String.format(
                        "ChargeBox: %s | Connector: %d | Status: %s",
                        chargeBoxId, connectorId, "Is Offline"
                );
                sendNotification(HEADING, content);
                secondaryContext.update(CHARGER_STATUS)
                        .set(CHARGER_STATUS.CHARGER_OFFLINE_TIMESTAMP, DateTime.now())
                        .set(CHARGER_STATUS.IS_ONLINE, false)
                        .where(CHARGER_STATUS.CHARGE_POINT.eq(chargeBoxId)
                                .and(CHARGER_STATUS.CONNECTOR_ID.eq(connectorId)))
                        .execute();
            }

        } else {
            secondaryContext.insertInto(CHARGER_STATUS)
                    .set(CHARGER_STATUS.CHARGE_POINT, chargeBoxId)
                    .set(CHARGER_STATUS.CONNECTOR_ID, connectorId)
                    .set(CHARGER_STATUS.IS_ONLINE, isOnline)
                    .set(isOnline ? CHARGER_STATUS.CHARGER_ONLINE_TIMESTAMP : CHARGER_STATUS.CHARGER_OFFLINE_TIMESTAMP,
                            DateTime.now())
                    .execute();
        }

    }

    public void get(final String chargeBoxId, final boolean isOnline) {

        List<Integer> connectorIds = Optional.of(
                dslContext.select(CONNECTOR.CONNECTOR_ID)
                        .from(CONNECTOR)
                        .where(CONNECTOR.CHARGE_BOX_ID.eq(chargeBoxId))
                        .fetchInto(Integer.class)
        ).orElse(Collections.emptyList());


        for (Integer i : connectorIds) {
            insertChargerStatus(chargeBoxId, i, isOnline);
        }
    }


}
