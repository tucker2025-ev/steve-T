/*
 * SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
 * Copyright (C) 2013-2025 SteVe Community Team
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import de.rwth.idsg.steve.service.dto.WebSocketLog;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.socket.WebSocketSession;

import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static jooq.steve.db.Tables.WEBSOCKET_LOG;

@Slf4j
@Service
public class WebSocketLogParser {

    private static DSLContext dslContext;
    private static ExtractMac extractMac;
    private static final ObjectMapper mapper = new ObjectMapper();


    @Autowired
    public void setDslContext(DSLContext dslContext) {
        WebSocketLogParser.dslContext = dslContext;
    }

    @Autowired
    public void setExtractMac(ExtractMac extractMac) {
        WebSocketLogParser.extractMac = extractMac;
    }


    public static WebSocketLog parse(String directionType, String chargeBoxId, WebSocketSession session, String msg) {
        try {
            JsonNode root = mapper.readTree(msg);

            int messageType = root.get(0).asInt();
            if (messageType != 2) {
                return null;
            }

            String event = root.get(2).asText();
            JsonNode payloadNode = root.get(3);

            String transactionId = payloadNode.has("transactionId")
                    ? payloadNode.get("transactionId").asText()
                    : null;

            if ("Heartbeat".equals(event)) {
                return null;
            }
            if ("DataTransfer".equals(event)) {
                if (payloadNode.has("data") && payloadNode.get("data").isTextual()) {
                    String nestedData = payloadNode.get("data").asText();
                    try {
                        JsonNode nestedJson = mapper.readTree(nestedData);

                        String mac = nestedJson.has("mac") ? nestedJson.get("mac").asText() : null;
                        String txIdStr = nestedJson.has("transactionId") ? nestedJson.get("transactionId").asText() : null;

                        if (mac != null && txIdStr != null) {
                            try {
                                Integer txId = Integer.parseInt(txIdStr);
                                extractMac.getIdTagFromTransaction(txId, mac);
                            } catch (NumberFormatException e) {
                                log.error(e.getMessage());
                            }
                        }

                    } catch (Exception nestedEx) {
                        log.error(nestedEx.getMessage());
                    }
                }
            }

            WebSocketLog view = new WebSocketLog();
            String direction = directionType.equalsIgnoreCase("sending")
                    ? "Sent by Server to" : "Received from ";

            view.setTime(DateTime.now());
            view.setChargeBoxId(chargeBoxId);
            view.setSessionId(session.getId());
            view.setDirection(direction + " " + chargeBoxId);
            view.setEvent(event);
            view.setTransactionId(transactionId);
            view.setPayload(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payloadNode));

            addLogData(view);
            return view;

        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public static void addLogData(WebSocketLog view) {
        dslContext.insertInto(WEBSOCKET_LOG)
                .set(WEBSOCKET_LOG.TIME, view.getTime())
                .set(WEBSOCKET_LOG.CHARGE_BOX_ID, view.getChargeBoxId())
                .set(WEBSOCKET_LOG.SESSION_ID, view.getSessionId())
                .set(WEBSOCKET_LOG.TRANSACTION_ID, view.getTransactionId())
                .set(WEBSOCKET_LOG.EVENT, view.getEvent())
                .set(WEBSOCKET_LOG.PAYLOAD, view.getPayload())
                .set(WEBSOCKET_LOG.DIRECTION, view.getDirection())
                .execute();
    }


}
