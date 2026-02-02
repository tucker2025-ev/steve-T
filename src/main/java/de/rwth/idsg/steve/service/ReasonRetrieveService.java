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
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ocpp.cs._2015._10.Reason;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class ReasonRetrieveService {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final Map<Integer, String> customReasonMap = new HashMap<>();

    public static String retrieve(String msg) {


        try {
            JsonNode root = mapper.readTree(msg);
            int messageType = root.get(0).asInt();

            if (messageType == 2) {
                String action = root.get(2).asText();

                if ("StopTransaction".equals(action)) {
                    ObjectNode payload = (ObjectNode) root.get(3);

                    if (payload.has("reason") && payload.has("transactionId")) {
                        int txId = payload.get("transactionId").asInt();
                        String reason = payload.get("reason").asText();

                        if (!isStandardReason(reason)) {
                            customReasonMap.put(txId, reason);
                            payload.put("reason", "Other");

                        }
                        customReasonMap.put(txId, reason);
                    }
                }
            }

            return mapper.writeValueAsString(root);

        } catch (Exception e) {
            log.error(e.getMessage());
            return msg;
        }
    }

    private static boolean isStandardReason(String value) {
        for (Reason r : Reason.values()) {
            if (r.value().equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    public static String getCustomReason(int txId) {
        return customReasonMap.get(txId);
    }

    public static void removeCustomReason(int txId) {
        if (customReasonMap.containsKey(txId)) {
            customReasonMap.remove(txId);
        }

    }
}
