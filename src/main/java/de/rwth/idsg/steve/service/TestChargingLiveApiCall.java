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

import de.rwth.idsg.steve.service.dto.LiveChargingRuntimeData;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TestChargingLiveApiCall {

    private final Map<Integer, LiveChargingRuntimeData> runtimeCache = new ConcurrentHashMap<>();

    

    public void putRuntimeData(Integer transactionId, LiveChargingRuntimeData data) {
        runtimeCache.put(transactionId, data);
    }

    public LiveChargingRuntimeData getRuntimeData(Integer transactionId) {
        return runtimeCache.get(transactionId);
    }

    public String mapChargerType(String type) {
        if (type == null) return "UNKNOWN";
        return switch (type) {
            case "1" -> "AC";
            case "2" -> "DC";
            case "3" -> "FAST_DC";
            default -> "UNKNOWN";
        };
    }

    public String resolveUserRank(int totalTransactions) {
        if (totalTransactions >= 100) return "Platinum";
        if (totalTransactions >= 50) return "Gold";
        if (totalTransactions >= 10) return "Silver";
        return "Normal";
    }
}
