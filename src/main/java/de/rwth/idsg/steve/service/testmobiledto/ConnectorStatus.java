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
package de.rwth.idsg.steve.service.testmobiledto;

import lombok.Getter;

import java.util.Arrays;

public enum ConnectorStatus {

    AVAILABLE("Available", 1),
    PREPARING("Preparing", 2),
    CHARGING("Charging", 3),
    FINISHING("Finishing", 4);

    private final String ocppStatus;
    @Getter
    private final int code;

    ConnectorStatus(String ocppStatus, int code) {
        this.ocppStatus = ocppStatus;
        this.code = code;
    }

    public static int fromOcppStatus(String status) {
        return Arrays.stream(values())
                .filter(s -> s.ocppStatus.equalsIgnoreCase(status))
                .findFirst()
                .map(ConnectorStatus::getCode)
                .orElse(0);
    }
}

