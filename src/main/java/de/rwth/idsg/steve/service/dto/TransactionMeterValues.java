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
package de.rwth.idsg.steve.service.dto;

import lombok.Data;

@Data
public class TransactionMeterValues {

    private String ocppTagPk;
    private String chargeBoxId;
    private int transactionPk;
    private int connectorPk;
    private double voltage;
    private double power;
    private double energy;
    private double soc;
    private double current;

    @Override
    public String toString() {
        return "TransactionMeterValues{" +
                "ocppTagPk='" + ocppTagPk + '\'' +
                ", chargeBoxId='" + chargeBoxId + '\'' +
                ", transactionPk=" + transactionPk +
                ", connectorPk=" + connectorPk +
                ", voltage=" + voltage +
                ", power=" + power +
                ", energy=" + energy +
                ", soc=" + soc +
                ", current=" + current +
                '}';
    }
}
