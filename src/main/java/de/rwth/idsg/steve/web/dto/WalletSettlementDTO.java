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
package de.rwth.idsg.steve.web.dto;

import lombok.Data;

@Data
public class WalletSettlementDTO {

    public Integer id;
    public Integer transactionId;
    public String stationId;
    public String cpoId;
    public String stationName;
    public String stationCity;
    public String stationState;
    public String idTag;
    public String chargerId;
    public String chargerQrCode;
    public Integer conNo;
    public Double startEnergy;
    public Double tariffAmount;
    public Double gstWithTariffAmount;
    public Double lastEnergy;
    public Double walletAmount;
    public Double consumedEnergy;
    public Double consumedAmount;
    public Double totalConsumedAmount;
    public String startTimestamp;
    public String stopTimestamp;
    public Boolean isActiveTransaction;
    public Double dealerUnitCost;
    public Double dealerTotalAmount;
    public Double customerShareAmount;
    public Double totalShareAmount;

}
