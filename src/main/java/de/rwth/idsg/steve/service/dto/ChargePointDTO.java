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
package de.rwth.idsg.steve.service.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class ChargePointDTO implements Serializable {
    private String chargerId;
    private String chargerQrCode;
    private String stationId;
    private String stationName;
    private String stationMobile;
    private String cpoId;
    private String stationAddressOne;
    private String stationAddressTwo;
    private String stationPincode;
    private String stationCountry;
    private String stationState;
    private String stationLongitude;
    private String stationLatitude;
    private String stationInvoice;
    private String chargerType;
    private Integer connectorNo;
    private String connectorId;

    @Override
    public String toString() {
        return "ChargePointDTO{" +
                "chargerId='" + chargerId + '\'' +
                ", chargerQrCode='" + chargerQrCode + '\'' +
                ", stationId='" + stationId + '\'' +
                ", stationName='" + stationName + '\'' +
                ", stationMobile='" + stationMobile + '\'' +
                ", cpoId='" + cpoId + '\'' +
                ", stationAddressOne='" + stationAddressOne + '\'' +
                ", stationAddressTwo='" + stationAddressTwo + '\'' +
                ", stationPincode='" + stationPincode + '\'' +
                ", stationCountry='" + stationCountry + '\'' +
                ", stationState='" + stationState + '\'' +
                ", stationLongitude='" + stationLongitude + '\'' +
                ", stationLatitude='" + stationLatitude + '\'' +
                ", stationInvoice='" + stationInvoice + '\'' +
                ", chargerType='" + chargerType + '\'' +
                ", connectorNo=" + connectorNo +
                ", connectorId='" + connectorId + '\'' +
                '}';
    }
}