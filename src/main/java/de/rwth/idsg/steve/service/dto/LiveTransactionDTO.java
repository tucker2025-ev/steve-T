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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class LiveTransactionDTO {

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("con_id")
    private String conId;

    @JsonProperty("idtag")
    private String idtag;

    @JsonProperty("stop_reason")
    private String stopReason;

    @JsonProperty("rank")
    private String rank;

    @JsonProperty("transaction count")
    private Integer transactionCount;

    @JsonProperty("name")
    private String name;

    @JsonProperty("mobile")
    private String mobile;

    @JsonProperty("email")
    private String email;

    @JsonProperty("wallet_amount")
    private String walletAmount;

    // VEHICLE
    @JsonProperty("vehicle")
    private String vehicle;

    @JsonProperty("vname")
    private String vname;

    @JsonProperty("vmodel")
    private String vmodel;

    // STATION
    @JsonProperty("station_mobile")
    private String stationMobile;

    @JsonProperty("station_name")
    private String stationName;

    @JsonProperty("station_city")
    private String stationCity;

    @JsonProperty("station_state")
    private String stationState;

    // CONNECTOR
    @JsonProperty("con_type")
    private String conType;

    @JsonProperty("con_qr_code")
    private String conQrCode;

    @JsonProperty("charger_id")
    private String chargerId;

    @JsonProperty("con_no")
    private String conNo;

    // SOC
    @JsonProperty("start_soc")
    private String startSoc;

    @JsonProperty("stop_soc")
    private String stopSoc;

    // LIVE ELECTRICAL
    @JsonProperty("voltage")
    private double voltage;

    @JsonProperty("current")
    private double current;

    @JsonProperty("power")
    private double power;

    // BILLING
    @JsonProperty("fare_breakdown")
    private List<FareBreakdownDTO> fareBreakdown;

    @JsonProperty("units_consumed")
    private String unitsConsumed;

    @JsonProperty("unit_cost")
    private String unitCost;

    @JsonProperty("base_fare")
    private String baseFare;

    @JsonProperty("gst_amount")
    private String gstAmount;

    @JsonProperty("razorpay_amount")
    private String razorpayAmount;

    @JsonProperty("total_cost")
    private String totalCost;

    // TIME
    @JsonProperty("start_time")
    private String startTime;

    @JsonProperty("total_time")
    private double totalTime;

    @JsonProperty("time_consumed")
    private String timeConsumed;

    @JsonProperty("unitfare")
    private String unitFare;
}

