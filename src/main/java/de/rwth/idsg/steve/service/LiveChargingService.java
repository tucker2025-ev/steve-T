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

import de.rwth.idsg.steve.service.dto.LiveChargingResponse;
import de.rwth.idsg.steve.service.dto.LiveChargingRuntimeData;
import jooq.steve.db2.tables.records.LiveChargingDataRecord;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static jooq.steve.db2.Tables.LIVE_CHARGING_DATA;

@Service
@Slf4j
public class LiveChargingService {

    @Autowired
    private DSLContext dsl;

    @Autowired
    @Qualifier("php")
    private DSLContext php;

    @Autowired
    private LiveChargingTariffCalculation tariffCalculation;
    @Autowired private LiveChargingRuntimeService runtimeService;

    private static final Table<?> CHARGE_POINT_VIEW = DSL.table("bigtot_cms.view_charger_station");

    private static final Field<String> CHARGER_ID = DSL.field("charger_id", String.class);
    private static final Field<String> STATION_CITY = DSL.field("con_no", String.class);

    public List<LiveChargingResponse> getLiveChargingResponses() {
        var records = dsl.selectFrom(LIVE_CHARGING_DATA)
                .where(LIVE_CHARGING_DATA.STOP_TIMESTAMP.isNull())
                .fetch();

        if (records.isEmpty()) {
            return List.of();
        }

        Map<Integer, List<LiveChargingDataRecord>> grouped =
                records.stream()
                        .filter(r -> r.getTransactionId() != null)
                        .collect(Collectors.groupingBy(LiveChargingDataRecord::getTransactionId));

        List<LiveChargingResponse> responseList = new ArrayList<>();

        for (List<LiveChargingDataRecord> rows : grouped.values()) {
            if (rows.isEmpty()) continue;

            LiveChargingDataRecord first = rows.get(0);

            Integer transactionId = first.getTransactionId();

            LiveChargingRuntimeData runtime = runtimeService.getRuntimeData(transactionId);

            DateTime startTime = rows.stream()
                    .map(LiveChargingDataRecord::getStartTimestamp)
                    .filter(Objects::nonNull)
                    .min(DateTime::compareTo)
                    .orElse(null);

            DateTime endTime = rows.stream()
                    .map(r -> r.getStopTimestamp() != null ? r.getStopTimestamp() : DateTime.now())
                    .max(DateTime::compareTo)
                    .orElse(DateTime.now());

            int totalSeconds =
                    startTime != null
                            ? Seconds.secondsBetween(startTime, endTime).getSeconds()
                            : 0;

            int hours = totalSeconds / 3600;
            int minutes = (totalSeconds % 3600) / 60;
            int seconds = totalSeconds % 60;

            String stationCity = String.valueOf(php
                    .select(STATION_CITY)
                    .from(CHARGE_POINT_VIEW)
                    .where(CHARGER_ID.eq(first.getChargeBoxId()))
                    .fetchOne(STATION_CITY));

            LiveChargingResponse.LiveChargingResponseBuilder builder =
                    LiveChargingResponse.builder()
                            .transactionId(first.getTransactionId())
                            .conId(first.getConnectorId())
                            .conNo(runtime != null ? runtime.getConnectorNo() : null)
                            .conType(runtime != null ? runtime.getChargerType() : "UNKNOWN")
                            .idtag(first.getIdTag())
                            .rank(runtime != null ? runtime.getUserRank() : "NORMAL")
                            .transactionCount(runtime != null ? runtime.getTransactionCount() : 0)
                            .name(first.getUserName())
                            .mobile(first.getUserMobileNo())
                            .email(first.getUserEmail())
                            .stationMobile(first.getStationMobile())
                            .stopReason(first.getStopReason() != null ? first.getStopReason() : "")
                            .walletAmount(
                                    first.getUserWalletAmount() != null
                                            ? first.getUserWalletAmount().toString()
                                            : "0.00"
                            )
                            .vehicle(first.getChargingVehicleNumber())
                            .vname(first.getChargingVehicleName())
                            .vmodel(first.getChargingVehicleModel())
                            .stationName(first.getStationName())
                            .stationCity(stationCity != null ? stationCity : "N/A")
                            .stationState(first.getStationState())
                            .conQrCode(first.getChargerQrCode())
                            .chargerId(first.getChargeBoxId())
                            .startSoc(first.getStartSoc())
                            .stopSoc(first.getEndSoc())
                            .voltage(first.getEndVoltage())
                            .current(first.getEndCurrent())
                            .power(first.getEndPower())
                            .startTime(startTime != null ? startTime.toString() : null)
                            .totalTime(String.valueOf(totalSeconds));

            Map<String, Object> liveBilling = tariffCalculation.getLiveBillingForTransaction(first.getTransactionId());

            if (liveBilling.isEmpty()) {
                builder
                        .fareBreakdown(List.of())
                        .unitsConsumed("0.0000")
                        .unitCost("0.00")
                        .baseFare("0.00")
                        .gstAmount("0.00")
                        .razorpayAmount("0.00")
                        .totalCost("0.00")
                        .unitfare(String.valueOf(0.0))
                        .timeConsumed("0Hr:0Min:0Sec");
            } else {
                builder
                        .fareBreakdown(extractFareBreakdown(liveBilling.get("fare_breakdown")))
                        .unitsConsumed((String) liveBilling.getOrDefault("units_consumed", "0.0000"))
                        .unitCost((String) liveBilling.getOrDefault("unit_cost", "0.00"))
                        .baseFare((String) liveBilling.getOrDefault("base_fare", "0.00"))
                        .gstAmount((String) liveBilling.getOrDefault("gst_amount", "0.00"))
                        .razorpayAmount((String) liveBilling.getOrDefault("razorpay_amount", "0.00"))
                        .totalCost((String) liveBilling.getOrDefault("total_cost", "0.00"))
                        .unitfare(String.valueOf(liveBilling.getOrDefault("unitfare", "0.0")))
                        .timeConsumed(hours + "Hr:" + minutes + "Min:" + seconds + "Sec");
            }

            responseList.add(builder.build());
        }

        return responseList;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractFareBreakdown(Object obj) {
        if (!(obj instanceof List<?> list)) return List.of();

        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> map) {
                result.add((Map<String, Object>) map);
            }
        }
        return result;
    }
}
