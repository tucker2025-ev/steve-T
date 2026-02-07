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

import de.rwth.idsg.steve.service.dto.FareBreakdownDTO;
import de.rwth.idsg.steve.service.dto.LiveTransactionDTO;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.jooq.Record;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class LiveTransactionService {

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;

    @Autowired
    @Qualifier("php")
    private DSLContext php;

    @Autowired private DSLContext dsl;

    public List<LiveTransactionDTO> getLiveTransactions() {
        List<LiveTransactionDTO> list = new ArrayList<>();

        Result<Record> txRecords = secondary.select()
                .from("ev_history.live_transaction_details")
                .where(DSL.field("is_active_transaction", Boolean.class).isTrue())
                .orderBy(DSL.field("transaction_id"), DSL.field("start_timestamp"))
                .fetch();

        Map<Integer, List<Record>> grouped = txRecords.stream()
                .collect(Collectors.groupingBy(r -> r.get("transaction_id", Integer.class),
                        LinkedHashMap::new, Collectors.toList()));

        for (Map.Entry<Integer, List<Record>> entry : grouped.entrySet()) {
            Integer transactionId = entry.getKey();
            List<Record> intervals = entry.getValue();
            if (intervals.isEmpty()) continue;

            Record first = intervals.get(0);
            String idTag = first.get("id_tag", String.class);
            String chargerId = first.get("charge_box_id", String.class);

            // User & Charger info
            Record user = fetchUser(idTag);
            Record charger = fetchCharger(chargerId);


            // Charging metrics
            String startSoc = safe(first, "end_soc", "N/A");
            String stopSoc = safe(intervals.get(intervals.size() - 1), "end_soc", "N/A");
            double voltage = getDouble(intervals.get(intervals.size() - 1), "last_voltage");
            double current = getDouble(intervals.get(intervals.size() - 1), "last_current");
            double power = getDouble(intervals.get(intervals.size() - 1), "last_power");

            // ------------------ Fare Breakdown ------------------
            List<FareBreakdownDTO> fareBreakdown = new ArrayList<>();
            double totalUnits = 0.0;
            double totalCost = 0.0;

            double currentUnits = getDouble(intervals.get(0), "consumed_energy");
            double currentFare = getDouble(intervals.get(0), "tariff_amount");
            Timestamp intervalStartTs = intervals.get(0).get("start_timestamp", Timestamp.class);
            Timestamp intervalEndTs = intervals.get(0).get("stop_timestamp", Timestamp.class);

            for (int i = 1; i < intervals.size(); i++) {
                Record interval = intervals.get(i);
                double tariff = getDouble(interval, "tariff_amount");
                double units = getDouble(interval, "consumed_energy");
                Timestamp startTs = interval.get("start_timestamp", Timestamp.class);
                Timestamp stopTs = interval.get("stop_timestamp", Timestamp.class);

                if (Double.compare(tariff, currentFare) == 0) {
                    currentUnits += units;
                    intervalEndTs = stopTs;
                } else {
                    fareBreakdown.add(new FareBreakdownDTO(
                            currentFare,
                            currentUnits,
                            roundDouble(currentUnits * currentFare, 2),
                            new DateTime(intervalStartTs.getTime(), DateTimeZone.forID("Asia/Kolkata"))
                                    .toString("yyyy-MM-dd HH:mm:ss"),
                            new DateTime(intervalEndTs.getTime(), DateTimeZone.forID("Asia/Kolkata"))
                                    .toString("yyyy-MM-dd HH:mm:ss")
                    ));
                    totalUnits += currentUnits;
                    totalCost += currentUnits * currentFare;

                    currentFare = tariff;
                    currentUnits = units;
                    intervalStartTs = startTs;
                    intervalEndTs = stopTs;
                }
            }

            // Last interval
            fareBreakdown.add(new FareBreakdownDTO(
                    currentFare,
                    currentUnits,
                    roundDouble(currentUnits * currentFare, 2),
                    new DateTime(intervalStartTs.getTime(), DateTimeZone.forID("Asia/Kolkata"))
                            .toString("yyyy-MM-dd HH:mm:ss"),
                    new DateTime(intervalEndTs.getTime(), DateTimeZone.forID("Asia/Kolkata"))
                            .toString("yyyy-MM-dd HH:mm:ss")
            ));
            totalUnits += currentUnits;
            totalCost += roundDouble(currentUnits * currentFare, 2);

            double gstAmount = 0.18;
            double totalCostWithGst = totalCost * gstAmount;

            double totalCostUpdate = totalCost + totalCostWithGst;

            // ------------------ Total Time ------------------
            Timestamp startTs = first.get("start_timestamp", Timestamp.class);
            Timestamp stopTs = intervals.get(intervals.size() - 1).get("stop_timestamp", Timestamp.class);

            long totalSeconds = startTs != null && stopTs != null
                    ? (stopTs.getTime() - startTs.getTime()) / 1000
                    : 0;

            String timeConsumed = formatTime(totalSeconds);

            // ------------------ Unit Fare String ------------------
            String unitFareStr = fareBreakdown.stream()
                    .map(f -> String.valueOf((int) f.getUnitFare()))
                    .collect(Collectors.joining(", "));

            int transactionCount = fetchTransactionCount(idTag);
            String rank = calculateRank(transactionCount);

            // ------------------ Build DTO ------------------
            LiveTransactionDTO dto = new LiveTransactionDTO();
            dto.setTransactionId(String.valueOf(transactionId));
            dto.setConId(charger != null ? safe(charger, "con_id", "N/A") : "N/A");
            dto.setIdtag(idTag);
            dto.setStopReason("");
            dto.setTransactionCount(transactionCount);
            dto.setRank(rank);
            dto.setName(getSafeString(user, "name", ""));
            dto.setMobile(getSafeString(user, "mobile", ""));
            dto.setEmail(getSafeString(user, "email", ""));
            dto.setWalletAmount(getSafeString(user, "wallet_amount", "0.0"));
            dto.setVehicle(getSafeString(user, "vehicle", "N/A"));
            dto.setVname(getSafeString(user, "vehicle_name", "N/A"));
            dto.setVmodel(getSafeString(user, "vehicle_model", "N/A"));
            dto.setStationMobile(getSafeString(charger, "station_mobile", ""));
            dto.setStationName(getSafeString(charger, "station_name", ""));
            dto.setStationCity(getSafeString(charger, "station_city", ""));
            dto.setStationState(getSafeString(charger, "station_state", ""));
            dto.setConType(getSafeString(charger, "charger_type", ""));
            dto.setConQrCode(getSafeString(charger, "charger_qr_code", ""));
            dto.setChargerId(getSafeString(charger, "charger_id", ""));
            dto.setConNo(String.valueOf(charger != null ? charger.get("con_no") : "N/A"));

            dto.setStartSoc(startSoc);
            dto.setStopSoc(stopSoc);
            dto.setVoltage(voltage);
            dto.setCurrent(current);
            dto.setPower(power);
            dto.setFareBreakdown(fareBreakdown);
            dto.setUnitsConsumed(String.format("%.3f", totalUnits));
            dto.setUnitCost(String.valueOf(roundDouble(totalCost, 2)));
            dto.setBaseFare("0.0");
            dto.setGstAmount(String.valueOf(roundDouble(totalCostWithGst, 2)));
            dto.setRazorpayAmount("0.0");
            dto.setTotalCost(String.valueOf(roundDouble(totalCostUpdate, 2)));
            if (startTs != null) {
                DateTime istStart = new DateTime(startTs.getTime(), DateTimeZone.forID("Asia/Kolkata"));
                dto.setStartTime(istStart.toString("yyyy-MM-dd HH:mm:ss"));
            } else {
                dto.setStartTime("");
            }
            dto.setTotalTime(totalSeconds);
            dto.setTimeConsumed(timeConsumed);
            dto.setUnitFare(unitFareStr);

            list.add(dto);
        }
        return list;
    }

    private Record fetchUser(String idTag) {
        return php.select()
                .from("bigtot_cms.view_user_vehicle_show")
                .where(DSL.field("idtag").eq(idTag))
                .fetchOne();
    }

    private Record fetchCharger(String chargerId) {
        return php.select()
                .from("bigtot_cms.view_charger_station")
                .where(DSL.field("charger_id").eq(chargerId))
                .fetchOne();
    }

    private String safe(Record r, String col, String defaultVal) {
        if (r == null) return defaultVal;
        Object v = r.get(col);
        return v != null ? v.toString() : defaultVal;
    }

    private String getSafeString(Record r, String col, String defaultVal) {
        return safe(r, col, defaultVal);
    }

    private Integer getSafeInteger(Record r, String col) {
        if (r == null) return null;
        Object v = r.get(col);
        return v instanceof Number ? ((Number) v).intValue() : null;
    }

    private double getDouble(Record r, String col) {
        Double value = r.get(col, Double.class);
        return value != null ? value : 0.0;
    }

    private double roundDouble(double value, int precision) {
        return new BigDecimal(value).setScale(precision, RoundingMode.HALF_UP).doubleValue();
    }

    private int fetchTransactionCount(String idTag) {

        Integer count = dsl.selectCount()
                .from("stevedb.transaction")
                .where(DSL.field("id_tag").eq(idTag))
                .fetchOne(0, Integer.class);

        return count != null ? count : 0;
    }

    private String calculateRank(int count) {
        if (count >= 100) return "Platinum";
        if (count >= 50)  return "Gold";
        if (count >= 10)  return "Silver";
        return "Normal";
    }

    private String formatTime(long totalSeconds) {
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        return String.format("%02dHr:%02dMin:%02dSec", hours, minutes, seconds);
    }
}




