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

        // ------------------ FETCH INTERVAL + MASTER DATA ------------------
        Result<?> records = secondary.select(
                        DSL.field("w.transaction_id").as("transaction_id"),
                        DSL.field("w.start_timestamp").as("interval_start"),
                        DSL.field("w.stop_timestamp").as("interval_stop"),
                        DSL.field("w.consumed_energy").as("consumed_energy"),
                        DSL.field("w.tariff_amount").as("tariff_amount"),

                        DSL.field("f.id_tag").as("id_tag"),
                        DSL.field("f.latest_soc").as("latest_soc"),
                        DSL.field("f.last_voltage").as("last_voltage"),
                        DSL.field("f.last_current").as("last_current"),
                        DSL.field("f.last_power").as("last_power"),
                        DSL.field("f.name").as("name"),
                        DSL.field("f.mobile").as("mobile"),
                        DSL.field("f.email").as("email"),
                        DSL.field("f.user_wallet_amount").as("wallet_amount"),
                        DSL.field("f.station_name").as("station_name"),
                        DSL.field("f.station_city").as("station_city"),
                        DSL.field("f.station_state").as("station_state"),
                        DSL.field("f.station_mobile").as("station_mobile"),
                        DSL.field("f.charger_type").as("charger_type"),
                        DSL.field("f.charger_qr_code").as("charger_qr_code"),
                        DSL.field("f.charge_box_id").as("charger_id"),
                        DSL.field("f.con_no").as("con_no"),
                        DSL.field("f.con_id").as("con_id"),
                        DSL.field("f.vehicle").as("vehicle"),
                        DSL.field("f.vehicle_name").as("vehicle_name"),
                        DSL.field("f.vehicle_model").as("vehicle_model")
                )
                .from("ev_history.wallet_track w")
                .leftJoin("ev_history.live_full_details f")
                .on(DSL.field("w.transaction_id").eq(DSL.field("f.transaction_id")))
                .where(DSL.field("w.is_active_transaction", Integer.class).eq(1))
                .orderBy(DSL.field("w.transaction_id"), DSL.field("w.start_timestamp"))
                .fetch();

        // ------------------ GROUP BY TRANSACTION ------------------
        Map<Integer, List<Record>> grouped = records.stream()
                .collect(Collectors.groupingBy(
                        r -> r.get("transaction_id", Integer.class),
                        LinkedHashMap::new,
                        Collectors.toList()
                ));

        // ------------------ LOOP PER TRANSACTION ------------------
        for (Map.Entry<Integer, List<Record>> entry : grouped.entrySet()) {

            Integer transactionId = entry.getKey();
            List<Record> intervals = entry.getValue();
            if (intervals.isEmpty()) continue;

            Record first = intervals.get(0);
            Record last  = intervals.get(intervals.size() - 1);

            String idTag = first.get("id_tag", String.class);

            // ---------------- TRANSACTION COUNT + RANK ----------------
            int transactionCount = fetchTransactionCount(idTag);
            String rank = calculateRank(transactionCount);

            // ---------------- METRICS ----------------
            String startSoc = safe(first, "latest_soc", "0");
            String stopSoc  = safe(last, "latest_soc", "0");
            double voltage  = getDouble(last, "last_voltage");
            double current  = getDouble(last, "last_current");
            double power    = getDouble(last, "last_power");

            // =====================================================
            //                  FARE BREAKDOWN
            // =====================================================
            List<FareBreakdownDTO> fareBreakdown = new ArrayList<>();
            double totalUnits = 0.0;
            double totalCost  = 0.0;

            double currentUnits = getDouble(intervals.get(0), "consumed_energy");
            double currentFare  = getDouble(intervals.get(0), "tariff_amount");
            Timestamp intervalStartTs = intervals.get(0).get("interval_start", Timestamp.class);
            Timestamp intervalEndTs   = intervals.get(0).get("interval_stop", Timestamp.class);

            for (int i = 1; i < intervals.size(); i++) {

                Record in = intervals.get(i);
                double tariff = getDouble(in, "tariff_amount");
                double units  = getDouble(in, "consumed_energy");

                Timestamp startTs = in.get("interval_start", Timestamp.class);
                Timestamp stopTs  = in.get("interval_stop", Timestamp.class);

                if (Double.compare(tariff, currentFare) == 0) {
                    currentUnits += units;
                    intervalEndTs = stopTs;
                } else {

                    double cost = roundDouble(currentUnits * currentFare, 2);

                    fareBreakdown.add(new FareBreakdownDTO(
                            currentFare,
                            currentUnits,
                            cost,
                            formatTs(intervalStartTs),
                            formatTs(intervalEndTs)
                    ));

                    totalUnits += currentUnits;
                    totalCost  += cost;

                    currentFare = tariff;
                    currentUnits = units;
                    intervalStartTs = startTs;
                    intervalEndTs   = stopTs;
                }
            }

            // -------- LAST INTERVAL --------
            double lastCost = roundDouble(currentUnits * currentFare, 2);

            fareBreakdown.add(new FareBreakdownDTO(
                    currentFare,
                    currentUnits,
                    lastCost,
                    formatTs(intervalStartTs),
                    formatTs(intervalEndTs)
            ));

            totalUnits += currentUnits;
            totalCost  += lastCost;

            // ---------------- GST ----------------
            double gst = roundDouble(totalCost * 0.18, 2);
            double finalCost = totalCost + gst;

            // ---------------- TIME ----------------
            Timestamp startTs = first.get("interval_start", Timestamp.class);
            Timestamp stopTs  = last.get("interval_stop", Timestamp.class);

            long totalSeconds = (startTs != null && stopTs != null)
                    ? (stopTs.getTime() - startTs.getTime()) / 1000
                    : 0;

            String unitFareStr = fareBreakdown.stream()
                    .map(f -> String.valueOf((int) f.getUnitFare()))
                    .collect(Collectors.joining(", "));

            // ---------------- DTO BUILD ----------------
            LiveTransactionDTO dto = new LiveTransactionDTO();

            dto.setTransactionId(String.valueOf(transactionId));
            dto.setIdtag(idTag);
            dto.setTransactionCount(transactionCount);
            dto.setRank(rank);
            dto.setStopReason("");

            dto.setName(safe(first, "name", ""));
            dto.setMobile(safe(first, "mobile", ""));
            dto.setEmail(safe(first, "email", ""));
            dto.setWalletAmount(safe(first, "wallet_amount", "0.0"));

            dto.setStationName(safe(first, "station_name", ""));
            dto.setStationCity(safe(first, "station_city", ""));
            dto.setStationState(safe(first, "station_state", ""));
            dto.setStationMobile(safe(first, "station_mobile", ""));
            dto.setConType(safe(first, "charger_type", ""));
            dto.setConQrCode(safe(first, "charger_qr_code", ""));
            dto.setChargerId(safe(first, "charger_id", ""));
            dto.setConNo(safe(first, "con_no", "N/A"));
            dto.setConId(safe(first, "con_id", "N/A"));
            dto.setVehicle(safe(first, "vehicle", "N/A"));
            dto.setVname(safe(first, "vehicle_name", "N/A"));
            dto.setVmodel(safe(first, "vehicle_model", "N/A"));

            dto.setStartSoc(startSoc);
            dto.setStopSoc(stopSoc);
            dto.setVoltage(voltage);
            dto.setCurrent(current);
            dto.setPower(power);
            dto.setFareBreakdown(fareBreakdown);
            dto.setUnitsConsumed(String.format("%.3f", totalUnits));
            dto.setUnitCost(String.valueOf(roundDouble(totalCost, 2)));
            dto.setGstAmount(String.valueOf(roundDouble(gst, 2)));
            dto.setTotalCost(String.valueOf(roundDouble(finalCost, 2)));
            dto.setBaseFare("0.00");
            dto.setRazorpayAmount("0.00");
            dto.setStartTime(startTs != null ? new DateTime(startTs.getTime(), DateTimeZone.forID("Asia/Kolkata"))
                            .toString("yyyy-MM-dd HH:mm:ss") : "");
            dto.setTimeConsumed(formatTime(totalSeconds));
            dto.setTotalTime(totalSeconds);
            dto.setUnitFare(unitFareStr);

            list.add(dto);
        }

        return list;
    }

    private String safe(Record r, String col, String defaultVal) {
        if (r == null) return defaultVal;
        Object v = r.get(col);
        return v != null ? v.toString() : defaultVal;
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

    private String formatTs(Timestamp ts) {
        if (ts == null) return "";
        return new DateTime(ts.getTime(), DateTimeZone.forID("Asia/Kolkata"))
                .toString("yyyy-MM-dd HH:mm:ss");
    }

    private String formatTime(long totalSeconds) {
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        return String.format("%02dHr:%02dMin:%02dSec", hours, minutes, seconds);
    }
}




