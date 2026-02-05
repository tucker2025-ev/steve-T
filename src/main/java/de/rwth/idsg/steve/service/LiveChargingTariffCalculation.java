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

import de.rwth.idsg.steve.service.dto.Tariff;
import de.rwth.idsg.steve.service.dto.TariffResponse;
import de.rwth.idsg.steve.service.dto.TariffSlice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.joda.time.Seconds;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static jooq.steve.db.Tables.CONNECTOR_METER_VALUE;

@Service
public class LiveChargingTariffCalculation {

    @Autowired private DSLContext ctx;

    @Autowired private TariffAmountCalculation tariffAmountCalculation;

    private static final DateTimeZone IST = DateTimeZone.forID("Asia/Kolkata");
    private final Map<Integer, Map<String, Object>> liveBillingCache = new ConcurrentHashMap<>();

    /* ============================================================
       TARIFF SPLIT LOGIC
       ============================================================ */
    public List<TariffSlice> splitByTariff(String chargeBoxId, DateTime fromUtc, DateTime toUtc, Double totalEnergy) {
        List<TariffSlice> slices = new ArrayList<>();
        long totalSeconds = Seconds.secondsBetween(fromUtc, toUtc).getSeconds();
        if (totalSeconds <= 0 || totalEnergy <= 0) return slices;

        DateTime cursorUtc = fromUtc;

        Map<String, Double> tariffEnergyMap = new LinkedHashMap<>();
        Map<String, DateTime> tariffStartMap = new HashMap<>();
        Map<String, DateTime> tariffEndMap = new HashMap<>();

        while (cursorUtc.isBefore(toUtc)) {

            Tariff tariff = getTariffForTime(chargeBoxId, cursorUtc);

            DateTime cursorIst = cursorUtc.withZone(IST);
            LocalTime tariffStart = LocalTime.parse(tariff.getStart_time());
            LocalTime tariffEnd = LocalTime.parse(tariff.getEnd_time());

            DateTime tariffEndIst;
            if (tariffEnd.isAfter(tariffStart)) {
                tariffEndIst = cursorIst.withTime(tariffEnd);
            } else {
                tariffEndIst = cursorIst.withTime(tariffEnd);
                if (!tariffEndIst.isAfter(cursorIst)) tariffEndIst = tariffEndIst.plusDays(1);
            }

            DateTime sliceEndUtc = tariffEndIst.withZone(DateTimeZone.UTC);
            if (sliceEndUtc.isAfter(toUtc)) sliceEndUtc = toUtc;

            long sliceSeconds = Seconds.secondsBetween(cursorUtc, sliceEndUtc).getSeconds();
            if (sliceSeconds <= 0) break;

            // proportional energy for this slice
            double sliceEnergy = totalEnergy * sliceSeconds / totalSeconds;

            String tariffKey = tariff.getStart_time() + "-" + tariff.getEnd_time();

            // track start/end
            tariffStartMap.putIfAbsent(tariffKey, cursorUtc);
            tariffEndMap.put(tariffKey, sliceEndUtc);

            // accumulate energy per tariff window
            tariffEnergyMap.put(tariffKey, tariffEnergyMap.getOrDefault(tariffKey, 0.0) + sliceEnergy);

            cursorUtc = sliceEndUtc;
        }

        // Adjust for rounding errors per tariff
        for (Map.Entry<String, Double> entry : tariffEnergyMap.entrySet()) {
            String tariffKey = entry.getKey();
            Tariff tariff = getTariffForTime(chargeBoxId, tariffStartMap.get(tariffKey));
            double energy = entry.getValue();

            // round to 3 decimals
            energy = Math.round(energy * 1000.0) / 1000.0;

            slices.add(new TariffSlice(
                    tariff,
                    energy,
                    tariffStartMap.get(tariffKey),
                    tariffEndMap.get(tariffKey)
            ));
        }

        // Ensure total sum of slices = totalEnergy
        double sumSlices = slices.stream().mapToDouble(s -> s.energy).sum();
        double diff = Math.round((totalEnergy - sumSlices) * 1000.0) / 1000.0;
        if (!slices.isEmpty() && Math.abs(diff) > 0.0001) {
            TariffSlice lastSlice = slices.get(slices.size() - 1);
            lastSlice.energy += diff;
            lastSlice.energy = Math.round(lastSlice.energy * 1000.0) / 1000.0;
        }

        return slices;
    }

    /* ============================================================
       GET TARIFF FOR CURRENT TIME
       ============================================================ */
    private Tariff getTariffForTime (String chargerId, DateTime utcTime){

        LocalTime istTime = utcTime.withZone(IST).toLocalTime();
        TariffResponse tariffResponse = tariffAmountCalculation.fetchTariffResponse(chargerId);

        // SAME-DAY tariffs first
        for (Tariff tariff : tariffResponse.getTariffs()) {
            LocalTime start = LocalTime.parse(tariff.getStart_time());
            LocalTime end = LocalTime.parse(tariff.getEnd_time());

            if (end.isAfter(start)) {
                if (isInTimeRange(istTime, tariff.getStart_time(), tariff.getEnd_time())) {
                    return tariff;
                }
            }
        }

        // OVERNIGHT tariffs
        for (Tariff tariff : tariffResponse.getTariffs()) {
            LocalTime start = LocalTime.parse(tariff.getStart_time());
            LocalTime end = LocalTime.parse(tariff.getEnd_time());

            if (!end.isAfter(start)) {
                if (isInTimeRange(istTime, tariff.getStart_time(), tariff.getEnd_time())) {
                    return tariff;
                }
            }
        }

        throw new RuntimeException("No tariff found for IST time: " + istTime);
    }

    // ================= GETTER FOR LIVE API =================
    public Map<String, Object> getLiveBillingForTransaction (Integer transactionId){
        return liveBillingCache.getOrDefault(transactionId, Map.of());
    }

    private boolean isInTimeRange(LocalTime now, String startStr, String endStr
    ) {
        LocalTime start = LocalTime.parse(startStr);
        LocalTime end   = LocalTime.parse(endStr);

        if (end.isAfter(start)) {
            return !now.isBefore(start) && now.isBefore(end);
        }

        // OVERNIGHT (22:00 → 06:00)
        return !now.isBefore(start) || now.isBefore(end);
    }

    public Map<String, Object> buildLiveChargingPayload(
            Integer transactionId,
            String chargeBoxId,
            Tariff tariff,
            double consumedEnergy,
            DateTime startTime,
            DateTime endTime
    ) {

        // Get existing payload or start fresh
        Map<String, Object> payload =
                new LinkedHashMap<>(liveBillingCache.getOrDefault(
                        transactionId, new LinkedHashMap<>()
                ));

        List<Map<String, Object>> fareBreakdown = new ArrayList<>();

        // Safely extract existing fare breakdown
        Object obj = payload.get("fare_breakdown");
        if (obj instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> rawMap) {
                    Map<String, Object> safeMap = new LinkedHashMap<>();
                    for (Map.Entry<?, ?> e : rawMap.entrySet()) {
                        if (e.getKey() instanceof String key) {
                            safeMap.put(key, e.getValue());
                        }
                    }
                    fareBreakdown.add(safeMap);
                }
            }
        }

        String currentWindow = tariff.getStart_time() + "-" + tariff.getEnd_time();

        // ---------------- UPDATE LAST SLICE OR ADD NEW ----------------
        if (!fareBreakdown.isEmpty()) {
            Map<String, Object> last = fareBreakdown.get(fareBreakdown.size() - 1);
            String lastWindow = (String) last.get("Tariff Window");

            if (currentWindow.equals(lastWindow)) {
                BigDecimal lastUnits = (BigDecimal) last.get("Total Units");
                BigDecimal lastCost  = (BigDecimal) last.get("Total Cost");

                BigDecimal consumedEnergyBD = BigDecimal.valueOf(consumedEnergy);
                BigDecimal unitFareBD = BigDecimal.valueOf(tariff.getUnit_fare());

                // ---------------- NEW: raw slice cost ----------------
                BigDecimal sliceBaseCost = consumedEnergyBD.multiply(unitFareBD);

                // Cumulative cost, rounded to 2 decimals
                BigDecimal updatedTotalCost = lastCost.add(sliceBaseCost)
                        .setScale(2, RoundingMode.HALF_UP);

                // Update last slice
                last.put("Total Units", lastUnits.add(consumedEnergyBD).setScale(3, RoundingMode.HALF_UP));
                last.put("Total Cost", updatedTotalCost);
                last.put("End Timestamp", formatToIst(endTime));
            } else {
                fareBreakdown.add(newInterval(tariff, consumedEnergy, startTime, endTime));
            }
        } else {
            fareBreakdown.add(newInterval(tariff, consumedEnergy, startTime, endTime));
        }

        // ---------------- CALCULATE TOTALS ----------------
        BigDecimal totalUnitsBD = fareBreakdown.stream()
                .map(e -> (BigDecimal) e.get("Total Units"))
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(3, RoundingMode.HALF_UP);

        BigDecimal totalCostBD = fareBreakdown.stream()
                .map(e -> (BigDecimal) e.get("Total Cost"))
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(3, RoundingMode.HALF_UP);

        // GST 18% on total cost
        BigDecimal totalGst = totalCostBD.multiply(BigDecimal.valueOf(0.18))
                .setScale(4, RoundingMode.HALF_UP);

        BigDecimal grandTotal = totalCostBD.add(totalGst)
                .setScale(2, RoundingMode.HALF_UP);

        // ---------------- UPDATE PAYLOAD ----------------
        payload.put("chargeBoxId", chargeBoxId);
        payload.put("transactionId", transactionId);
        payload.put("fare_breakdown", fareBreakdown);
        payload.put("units_consumed", totalUnitsBD.toPlainString());
        payload.put("unit_cost", totalCostBD.toPlainString());
        payload.put("gst_amount", totalGst.toPlainString());
        payload.put("total_cost", grandTotal.toPlainString());

        // Collect unique unit fares
        Set<String> unitFareSet = new LinkedHashSet<>();
        for (Map<String, Object> fb : fareBreakdown) {
            Object uf = fb.get("Unit Fare");
            if (uf != null) {
                unitFareSet.add(new BigDecimal(String.valueOf(uf))
                        .setScale(2, RoundingMode.HALF_UP).toPlainString());
            }
        }
        payload.put("unitfare", String.join(", ", unitFareSet));

        // Cache payload for next call
        liveBillingCache.put(transactionId, payload);
        return payload;
    }

    // ----------------- NEW INTERVAL -----------------
    private Map<String, Object> newInterval(Tariff tariff, double energy, DateTime startUtc, DateTime endUtc) {
        Map<String, Object> m = new LinkedHashMap<>();

        BigDecimal energyBD = BigDecimal.valueOf(energy).setScale(3, RoundingMode.HALF_UP);
        BigDecimal unitFareBD = BigDecimal.valueOf(tariff.getUnit_fare());
        BigDecimal baseCost = energyBD.multiply(unitFareBD)
                .setScale(2, RoundingMode.HALF_UP); // cumulative rounding

        m.put("Unit Fare", unitFareBD.setScale(2, RoundingMode.HALF_UP));
        m.put("Tariff Window", tariff.getStart_time() + "-" + tariff.getEnd_time());
        m.put("Total Units", energyBD);
        m.put("Total Cost", baseCost);
        m.put("Start Timestamp", formatToIst(startUtc));
        m.put("End Timestamp", formatToIst(endUtc));

        return m;
    }


    private String formatToIst(DateTime utcTime) {
        return utcTime
                .withZone(IST)
                .toString("yyyy-MM-dd HH:mm:ss");
    }

    public DateTime retrievePreviousMeterTimestamp(Integer connectorPk, Integer transactionId) {
        return ctx.select(CONNECTOR_METER_VALUE.VALUE_TIMESTAMP)
                .from(CONNECTOR_METER_VALUE)
                .where(CONNECTOR_METER_VALUE.CONNECTOR_PK.eq(connectorPk))
                .and(CONNECTOR_METER_VALUE.TRANSACTION_PK.eq(transactionId))
                .orderBy(CONNECTOR_METER_VALUE.VALUE_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(DateTime.class);
    }
}




