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

import de.rwth.idsg.steve.service.dto.TransactionMeterValues;
import lombok.extern.slf4j.Slf4j;
import ocpp.cs._2015._10.MeterValue;
import ocpp.cs._2015._10.SampledValue;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

import static jooq.steve.db.Tables.TRANSACTION_METER_VALUES;
import static jooq.steve.db.tables.Connector.CONNECTOR;
import static jooq.steve.db.tables.TransactionStart.TRANSACTION_START;
import static jooq.steve.db2.Tables.LIVE_CHARGING_DATA;
import static org.jooq.impl.DSL.iif;
import static org.jooq.impl.DSL.val;

@Slf4j
@Service
public class RetrieveTransactionMeterValues {

    @Autowired
    private DSLContext dslContext;
    @Autowired
    private ManuallyStopTransaction manuallyStopTransaction;
    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;
    @Autowired
    private TariffAmountCalculation tariffAmountCalculation;
    @Autowired
    private ChargerFeeExceptUserService chargerFeeExceptUserService;


    public TransactionMeterValues buildTransactionMeterValues(
            List<MeterValue> list,
            int connectorPk,
            Integer transactionId) {
        String chargeBoxId = dslContext.select(CONNECTOR.CHARGE_BOX_ID)
                .from(CONNECTOR)
                .where(CONNECTOR.CONNECTOR_PK.eq(connectorPk))
                .fetchOne(CONNECTOR.CHARGE_BOX_ID);

        String idTag = dslContext.select(TRANSACTION_START.ID_TAG)
                .from(TRANSACTION_START)
                .where(TRANSACTION_START.TRANSACTION_PK.eq(transactionId))
                .fetchOne(TRANSACTION_START.ID_TAG);
        TransactionMeterValues tmv = new TransactionMeterValues();
        tmv.setConnectorPk(connectorPk);
        tmv.setTransactionPk(transactionId);

        double voltageSum = 0;
        int voltageCount = 0;
        double currentSum = 0;
        double powerSum = 0;

        for (MeterValue mv : list) {
            for (SampledValue sv : mv.getSampledValue()) {

                if (!sv.isSetMeasurand() || !sv.isSetValue()) continue;

                String measurand = sv.getMeasurand().value();
                String location = sv.isSetLocation()
                        ? sv.getLocation().value()
                        : "Outlet";

                double value;
                try {
                    value = Double.parseDouble(sv.getValue());
                } catch (Exception e) {
                    continue;
                }

                if (sv.isSetUnit() && "kW".equalsIgnoreCase(sv.getUnit().value())) {
                    value *= 1000;
                }

                // ENERGY (common)
                if ("Energy.Active.Import.Register".equals(measurand)
                        && "Outlet".equalsIgnoreCase(location)) {
                    tmv.setEnergy(value);
                }

                // DC (non-phase)
                if (!sv.isSetPhase()) {
                    if ("Voltage".equals(measurand)) tmv.setVoltage(value);
                    if ("Current.Import".equals(measurand)) tmv.setCurrent(value);
                    if ("Power.Active.Import".equals(measurand)) tmv.setPower(value);
                    if ("SoC".equals(measurand)) tmv.setSoc(value);
                }

                // AC (phase-wise)
                if (sv.isSetPhase()) {
                    if ("Voltage".equals(measurand)) {
                        voltageSum += value;
                        voltageCount++;
                    }
                    if ("Current.Import".equals(measurand)) {
                        currentSum += value;
                    }
                    if ("Power.Active.Import".equals(measurand)) {
                        powerSum += value;
                    }
                }
            }
        }

        // Apply AC totals
        if (voltageCount > 0) tmv.setVoltage(voltageSum / voltageCount);
        if (currentSum > 0) tmv.setCurrent(currentSum);
        if (powerSum > 0) tmv.setPower(powerSum);
        DateTime latestTimestamp = list.stream()
                .map(MeterValue::getTimestamp)
                .reduce((first, second) -> second)
                .orElse(null);
        tariffAmountCalculation.sets(idTag, tmv.getEnergy(), chargeBoxId, latestTimestamp, transactionId, connectorPk);
        // ✅ GUARANTEED INSERT
        dslContext.insertInto(TRANSACTION_METER_VALUES)
                .set(TRANSACTION_METER_VALUES.TRANSACTION_PK, transactionId)
                .set(TRANSACTION_METER_VALUES.OCPP_TAG_ID, idTag)
                .set(TRANSACTION_METER_VALUES.CHARGE_BOX_ID, chargeBoxId)
                .set(TRANSACTION_METER_VALUES.CONNECTOR_PK, connectorPk)
                .set(TRANSACTION_METER_VALUES.VOLTAGE, tmv.getVoltage())
                .set(TRANSACTION_METER_VALUES.CURRENT, tmv.getCurrent())
                .set(TRANSACTION_METER_VALUES.POWER, tmv.getPower())
                .set(TRANSACTION_METER_VALUES.ENERGY, tmv.getEnergy())
                .set(TRANSACTION_METER_VALUES.SOC, tmv.getSoc())
                .execute();
        insertTransactionLiveData(transactionId, tmv);
        return tmv;
    }


    private void insertTransactionLiveData(final Integer transactionId, final TransactionMeterValues transactionMeterValues) {
        try {
            Field<Double> incomingSoc = val(transactionMeterValues.getSoc());

            Field<Double> startEnergyCondition =
                    iif(LIVE_CHARGING_DATA.START_ENERGY.isNull(),
                            val(transactionMeterValues.getEnergy()),
                            LIVE_CHARGING_DATA.START_ENERGY);

            Field<Double> startCurrentCondition =
                    iif(LIVE_CHARGING_DATA.START_CURRENT.isNull(),
                            val(transactionMeterValues.getCurrent()),
                            LIVE_CHARGING_DATA.START_CURRENT);

            Field<Double> startPowerCondition =
                    iif(LIVE_CHARGING_DATA.START_POWER.isNull(),
                            val(transactionMeterValues.getPower()),
                            LIVE_CHARGING_DATA.START_POWER);

            Field<Double> startVoltageCondition =
                    iif(LIVE_CHARGING_DATA.START_VOLTAGE.isNull(),
                            val(transactionMeterValues.getVoltage()),
                            LIVE_CHARGING_DATA.START_VOLTAGE);

            Field<Double> startSocCondition =
                    iif(LIVE_CHARGING_DATA.START_SOC.isNull(),
                            incomingSoc,
                            LIVE_CHARGING_DATA.START_SOC);

            Field<Double> endSocCondition =
                    iif(incomingSoc.gt(0.0),
                            incomingSoc,
                            LIVE_CHARGING_DATA.END_SOC);

            secondary.update(LIVE_CHARGING_DATA)

                    .set(LIVE_CHARGING_DATA.START_ENERGY, startEnergyCondition)
                    .set(LIVE_CHARGING_DATA.START_CURRENT, startCurrentCondition)
                    .set(LIVE_CHARGING_DATA.START_POWER, startPowerCondition)
                    .set(LIVE_CHARGING_DATA.START_VOLTAGE, startVoltageCondition)
                    .set(LIVE_CHARGING_DATA.START_SOC, startSocCondition)

                    .set(LIVE_CHARGING_DATA.END_ENERGY, transactionMeterValues.getEnergy())
                    .set(LIVE_CHARGING_DATA.END_CURRENT, transactionMeterValues.getCurrent())
                    .set(LIVE_CHARGING_DATA.END_POWER, transactionMeterValues.getPower())
                    .set(LIVE_CHARGING_DATA.END_VOLTAGE, transactionMeterValues.getVoltage())

                    .set(LIVE_CHARGING_DATA.END_SOC, endSocCondition)
                    .where(LIVE_CHARGING_DATA.TRANSACTION_ID.eq(transactionId))
                    .execute();

        } catch (Exception e) {
            log.error("insertTransactionLiveData Method Exception Occur : " + e.getMessage());
        }
    }


    public void insertTran(final Integer transactionPk, final Integer connectorPk, final double voltage, final double power, final double energy, final double soc, final double current, final String idTag, final String chargerBoxId) {
        try {
            dslContext.insertInto(TRANSACTION_METER_VALUES)
                    .set(TRANSACTION_METER_VALUES.TRANSACTION_PK, transactionPk)
                    .set(TRANSACTION_METER_VALUES.CONNECTOR_PK, connectorPk)
                    .set(TRANSACTION_METER_VALUES.VOLTAGE, voltage)
                    .set(TRANSACTION_METER_VALUES.POWER, power)
                    .set(TRANSACTION_METER_VALUES.ENERGY, energy)
                    .set(TRANSACTION_METER_VALUES.SOC, soc)
                    .set(TRANSACTION_METER_VALUES.CURRENT, current)
                    .set(TRANSACTION_METER_VALUES.OCPP_TAG_ID, idTag)
                    .set(TRANSACTION_METER_VALUES.CHARGE_BOX_ID, chargerBoxId)
                    .execute();
        } catch (Exception e) {
            log.error("Exception Occur in Retrieve Transaction Meter Values : " + e.getMessage());
        }
    }

}


