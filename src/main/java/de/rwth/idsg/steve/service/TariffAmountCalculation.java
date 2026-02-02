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

import de.rwth.idsg.steve.externalconfig.WalletResponse;
import de.rwth.idsg.steve.repository.TransactionRepository;
import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.service.dto.Tariff;
import de.rwth.idsg.steve.service.dto.TariffResponse;
import de.rwth.idsg.steve.web.controller.PaymentController;
import de.rwth.idsg.steve.web.dto.TransactionQueryForm;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.stream.Collectors;

import static jooq.steve.db.Tables.*;
import static jooq.steve.db.tables.Connector.CONNECTOR;
import static jooq.steve.db.tables.TransactionMeterValues.TRANSACTION_METER_VALUES;
import static jooq.steve.db.tables.TransactionStart.TRANSACTION_START;
import static jooq.steve.db2.Tables.WALLET_TRACK;

@Slf4j
@Service
public class TariffAmountCalculation {

    @Autowired
    @Qualifier("secondary")
    private DSLContext dslContext;

    @Autowired
    private DSLContext dslContextOne;

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private ManuallyStopTransaction stopTransaction;
    @Autowired
    private TransactionRepository transactionRepository;
    @Autowired
    private ChargerFeeExceptUserService chargerFeeExceptUserService;

    @Autowired
    private PaymentController paymentController;

    private static final String LIVE_TARIFF_API_URL = "http://cms.tuckerio.bigtot.in/test/tod.php?charger_id=";
    private static final String LIVE_WALLET_API_URL = "http://cms.tuckerio.bigtot.in/auto_charge/auto.php?idtag=";

    private static final String TEST_TARIFF_API_URL = "https://tuckerio.com/test/tod.php?charger_id=";
    private static final String TEST_WALLET_API_URL = "https://tuckerio.com/auto_charge/auto.php?idtag=";


    private static final DateTimeFormatter TIME_ONLY_FORMATTER = DateTimeFormat.forPattern("HH:mm:ss");
    private static final DateTimeZone IST = DateTimeZone.forID("Asia/Kolkata");
    private static boolean isHeaderPrinted = false;

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }


    /**
     * Core method: called for each incoming MeterValues reading (~ every 30 sec)
     */
    public synchronized void sets(final String idTag,
                                  final double lastEnergy,
                                  final String chargeBoxId,
                                  final DateTime latestTimestamp,
                                  final Integer transactionId,
                                  final Integer connectorPk) {


        if (check(lastEnergy, chargeBoxId, transactionId, connectorPk)) {
            return;
        }
        double unitFare = getUnitFareFromUtcTime(chargeBoxId, latestTimestamp);
        double previousEnergy = retrieveCurrentTransactionPreviewsEnergy(connectorPk, transactionId);
        double walletBalance = retrieveUserWalletAmount(idTag);

        double consumedEnergy = (lastEnergy - previousEnergy) / 1000.0;

        double baseCost = consumedEnergy * unitFare;
        double gstCost = baseCost * 0.18;
        double consumedAmount = baseCost + gstCost;

        double previousTotal = previousConsumedAmount(transactionId);
        double updateConsumedAmount = previousTotal + consumedAmount;
        double totalConsumedAmount = previousTotalConsumedAmount(transactionId) + consumedAmount;

        if (isIdTagIsAlreadyTransaction(idTag)) {
            if (isQrPaymentUser(idTag)) {
                if (paymentController.isDcCharger(chargeBoxId, 1)) {
                    if ((totalConsumedAmount + 8) > walletBalance) {
                        stopTransaction.manuallyStopTransaction(chargeBoxId, transactionId, "Charging Finished");
                    }
                } else if (totalConsumedAmount > walletBalance) {
                    stopTransaction.manuallyStopTransaction(chargeBoxId, transactionId, "Charging Finished");
                }

            } else if ((totalConsumedAmount + 30) >= walletBalance) {
                if (!chargerFeeExceptUserService.liveChargerFeeExceptUser(idTag, chargeBoxId)) {
                    stopTransaction.manuallyStopTransaction(chargeBoxId, transactionId, "Low Wallet");
                }
            }
        } else {
            List<Integer> activeTxIds = getActiveTransactionIds(idTag);
            double totalConsumedAmountMultiTransaction = 0;

            for (Integer txId : activeTxIds) {
                totalConsumedAmountMultiTransaction += previousTotalConsumedAmount(txId);
            }


            if ((totalConsumedAmountMultiTransaction + 30) >= walletBalance) {
                List<Integer> activeTxId = getActiveTransactionIds(idTag);
                for (Integer txId : activeTxId) {
                    String chargerId = fetchCurrentlyChargingPointsForIdTag(txId);
                    stopTransaction.manuallyStopTransaction(chargerId, txId, "Low Wallet");
                }
            }


        }


        if (isAnotherTariff(transactionId, unitFare)) {
            log.info("Tariff changed → insert new tariff record : consumedAmount = " + consumedAmount + " , totalConsumedAmount = " + totalConsumedAmount);

            insertChargerTariffAmountAgeignestTransactionIdData(
                    transactionId,
                    previousEnergy,
                    lastEnergy,
                    idTag,
                    unitFare,
                    walletBalance,
                    consumedEnergy,
                    consumedAmount,
                    totalConsumedAmount,
                    latestTimestamp
            );

        } else {
            log.info("Tariff same  update existing tariff record : consumedAmount = " + consumedAmount + " , totalConsumedAmount = " + totalConsumedAmount);

            updateConsumedAmount(
                    transactionId,
                    unitFare,
                    lastEnergy,
                    updateConsumedAmount,
                    totalConsumedAmount,
                    latestTimestamp, walletBalance
            );
        }
    }

    public String fetchCurrentlyChargingPointsForIdTag(Integer transactionId) {
        Integer connectorPk = dslContext
                .select(TRANSACTION_START.CONNECTOR_PK)
                .from(TRANSACTION_START)
                .where(TRANSACTION_START.TRANSACTION_PK.eq(transactionId))
                .fetchOne(TRANSACTION_START.CONNECTOR_PK);

        return dslContext
                .select(CONNECTOR.CHARGE_BOX_ID)
                .from(CONNECTOR)
                .where(CONNECTOR.CONNECTOR_PK.eq(connectorPk))
                .fetchOne(CONNECTOR.CHARGE_BOX_ID);
    }


    private double previousConsumedAmount(final Integer transactionId) {
        Double consumed = dslContext
                .select(WALLET_TRACK.CONSUMED_AMOUNT)
                .from(WALLET_TRACK)
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Double.class);

        return consumed != null ? consumed : 0.0;
    }

    public double previousTotalConsumedAmount(final Integer transactionId) {
        Double consumed = dslContext
                .select(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT)
                .from(WALLET_TRACK)
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Double.class);

        return consumed != null ? consumed : 0.0;
    }

    /**
     * Update latest record for same-tariff group
     */
    private void updateConsumedAmount(final Integer transactionId, final double unitFare,
                                      final double lastEnergy,
                                      final double consumedAmount,
                                      final double totalConsumedAmount,
                                      final DateTime lastTimeStamp, final double walletAmount) {

        double startEnergy = getStartEnergy(transactionId);
        double consumedEnergy = (lastEnergy - startEnergy) / 1000;
        dslContext.update(WALLET_TRACK)
                .set(WALLET_TRACK.LAST_ENERGY, lastEnergy)
                .set(WALLET_TRACK.CONSUMED_ENERGY, consumedEnergy)
                .set(WALLET_TRACK.CONSUMED_AMOUNT, consumedAmount)
                .set(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT, totalConsumedAmount)
                .set(WALLET_TRACK.STOP_TIMESTAMP, lastTimeStamp)
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                .limit(1)
                .execute();
    }

    private double getStartEnergy(final Integer transactionId) {
        Double startEnergy = dslContext
                .select(WALLET_TRACK.START_ENERGY)
                .from(WALLET_TRACK)
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Double.class);

        return startEnergy != null ? startEnergy : 0.0;
    }


    /**
     * Insert new record when tariff changes
     */
    private void insertChargerTariffAmountAgeignestTransactionIdData(final Integer transactionId,
                                                                     final double startEnergy,
                                                                     final double lastEnergy,
                                                                     final String idTag,
                                                                     final double tariffAmount,
                                                                     final double walletAmount,
                                                                     final double consumedEnergy,
                                                                     final double consumedAmount,
                                                                     final double totalConsumedAmount,
                                                                     final DateTime startTime) {
        try {
            dslContext.insertInto(WALLET_TRACK)
                    .set(WALLET_TRACK.TRANSACTION_ID, transactionId)
                    .set(WALLET_TRACK.ID_TAG, idTag)
                    .set(WALLET_TRACK.START_ENERGY, startEnergy)
                    .set(WALLET_TRACK.LAST_ENERGY, lastEnergy)
                    .set(WALLET_TRACK.CONSUMED_ENERGY, consumedEnergy)
                    .set(WALLET_TRACK.TARIFF_AMOUNT, tariffAmount)
                    .set(WALLET_TRACK.WALLET_AMOUNT, walletAmount)
                    .set(WALLET_TRACK.CONSUMED_AMOUNT, consumedAmount)
                    .set(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT, totalConsumedAmount)
                    .set(WALLET_TRACK.START_TIMESTAMP, startTime)
                    .set(WALLET_TRACK.STOP_TIMESTAMP, startTime)
                    .execute();

        } catch (Exception e) {
            log.error("Error inserting tariff record for tx {}: {}", transactionId, e.getMessage(), e);
        }
    }

    /**
     * Detect tariff change
     */
    private boolean isAnotherTariff(final Integer transactionId, final double tariffAmount) {
        Double existingTariff = dslContext.select(WALLET_TRACK.TARIFF_AMOUNT)
                .from(WALLET_TRACK)
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Double.class);

        return existingTariff == null || Double.compare(existingTariff, tariffAmount) != 0;
    }

    /**
     * Tariff lookup by charger and time (IST)
     */
    public double getUnitFareFromUtcTime(String chargerId, DateTime utcTime) {
        LocalTime istTime = utcTime.withZone(IST).toLocalTime();
        TariffResponse tariffResponse = fetchTariffResponse(chargerId);

        for (Tariff tariff : tariffResponse.getTariffs()) {
            if (isInTimeRange(istTime, tariff.getStart_time(), tariff.getEnd_time())) {
                return tariff.getUnit_fare();
            }
        }
        throw new RuntimeException("No matching tariff found for time: " + istTime);
    }

    /**
     * Helper: fetch tariff from external API
     */
    private TariffResponse fetchTariffResponse(String chargerId) {
        try {
            String url = TEST_TARIFF_API_URL + chargerId;
            TariffResponse response = restTemplate.getForObject(url, TariffResponse.class);
            if (response == null || response.getTariffs() == null || response.getTariffs().isEmpty()) {
                throw new RuntimeException("No tariff data for charger: " + chargerId);
            }
            return response;
        } catch (Exception e) {
            //log.error("Failed to fetch tariff for charger: {}", chargerId, e);
            throw new RuntimeException("Failed to fetch tariff for charger: " + chargerId, e);
        }
    }

    private boolean isInTimeRange(LocalTime now, String startStr, String endStr) {
        LocalTime start = TIME_ONLY_FORMATTER.parseLocalTime(startStr);
        LocalTime end = TIME_ONLY_FORMATTER.parseLocalTime(endStr);

        if (end.isBefore(start)) {
            return now.isAfter(start) || now.isBefore(end);
        } else {
            return !now.isBefore(start) && now.isBefore(end);
        }
    }

    /**
     * Fetch latest energy for current transaction
     */
    public Double retrieveCurrentTransactionPreviewsEnergy(Integer connectorPk, Integer transactionPk) {
        Double lastEnergy = dslContext
                .select(TRANSACTION_METER_VALUES.ENERGY)
                .from(TRANSACTION_METER_VALUES)
                .where(TRANSACTION_METER_VALUES.CONNECTOR_PK.eq(connectorPk))
                .and(TRANSACTION_METER_VALUES.TRANSACTION_PK.eq(transactionPk))
                .orderBy(TRANSACTION_METER_VALUES.EVENT_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Double.class);

        return lastEnergy != null ? lastEnergy : retrieveStartEnergy(transactionPk);
    }

    public Double retrieveStartEnergy(Integer transactionId) {
        try {
            return dslContext.select(TRANSACTION_START.START_VALUE)
                    .from(TRANSACTION_START)
                    .where(TRANSACTION_START.TRANSACTION_PK.eq(transactionId))
                    .fetchOneInto(Double.class);
        } catch (Exception e) {
            throw new RuntimeException("Transaction Id : " + transactionId + " Start Value Null ");
        }

    }

    public Double retrieveUserWalletAmount(String idTag) {
        try {
            String url = TEST_WALLET_API_URL + idTag;
            WalletResponse response = restTemplate.getForObject(url, WalletResponse.class);
            if (response == null) {
                // throw new RuntimeException("No wallet data received for idTag: " + idTag);
                return QrUserAmount(idTag);
            }
            return response.getWallet_amount();
        } catch (Exception e) {
            log.error("Tariff Amount Calculation That Transaction Should Qr Payment " + e);
            return QrUserAmount(idTag);
        }
    }

    private boolean isIdTagIsAlreadyTransaction(final String idTag) {
        Integer count = dslContextOne
                .select(DSL.count())
                .from(OCPP_TAG_ACTIVITY)
                .where(OCPP_TAG_ACTIVITY.ID_TAG.eq(idTag)
                        .and(OCPP_TAG_ACTIVITY.ACTIVE_TRANSACTION_COUNT.eq(1L)))
                .fetchOneInto(Integer.class);

        return count != null && count == 1;
    }


    private List<Integer> getActiveTransactionIds(final String idTag) {
        TransactionQueryForm form = new TransactionQueryForm();
        form.setChargeBoxId(null);
        form.setOcppIdTag(null);
        form.setFrom(null);
        form.setTo(null);
        form.setTransactionPk(null);
        form.setReturnCSV(false);
        form.setType(TransactionQueryForm.QueryType.ACTIVE);
        form.setPeriodType(TransactionQueryForm.QueryPeriodType.ALL);

        // Fetch all active transactions
        List<Transaction> txList = transactionRepository.getTransactions(form);

        // Filter by idTag and collect transaction IDs
        return txList.stream()
                .filter(tx -> tx.getOcppIdTag() != null && tx.getOcppIdTag().equals(idTag))
                .map(Transaction::getId)
                .collect(Collectors.toList());
    }


    private boolean isQrPaymentUser(final String rrnId) {

        Integer count = dslContext
                .selectCount()
                .from(PAYMENT_REQUEST)
                .where(PAYMENT_REQUEST.RRNID.eq(rrnId))
                .fetchOne(0, Integer.class);

        return count != null && count > 0;
    }

    private Double QrUserAmount(final String payId) {

        return dslContext
                .select(PAYMENT_REQUEST.AMOUNT)
                .from(PAYMENT_REQUEST)
                .where(PAYMENT_REQUEST.RRNID.eq(payId))
                .fetchOneInto(Double.class);   // Correct type
    }

    private boolean check(
            final double currentEnergy,
            final String chargeBoxId,
            final Integer transactionId,
            final Integer connectorPk) {

        double startEnergy = retrieveStartEnergy(transactionId);
        double previousEnergy = retrieveCurrentTransactionPreviewsEnergy(connectorPk, transactionId);

        if (startEnergy == 0) {
            if (previousEnergy > currentEnergy) {
                stopTransaction.manuallyStopTransaction(chargeBoxId, transactionId, "Charger Faulted");
                return true;   // STOP SIGNAL
            }
        } else {
            double lastStopValue = retrievePreviousTransactionLastEnergy(connectorPk);
            if (lastStopValue > currentEnergy) {
                stopTransaction.manuallyStopTransaction(chargeBoxId, transactionId, "Charger Faulted");
                return true;
            } else if (previousEnergy > currentEnergy) {
                stopTransaction.manuallyStopTransaction(chargeBoxId, transactionId, "Charger Faulted");
                return true;
            }
        }
        return false;
    }


    private double retrievePreviousTransactionLastEnergy(Integer connectorPk) {

        Integer txPk = dslContext
                .select(TRANSACTION_START.TRANSACTION_PK)
                .from(TRANSACTION_START)
                .where(TRANSACTION_START.CONNECTOR_PK.eq(connectorPk))
                .orderBy(TRANSACTION_START.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Integer.class);

        if (txPk == null) return 0.0;

        return dslContext
                .select(TRANSACTION_STOP.STOP_VALUE)
                .from(TRANSACTION_STOP)
                .where(TRANSACTION_STOP.TRANSACTION_PK.eq(txPk))
                .fetchOptionalInto(String.class)
                .map(Double::parseDouble)
                .orElse(0.0);
    }


}
