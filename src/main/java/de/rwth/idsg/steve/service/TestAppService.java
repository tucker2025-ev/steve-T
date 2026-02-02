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

import de.rwth.idsg.steve.ocpp.ChargePointService16_InvokerImpl;
import de.rwth.idsg.steve.ocpp.OcppCallback;
import de.rwth.idsg.steve.ocpp.OcppTransport;
import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.ocpp.task.RemoteStopTransactionTask;
import de.rwth.idsg.steve.ocpp.ws.data.OcppJsonError;
import de.rwth.idsg.steve.repository.dto.ChargePointSelect;
import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.repository.dto.TransactionDetails;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.service.remote.OcppRemoteCommandExecutor;
import de.rwth.idsg.steve.service.testmobiledto.*;
import de.rwth.idsg.steve.service.testmobiledto.ResponseDTO;
import de.rwth.idsg.steve.web.dto.TransactionQueryForm;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStartTransactionParams;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStopTransactionParams;
import lombok.extern.slf4j.Slf4j;
import ocpp.cs._2015._10.RegistrationStatus;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static jooq.steve.db.Tables.*;
import static jooq.steve.db2.Tables.LIVE_CHARGING_DATA;
import static jooq.steve.db2.Tables.WALLET_TRACK;


@Slf4j
@Service
public class TestAppService {

    @Autowired
    private OcppRemoteCommandExecutor commandExecutor;

    @Autowired
    private DSLContext dslContext;

    @Autowired
    @Qualifier("secondary")
    private DSLContext evHistory;

    @Autowired
    private ChargePointHelperService chargePointHelperService;

    @Autowired
    private TestChargingData testChargingData;

    @Autowired
    private LiveChargingData liveChargingData;

    @Autowired
    private TransactionRepositoryImpl transactionRepository;

    @Autowired
    private ChargePointService16_InvokerImpl chargePointService16Invoker;

    @Autowired
    private ManuallyStopTransaction manuallyStopTransaction;

    @Autowired
    private TariffAmountCalculation tariffAmountCalculation;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public ResponseDTO startTransaction(
            final String connectorQrCode,
            final String idTag) {
        final String chargeBoxId = retrieveChargeBoxId(connectorQrCode);
        final int connectorId = getSequenceNumber(connectorQrCode);
        final Integer connectorPk = getConnectorPk(chargeBoxId, connectorId);
        final String lastStatus = getConnectorLastStatusByPk(connectorPk);
        double walletAmount = tariffAmountCalculation.retrieveUserWalletAmount(idTag);
        if (walletAmount <= 30) {
            return buildFailureResponse(connectorQrCode, lastStatus, "LOW_WALLET");
        }
        final List<RegistrationStatus> allowedStatuses =
                Arrays.asList(RegistrationStatus.ACCEPTED, RegistrationStatus.PENDING);

        final List<ChargePointSelect> availableChargers =
                chargePointHelperService.getChargePoints(
                        OcppVersion.V_16, allowedStatuses);


        if (!isChargeBoxAvailable(chargeBoxId, availableChargers)) {
            return buildFailureResponse(
                    connectorQrCode, lastStatus, "CHARGER_UN_AVAILABLE");
        }

        // 2️⃣ Invalid connector / QR
        if ("UNKNOWN".equalsIgnoreCase(lastStatus)) {
            return buildFailureResponse(
                    connectorQrCode, lastStatus, "CONNECTOR_UN_AVAILABLE");
        }

        final boolean isType6 = isType6Charger(chargeBoxId, connectorId);

        if (!"Preparing".equalsIgnoreCase(lastStatus) && !isType6) {

            String reason = resolveStartFailureReason(lastStatus);

            return buildFailureResponse(
                    connectorQrCode,
                    lastStatus,
                    reason
            );
        }


        // 4️⃣ RemoteStart
        final RemoteStartTransactionParams params =
                new RemoteStartTransactionParams();
        params.setIdTag(idTag);
        params.setConnectorId(connectorId);

        try {
            final boolean started = commandExecutor
                    .sendRemoteStart(chargeBoxId, connectorId, params, idTag)
                    .join();

            return buildFinalStartResponse(
                    started, connectorQrCode, connectorId, chargeBoxId);

        } catch (Exception ex) {
            log.error("RemoteStart failed chargeBoxId={}, connectorId={}",
                    chargeBoxId, connectorId, ex);

            return buildFinalStartResponse(
                    false, connectorQrCode, connectorId, chargeBoxId);
        }
    }


    public ResponseDTO stopTransaction(
            final Integer transactionId,
            final String stopReason) {

        final String finalStopReason =
                (stopReason == null || stopReason.isBlank())
                        ? "Manual Stop"
                        : stopReason;

        final CompletableFuture<ResponseDTO> future =
                new CompletableFuture<>();

        final AtomicBoolean completed =
                new AtomicBoolean(false);

        // 1️⃣ Load transaction
        TransactionDetails transactionDetails =
                transactionRepository.getDetails(transactionId);

        if (transactionDetails == null ||
                transactionDetails.getTransaction() == null) {

            return buildStopResponse(
                    false,
                    String.valueOf(transactionId),
                    null,
                    "INVALID_TRANSACTION"
            );
        }

        Transaction transaction =
                transactionDetails.getTransaction();

        final String chargeBoxId =
                transaction.getChargeBoxId();

        ChargePointSelect select =
                new ChargePointSelect(
                        OcppTransport.JSON,
                        chargeBoxId
                );

        // 2️⃣ RemoteStop params
        RemoteStopTransactionParams params =
                new RemoteStopTransactionParams();

        params.setTransactionId(transactionId);
        params.setChargePointSelectList(
                Collections.singletonList(select)
        );

        RemoteStopTransactionTask task =
                new RemoteStopTransactionTask(
                        OcppVersion.V_16,
                        params
                );

        // 3️⃣ Timeout handling
        ScheduledFuture<?> timeoutTask =
                scheduler.schedule(() -> {

                    if (completed.compareAndSet(false, true)) {

                        log.warn(
                                "RemoteStop timeout chargeBoxId={}, transactionId={}",
                                chargeBoxId, transactionId
                        );

                        manuallyStopTransaction.manuallyStopTransaction(
                                chargeBoxId,
                                transactionId,
                                finalStopReason + " - Timeout"
                        );

                        future.complete(
                                buildStopResponse(
                                        false,
                                        String.valueOf(transactionId),
                                        chargeBoxId,
                                        "TIMEOUT_STOPPED_MANUALLY"
                                )
                        );
                    }

                }, 10, TimeUnit.SECONDS);

        // 4️⃣ OCPP Callback
        task.addCallback(new OcppCallback<String>() {

            @Override
            public void success(String cbId, String response) {

                if (completed.compareAndSet(false, true)) {

                    timeoutTask.cancel(false);

                    boolean accepted =
                            "Accepted".equalsIgnoreCase(response);

                    future.complete(
                            buildStopResponse(
                                    accepted,
                                    String.valueOf(transactionId),
                                    chargeBoxId,
                                    accepted
                                            ? "REMOTE_STOP_ACCEPTED"
                                            : "REMOTE_STOP_REJECTED"
                            )
                    );
                }
            }

            @Override
            public void success(String cbId, OcppJsonError error) {

                if (completed.compareAndSet(false, true)) {

                    timeoutTask.cancel(false);

                    manuallyStopTransaction.manuallyStopTransaction(
                            chargeBoxId,
                            transactionId,
                            finalStopReason + " - OCPP_ERROR"
                    );

                    future.complete(
                            buildStopResponse(
                                    false,
                                    String.valueOf(transactionId),
                                    chargeBoxId,
                                    "OCPP_ERROR_STOPPED_MANUALLY"
                            )
                    );
                }
            }

            @Override
            public void failed(String cbId, Exception ex) {

                if (completed.compareAndSet(false, true)) {

                    timeoutTask.cancel(false);

                    manuallyStopTransaction.manuallyStopTransaction(
                            chargeBoxId,
                            transactionId,
                            finalStopReason + " - FAILED"
                    );

                    future.complete(
                            buildStopResponse(
                                    false,
                                    String.valueOf(transactionId),
                                    chargeBoxId,
                                    "REMOTE_STOP_FAILED"
                            )
                    );
                }
            }
        });

        chargePointService16Invoker
                .remoteStopTransaction(select, task);

        return future.join();
    }


    public ResponseDTO getActiveTransactionByIdTag(final String idTag) {

        List<LiveChargingDataResponseDTO> liveData =
                getLiveChargingDataByIdTag(idTag);

        ResponseDTO response = new ResponseDTO();

        if (liveData == null || liveData.isEmpty()) {
            response.setStatus(false);
            response.setData(null);
        } else {
            response.setStatus(true);
            response.setData(liveData);
        }

        return response;
    }

    public ResponseDTO getAlleTransactionByIdTag(final String idTag) {

        List<ChargingHistoryDTO> liveData =
                getChargingHistoryByIdTag(idTag);

        ResponseDTO response = new ResponseDTO();

        if (liveData == null || liveData.isEmpty()) {
            response.setStatus(false);
            response.setData(null);
        } else {
            response.setStatus(true);
            response.setData(liveData);
        }

        return response;
    }

    public ResponseDTO transactionSummary(final Integer transactionId) {
        ChargingSessionSummaryDTO liveData = getChargingSessionSummary(transactionId);

        ResponseDTO response = new ResponseDTO();

        if (liveData == null) {
            response.setStatus(false);
            response.setData(null);
        } else {
            response.setStatus(true);
            response.setData(liveData);
        }

        return response;

    }

    public ResponseDTO retrieveGraphDataByTransactionId(final Integer transactionId) {
        final List<PowerAndSocGraphResponseDTO> liveData = retrieveGraphData(transactionId);
        ResponseDTO response = new ResponseDTO();

        if (liveData == null) {
            response.setStatus(false);
            response.setData(null);
        } else {
            response.setStatus(true);
            response.setData(liveData);
        }

        return response;
    }

    private ChargingSessionSummaryDTO getChargingSessionSummary(final Integer transactionId) {

        Field<DateTime> effectiveStopTimestamp =
                DSL.coalesce(
                        LIVE_CHARGING_DATA.STOP_TIMESTAMP,
                        DSL.currentTimestamp()
                );

        // IST (+05:30)
        Field<DateTime> startTimeIst =
                DSL.field(
                        "TIMESTAMPADD(MINUTE, 330, {0})",
                        DateTime.class,
                        LIVE_CHARGING_DATA.START_TIMESTAMP
                );

        Field<DateTime> stopTimeIst =
                DSL.field(
                        "TIMESTAMPADD(MINUTE, 330, {0})",
                        DateTime.class,
                        effectiveStopTimestamp
                );

        Field<Integer> durationMinutes =
                DSL.field(
                        "TIMESTAMPDIFF(MINUTE, {0}, {1})",
                        Integer.class,
                        DSL.min(LIVE_CHARGING_DATA.START_TIMESTAMP),
                        DSL.max(effectiveStopTimestamp)
                );

        Field<Double> energyConsumed =
                DSL.max(LIVE_CHARGING_DATA.END_ENERGY)
                        .minus(DSL.min(LIVE_CHARGING_DATA.START_ENERGY));

        return evHistory
                .select(
                        LIVE_CHARGING_DATA.TRANSACTION_ID.as("transactionId"),
                        LIVE_CHARGING_DATA.CHARGE_BOX_ID.as("chargerId"),
                        LIVE_CHARGING_DATA.CONNECTOR_ID.as("connectorId"),

                        DSL.max(LIVE_CHARGING_DATA.CHARGER_QR_CODE)
                                .as("connectorQrCode"),

                        DSL.max(LIVE_CHARGING_DATA.STATION_NAME)
                                .as("station"),
                        DSL.max(LIVE_CHARGING_DATA.CHARGING_VEHICLE_TYPE)
                                .as("connectorType"),
                        DSL.max(LIVE_CHARGING_DATA.STATION_ADDRESS_ONE)
                                .as("location"),

                        // ✅ IST timestamps
                        DSL.min(startTimeIst).as("startTime"),
                        DSL.max(stopTimeIst).as("endTime"),
                        durationMinutes.as("durationMinutes"),

                        energyConsumed.as("energyConsumedKWh"),
                        DSL.avg(LIVE_CHARGING_DATA.END_POWER)
                                .as("averagePowerKW"),

                        DSL.min(LIVE_CHARGING_DATA.START_SOC).cast(Integer.class)
                                .as("startSOC"),
                        DSL.max(LIVE_CHARGING_DATA.END_SOC).cast(Integer.class)
                                .as("endSOC"),

                        DSL.when(DSL.max(LIVE_CHARGING_DATA.STOP_TIMESTAMP).isNull(),
                                        DSL.inline("ONGOING"))
                                .otherwise(DSL.inline("COMPLETED"))
                                .as("status")
                )
                .from(LIVE_CHARGING_DATA)
                .where(LIVE_CHARGING_DATA.TRANSACTION_ID.eq(transactionId))
                .groupBy(
                        LIVE_CHARGING_DATA.TRANSACTION_ID,
                        LIVE_CHARGING_DATA.CHARGE_BOX_ID,
                        LIVE_CHARGING_DATA.CONNECTOR_ID
                )
                .fetchOneInto(ChargingSessionSummaryDTO.class);
    }


    public List<ChargingHistoryDTO> getChargingHistoryByIdTag(final String idTag) {
        TransactionQueryForm form = new TransactionQueryForm();
        form.setChargeBoxId(null);
        form.setOcppIdTag(idTag);
        form.setFrom(null);
        form.setTo(null);
        form.setTransactionPk(null);
        form.setReturnCSV(false);
        form.setType(TransactionQueryForm.QueryType.ALL);
        form.setPeriodType(TransactionQueryForm.QueryPeriodType.ALL);

        return transactionRepository.getTransactions(form)
                .stream()
                .map(tx -> {
                    ChargingHistoryDTO dto = new ChargingHistoryDTO();

                    dto.setTransactionId(String.valueOf(tx.getId()));
                    dto.setChargePoint(tx.getChargeBoxId());

                    dto.setChargerConnectorQrCode(
                            getChargerQr(tx.getChargeBoxId(), tx.getConnectorId()).orElse(null)
                    );
                    dto.setConsumedAmount(String.valueOf(tariffAmountCalculation.previousTotalConsumedAmount(tx.getId())));
                    dto.setConsumedUnit(String.valueOf(retrieveConsumedTotalEnergy(tx.getId())));
                    dto.setStopReason(tx.getStopReason());

                    return dto;
                })
                .collect(Collectors.toList());
    }


    private List<LiveChargingDataResponseDTO> getLiveChargingDataByIdTag(final String idTag) {

        TransactionQueryForm form = new TransactionQueryForm();
        form.setChargeBoxId(null);
        form.setOcppIdTag(idTag);
        form.setFrom(null);
        form.setTo(null);
        form.setTransactionPk(null);
        form.setReturnCSV(false);
        form.setType(TransactionQueryForm.QueryType.ACTIVE);
        form.setPeriodType(TransactionQueryForm.QueryPeriodType.ALL);

        return transactionRepository.getTransactions(form)
                .stream()
                .map(tx -> {
                    LiveChargingDataResponseDTO dto = new LiveChargingDataResponseDTO();
                    dto.setTransactionId(String.valueOf(tx.getId()));
                    dto.setChargePoint(tx.getChargeBoxId());
                    String chargerQr = getChargerQr(tx.getChargeBoxId(), tx.getConnectorId())
                            .orElse(null);
                    dto.setChargerConnectorQrCode(chargerQr);
                    dto.setConsumedAmount(String.valueOf(tariffAmountCalculation.previousTotalConsumedAmount(tx.getId())));
                    dto.setConsumedUnit(String.valueOf(retrieveConsumedTotalEnergy(tx.getId())));
                    return dto;
                })
                .collect(Collectors.toList());
    }


    private ResponseDTO buildStopResponse(
            final boolean status,
            final String transactionId,
            final String chargerConnectorQrCode,
            final String reason) {

        StopTransactionResponseDTO response =
                new StopTransactionResponseDTO();

        response.setTransactionId(transactionId);
        response.setChargerConnectorQrCode(chargerConnectorQrCode);
        response.setReason(reason);

        ResponseDTO dto = new ResponseDTO();
        dto.setStatus(status);
        dto.setData(response);

        return dto;
    }


    private ResponseDTO buildFailureResponse(
            final String connectorQrCode,
            final String ocppStatus,
            final String reason) {

        StartTransactionResponseDTO response =
                new StartTransactionResponseDTO();

        response.setTransactionId(null);
        response.setChargerConnectorQrCode(connectorQrCode);
        response.setStatusNotification(ocppStatus);
        response.setConnectorStatus(
                ConnectorStatus.fromOcppStatus(ocppStatus));
        response.setReason(reason);

        return buildResponse(false, response);
    }

    private ResponseDTO buildFinalStartResponse(
            final boolean started,
            final String connectorQrCode,
            final int connectorId,
            final String chargeBoxId) {

        sleepSafely(2000);

        final Integer connectorPk =
                getConnectorPk(chargeBoxId, connectorId);

        final String status =
                getConnectorLastStatusByPk(connectorPk);

        final Integer txnId =
                started ? getLastTransactionId(connectorPk) : null;

        StartTransactionResponseDTO response =
                new StartTransactionResponseDTO();

        response.setChargerConnectorQrCode(connectorQrCode);
        response.setStatusNotification(status);
        response.setConnectorStatus(
                ConnectorStatus.fromOcppStatus(status));
        response.setTransactionId(txnId);
        if (started) {
            response.setReason("STARTED");
        } else {
            response.setReason("CHARGING_COULD_NOT_BE_INITIATED");
        }

        return buildResponse(started, response);
    }

    private ResponseDTO buildResponse(
            final boolean status,
            final Object data) {

        ResponseDTO dto = new ResponseDTO();
        dto.setStatus(status);
        dto.setData(data);
        return dto;
    }

    // ===========================
    // VALIDATIONS
    // ===========================

    private boolean isChargeBoxAvailable(
            final String chargeBoxId,
            final List<ChargePointSelect> availableChargers) {

        return chargeBoxId != null &&
                availableChargers.stream()
                        .anyMatch(cp ->
                                chargeBoxId.equals(cp.getChargeBoxId()));
    }

    private boolean isType6Charger(
            final String chargeBoxId,
            final Integer connectorId) {

        return "TYPE6".equalsIgnoreCase(
                liveChargingData.isType6Charger(
                        chargeBoxId, connectorId));
    }

    // ===========================
    // DB HELPERS
    // ===========================

    private Integer getLastTransactionId(Integer connectorPk) {
        return connectorPk == null ? null :
                dslContext.select(TRANSACTION_START.TRANSACTION_PK)
                        .from(TRANSACTION_START)
                        .where(TRANSACTION_START.CONNECTOR_PK.eq(connectorPk))
                        .orderBy(TRANSACTION_START.EVENT_TIMESTAMP.desc())
                        .limit(1)
                        .fetchOptional(TRANSACTION_START.TRANSACTION_PK)
                        .orElse(null);
    }

    private String retrieveChargeBoxId(String connectorQrCode) {
        return dslContext.select(CHARGER_SERVER.CHARGER_BOX_ID)
                .from(CHARGER_SERVER)
                .where(CHARGER_SERVER.CHARGER_QR_CODE.eq(connectorQrCode))
                .fetchOne(CHARGER_SERVER.CHARGER_BOX_ID);
    }

    private Integer getConnectorPk(String chargeBoxId, int connectorId) {
        return dslContext.select(CONNECTOR.CONNECTOR_PK)
                .from(CONNECTOR)
                .where(CONNECTOR.CHARGE_BOX_ID.eq(chargeBoxId))
                .and(CONNECTOR.CONNECTOR_ID.eq(connectorId))
                .fetchOptional(CONNECTOR.CONNECTOR_PK)
                .orElse(null);
    }

    private String getConnectorLastStatusByPk(Integer connectorPk) {
        return connectorPk == null ? "UNKNOWN" :
                dslContext.select(CONNECTOR_STATUS.STATUS)
                        .from(CONNECTOR_STATUS)
                        .where(CONNECTOR_STATUS.CONNECTOR_PK.eq(connectorPk))
                        .orderBy(CONNECTOR_STATUS.STATUS_TIMESTAMP.desc())
                        .limit(1)
                        .fetchOptional(CONNECTOR_STATUS.STATUS)
                        .orElse("UNKNOWN");
    }


    public static int getSequenceNumber(String value) {
        if (value == null || value.isBlank()) return 1;

        int idx = value.lastIndexOf('_');
        if (idx == -1) return 1;

        try {
            return Integer.parseInt(value.substring(idx + 1));
        } catch (Exception e) {
            return 1;
        }
    }

    private void sleepSafely(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String resolveStartFailureReason(String lastStatus) {

        if (lastStatus == null) {
            return "CONNECTOR_NOT_READY";
        }

        switch (lastStatus.toUpperCase()) {

            case "AVAILABLE":
                return "PLEASE INSERT THE GUN";

            case "CHARGING":
                return "CONNECTOR BUSY";

            case "FINISHING":
                return "UNPLUG THE GUN AFTER INSERT TO START";

            default:
                return "CONNECTOR_NOT_READY";
        }
    }

    private Optional<String> getChargerQr(final String chargeBoxId, final Integer connectorId) {
        return dslContext
                .select(CHARGER_SERVER.CHARGER_QR_CODE)
                .from(CHARGER_SERVER)
                .where(CHARGER_SERVER.CHARGER_BOX_ID.eq(chargeBoxId))
                .and(CHARGER_SERVER.CONNECTORID.eq(connectorId))
                .fetchOptional(CHARGER_SERVER.CHARGER_QR_CODE);
    }

    private double retrieveConsumedTotalEnergy(final Integer transactionId) {

        return Optional.ofNullable(
                evHistory
                        .select(WALLET_TRACK.CONSUMED_ENERGY)
                        .from(WALLET_TRACK)
                        .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                        .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                        .limit(1)
                        .fetchOne(WALLET_TRACK.CONSUMED_ENERGY)
        ).orElse(0.0);
    }

    public List<PowerAndSocGraphResponseDTO> retrieveGraphData(final Integer transactionId) {

        return dslContext
                .select(
                        TRANSACTION_METER_VALUES.SOC.as("soc"),
                        TRANSACTION_METER_VALUES.POWER.as("power"),
                        TRANSACTION_METER_VALUES.EVENT_TIMESTAMP.as("timestamp")
                )
                .from(TRANSACTION_METER_VALUES)
                .where(TRANSACTION_METER_VALUES.TRANSACTION_PK.eq(transactionId))
                .orderBy(TRANSACTION_METER_VALUES.EVENT_TIMESTAMP.asc())
                .fetchInto(PowerAndSocGraphResponseDTO.class);
    }


}
