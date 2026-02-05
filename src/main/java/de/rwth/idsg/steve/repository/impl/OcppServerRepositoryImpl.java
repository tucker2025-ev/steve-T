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
package de.rwth.idsg.steve.repository.impl;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Striped;
import de.rwth.idsg.steve.SteveException;
import de.rwth.idsg.steve.externalconfig.ScheduledChargingMessages;
import de.rwth.idsg.steve.ocpp.OcppProtocol;
import de.rwth.idsg.steve.repository.OcppServerRepository;
import de.rwth.idsg.steve.repository.ReservationRepository;
import de.rwth.idsg.steve.repository.dto.*;
import de.rwth.idsg.steve.service.*;
import jooq.steve.db.enums.TransactionStopEventActor;
import jooq.steve.db.enums.TransactionStopFailedEventActor;
import jooq.steve.db.tables.records.ConnectorMeterValueRecord;
import jooq.steve.db.tables.records.ScheduleChargingRecord;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ocpp.cs._2015._10.MeterValue;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static jooq.steve.db.Tables.*;
import static jooq.steve.db.tables.ChargeBox.CHARGE_BOX;
import static jooq.steve.db.tables.Connector.CONNECTOR;
import static jooq.steve.db.tables.ConnectorMeterValue.CONNECTOR_METER_VALUE;
import static jooq.steve.db.tables.ConnectorStatus.CONNECTOR_STATUS;
import static jooq.steve.db.tables.TransactionStart.TRANSACTION_START;
import static jooq.steve.db.tables.TransactionStop.TRANSACTION_STOP;
import static jooq.steve.db.tables.TransactionStopFailed.TRANSACTION_STOP_FAILED;
import static jooq.steve.db2.Tables.LIVE_CHARGING_DATA;
import static jooq.steve.db2.Tables.WALLET_TRACK;

/**
 * This class has methods for database access that are used by the OCPP service.
 * <p>
 * http://www.jooq.org/doc/3.4/manual/sql-execution/transaction-management/
 *
 * @author Sevket Goekay <sevketgokay@gmail.com>
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class OcppServerRepositoryImpl implements OcppServerRepository {

    private final DSLContext ctx;
    private final ReservationRepository reservationRepository;

    private final Striped<Lock> transactionTableLocks = Striped.lock(16);

    @Autowired
    private RetrieveTransactionMeterValues retrieveTransactionMeterValues;
    @Autowired
    private ScheduleChargingService scheduleChargingService;

    @Autowired
    private RazorpayRefundService razorpayRefundService;

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;

    @Autowired
    private ChargerStatusService chargerStatusService;

    @Autowired
    private ChargerFeeExceptUserService chargerFeeExceptUserService;

    @Autowired
    private TariffAmountCalculation tariffAmountCalculation;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public void updateChargebox(UpdateChargeboxParams p) {
        ctx.update(CHARGE_BOX)
                .set(CHARGE_BOX.OCPP_PROTOCOL, p.getOcppProtocol().getCompositeValue())
                .set(CHARGE_BOX.CHARGE_POINT_VENDOR, p.getVendor())
                .set(CHARGE_BOX.CHARGE_POINT_MODEL, p.getModel())
                .set(CHARGE_BOX.CHARGE_POINT_SERIAL_NUMBER, p.getPointSerial())
                .set(CHARGE_BOX.CHARGE_BOX_SERIAL_NUMBER, p.getBoxSerial())
                .set(CHARGE_BOX.FW_VERSION, p.getFwVersion())
                .set(CHARGE_BOX.ICCID, p.getIccid())
                .set(CHARGE_BOX.IMSI, p.getImsi())
                .set(CHARGE_BOX.METER_TYPE, p.getMeterType())
                .set(CHARGE_BOX.METER_SERIAL_NUMBER, p.getMeterSerial())
                .set(CHARGE_BOX.LAST_HEARTBEAT_TIMESTAMP, p.getHeartbeatTimestamp())
                .where(CHARGE_BOX.CHARGE_BOX_ID.equal(p.getChargeBoxId()))
                .execute();
    }

    @Override
    public void updateOcppProtocol(String chargeBoxIdentity, OcppProtocol protocol) {
        ctx.update(CHARGE_BOX)
                .set(CHARGE_BOX.OCPP_PROTOCOL, protocol.getCompositeValue())
                .where(CHARGE_BOX.CHARGE_BOX_ID.equal(chargeBoxIdentity))
                .execute();
    }

    @Override
    public void updateEndpointAddress(String chargeBoxIdentity, String endpointAddress) {
        ctx.update(CHARGE_BOX)
                .set(CHARGE_BOX.ENDPOINT_ADDRESS, endpointAddress)
                .where(CHARGE_BOX.CHARGE_BOX_ID.equal(chargeBoxIdentity))
                .execute();
    }

    @Override
    public void updateChargeboxFirmwareStatus(String chargeBoxIdentity, String firmwareStatus) {
        ctx.update(CHARGE_BOX)
                .set(CHARGE_BOX.FW_UPDATE_STATUS, firmwareStatus)
                .set(CHARGE_BOX.FW_UPDATE_TIMESTAMP, DateTime.now())
                .where(CHARGE_BOX.CHARGE_BOX_ID.equal(chargeBoxIdentity))
                .execute();
    }

    @Override
    public void updateChargeboxDiagnosticsStatus(String chargeBoxIdentity, String status) {
        ctx.update(CHARGE_BOX)
                .set(CHARGE_BOX.DIAGNOSTICS_STATUS, status)
                .set(CHARGE_BOX.DIAGNOSTICS_TIMESTAMP, DateTime.now())
                .where(CHARGE_BOX.CHARGE_BOX_ID.equal(chargeBoxIdentity))
                .execute();
    }

    @Override
    public void updateChargeboxHeartbeat(String chargeBoxIdentity, DateTime ts) {
        ctx.update(CHARGE_BOX)
                .set(CHARGE_BOX.LAST_HEARTBEAT_TIMESTAMP, ts)
                .where(CHARGE_BOX.CHARGE_BOX_ID.equal(chargeBoxIdentity))
                .execute();
    }

    @Override
    public void insertConnectorStatus(InsertConnectorStatusParams p) {
        ctx.transaction(configuration -> {
            DSLContext ctx = DSL.using(configuration);

            // Step
            insertIgnoreConnector(ctx, p.getChargeBoxId(), p.getConnectorId());
            // -------------------------------------------------------------------------
            // Step 2: We store a log of connector statuses
            // -------------------------------------------------------------------------

            ctx.insertInto(CONNECTOR_STATUS)
                    .set(CONNECTOR_STATUS.CONNECTOR_PK, DSL.select(CONNECTOR.CONNECTOR_PK)
                            .from(CONNECTOR)
                            .where(CONNECTOR.CHARGE_BOX_ID.equal(p.getChargeBoxId()))
                            .and(CONNECTOR.CONNECTOR_ID.equal(p.getConnectorId()))
                    )
                    .set(CONNECTOR_STATUS.STATUS_TIMESTAMP, p.getTimestamp())
                    .set(CONNECTOR_STATUS.STATUS, p.getStatus())
                    .set(CONNECTOR_STATUS.ERROR_CODE, p.getErrorCode())
                    .set(CONNECTOR_STATUS.ERROR_INFO, p.getErrorInfo())
                    .set(CONNECTOR_STATUS.VENDOR_ID, p.getVendorId())
                    .set(CONNECTOR_STATUS.VENDOR_ERROR_CODE, p.getVendorErrorCode())
                    .execute();

            if (!p.getStatus().equalsIgnoreCase("finishing")) {
                chargerStatusService.sendNotification(p.getChargeBoxId(), p.getConnectorId(), p.getStatus(), p.getErrorCode(), p.getErrorInfo(), p.getVendorErrorCode());
            }

            log.debug("Stored a new connector status for {}/{}.", p.getChargeBoxId(), p.getConnectorId());
        });
    }

    @Override
    public void insertMeterValues(String chargeBoxIdentity, List<MeterValue> list, int connectorId, Integer transactionId) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        ctx.transaction(configuration -> {
            try {
                DSLContext ctx = DSL.using(configuration);

                insertIgnoreConnector(ctx, chargeBoxIdentity, connectorId);
                int connectorPk = getConnectorPkFromConnector(ctx, chargeBoxIdentity, connectorId);
                batchInsertMeterValues(ctx, list, connectorPk, transactionId);
            } catch (Exception e) {
                log.error("Exception occurred  line 226 :", e);
            }
        });
    }

    @Override
    public void insertMeterValues(String chargeBoxIdentity, List<MeterValue> list, int transactionId) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        ctx.transaction(configuration -> {
            try {
                DSLContext ctx = DSL.using(configuration);

                // First, get connector primary key from transaction table
                int connectorPk = ctx.select(TRANSACTION_START.CONNECTOR_PK)
                        .from(TRANSACTION_START)
                        .where(TRANSACTION_START.TRANSACTION_PK.equal(transactionId))
                        .fetchOne()
                        .value1();

                batchInsertMeterValues(ctx, list, connectorPk, transactionId);
            } catch (Exception e) {
                log.error("Exception occurred 250 line", e);
            }
        });
    }

    @Override
    public int insertTransaction(InsertTransactionParams p) {

        SelectConditionStep<Record1<Integer>> connectorPkQuery =
                DSL.select(CONNECTOR.CONNECTOR_PK)
                        .from(CONNECTOR)
                        .where(CONNECTOR.CHARGE_BOX_ID.equal(p.getChargeBoxId()))
                        .and(CONNECTOR.CONNECTOR_ID.equal(p.getConnectorId()));

        // -------------------------------------------------------------------------
        // Step 1: Insert connector and idTag, if they are new to us
        // -------------------------------------------------------------------------

        insertIgnoreConnector(ctx, p.getChargeBoxId(), p.getConnectorId());

        // it is important to insert idTag before transaction, since the transaction table references it
        boolean unknownTagInserted = insertIgnoreIdTag(ctx, p);

        // -------------------------------------------------------------------------
        // Step 2: Insert transaction if it does not exist already
        // -------------------------------------------------------------------------

        TransactionDataHolder data = insertIgnoreTransaction(p, connectorPkQuery);
        int transactionId = data.transactionId;

        if (data.existsAlready) {
            return transactionId;
        }

        if (unknownTagInserted) {
            log.warn("The transaction '{}' contains an unknown idTag '{}' which was inserted into DB "
                    + "to prevent information loss and has been blocked", transactionId, p.getIdTag());
        }

        // -------------------------------------------------------------------------
        // Step 3 for OCPP >= 1.5: A startTransaction may be related to a reservation
        // -------------------------------------------------------------------------

        if (p.isSetReservationId()) {
            reservationRepository.used(connectorPkQuery, p.getIdTag(), p.getReservationId(), transactionId);
        }

        // -------------------------------------------------------------------------
        // Step 4: Set connector status
        // -------------------------------------------------------------------------

        if (shouldInsertConnectorStatusAfterTransactionMsg(p.getChargeBoxId())) {
            insertConnectorStatus(ctx, connectorPkQuery, p.getStartTimestamp(), p.getStatusUpdate());
        }
        insertTransactionConnectorMeterValue(p);
        if (isPayUser(p.getIdTag())) {
            try {
                updateInvoiceUrl(p.getIdTag(), transactionId);
                String url = "http://star.tuckermotors.com/ev_qr/update_transaction.php";
                UpdateTransactionRequest request = new UpdateTransactionRequest();
                request.setTransaction_id(String.valueOf(transactionId));
                request.setPayment_id(p.getIdTag());
                request.setMode("Test");

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<UpdateTransactionRequest> entity =
                        new HttpEntity<>(request, headers);

                ResponseEntity<String> response =
                        restTemplate.postForEntity(url, entity, String.class);
            } catch (Exception e) {
                log.error("Exception occurred 322 line", e);

            }

        }

        return transactionId;
    }

    private void insertTransactionConnectorMeterValue(InsertTransactionParams p) {
        //ctx.insertInto(CONNECTOR_METER_VALUE.CONNECTOR_PK, 1)
    }

    @Override
    public void updateTransaction(UpdateTransactionParams p) {

        // -------------------------------------------------------------------------
        // Step 1: insert transaction stop data
        // -------------------------------------------------------------------------

        // JOOQ will throw an exception, if something goes wrong

        if (p.getStopReason().equalsIgnoreCase("PowerLoss")) {
            String stopValue = retrieveStopEnergy(p.getStopMeterValue(), p.getTransactionId());

            if (!stopValue.equals(p.getStopMeterValue())) {
                log.error("PowerLoss Value Mismatch! Transaction ID: {}. Charger sent: {}, but DB last known value was: {}. Using DB value.",
                        p.getTransactionId(), p.getStopMeterValue(), stopValue);
            }

            p = p.toBuilder().stopMeterValue(stopValue).build();
        }

        String reason = p.getStopReason();

        if (ReasonRetrieveService.getCustomReason(p.getTransactionId()) != null) {
            reason = ReasonRetrieveService.getCustomReason(p.getTransactionId());
        }
        if (ManuallyStopTransaction.getCustomReason(p.getTransactionId()) != null) {
            reason = ManuallyStopTransaction.getCustomReason(p.getTransactionId());
        }

        try {
            secondary.update(LIVE_CHARGING_DATA)
                    .set(LIVE_CHARGING_DATA.STOP_REASON, reason)
                    .set(LIVE_CHARGING_DATA.STOP_TIMESTAMP, p.getStopTimestamp())
                    .where(LIVE_CHARGING_DATA.TRANSACTION_ID.eq(p.getTransactionId()))
                    .execute();
            ctx.insertInto(TRANSACTION_STOP)
                    .set(TRANSACTION_STOP.TRANSACTION_PK, p.getTransactionId())
                    .set(TRANSACTION_STOP.EVENT_TIMESTAMP, p.getEventTimestamp())
                    .set(TRANSACTION_STOP.EVENT_ACTOR, p.getEventActor())
                    .set(TRANSACTION_STOP.STOP_TIMESTAMP, p.getStopTimestamp())
                    .set(TRANSACTION_STOP.STOP_VALUE, p.getStopMeterValue())
                    .set(TRANSACTION_STOP.STOP_REASON, reason)
                    .execute();
            ReasonRetrieveService.removeCustomReason(p.getTransactionId());
            Integer connectorPk = ctx.select(TRANSACTION_START.CONNECTOR_PK)
                    .from(TRANSACTION_START)
                    .where(TRANSACTION_START.TRANSACTION_PK.eq(p.getTransactionId()))
                    .fetchOne(TRANSACTION_START.CONNECTOR_PK);


            ctx.insertInto(CONNECTOR_METER_VALUE)
                    .set(CONNECTOR_METER_VALUE.CONNECTOR_PK, connectorPk)
                    .set(CONNECTOR_METER_VALUE.TRANSACTION_PK, p.getTransactionId())
                    .set(CONNECTOR_METER_VALUE.VALUE_TIMESTAMP, p.getStopTimestamp())
                    .set(CONNECTOR_METER_VALUE.VALUE, p.getStopMeterValue())
                    .set(CONNECTOR_METER_VALUE.READING_CONTEXT, "reading context")
                    .set(CONNECTOR_METER_VALUE.FORMAT, "format")
                    .set(CONNECTOR_METER_VALUE.MEASURAND, "Energy.Active.Import.Register")
                    .set(CONNECTOR_METER_VALUE.LOCATION, "location")
                    .set(CONNECTOR_METER_VALUE.UNIT, "wh")
                    .execute();

            String idTag = ctx.select(TRANSACTION_START.ID_TAG)
                    .from(TRANSACTION_START)
                    .where(TRANSACTION_START.TRANSACTION_PK.eq(p.getTransactionId()))
                    .fetchOne(TRANSACTION_START.ID_TAG);

            tariffAmountCalculation.
                    sets(idTag, Double.parseDouble(p.getStopMeterValue()), p.getChargeBoxId(), p.getStopTimestamp(), p.getTransactionId(), connectorPk);
            retrieveTransactionMeterValues.insertTran(p.getTransactionId(), connectorPk, 0.0, 0.0, Double.parseDouble(p.getStopMeterValue()), 0.0, 0.0, idTag, p.getChargeBoxId());
            if (isPayUser(retrieveQrPaymentIdTag(p.getTransactionId()))) {
                updateQrPaymentActiveTransaction(p.getTransactionId(), retrieveChargedAmount(p.getTransactionId()));
            } else {
                updateActiveTransaction(p.getTransactionId(), retrieveChargedAmount(p.getTransactionId()));
            }

            if (chargerFeeExceptUserService.testChargerFeeExceptUser(idTag, p.getChargeBoxId())) {
                updateChargeFeeExpectUserConsumedAmountUpdateZero(idTag, p.getTransactionId());
            }
            updateScheduleCharging(p.getChargeBoxId(), connectorPk, idTag, reason);
            Set<String> ignoreReasons = Set.of("Remote", "Local", "Low wallet", "Charging Finished");

            if (!ignoreReasons.contains(reason)) {
                chargerStatusService.sendNotification(p.getChargeBoxId(), 1, reason, null, null, null);
            }

        } catch (Exception e) {
            log.error("Exception occurred 406 line", e);
            tryInsertingFailed(p, e);
        }

        // -------------------------------------------------------------------------
        // Step 2: Set connector status back. We do this even in cases where step 1
        // fails. It probably and hopefully makes sense.
        // -------------------------------------------------------------------------

        if (shouldInsertConnectorStatusAfterTransactionMsg(p.getChargeBoxId())) {
            SelectConditionStep<Record1<Integer>> connectorPkQuery =
                    DSL.select(TRANSACTION_START.CONNECTOR_PK)
                            .from(TRANSACTION_START)
                            .where(TRANSACTION_START.TRANSACTION_PK.equal(p.getTransactionId()));

            insertConnectorStatus(ctx, connectorPkQuery, p.getStopTimestamp(), p.getStatusUpdate());
        }
    }

    private void updateActiveTransaction(int transactionId, double consumedAMount) {
        secondary.update(WALLET_TRACK)
                .set(WALLET_TRACK.IS_ACTIVE_TRANSACTION, false)
                .set(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT, round2(consumedAMount))
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                .execute();
    }


    private double retrieveChargedAmount(final Integer transaction) {
        return secondary
                .select(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT)
                .from(WALLET_TRACK)
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transaction))
                .orderBy(WALLET_TRACK.STOP_TIMESTAMP.desc())   // or CREATED_AT / UPDATED_AT
                .limit(1)
                .fetchOptional()
                .map(r -> r.get(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT))
                .orElse(0.0);

    }


    private void updateBalanceAmount(final String rrnId, final double balanceAmount) {
        ctx.update(PAYMENT_REQUEST)
                .set(PAYMENT_REQUEST.PAY_BALANCE, balanceAmount)
                .where(PAYMENT_REQUEST.RRNID.eq(rrnId))
                .execute();
    }


    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class TransactionDataHolder {
        final boolean existsAlready;
        final int transactionId;
    }

    /**
     * Use case: If the station sends identical StartTransaction messages multiple times (e.g. due to connection
     * problems the response of StartTransaction could not be delivered and station tries again later), we do not want
     * to insert this into database multiple times.
     */
    private TransactionDataHolder insertIgnoreTransaction(InsertTransactionParams p,
                                                          SelectConditionStep<Record1<Integer>> connectorPkQuery) {
        Lock l = transactionTableLocks.get(p.getChargeBoxId());
        l.lock();
        try {
            Record1<Integer> r = ctx.select(TRANSACTION_START.TRANSACTION_PK)
                    .from(TRANSACTION_START)
                    .where(TRANSACTION_START.CONNECTOR_PK.eq(connectorPkQuery))
                    .and(TRANSACTION_START.ID_TAG.eq(p.getIdTag()))
                    .and(TRANSACTION_START.START_TIMESTAMP.eq(p.getStartTimestamp()))
                    .and(TRANSACTION_START.START_VALUE.eq(p.getStartMeterValue()))
                    .fetchOne();

            if (r != null) {
                return new TransactionDataHolder(true, r.value1());
            }

            Integer transactionId = ctx.insertInto(TRANSACTION_START)
                    .set(TRANSACTION_START.EVENT_TIMESTAMP, p.getEventTimestamp())
                    .set(TRANSACTION_START.CONNECTOR_PK, connectorPkQuery)
                    .set(TRANSACTION_START.ID_TAG, p.getIdTag())
                    .set(TRANSACTION_START.START_TIMESTAMP, p.getStartTimestamp())
                    .set(TRANSACTION_START.START_VALUE, p.getStartMeterValue())
                    .returning(TRANSACTION_START.TRANSACTION_PK)
                    .fetchOne()
                    .getTransactionPk();

            if (transactionId != null) {
                ctx.insertInto(CONNECTOR_METER_VALUE)
                        .set(CONNECTOR_METER_VALUE.CONNECTOR_PK, connectorPkQuery)
                        .set(CONNECTOR_METER_VALUE.TRANSACTION_PK, transactionId)
                        .set(CONNECTOR_METER_VALUE.VALUE_TIMESTAMP, p.getStartTimestamp())
                        .set(CONNECTOR_METER_VALUE.VALUE, p.getStartMeterValue())
                        .set(CONNECTOR_METER_VALUE.READING_CONTEXT, "Start-Value-Updated")
                        .set(CONNECTOR_METER_VALUE.FORMAT, "Raw")
                        .set(CONNECTOR_METER_VALUE.MEASURAND, "Energy.Active.Import.Register")
                        .set(CONNECTOR_METER_VALUE.LOCATION, "Outlet")
                        .set(CONNECTOR_METER_VALUE.UNIT, "Wh")
                        .execute();
                ctx.insertInto(CONNECTOR_STATUS)
                        .set(CONNECTOR_STATUS.STATUS, "Charging")
                        .set(CONNECTOR_STATUS.STATUS_TIMESTAMP, DateTime.now())
                        .set(CONNECTOR_STATUS.CONNECTOR_PK, connectorPkQuery)
                        .set(CONNECTOR_STATUS.ERROR_CODE, "NoError")
                        .set(CONNECTOR_STATUS.ERROR_INFO, "Manually Inserted ")
                        .execute();
            }

            // Actually unnecessary, because JOOQ will throw an exception, if something goes wrong
            if (transactionId == null) {
                throw new SteveException("Failed to INSERT transaction into database");
            }

            return new TransactionDataHolder(false, transactionId);
        } finally {
            l.unlock();
        }
    }

    /**
     * After a transaction start/stop event, a charging station _might_ send a connector status notification, but it is
     * not required. With this, we make sure that the status is updated accordingly. Since we use the timestamp of the
     * transaction data, we do not necessarily insert a "most recent" status.
     * <p>
     * If the station sends a notification, we will have a more recent timestamp, and therefore the status of the
     * notification will be used as current. Or, if this transaction data was sent to us for a failed push from the past
     * and we have a "more recent" status, it will still be the current status.
     */
    private void insertConnectorStatus(DSLContext ctx,
                                       SelectConditionStep<Record1<Integer>> connectorPkQuery,
                                       DateTime timestamp,
                                       TransactionStatusUpdate statusUpdate) {
        try {
            //   chargerStatusService.sendNotification(p.getChargeBoxId(), p.getConnectorId(), p.getStatus(),p.getErrorCode(), p.getErrorInfo(), p.getVendorErrorCode());.0
            ctx.insertInto(CONNECTOR_STATUS)
                    .set(CONNECTOR_STATUS.CONNECTOR_PK, connectorPkQuery)
                    .set(CONNECTOR_STATUS.STATUS_TIMESTAMP, timestamp)
                    .set(CONNECTOR_STATUS.STATUS, statusUpdate.getStatus())
                    .set(CONNECTOR_STATUS.ERROR_CODE, statusUpdate.getErrorCode())
                    .execute();
        } catch (Exception e) {
            log.error("Exception occurred 508 line", e);
        }
    }

    /**
     * If the connector information was not received before, insert it. Otherwise, ignore.
     */
    public static void insertIgnoreConnector(DSLContext ctx, String chargeBoxIdentity, int connectorId) {


        int count = ctx.insertInto(CONNECTOR,
                        CONNECTOR.CHARGE_BOX_ID, CONNECTOR.CONNECTOR_ID)
                .values(chargeBoxIdentity, connectorId)
                .onDuplicateKeyIgnore() // Important detail
                .execute();

        if (count == 1) {
            log.info("The connector {}/{} is NEW, and inserted into DB.", chargeBoxIdentity, connectorId);
        }
    }

    /**
     * Use case: An offline charging station decides to allow an unknown idTag to start a transaction. Later, when it
     * is online, it sends a StartTransactionRequest with this idTag. If we do not insert this idTag, the transaction
     * details will not be inserted into DB and we will lose valuable information.
     */
    private boolean insertIgnoreIdTag(DSLContext ctx, InsertTransactionParams p) {
        String note = "This unknown idTag was used in a transaction that started @ " + p.getStartTimestamp()
                + ". It was reported @ " + DateTime.now() + ".";

//        int count = ctx.insertInto(OCPP_TAG)
//                .set(OCPP_TAG.ID_TAG, p.getIdTag())
//                .set(OCPP_TAG.NOTE, note)
//                .set(OCPP_TAG.MAX_ACTIVE_TRANSACTION_COUNT, 0)
//                .onDuplicateKeyIgnore() // Important detail
//                .execute();
        int count = 0;

        return count == 1;
    }

    private boolean shouldInsertConnectorStatusAfterTransactionMsg(String chargeBoxId) {
        Record1<Integer> r = ctx.selectOne()
                .from(CHARGE_BOX)
                .where(CHARGE_BOX.CHARGE_BOX_ID.eq(chargeBoxId))
                .and(CHARGE_BOX.INSERT_CONNECTOR_STATUS_AFTER_TRANSACTION_MSG.isTrue())
                .fetchOne();

        return (r != null) && (r.value1() == 1);
    }

    private int getConnectorPkFromConnector(DSLContext ctx, String chargeBoxIdentity, int connectorId) {
        return ctx.select(CONNECTOR.CONNECTOR_PK)
                .from(CONNECTOR)
                .where(CONNECTOR.CHARGE_BOX_ID.equal(chargeBoxIdentity))
                .and(CONNECTOR.CONNECTOR_ID.equal(connectorId))
                .fetchOne()
                .value1();
    }

    private static final Set<String> ALLOWED_MEASURANDS = Set.of(
            "Energy.Active.Import.Register",
            "Power.Active.Import",
            "Current.Import",
            "Voltage",
            "SoC"
    );

    private void batchInsertMeterValues(
            DSLContext ctx,
            List<MeterValue> list,
            int connectorPk,
            Integer transactionId) {

        List<ConnectorMeterValueRecord> batch =
                list.stream()
                        .flatMap(mv ->
                                mv.getSampledValue().stream()
                                        .filter(sv ->

                                                // 1️⃣ Only allowed measurands
                                                sv.isSetMeasurand()
                                                        && ALLOWED_MEASURANDS.contains(sv.getMeasurand().value())

                                                        // 2️⃣ Ignore phase-wise values
                                                        && !sv.isSetPhase()
                                        )
                                        .map(sv -> {

                                            String measurand = sv.getMeasurand().value();

                                            // 🔑 LOCATION FIX
                                            // AC chargers do NOT send location → assume Outlet
                                            String location = sv.isSetLocation()
                                                    ? sv.getLocation().value()
                                                    : "Outlet";

                                            // SoC special handling
                                            if ("SoC".equalsIgnoreCase(measurand)) {
                                                // Accept EV or Outlet (AC chargers send Outlet)
                                                if (!"EV".equalsIgnoreCase(location)
                                                        && !"Outlet".equalsIgnoreCase(location)) {
                                                    return null;
                                                }
                                            } else {
                                                // Non-SoC → must be Outlet
                                                if (!"Outlet".equalsIgnoreCase(location)) {
                                                    return null;
                                                }
                                            }

                                            String unit = sv.isSetUnit()
                                                    ? sv.getUnit().value()
                                                    : null;

                                            String value = sv.getValue();

                                            // 3️⃣ Convert kW → W
                                            if ("kW".equalsIgnoreCase(unit)) {
                                                try {
                                                    value = String.valueOf(
                                                            Math.round(Double.parseDouble(value) * 1000)
                                                    );
                                                    unit = "W";
                                                } catch (Exception ignored) {
                                                }
                                            }

                                            return ctx.newRecord(CONNECTOR_METER_VALUE)
                                                    .setConnectorPk(connectorPk)
                                                    .setTransactionPk(transactionId)
                                                    .setValueTimestamp(mv.getTimestamp())
                                                    .setValue(value)
                                                    .setReadingContext(
                                                            sv.isSetContext() ? sv.getContext().value() : null)
                                                    .setFormat(
                                                            sv.isSetFormat() ? sv.getFormat().value() : null)
                                                    .setMeasurand(measurand)
                                                    .setLocation(location)
                                                    .setUnit(unit)
                                                    .setPhase(null);
                                        })
                                        .filter(Objects::nonNull)
                        )
                        .collect(Collectors.toList());

        if (!batch.isEmpty()) {
            ctx.batchInsert(batch).execute();

            retrieveTransactionMeterValues.buildTransactionMeterValues(
                    list, connectorPk, transactionId);
        }
    }

    private void tryInsertingFailed(UpdateTransactionParams p, Exception e) {
        try {
            ctx.insertInto(TRANSACTION_STOP_FAILED)
                    .set(TRANSACTION_STOP_FAILED.TRANSACTION_PK, p.getTransactionId())
                    .set(TRANSACTION_STOP_FAILED.CHARGE_BOX_ID, p.getChargeBoxId())
                    .set(TRANSACTION_STOP_FAILED.EVENT_TIMESTAMP, p.getEventTimestamp())
                    .set(TRANSACTION_STOP_FAILED.EVENT_ACTOR, mapActor(p.getEventActor()))
                    .set(TRANSACTION_STOP_FAILED.STOP_TIMESTAMP, p.getStopTimestamp())
                    .set(TRANSACTION_STOP_FAILED.STOP_VALUE, p.getStopMeterValue())
                    .set(TRANSACTION_STOP_FAILED.STOP_REASON, p.getStopReason())
                    .set(TRANSACTION_STOP_FAILED.FAIL_REASON, Throwables.getStackTraceAsString(e))
                    .execute();
        } catch (Exception ex) {
            // This is where we give up and just log
            log.error("Exception occurred", e);
        }
    }

    private static TransactionStopFailedEventActor mapActor(TransactionStopEventActor a) {
        for (TransactionStopFailedEventActor b : TransactionStopFailedEventActor.values()) {
            if (b.getLiteral().equalsIgnoreCase(a.getLiteral())) {
                return b;
            }
        }
        // if unknown, do not throw exceptions. just insert manual.
        return TransactionStopFailedEventActor.manual;
    }

    private void updateScheduleCharging(final String chargeBoxId, final Integer connectorPk, final String idTag, final String reason) {

        try {
            Integer connectorId = ctx
                    .select(CONNECTOR.CONNECTOR_ID)
                    .from(CONNECTOR)
                    .where(CONNECTOR.CONNECTOR_PK.eq(connectorPk))
                    .fetchOneInto(Integer.class);
            String connectorIdStr = String.valueOf(connectorId);

            DateTime startTime = ctx
                    .select(TRANSACTION_START.EVENT_TIMESTAMP)
                    .from(TRANSACTION_START)
                    .where(TRANSACTION_START.CONNECTOR_PK.eq(connectorPk))
                    .orderBy(TRANSACTION_START.EVENT_TIMESTAMP.desc())
                    .limit(1)
                    .fetchOneInto(DateTime.class);

            assert startTime != null;
            LocalDateTime localDateTime = LocalDateTime.ofInstant(
                    startTime.toInstant().toDate().toInstant(),
                    ZoneId.systemDefault()
            ).plusHours(5).plusMinutes(30);
            Result<ScheduleChargingRecord> toStart = ctx
                    .selectFrom(SCHEDULE_CHARGING)
                    .where("DATE_FORMAT({0}, '%Y-%m-%d %H:%i') = DATE_FORMAT({1}, '%Y-%m-%d %H:%i')",
                            SCHEDULE_CHARGING.START_TIME,
                            Timestamp.valueOf(localDateTime))
                    .and(SCHEDULE_CHARGING.CHARGE_BOX_ID.equal(chargeBoxId)
                            .and(SCHEDULE_CHARGING.CONNECTOR_ID.equal(connectorIdStr))).fetch();
            if (!toStart.isEmpty()) {
                if (reason.equalsIgnoreCase("Remote")) {
                    scheduleChargingService.sendNotification(idTag, "Charging Stoped", ScheduledChargingMessages.STOPPED_BY_USER);
                    try {
                        ctx.update(SCHEDULE_CHARGING)
                                .set(SCHEDULE_CHARGING.IS_ENABLE, false)
                                .where(SCHEDULE_CHARGING.ID.eq(toStart.get(0).getId()))
                                .execute();
                    } catch (Exception e) {
                        log.error("OcppServerRepository implement updateScheduleCharging method Error Occur : " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Update Schedule Charging update is_enable Exception occur :" + e.getMessage());
        }
    }

    private void updateChargeFeeExpectUserConsumedAmountUpdateZero(final String idTag, final int transactionId) {

        try {
            secondary.update(WALLET_TRACK)
                    .set(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT, 0.0)
                    .where(WALLET_TRACK.ID_TAG.eq(idTag)
                            .and(WALLET_TRACK.TRANSACTION_ID.eq(transactionId)))
                    .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                    .limit(1)
                    .execute();

        } catch (Exception e) {
            log.error("OcppServerRepositoryImple class updateChargeFeeExpectUserConsumedAmountUpdateZero Method Error Occur : " + e.getMessage());
        }


    }

    private String retrieveQrPaymentIdTag(final Integer transactionId) {
        return ctx
                .select(TRANSACTION_START.ID_TAG)
                .from(TRANSACTION_START)
                .where(TRANSACTION_START.TRANSACTION_PK.eq(transactionId))
                .fetchOne(TRANSACTION_START.ID_TAG, String.class);

    }

    private Double retrieveBalanceAmount(final String idTag, final Integer transactionId) {

        // 1. Latest consumed amount
        Double consumed = secondary
                .select(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT)
                .from(WALLET_TRACK)
                .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                .orderBy(WALLET_TRACK.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Double.class);

        // 2. Pay amount (wallet amount loaded)
        Double payAmount = ctx
                .select(PAYMENT_REQUEST.AMOUNT)
                .from(PAYMENT_REQUEST)
                .where(PAYMENT_REQUEST.RRNID.eq(idTag))
                .fetchOne(PAYMENT_REQUEST.AMOUNT, Double.class);

        // Null-safety
        double consumedVal = consumed != null ? consumed : 0.0;
        double payAmountVal = payAmount != null ? payAmount : 0.0;

        // 3. Remaining balance
        return payAmountVal - consumedVal;
    }

    private boolean isPayUser(final String idTag) {
        return ctx.fetchExists(
                ctx.selectOne()
                        .from(PAYMENT_REQUEST)
                        .where(PAYMENT_REQUEST.RRNID.eq(idTag))
        );
    }

    private void updateInvoiceUrl(final String payId, final Integer transactionId) {
        String liveInvoiceURL = " http://cms.tuckerio.bigtot.in/flutter/FlutterInvoice/inv.php?transid=" + transactionId;
        String testInvoiceURL = "http://15.207.37.132/new/inv.php?transid=" + transactionId;

        ctx.update(PAYMENT_REQUEST)
                .set(PAYMENT_REQUEST.INVOICE_URL, liveInvoiceURL)
                .where(PAYMENT_REQUEST.RRNID.eq(payId))
                .execute();

    }

    private void updateQrPaymentActiveTransaction(final Integer transactionId, double totalConsumedAmount) {
        try {
            String idTag = retrieveQrPaymentIdTag(transactionId);

            double paidAmount = retrievePaidAmount(idTag);
            double balanceAmount = retrieveBalanceAmount(idTag, transactionId);

            double finalBalance = balanceAmount > 0.99 ? balanceAmount : 0.0;

            double finalConsumedAmount =
                    finalBalance == 0.0 ? paidAmount : totalConsumedAmount;

            updateBalanceAmount(idTag, balanceAmount);

            secondary.update(WALLET_TRACK)
                    .set(WALLET_TRACK.IS_ACTIVE_TRANSACTION, false)
                    .set(WALLET_TRACK.TOTAL_CONSUMED_AMOUNT, round2(finalConsumedAmount))
                    .where(WALLET_TRACK.TRANSACTION_ID.eq(transactionId))
                    .execute();

            if (finalBalance > 0.0) {
                razorpayRefundService.refound(
                        idTag,
                        retrieveChargedAmount(transactionId),
                        finalBalance
                );
            } else {
                razorpayRefundService.refound(
                        idTag,
                        retrieveChargedAmount(transactionId),
                        0.0
                );
            }
        } catch (Exception e) {
            log.error("Exception Occur  Line 811 : " + e.getMessage());
        }
    }


    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private String retrieveStopEnergy(String stopValue, Integer transactionPk) {
        try {
            BigDecimal lastEnergy = ctx
                    .select(TRANSACTION_METER_VALUES.ENERGY)
                    .from(TRANSACTION_METER_VALUES)
                    .where(TRANSACTION_METER_VALUES.TRANSACTION_PK.eq(transactionPk))
                    .orderBy(TRANSACTION_METER_VALUES.EVENT_TIMESTAMP.desc())
                    .limit(1)
                    .fetchOneInto(BigDecimal.class);

            BigDecimal stopVal = (stopValue == null)
                    ? BigDecimal.ZERO
                    : new BigDecimal(stopValue);

            if (lastEnergy != null) {
                return lastEnergy.max(stopVal).toPlainString(); // 👈 KEY FIX
            }

            return stopVal.toPlainString();

        } catch (Exception e) {
            return stopValue;
        }
    }

    private Double retrievePaidAmount(final String idTag) {

        Double payAmount = ctx
                .select(PAYMENT_REQUEST.AMOUNT)
                .from(PAYMENT_REQUEST)
                .where(PAYMENT_REQUEST.RRNID.eq(idTag))
                .fetchOne(PAYMENT_REQUEST.AMOUNT, Double.class);

        return payAmount != null ? payAmount : 0.0;
    }


}
