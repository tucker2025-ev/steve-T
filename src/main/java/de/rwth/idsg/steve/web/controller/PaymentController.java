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
package de.rwth.idsg.steve.web.controller;

import de.rwth.idsg.steve.ocpp.ChargePointService16_InvokerImpl;
import de.rwth.idsg.steve.ocpp.OcppCallback;
import de.rwth.idsg.steve.ocpp.OcppTransport;
import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.ocpp.task.RemoteStartTransactionTask;
import de.rwth.idsg.steve.ocpp.task.RemoteStopTransactionTask;
import de.rwth.idsg.steve.ocpp.ws.data.OcppJsonError;
import de.rwth.idsg.steve.repository.dto.ChargePointSelect;
import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.repository.dto.TransactionDetails;
import de.rwth.idsg.steve.repository.impl.OcppTagRepositoryImpl;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.service.ManuallyStopTransaction;
import de.rwth.idsg.steve.service.RazorpayRefundService;
import de.rwth.idsg.steve.service.TariffAmountCalculation;
import de.rwth.idsg.steve.web.dto.OcppTagForm;
import de.rwth.idsg.steve.web.dto.PaymentRequest;
import de.rwth.idsg.steve.web.dto.PaymentStartResponse;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStartTransactionParams;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStopTransactionParams;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static jooq.steve.db.Tables.*;

@RestController
@RequestMapping("/api")
@Slf4j
public class PaymentController {

    @Autowired
    private ChargePointService16_InvokerImpl chargePointService16Invoker;

    @Autowired
    private DSLContext dslContext;

    @Autowired
    private OcppTagRepositoryImpl ocppTagRepositoryImpl;

    @Autowired
    private RazorpayRefundService razorpayRefundService;

    @Autowired
    private TariffAmountCalculation tariffAmountCalculation;

    @Autowired
    private ManuallyStopTransaction manuallyStopTransaction;

    @Autowired
    private TransactionRepositoryImpl transactionRepository;

    @Autowired
    @Qualifier("php")
    private DSLContext php;

    private static final Table<?> CHARGE_POINT_VIEW =
            DSL.table("bigtot_cms.v_station_charger_details");
    private static final Field<String> CHARGER_ID = DSL.field("charger_id", String.class);
    private static final Field<String> CHARGER_TYPE = DSL.field("charger_type", String.class);
    private static final Field<String> CONNECTOR_ID = DSL.field("con_no", String.class);


    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();


    @PostMapping("/payments")
    public PaymentStartResponse createPayment(@RequestBody PaymentRequest request) {
        PaymentStartResponse response = new PaymentStartResponse();

        if (!isAlreadyIdTag(request.getRrnId())) {
            OcppTagForm form = new OcppTagForm();
            form.setIdTag(request.getRrnId());
            ocppTagRepositoryImpl.addOcppTag(form);


            try {
                boolean started = start(request).get();
                if (started) {
                    response.setStatus(started);
                    insertPayment(request, started);
                } else {
                    response.setStatus(started);
                    insertPayment(request, started);
                }
            } catch (Exception e) {
                response.setStatus(false);
                insertPayment(request, false);

                log.error("Payment Controller class Exception Occur 107 line : " + e);
            }
            if (!response.isStatus()) {
                //  razorpayRefundService.instantRefundPayment(request.getUpiId(), request.getAmount().intValue(), request.getRrnId());
                razorpayRefundService.refound(request.getRrnId(), 0.0, request.getAmount());
            }

            return response;
        } else {
            PaymentStartResponse paymentStartResponse = new PaymentStartResponse();
            paymentStartResponse.setStatus(false);
            return paymentStartResponse;
        }
    }


    private void insertPayment(final PaymentRequest paymentRequest, final boolean isStarted) {
        dslContext.insertInto(PAYMENT_REQUEST)
                .set(PAYMENT_REQUEST.PAY_ID, paymentRequest.getPayId())
                .set(PAYMENT_REQUEST.CHARGER_ID, paymentRequest.getChargerId())
                .set(PAYMENT_REQUEST.CONNECTOR_ID, paymentRequest.getConnectorId())
                .set(PAYMENT_REQUEST.AMOUNT, paymentRequest.getAmount())
                .set(PAYMENT_REQUEST.IS_STARTED, isStarted)
                .set(PAYMENT_REQUEST.CREATED_AT, DateTime.now())
                .set(PAYMENT_REQUEST.RRNID, paymentRequest.getRrnId())
                .set(PAYMENT_REQUEST.UPIID, paymentRequest.getUpiId())
                .execute();
    }

    private boolean isAlreadyIdTag(final String idTag) {
        return dslContext.fetchExists(
                dslContext.selectOne()
                        .from(OCPP_TAG)
                        .where(OCPP_TAG.ID_TAG.eq(idTag))
        );
    }

    public CompletableFuture<Boolean> start(final PaymentRequest request) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        double unitFare = tariffAmountCalculation.getUnitFareFromUtcTime(request.getChargerId(), DateTime.now());

        double baseAmount = request.getAmount() / (1 + (18 / 100.0));
        double unit = baseAmount / unitFare;
        String idTagWithUnit = request.getRrnId() + "-" + unit * 1000;
        RemoteStartTransactionParams params = new RemoteStartTransactionParams();
        if (isDcCharger(request.getChargerId(), request.getConnectorId())) {
            params.setIdTag(request.getRrnId());
            params.setConnectorId(request.getConnectorId());
        } else {
            params.setIdTag(idTagWithUnit);
            params.setConnectorId(request.getConnectorId());
        }


        ChargePointSelect select = new ChargePointSelect(OcppTransport.JSON, request.getChargerId());
        RemoteStartTransactionTask task = new RemoteStartTransactionTask(OcppVersion.V_16, params);

        task.addCallback(new OcppCallback<String>() {

            private final ScheduledFuture<?> timeout = scheduler.schedule(() -> {
                insertPayment(request, false);
                future.complete(false);
            }, 10, TimeUnit.SECONDS);


            @Override
            public void success(String cbId, String response) {
                timeout.cancel(false);

                if ("Accepted".equalsIgnoreCase(response)) {
                    future.complete(true);
                } else {
                    future.complete(false);
                }
            }

            @Override
            public void success(String cbId, OcppJsonError error) {
                timeout.cancel(false);
                future.complete(false);
            }

            @Override
            public void failed(String cbId, Exception e) {
                timeout.cancel(false);
                future.complete(false);
            }
        });

        chargePointService16Invoker.remoteStartTransaction(select, task, request.getPayId(), request.getConnectorId());

        return future;

    }

    @PostMapping("/upi-stop")
    public CompletableFuture<PaymentStartResponse> upiStopTransaction(@RequestParam Integer transactionId) {


        TransactionDetails transactionDetails = transactionRepository.getDetails(transactionId);

        Transaction transaction = transactionDetails.getTransaction();
        CompletableFuture<PaymentStartResponse> future = new CompletableFuture<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        ChargePointSelect select = new ChargePointSelect(OcppTransport.JSON, transaction.getChargeBoxId());

        RemoteStopTransactionParams params = new RemoteStopTransactionParams();
        params.setTransactionId(transactionId);
        params.setChargePointSelectList(Collections.singletonList(select));

        RemoteStopTransactionTask task = new RemoteStopTransactionTask(OcppVersion.V_16, params);

        ScheduledFuture<?> timeout = scheduler.schedule(() -> {
            if (completed.compareAndSet(false, true)) {
                log.warn("Timeout - Charger Offline, triggering manual stop");
                future.complete(buildResponse(true));
            }
        }, 10, TimeUnit.SECONDS);

        task.addCallback(new OcppCallback<String>() {

            @Override
            public void success(String cbId, String response) {
                if (completed.compareAndSet(false, true)) {
                    timeout.cancel(false);
                    boolean accepted = "Accepted".equalsIgnoreCase(response);
                    log.info("RemoteStop response: {}", response);

                    future.complete(buildResponse(accepted));
                }
            }

            @Override
            public void success(String cbId, OcppJsonError error) {
                if (completed.compareAndSet(false, true)) {
                    timeout.cancel(false);
                    log.error("OCPP Error");

                    future.complete(buildResponse(false));
                }
            }

            @Override
            public void failed(String chargeBoxId, Exception e) {
                if (completed.compareAndSet(false, true)) {
                    timeout.cancel(false);
                    log.error("RemoteStop failed", e);

                    future.complete(buildResponse(false));
                }
            }
        });

        manuallyStopTransaction.manuallyStopTransaction(transaction.getChargeBoxId(), transactionId, "UPI Stop");

        return future;
    }

    private PaymentStartResponse buildResponse(boolean status) {
        PaymentStartResponse res = new PaymentStartResponse();
        res.setStatus(status);
        return res;
    }

    public boolean isDcCharger(final String chargeBoxId, final Integer connectorId) {

        String chargerType = php
                .select(CHARGER_TYPE)
                .from(CHARGE_POINT_VIEW)
                .where(CHARGER_ID.eq(chargeBoxId))
                .and(CONNECTOR_ID.eq(String.valueOf(connectorId)))
                .fetchOne(CHARGER_TYPE);

        return chargerType != null && chargerType.equalsIgnoreCase("CCS2");
    }

}
