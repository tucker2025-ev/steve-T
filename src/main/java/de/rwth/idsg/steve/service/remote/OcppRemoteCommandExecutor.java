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
package de.rwth.idsg.steve.service.remote;

import de.rwth.idsg.steve.ocpp.ChargePointService16_InvokerImpl;
import de.rwth.idsg.steve.ocpp.OcppCallback;
import de.rwth.idsg.steve.ocpp.OcppTransport;
import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.ocpp.task.RemoteStartTransactionTask;
import de.rwth.idsg.steve.ocpp.task.RemoteStopTransactionTask;
import de.rwth.idsg.steve.ocpp.ws.data.OcppJsonError;
import de.rwth.idsg.steve.repository.dto.ChargePointSelect;
import de.rwth.idsg.steve.service.ManuallyStopTransaction;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStartTransactionParams;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStopTransactionParams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class OcppRemoteCommandExecutor {

    @Autowired
    private ChargePointService16_InvokerImpl invoker;

    @Autowired
    private ScheduledExecutorService scheduler;

    @Autowired
    private ManuallyStopTransaction manuallyStopTransaction;

    public CompletableFuture<Boolean> sendRemoteStart(
            String chargeBoxId,
            Integer connectorId,
            RemoteStartTransactionParams params,
            String idTag
    ) {

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        ChargePointSelect select =
                new ChargePointSelect(OcppTransport.JSON, chargeBoxId);

        RemoteStartTransactionTask task =
                new RemoteStartTransactionTask(OcppVersion.V_16, params);

        ScheduledFuture<?> timeout = scheduler.schedule(
                () -> future.complete(false),
                10, TimeUnit.SECONDS
        );

        task.addCallback(new OcppCallback<String>() {

            @Override
            public void success(String cbId, String response) {
                timeout.cancel(false);
                future.complete("Accepted".equalsIgnoreCase(response));
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

        invoker.remoteStartTransaction(select, task, idTag, connectorId);

        return future;
    }


    public CompletableFuture<Boolean> sendRemoteStop(
            String chargeBoxId,
            Integer transactionId,
            String reason
    ) {

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        ChargePointSelect select =
                new ChargePointSelect(OcppTransport.JSON, chargeBoxId);

        RemoteStopTransactionParams params = new RemoteStopTransactionParams();
        params.setTransactionId(transactionId);
        params.setChargePointSelectList(Collections.singletonList(select));

        RemoteStopTransactionTask task =
                new RemoteStopTransactionTask(OcppVersion.V_16, params);

        ScheduledFuture<?> timeout = scheduler.schedule(() -> {
            if (completed.compareAndSet(false, true)) {
                log.warn("RemoteStop timeout for {}", chargeBoxId);

                manuallyStopTransaction.manuallyStopTransaction(
                        chargeBoxId,
                        transactionId,
                        reason + " - Timeout"
                );

                future.complete(false);
            }
        }, 10, TimeUnit.SECONDS);

        task.addCallback(new OcppCallback<String>() {

            @Override
            public void success(String cbId, String response) {
                if (completed.compareAndSet(false, true)) {
                    timeout.cancel(false);

                    boolean accepted = "Accepted".equalsIgnoreCase(response);
                    log.info("RemoteStop response from {} : {}", chargeBoxId, response);

                    future.complete(accepted);
                }
            }

            @Override
            public void success(String cbId, OcppJsonError error) {
                if (completed.compareAndSet(false, true)) {
                    timeout.cancel(false);

                    log.error("RemoteStop OCPP error from {} : {}", chargeBoxId, error);

                    manuallyStopTransaction.manuallyStopTransaction(
                            chargeBoxId,
                            transactionId,
                            reason + " - OCPP Error"
                    );

                    future.complete(false);
                }
            }

            @Override
            public void failed(String cbId, Exception e) {
                if (completed.compareAndSet(false, true)) {
                    timeout.cancel(false);

                    log.error("RemoteStop failed for {}", chargeBoxId, e);

                    manuallyStopTransaction.manuallyStopTransaction(
                            chargeBoxId,
                            transactionId,
                            reason + " - Failed"
                    );

                    future.complete(false);
                }
            }
        });

        invoker.remoteStopTransaction(select, task);

        return future;
    }


}

