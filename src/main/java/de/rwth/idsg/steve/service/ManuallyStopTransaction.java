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
import de.rwth.idsg.steve.ocpp.OcppTransport;
import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.ocpp.task.RemoteStopTransactionTask;
import de.rwth.idsg.steve.repository.TaskStore;
import de.rwth.idsg.steve.repository.dto.ChargePointSelect;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStopTransactionParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class ManuallyStopTransaction {

    @Autowired
    private TaskStore taskStore;

    @Autowired
    private ChargePointService16_InvokerImpl chargePointService16Invoker;

    private static final Map<Integer, String> customReasonMap = new HashMap<>();

    public void manuallyStopTransaction(String chargeBoxId, Integer transactionId, String reason) {
        ChargePointSelect select = new ChargePointSelect(OcppTransport.JSON, chargeBoxId);

        RemoteStopTransactionParams params = new RemoteStopTransactionParams();
        params.setTransactionId(transactionId);
        params.setChargePointSelectList(Collections.singletonList(select));

        RemoteStopTransactionTask task = new RemoteStopTransactionTask(OcppVersion.V_16, params) {
            @Override
            public ocpp.cp._2015._10.RemoteStopTransactionRequest getOcpp16Request() {
                ocpp.cp._2015._10.RemoteStopTransactionRequest req = new ocpp.cp._2015._10.RemoteStopTransactionRequest();
                req.setTransactionId(params.getTransactionId());
                return req;
            }
        };
        customReasonMap.put(transactionId, reason);
        chargePointService16Invoker.remoteStopTransaction(select, task);
        taskStore.add(task);

    }

    public static String getCustomReason(int txId) {
        return customReasonMap.get(txId);
    }

    public static void removeCustomReason(int txId) {
        customReasonMap.remove(txId);
    }

}
