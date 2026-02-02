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
package de.rwth.idsg.steve.externalconfig;

import de.rwth.idsg.steve.ocpp.ChargePointService16_InvokerImpl;
import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.service.TransactionStopService;
import de.rwth.idsg.steve.web.dto.TransactionQueryForm;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

import static jooq.steve.db.Tables.CONNECTOR_STATUS;
import static jooq.steve.db.tables.Connector.CONNECTOR;

@Service
public class CheckIsSessionClosedTransaction {

    @Autowired
    private TransactionRepositoryImpl transactionRepositoryImpl;
    @Autowired
    private DSLContext dslContext;
    @Autowired
    private TransactionStopService transactionStopService;
    @Autowired
    private ChargePointService16_InvokerImpl chargePointService16Invoker;


   // @Scheduled(fixedRate = 120000)
    private void get() {
        List<Transaction> obj = transactionRepositoryImpl.getTransactions(new TransactionQueryForm());
        for (Transaction transaction : obj) {
            String status = chargePointService16Invoker.lastStatusFromConnector(chargePointService16Invoker.getConnectorPk(transaction.getChargeBoxId(), transaction.getConnectorId()));
            if (status.equalsIgnoreCase("Available")) {
                transactionStopService.stop(transaction.getId());
            }

        }

        ;
    }
}
