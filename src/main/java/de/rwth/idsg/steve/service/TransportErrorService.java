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

import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.web.dto.TransactionQueryForm;
import jooq.steve.db.tables.ConnectorMeterValue;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

import static jooq.steve.db.Tables.*;
import static jooq.steve.db.Tables.CONNECTOR;

@Slf4j
@Service
public class TransportErrorService {

    @Autowired
    private TransactionRepositoryImpl transactionRepository;

    @Autowired
    private DSLContext dslContext;

    @Autowired
    private TransactionStopService transactionStopService;

    @Scheduled(fixedRate = 120000)
    public void avoidTransportError() {

        TransactionQueryForm form = new TransactionQueryForm();
        form.setChargeBoxId(null);
        form.setOcppIdTag(null);
        form.setFrom(null);
        form.setTo(null);
        form.setTransactionPk(null);
        form.setReturnCSV(false);
        form.setType(TransactionQueryForm.QueryType.ACTIVE);
        form.setPeriodType(TransactionQueryForm.QueryPeriodType.ALL);
        List<Transaction> txList = transactionRepository.getTransactions(form);
        for (Transaction tx : txList) {

            DateTime txStartTime = dslContext.select(TRANSACTION_START.START_TIMESTAMP)
                    .from(TRANSACTION_START)
                    .where(TRANSACTION_START.TRANSACTION_PK.eq(tx.getId()))
                    .fetchOneInto(DateTime.class);

            if (txStartTime != null && txStartTime.plusMinutes(1).isBefore(DateTime.now())) {

                boolean hasValues = dslContext.fetchCount(CONNECTOR_METER_VALUE,
                        CONNECTOR_METER_VALUE.TRANSACTION_PK.eq(tx.getId())) > 0;


                if (!hasValues) {
                    log.info("Stopping transaction with id: {} on chargeBox: {}", tx.getId(), tx.getChargeBoxId());
                    try {
                        Integer connectorPk = dslContext.select(CONNECTOR.CONNECTOR_PK)
                                .from(CONNECTOR)
                                .where(CONNECTOR.CONNECTOR_ID.eq(tx.getConnectorId())
                                        .and(CONNECTOR.CHARGE_BOX_ID.eq(tx.getChargeBoxId())))
                                .fetchOne(CONNECTOR.CONNECTOR_PK);
                        dslContext.insertInto(CONNECTOR_METER_VALUE)
                                .set(CONNECTOR_METER_VALUE.CONNECTOR_PK, connectorPk)
                                .set(CONNECTOR_METER_VALUE.TRANSACTION_PK, tx.getId())
                                .set(CONNECTOR_METER_VALUE.VALUE_TIMESTAMP, DateTime.now())
                                .set(CONNECTOR_METER_VALUE.VALUE, "")
                                .set(CONNECTOR_METER_VALUE.READING_CONTEXT, "reading context")
                                .set(CONNECTOR_METER_VALUE.FORMAT, "format")
                                .set(CONNECTOR_METER_VALUE.MEASURAND, "Energy.Active.Import.Register")
                                .set(CONNECTOR_METER_VALUE.LOCATION, "location")
                                .set(CONNECTOR_METER_VALUE.UNIT, "wh")
                                .execute();
                    } catch (Exception e) {
                        throw new RuntimeException("Auto Stoped Transaction Without get Any Meter Values" + e.getMessage());
                    }
                    transactionStopService.stop(tx.getId());

                }
            }
        }
    }


}
