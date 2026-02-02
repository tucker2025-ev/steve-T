/*
 * SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
 * Copyright (C) 2013-2025 SteVe Community Team
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
package de.rwth.idsg.steve.repository;

import de.rwth.idsg.steve.service.dto.WebSocketLog;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

import static jooq.steve.db.Tables.WEBSOCKET_LOG;

@Repository
public class WebSocketLogRepository {

    @Autowired
    private DSLContext dslContext;

    public List<WebSocketLog> getLogsByDate(LocalDate date) {
        DateTime start = date.toDateTimeAtStartOfDay();
        DateTime end = date.plusDays(1).toDateTimeAtStartOfDay(); 


        DataType<DateTime> jodaTimestamp =
                SQLDataType.TIMESTAMP.asConvertedDataType(new JodaDateTimeConverter());

        return dslContext
                .selectFrom(WEBSOCKET_LOG)
                .where(
                        DSL.field("time", jodaTimestamp).between(
                                DSL.val(start, jodaTimestamp),
                                DSL.val(end,   jodaTimestamp)
                        )
                )
                .orderBy(WEBSOCKET_LOG.TIME.desc())
                .fetchInto(WebSocketLog.class);
    }

}
