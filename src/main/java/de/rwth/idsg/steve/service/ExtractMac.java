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
package de.rwth.idsg.steve.service;

import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static jooq.steve.db.Tables.TRANSACTION_START;
import static jooq.steve.db.Tables.VEHICLE;

@Slf4j
@Service
public class ExtractMac {

    @Autowired
    private DSLContext dslContext;

    public final Map<Integer, String> transactionVidMap = new ConcurrentHashMap<>();


    public void getIdTagFromTransaction(Integer txId, String vid) {
        transactionVidMap.put(txId, vid);
        String idTag = dslContext.select(TRANSACTION_START.ID_TAG)
                .from(TRANSACTION_START)
                .where(TRANSACTION_START.TRANSACTION_PK.eq(txId))
                .fetchOne(TRANSACTION_START.ID_TAG);
        insertVid(idTag, vid);
    }

    public void insertVid(final String idTag, final String vid) {
        boolean exists = dslContext.fetchExists(
                dslContext.selectOne()
                        .from(VEHICLE)
                        .where(VEHICLE.ID_TAG.eq(idTag))
                        .and(VEHICLE.VID_NUMBER.eq(vid))
        );
        if (!exists) {
            dslContext.insertInto(VEHICLE)
                    .set(VEHICLE.ID_TAG, idTag)
                    .set(VEHICLE.VID_NUMBER, vid)
                    .execute();
        }
    }


}
