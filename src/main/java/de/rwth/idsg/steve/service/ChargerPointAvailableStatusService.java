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
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import static jooq.steve.db2.Tables.CHARGEBOX_STATUS;

@Slf4j
@Service
public class ChargerPointAvailableStatusService {

    @Autowired
    private HomeService homeService;

    @Autowired
    @Qualifier("secondary")
    private DSLContext dslContext;


    public void updateChargerStatus(final String chargeBoxId, final boolean status) {
        try {
            String chargeBox = dslContext.select(CHARGEBOX_STATUS.CHARGE_BOX_ID)
                    .from(CHARGEBOX_STATUS)
                    .where(CHARGEBOX_STATUS.CHARGE_BOX_ID.eq(chargeBoxId))
                    .fetchOne(CHARGEBOX_STATUS.CHARGE_BOX_ID);

            if (chargeBox != null) {
                if (status) {
                    dslContext.update(CHARGEBOX_STATUS)
                            .set(CHARGEBOX_STATUS.STATUS, status)
                            .set(CHARGEBOX_STATUS.CONNECTED_TIMESTAMP, DateTime.now())
                            .where(CHARGEBOX_STATUS.CHARGE_BOX_ID.eq(chargeBox))
                            .execute();
                } else {
                    dslContext.update(CHARGEBOX_STATUS)
                            .set(CHARGEBOX_STATUS.STATUS, status)
                            .set(CHARGEBOX_STATUS.DISCONNECTED_TIMESTAMP, DateTime.now())
                            .where(CHARGEBOX_STATUS.CHARGE_BOX_ID.eq(chargeBox))
                            .execute();
                }

            } else {
                if (status) {
                    dslContext.insertInto(CHARGEBOX_STATUS)
                            .set(CHARGEBOX_STATUS.CHARGE_BOX_ID, chargeBoxId)
                            .set(CHARGEBOX_STATUS.STATUS, status)
                            .set(CHARGEBOX_STATUS.CONNECTED_TIMESTAMP, DateTime.now())
                            .execute();
                } else {
                    dslContext.insertInto(CHARGEBOX_STATUS)
                            .set(CHARGEBOX_STATUS.CHARGE_BOX_ID, chargeBoxId)
                            .set(CHARGEBOX_STATUS.STATUS, status)
                            .set(CHARGEBOX_STATUS.DISCONNECTED_TIMESTAMP, DateTime.now())
                            .execute();

                }

            }
        } catch (Exception e) {
            log.error("Exception Occur In ChargerPointAvailableStatusService : " + e.getMessage());
        }
    }


}
