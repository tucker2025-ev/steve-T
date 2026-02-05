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

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class ChargerFeeExceptUserService {

    @Autowired
    @Qualifier("php")
    private DSLContext php;


    private static final Table<?> TEST_PERSONAL_CHARGER =
            DSL.table("bigtot_cms.person");

    private static final Table<?> LIVE_PERSONAL_CHARGER =
            DSL.table("bigtot_cms.personal_charger");

    private static final Field<String> CHARGER_ID = DSL.field("charger_id", String.class);
    private static final Field<String> ID_TAG = DSL.field("id_tag", String.class);

    public boolean testChargerFeeExceptUser(final String idTag, final String chargeBox) {

        Integer count = php.selectCount()
                .from(TEST_PERSONAL_CHARGER)
                .where(ID_TAG.eq(idTag))
                .and(CHARGER_ID.eq(chargeBox))
                .fetchOne(0, Integer.class);

        return count != null && count > 0;
    }

    public boolean liveChargerFeeExceptUser(final String idTag, final String chargeBox) {

        Integer count = php.selectCount()
                .from(LIVE_PERSONAL_CHARGER)
                .where(ID_TAG.eq(idTag))
                .and(CHARGER_ID.eq(chargeBox))
                .fetchOne(0, Integer.class);

        return count != null && count > 0;
    }

}
