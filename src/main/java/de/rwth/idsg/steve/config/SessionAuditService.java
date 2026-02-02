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
package de.rwth.idsg.steve.config;

import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Set;

import static jooq.steve.db.Tables.USER_SESSION_AUDIT;

@Service
@Slf4j
public class SessionAuditService {

    @Autowired
    private DSLContext dsl;

    private static final Set<String> IGNORED_IPS = Set.of(
            "15.207.37.132",
            "13.233.175.29"
    );

    public void updateLogout(String sessionId, String ip) {
        try {
            if (!IGNORED_IPS.contains(ip)) {
                dsl.update(USER_SESSION_AUDIT)
                        .set(USER_SESSION_AUDIT.SIGNOUT_TIME, DateTime.now())
                        .where(USER_SESSION_AUDIT.SESSION_ID.eq(sessionId))
                        .and(USER_SESSION_AUDIT.IP_ADDRESS.eq(ip))
                        .execute();

                log.info("Session logout updated for sessionId={}", sessionId);
            }
        } catch (Exception e) {
            log.error("Failed to update logout for sessionId={}", sessionId, e);
        }
    }
}
