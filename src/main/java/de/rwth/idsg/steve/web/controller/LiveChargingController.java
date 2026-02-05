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

import de.rwth.idsg.steve.service.LiveChargingService;
import de.rwth.idsg.steve.service.dto.LiveChargingResponse;
import de.rwth.idsg.steve.web.dto.LiveChargingApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
@Slf4j
public class LiveChargingController {

    @Autowired
    private LiveChargingService liveChargingService;

    @GetMapping("/live-charging")
    public ResponseEntity<LiveChargingApiResponse> getLiveCharging() {
        try {
            List<LiveChargingResponse> responses =
                    liveChargingService.getLiveChargingResponses();

            if (responses.isEmpty()) {
                return ResponseEntity.ok(
                        new LiveChargingApiResponse("false", List.of())
                );
            }

            return ResponseEntity.ok(
                    new LiveChargingApiResponse("true", responses)
            );

        } catch (Exception e) {
            log.error("Error fetching live charging data", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new LiveChargingApiResponse("false", List.of()));
        }
    }
}
