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

import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static jooq.steve.db.Tables.VEHICLE;

@RestController
@RequestMapping("/api")
public class VehicleController {

    @Autowired
    private DSLContext dsl;

    @PostMapping(value = "/vehicle/save")
    public ResponseEntity<String> saveVehicle(
            @RequestParam(value = "idtag", required = true) String idTag,
            @RequestParam(value = "vehiclenumber", required = true) String vehicleNumber,

            @RequestParam(value = "isautocharge", required = true) boolean isAutoCharge
    ) {

        try {
            dsl.update(VEHICLE)
                    .set(VEHICLE.VEHICLE_NUMBER, vehicleNumber)
                    .set(VEHICLE.IS_ENABLE_AUTO_CHARGING, isAutoCharge)
                    .where(VEHICLE.ID_TAG.eq(idTag))
                    .orderBy(VEHICLE.ID.asc())
                    .limit(1)
                    .execute();

     if (!isAutoCharge){
         return ResponseEntity.ok("Auto Charging Disable successfully.");
     }
            return ResponseEntity.ok("Auto Charging Enable successfully.");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("DB error: " + ex.getMessage());
        }
    }


}
