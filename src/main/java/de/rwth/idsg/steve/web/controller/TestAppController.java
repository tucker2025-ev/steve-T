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

import de.rwth.idsg.steve.service.TestAppService;
import de.rwth.idsg.steve.service.testmobiledto.ResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/test")
public class TestAppController {

    @Autowired
    private TestAppService service;

    @PostMapping("/start")
    public ResponseDTO startTransaction(
            @RequestParam final String chargerConnectorQrCode,
            @RequestParam final String idTag) {
        return service.startTransaction(chargerConnectorQrCode, idTag);
    }

    @PostMapping("/stop")
    public ResponseDTO stopTransaction(
            @RequestParam final Integer transactionId) {

        return service.stopTransaction(transactionId, "Remote");
    }

    @GetMapping("/transaction/active")
    public ResponseDTO getActiveTransactionByIdTag(
            @RequestParam String idTag) {
        return service.getActiveTransactionByIdTag(idTag);
    }

    @GetMapping("/transactions/all")
    public ResponseDTO getAllTransactionByIdTag(
            @RequestParam String idTag) {
        return service.getAlleTransactionByIdTag(idTag);
    }


    @GetMapping("/transaction/summary")
    public ResponseDTO transactionSummary(
            @RequestParam Integer transactionId) {

        return service.transactionSummary(transactionId);

    }

    @GetMapping("/transaction/Graph")
    public ResponseDTO transactionGraphData(
            @RequestParam Integer transactionId) {
        return service.retrieveGraphDataByTransactionId(transactionId);

    }

}
