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

import de.rwth.idsg.steve.ocpp.ChargePointService16_InvokerImpl;
import de.rwth.idsg.steve.ocpp.OcppTransport;
import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.ocpp.task.RemoteStartTransactionTask;
import de.rwth.idsg.steve.repository.TaskStore;
import de.rwth.idsg.steve.repository.dto.ChargePointSelect;
import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.service.HomeService;
import de.rwth.idsg.steve.web.dto.*;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStartTransactionParams;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static jooq.steve.db.Tables.CHARGER_SERVER;

@RestController
@RequestMapping("/api/Charger")
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class ChargerServerStausController {

    @Autowired
    private DSLContext dslContext;

    @Autowired
    private ChargePointService16_InvokerImpl chargePointService16Invoker;

    @Autowired
    protected TaskStore taskStore;

    @Autowired
    private HomeService homeService;

    @Autowired
    private TransactionRepositoryImpl transactionRepository;


    private static final String SERVER_URL_TEST =
            "ws://15.207.37.132:9081/tuckermotors";

    private static final String SERVER_URL_TEST_WSS =
            "wss://tuckerio.com:8443/tuckermotors";

    private static final String SERVER_URL_LIVE =
            "ws://cms.tuckerio.bigtot.in:9081/tuckermotors";

    @GetMapping("/server/{qrcode}")
    public ResponseDTO get(@PathVariable("qrcode") final String qrcode) {

        return getChargerServer(qrcode);

    }

    @PostMapping("/start")
    public void start(@RequestBody final RemoteStartDTO remoteStartDTO) {
        sendRemoteStart(remoteStartDTO.getChargeBoxId(), remoteStartDTO.getIdTag(), remoteStartDTO.getConnectorId());
    }

    private ResponseDTO getChargerServer(final String chargerQrCode) {

        ResponseDTO response = new ResponseDTO();
        String serverUrl = dslContext
                .select(CHARGER_SERVER.SERVER_URL)
                .from(CHARGER_SERVER)
                .where(CHARGER_SERVER.CHARGER_QR_CODE.eq(chargerQrCode))
                .fetchOneInto(String.class);
        if (serverUrl == null) {
            response.setStatus("NOT_FOUND");
            response.setIp(null);
            return response;
        }
        String status;
        if (SERVER_URL_LIVE.equalsIgnoreCase(serverUrl)) {
            status = "LIVE";
        } else if (SERVER_URL_TEST.equalsIgnoreCase(serverUrl)) {
            status = "TEST";
        } else if (SERVER_URL_TEST_WSS.equalsIgnoreCase(serverUrl)) {
            status = "TEST";
        } else {
            status = "UNKNOWN";
        }

        response.setStatus(status);
        response.setIp(serverUrl);
        return response;
    }


    public void sendRemoteStart(final String chargeBoxId, final String idTag, final int connectorId) {

        RemoteStartTransactionParams params = new RemoteStartTransactionParams();
        params.setIdTag(idTag);
        params.setConnectorId(connectorId);

        ChargePointSelect select = new ChargePointSelect(OcppTransport.JSON, chargeBoxId);
        RemoteStartTransactionTask task = new RemoteStartTransactionTask(OcppVersion.V_16, params);
        chargePointService16Invoker.remoteStartTransaction(select, task, idTag, connectorId);
        taskStore.add(task);
    }

//    @GetMapping("/wss/chargers/count")
//    public ServerChargerCountResponse wssChargerCount() {
//        Integer count = homeService.countWssChargePoints();
//        return new ServerChargerCountResponse(count);
//    }


    @GetMapping("/WssChargerAvailableCount")
    public ServerChargerCountResponse wssChargerAvailableCount() {
        Integer count = homeService.onlineChargePoint().size();
        return new ServerChargerCountResponse(count);
    }

//    @GetMapping("/WssChargerUnAvailableCount")
//    public ServerChargerCountResponse wssChargerUnAvailableCount() {
//        int totalWssChargers = homeService.countWssChargePoints();
//        int connected = homeService.onlineChargePoint().size();
//        Integer count = totalWssChargers - connected;
//        return new ServerChargerCountResponse(count);
//    }


    @GetMapping("/active/trans")
    public ServerChargerCountResponse activeTransactionWss() {

        // Fetch all active transactions
        List<Transaction> txList = transactionRepository.getTransactions(getActiveForm());

        return new ServerChargerCountResponse(txList.size());
    }

//    @GetMapping("/ws/connected/count")
//    public Integer wsChargerCount() {
//        return homeService.countWsChargePoints();
//    }

//    @GetMapping("/getAll/trans")
//    public List<Transaction> getAllTransaction() {
//        return  homeService.retrieveServerTransactionList();
//    }

    @GetMapping("/get/total/trans")
    public List<Transaction> getTotalTransaction() {
        return transactionRepository.getAllTransactions(getForm());
    }

    @GetMapping("active/transaction")
    public List<Transaction> GetActiveTransaction() {
        return transactionRepository.getTransactions(getActiveForm());
    }

    @GetMapping("/ocppJsonStatus")
    public List<OcppJsonStatus> retrieveOcppJsonStatus() {
        return homeService.onlineChargePoint();
    }


    private TransactionQueryForm getForm() {
        TransactionQueryForm form = new TransactionQueryForm();
        form.setChargeBoxId(null);
        form.setOcppIdTag(null);
        form.setFrom(null);
        form.setTo(null);
        form.setTransactionPk(null);
        form.setReturnCSV(false);
        form.setType(TransactionQueryForm.QueryType.ALL);
        form.setPeriodType(TransactionQueryForm.QueryPeriodType.ALL);

        return form;
    }


    private TransactionQueryForm getActiveForm() {
        TransactionQueryForm form = new TransactionQueryForm();
        form.setChargeBoxId(null);
        form.setOcppIdTag(null);
        form.setFrom(null);
        form.setTo(null);
        form.setTransactionPk(null);
        form.setReturnCSV(false);
        form.setType(TransactionQueryForm.QueryType.ALL);
        form.setPeriodType(TransactionQueryForm.QueryPeriodType.ALL);

        return form;
    }


}
