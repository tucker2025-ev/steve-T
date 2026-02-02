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
package de.rwth.idsg.steve.ocpp.soap;

import de.rwth.idsg.steve.ocpp.OcppProtocol;
import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.service.CentralSystemService16_Service;
import de.rwth.idsg.steve.service.TransactionStopService;
import de.rwth.idsg.steve.web.dto.TransactionQueryForm;
import lombok.extern.slf4j.Slf4j;
import ocpp.cs._2015._10.*;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.jws.WebService;
import javax.xml.ws.AsyncHandler;
import javax.xml.ws.BindingType;
import javax.xml.ws.Response;
import javax.xml.ws.soap.Addressing;
import javax.xml.ws.soap.SOAPBinding;
import java.util.List;
import java.util.concurrent.Future;

import static jooq.steve.db.Tables.*;
import static jooq.steve.db.tables.Connector.CONNECTOR;
import static jooq.steve.db2.Tables.LIVE_CHARGING_DATA;

/**
 * @author Sevket Goekay <sevketgokay@gmail.com>
 * @since 13.03.2018
 */
@Slf4j
@Service
@Addressing(enabled = true, required = false)
@BindingType(value = SOAPBinding.SOAP12HTTP_BINDING)
@WebService(
        serviceName = "CentralSystemService",
        portName = "CentralSystemServiceSoap12",
        targetNamespace = "urn://Ocpp/Cs/2015/10/",
        endpointInterface = "ocpp.cs._2015._10.CentralSystemService")
public class CentralSystemService16_SoapServer implements CentralSystemService {

    @Autowired
    private CentralSystemService16_Service service;

    @Autowired
    private DSLContext dslContext;
    @Autowired
    private TransactionRepositoryImpl transactionRepositoryImpl;
    @Autowired
    private TransactionStopService transactionStopService;

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;

    public BootNotificationResponse bootNotificationWithTransport(BootNotificationRequest parameters,
                                                                  String chargeBoxIdentity, OcppProtocol protocol) {
        if (protocol.getVersion() != OcppVersion.V_16) {
            throw new IllegalArgumentException("Unexpected OCPP version: " + protocol.getVersion());
        }
        return service.bootNotification(parameters, chargeBoxIdentity, protocol);
    }

    @Override
    public BootNotificationResponse bootNotification(BootNotificationRequest parameters, String chargeBoxIdentity) {
        return this.bootNotificationWithTransport(parameters, chargeBoxIdentity, OcppProtocol.V_16_SOAP);
    }

    @Override
    public FirmwareStatusNotificationResponse firmwareStatusNotification(FirmwareStatusNotificationRequest parameters,
                                                                         String chargeBoxIdentity) {
        return service.firmwareStatusNotification(parameters, chargeBoxIdentity);
    }

    @Override
    public StatusNotificationResponse statusNotification(StatusNotificationRequest parameters,
                                                         String chargeBoxIdentity) {
        return service.statusNotification(parameters, chargeBoxIdentity);
    }

    @Override
    public MeterValuesResponse meterValues(MeterValuesRequest parameters, String chargeBoxIdentity) {
        return service.meterValues(parameters, chargeBoxIdentity);
    }

    @Override
    public DiagnosticsStatusNotificationResponse diagnosticsStatusNotification(
            DiagnosticsStatusNotificationRequest parameters, String chargeBoxIdentity) {
        return service.diagnosticsStatusNotification(parameters, chargeBoxIdentity);
    }

    @Override
    public StartTransactionResponse startTransaction(StartTransactionRequest parameters, String chargeBoxIdentity) {
        if (isConnectorEligibleForStartTransaction(chargeBoxIdentity, parameters.getConnectorId())) {
            String originalIdTag = parameters.getIdTag();
            parameters.setIdTag(retrieveIdTagFromVehicleOrRfidDetails(parameters.getIdTag()));
            StartTransactionResponse response = service.startTransaction(parameters, chargeBoxIdentity);
            setStartTransactionRequest(response.getTransactionId(), identifyStartTransactionRequest(originalIdTag), originalIdTag);
            return response;
        } else {
            StartTransactionResponse response = new StartTransactionResponse();
            IdTagInfo idTagInfo = new IdTagInfo();
            idTagInfo.setStatus(AuthorizationStatus.CONCURRENT_TX);
            response.setIdTagInfo(idTagInfo);

            return response;
        }
    }

    @Override
    public StopTransactionResponse stopTransaction(StopTransactionRequest parameters, String chargeBoxIdentity) {
        return service.stopTransaction(parameters, chargeBoxIdentity);
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest parameters, String chargeBoxIdentity) {
        return service.heartbeat(parameters, chargeBoxIdentity);
    }

    @Override
    public AuthorizeResponse authorize(AuthorizeRequest parameters, String chargeBoxIdentity) {
        parameters.setIdTag(retrieveIdTagFromVehicleOrRfidDetails(parameters.getIdTag()));
        return service.authorize(parameters, chargeBoxIdentity);
    }

    @Override
    public DataTransferResponse dataTransfer(DataTransferRequest parameters, String chargeBoxIdentity) {
        return service.dataTransfer(parameters, chargeBoxIdentity);
    }

    // -------------------------------------------------------------------------
    // No-op
    // -------------------------------------------------------------------------

    @Override
    public Response<StopTransactionResponse> stopTransactionAsync(StopTransactionRequest parameters,
                                                                  String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> stopTransactionAsync(StopTransactionRequest parameters, String chargeBoxIdentity,
                                          AsyncHandler<StopTransactionResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<StatusNotificationResponse> statusNotificationAsync(StatusNotificationRequest parameters,
                                                                        String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> statusNotificationAsync(StatusNotificationRequest parameters, String chargeBoxIdentity,
                                             AsyncHandler<StatusNotificationResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<AuthorizeResponse> authorizeAsync(AuthorizeRequest parameters, String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> authorizeAsync(AuthorizeRequest parameters, String chargeBoxIdentity,
                                    AsyncHandler<AuthorizeResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<StartTransactionResponse> startTransactionAsync(StartTransactionRequest parameters,
                                                                    String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> startTransactionAsync(StartTransactionRequest parameters, String chargeBoxIdentity,
                                           AsyncHandler<StartTransactionResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<FirmwareStatusNotificationResponse> firmwareStatusNotificationAsync(
            FirmwareStatusNotificationRequest parameters, String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> firmwareStatusNotificationAsync(FirmwareStatusNotificationRequest parameters,
                                                     String chargeBoxIdentity,
                                                     AsyncHandler<FirmwareStatusNotificationResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<BootNotificationResponse> bootNotificationAsync(BootNotificationRequest parameters,
                                                                    String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> bootNotificationAsync(BootNotificationRequest parameters, String chargeBoxIdentity,
                                           AsyncHandler<BootNotificationResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<HeartbeatResponse> heartbeatAsync(HeartbeatRequest parameters, String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> heartbeatAsync(HeartbeatRequest parameters, String chargeBoxIdentity,
                                    AsyncHandler<HeartbeatResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<MeterValuesResponse> meterValuesAsync(MeterValuesRequest parameters, String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> meterValuesAsync(MeterValuesRequest parameters, String chargeBoxIdentity,
                                      AsyncHandler<MeterValuesResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<DataTransferResponse> dataTransferAsync(DataTransferRequest parameters, String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> dataTransferAsync(DataTransferRequest parameters, String chargeBoxIdentity,
                                       AsyncHandler<DataTransferResponse> asyncHandler) {
        return null;
    }

    @Override
    public Response<DiagnosticsStatusNotificationResponse> diagnosticsStatusNotificationAsync(
            DiagnosticsStatusNotificationRequest parameters, String chargeBoxIdentity) {
        return null;
    }

    @Override
    public Future<?> diagnosticsStatusNotificationAsync(DiagnosticsStatusNotificationRequest parameters,
                                                        String chargeBoxIdentity,
                                                        AsyncHandler<DiagnosticsStatusNotificationResponse> asyncHandler) {
        return null;
    }


    private String retrieveIdTagFromVehicleOrRfidDetails(String value) {
        boolean tagExists = dslContext
                .fetchExists(
                        dslContext.selectFrom(OCPP_TAG)
                                .where(OCPP_TAG.ID_TAG.eq(value))
                );

        if (!tagExists) {
            value = dslContext.select(RFID_CARD.ID_TAG)
                    .from(RFID_CARD)
                    .where(
                            RFID_CARD.ISTRUE.eq(true)
                                    .and(
                                            RFID_CARD.AC_TAG.eq(value)
                                                    .or(RFID_CARD.DC_TAG.eq(value))
                                                    .or(RFID_CARD.HEX_TAG.eq(value))
                                    )
                    )
                    .union(
                            dslContext.select(VEHICLE.ID_TAG)
                                    .from(VEHICLE)
                                    .where(VEHICLE.VID_NUMBER.eq(value))
                                    .and(VEHICLE.IS_ENABLE_AUTO_CHARGING.eq(true))
                    )
                    .fetchOneInto(String.class);

        }


        return value;
    }

    private String identifyStartTransactionRequest(String value) {
        boolean tagExists = dslContext.fetchExists(
                dslContext.selectFrom(OCPP_TAG)
                        .where(OCPP_TAG.ID_TAG.eq(value))
        );

        if (tagExists) {
            return "APP";
        }

        String rfidTag = dslContext.select(RFID_CARD.ID_TAG)
                .from(RFID_CARD)
                .where(RFID_CARD.ISTRUE.eq(true)
                        .and(
                                RFID_CARD.AC_TAG.eq(value)
                                        .or(RFID_CARD.DC_TAG.eq(value))
                                        .or(RFID_CARD.HEX_TAG.eq(value))
                        )
                )
                .fetchOneInto(String.class);

        if (rfidTag != null) {
            return "RFID_CARD";
        }

        String vehicleTag = dslContext.select(VEHICLE.ID_TAG)
                .from(VEHICLE)
                .where(VEHICLE.VID_NUMBER.eq(value))
                .and(VEHICLE.IS_ENABLE_AUTO_CHARGING.eq(true))
                .fetchOneInto(String.class);

        if (vehicleTag != null) {
            return "AUTO-CHARGE";
        }

        return null;
    }


    private void setStartTransactionRequest(Integer txId, String startType, String idTag) {
        try {
            dslContext.update(TRANSACTION_START)
                    .set(TRANSACTION_START.START_TYPE, startType)
                    .set(TRANSACTION_START.START_TAG_ID, idTag)
                    .set(TRANSACTION_START.START_SERVER,"Test")
                    .where(TRANSACTION_START.TRANSACTION_PK.eq(txId))
                    .execute();

            DateTime dateTime = dslContext
                    .select(TRANSACTION_START.START_TIMESTAMP)
                    .from(TRANSACTION_START)
                    .where(TRANSACTION_START.TRANSACTION_PK.eq(txId))
                    .fetchOne(TRANSACTION_START.START_TIMESTAMP, DateTime.class);
            secondary.update(LIVE_CHARGING_DATA)
                    .set(LIVE_CHARGING_DATA.START_TYPE, startType)
                    .set(LIVE_CHARGING_DATA.START_TIMESTAMP, dateTime)
                    .where(LIVE_CHARGING_DATA.TRANSACTION_ID.eq(txId))
                    .execute();
        } catch (Exception e) {
            log.error("CentralSystemService16_SoapSer setStartTransactionRequest Method Error Occur : " + e.getMessage());
        }


    }


    public boolean isConnectorEligibleForStartTransaction(String chargeBoxId, Integer connectorId) {
        List<Transaction> obj = transactionRepositoryImpl.getTransactions(new TransactionQueryForm());

        for (Transaction transaction : obj) {
            String chargePoint = transaction.getChargeBoxId();
            Integer connector = transaction.getConnectorId();

            if (chargeBoxId.equalsIgnoreCase(chargePoint) && connector.equals(connectorId)) {

                String status = lastStatusFromConnector(getConnectorPk(chargeBoxId, connectorId));

                if (!status.equalsIgnoreCase("Charging")) {
                    transactionStopService.stop(transaction.getId());
                    return true;
                } else {
                    return false;
                }
            }
        }

        return true;
    }

    public String lastStatusFromConnector(Integer connectorPk) {
        return dslContext.select(CONNECTOR_STATUS.STATUS)
                .from(CONNECTOR_STATUS)
                .where(CONNECTOR_STATUS.CONNECTOR_PK.eq(connectorPk))
                .orderBy(CONNECTOR_STATUS.STATUS_TIMESTAMP.desc())
                .limit(1)
                .fetchOne(CONNECTOR_STATUS.STATUS);
    }

    private Integer getConnectorPk(String chargeBoxId, Integer connectorId) {

        return dslContext.select(CONNECTOR.CONNECTOR_PK)
                .from(CONNECTOR)
                .where(CONNECTOR.CHARGE_BOX_ID.eq(chargeBoxId))
                .and(CONNECTOR.CONNECTOR_ID.eq(connectorId))
                .fetchOne(CONNECTOR.CONNECTOR_PK);
    }

}
