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

import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.ocpp.ws.ocpp16.Ocpp16WebSocketEndpoint;
import de.rwth.idsg.steve.repository.dto.ChargePoint;
import de.rwth.idsg.steve.repository.dto.ConnectorStatus;
import de.rwth.idsg.steve.repository.dto.Transaction;
import de.rwth.idsg.steve.repository.impl.ChargePointRepositoryImpl;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.web.dto.OcppJsonStatus;
import de.rwth.idsg.steve.web.dto.ServerChargerCountResponse;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

import static jooq.steve.db.tables.ChargerServer.CHARGER_SERVER;

@Slf4j
@Service
public class HomeService {

    @Autowired
    private ChargePointRepositoryImpl chargePointRepository;

    @Autowired
    private ChargePointHelperService chargePointHelperService;

    @Autowired
    private Ocpp16WebSocketEndpoint ocpp16WebSocketEndpoint;

    @Autowired
    private DSLContext dslContext;

    @Autowired
    private RestTemplate restTemplate;


    @Autowired
    private TransactionRepositoryImpl transactionRepository;

    public List<OcppJsonStatus> onlineChargePoint() {
        List<OcppJsonStatus> allStatuses = this.chargePointHelperService.getOcppJsonStatus();
        Map<String, OcppJsonStatus> uniqueMap = new LinkedHashMap<>();

        for (OcppJsonStatus status : allStatuses) {
            uniqueMap.putIfAbsent(status.getChargeBoxId(), status);
        }

        return new ArrayList<>(uniqueMap.values());
    }


    public List<ConnectorStatus> findCConnectedChargeBoxConnectors(List<ConnectorStatus> connectorStatusList) {
        List<OcppJsonStatus> online = chargePointHelperService.getOcppJsonStatus();

        Set<String> onlineChargeBoxIds = online.stream()
                .map(OcppJsonStatus::getChargeBoxId)
                .collect(Collectors.toSet());

        return connectorStatusList.stream()
                .filter(cs -> onlineChargeBoxIds.contains(cs.getChargeBoxId()))
                .collect(Collectors.toList());
    }


    public List<OcppJsonStatus> offlineChargePointFind() {
        List<ChargePoint.Overview> overviewList = chargePointRepository.getChargePointDetails();
        List<OcppJsonStatus> online = chargePointHelperService.getOcppJsonStatus();


        Set<String> onlineIds = online.stream()
                .map(OcppJsonStatus::getChargeBoxId)
                .collect(Collectors.toSet());

        return overviewList.stream()
                .filter(overview -> !onlineIds.contains(overview.getChargeBoxId()))
                .map(overview -> {
                    DateTime connectedSinceDT = overview.getLastHeartbeatTimestampDT();
                    String connectedSinceStr = connectedSinceDT != null
                            ? connectedSinceDT.toString("yyyy-MM-dd HH:mm:ss")
                            : null;

                    String connectionDuration = getDurationString(connectedSinceDT);

                    return OcppJsonStatus.builder()
                            .chargeBoxPk(overview.getChargeBoxPk())
                            .chargeBoxId(overview.getChargeBoxId())
                            .connectedSince(connectedSinceStr)
                            .connectionDuration(connectionDuration)
                            .connectedSinceDT(connectedSinceDT)
                            .version(OcppVersion.V_16)
                            .build();
                })
                .collect(Collectors.toList());
    }


    private String getDurationString(DateTime connectedSince) {
        if (connectedSince == null) return null;
        Duration duration = new Duration(connectedSince, DateTime.now());

        long hours = duration.getStandardHours();
        long minutes = duration.getStandardMinutes() % 60;
        long seconds = duration.getStandardSeconds() % 60;

        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }


//    public int countWssChargePoints() {
//        Integer result = dslContext
//                .selectCount()
//                .from(CHARGER_SERVER)
//                .where(
//                        CHARGER_SERVER.SERVER_URL.in(
//                                "ws://15.207.37.132:9081/tuckermotors",
//                                "ws://tuckerio.com:9081/tuckermotors",
//                                "ws://tuckerio.com:8443/tuckermotors"
//                        )
//                )
//                .fetchOneInto(Integer.class);
//
//        return result != null ? result : 0;
//    }
//
//
//    public int countWsChargePoints() {
//        Integer result = dslContext
//                .selectCount()
//                .from(CHARGER_SERVER)
//                .where(
//                        CHARGER_SERVER.SERVER_URL.in(
//                                "ws://cms.tuckerio.bigtot.in:9081/tuckermotors"
//                        )
//                )
//                .fetchOneInto(Integer.class);
//
//        return result != null ? result : 0;
//    }
//
//
//    public Integer wssChargerAvailableCount() {
//        try {
//            RestTemplate restTemplate = new RestTemplate(getRequestFactory());
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("STEVE-API-KEY", "Tucker");
//
//            HttpEntity<String> entity = new HttpEntity<>(headers);
//
//            ResponseEntity<ServerChargerCountResponse> response =
//                    restTemplate.exchange(
//                            "http://cms.tuckerio.bigtot.in:9081/api/Charger/WssChargerAvailableCount",
//                            HttpMethod.GET,
//                            entity,
//                            ServerChargerCountResponse.class
//                    );
//
//            if (response.getBody() != null && response.getBody().getCount() != null) {
//                return response.getBody().getCount();
//            }
//
//            return 0;
//
//        } catch (Exception ex) {
//            log.error("WSS API unreachable: " + ex.getMessage());
//            return 0;
//        }
//    }
//
//
//    public Integer wssChargerUnAvailableCount() {
//        try {
//            RestTemplate restTemplate = new RestTemplate(getRequestFactory());
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("STEVE-API-KEY", "Tucker");
//
//            HttpEntity<String> entity = new HttpEntity<>(headers);
//
//            ResponseEntity<ServerChargerCountResponse> response =
//                    restTemplate.exchange(
//                            "http://cms.tuckerio.bigtot.in:9081/api/Charger/WssChargerUnAvailableCount",
//                            HttpMethod.GET,
//                            entity,
//                            ServerChargerCountResponse.class
//                    );
//
//            if (response.getBody() != null && response.getBody().getCount() != null) {
//                return response.getBody().getCount();
//            }
//
//            return 0;
//
//        } catch (Exception ex) {
//            log.error("WSS API unreachable: " + ex.getMessage());
//            return 0;
//        }
//    }
//
//
//    public Integer wssActiveTransactionCount() {
//        try {
//            RestTemplate restTemplate = new RestTemplate(getRequestFactory());
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("STEVE-API-KEY", "Tucker");
//
//            HttpEntity<String> entity = new HttpEntity<>(headers);
//
//            ResponseEntity<ServerChargerCountResponse> response =
//                    restTemplate.exchange(
//                            "http://cms.tuckerio.bigtot.in:9081/api/Charger/active/trans",
//                            HttpMethod.GET,
//                            entity,
//                            ServerChargerCountResponse.class
//                    );
//
//            if (response.getBody() != null && response.getBody().getCount() != null) {
//                return response.getBody().getCount();
//            }
//
//            return 0;
//
//        } catch (Exception ex) {
//            log.error("WSS API unreachable: " + ex.getMessage());
//            return 0;
//        }
//    }
//
//
//    public List<OcppJsonStatus> retrieveServerChargerDetails() {
//        try {
//            RestTemplate restTemplate = new RestTemplate(getRequestFactory());
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("STEVE-API-KEY", "Tucker");
//            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
//
//            HttpEntity<Void> entity = new HttpEntity<>(headers);
//
//            ResponseEntity<List<OcppJsonStatus>> response =
//                    restTemplate.exchange(
//                            "http://cms.tuckerio.bigtot.in:9081/api/Charger/ocppJsonStatus",
//                            HttpMethod.GET,
//                            entity,
//                            new ParameterizedTypeReference<List<OcppJsonStatus>>() {
//                            }
//                    );
//
//            return response.getBody();
//
//        } catch (Exception ex) {
//            log.error("WSS API unreachable", ex);
//            return Collections.emptyList();
//        }
//    }
//
//
//    public List<Transaction> retrieveServerTransactionList() {
//        try {
//            RestTemplate restTemplate = new RestTemplate(getRequestFactory());
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("STEVE-API-KEY", "Tucker");
//            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
//
//            HttpEntity<Void> entity = new HttpEntity<>(headers);
//
//            ResponseEntity<List<Transaction>> response =
//                    restTemplate.exchange(
//                            "http://cms.tuckerio.bigtot.in:9081/api/Charger/getAll/trans",
//                            HttpMethod.GET,
//                            entity,
//                            new ParameterizedTypeReference<List<Transaction>>() {
//                            }
//                    );
//
//            return response.getBody();
//
//        } catch (Exception ex) {
//            log.error("WSS API unreachable", ex);
//            return Collections.emptyList();
//        }
//    }
//
//
//    public List<Transaction> retrieveAllTransactionList() {
//        try {
//            RestTemplate restTemplate = new RestTemplate(getRequestFactory());
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("STEVE-API-KEY", "Tucker");
//            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
//
//            HttpEntity<Void> entity = new HttpEntity<>(headers);
//
//            ResponseEntity<List<Transaction>> response =
//                    restTemplate.exchange(
//                            "http://cms.tuckerio.bigtot.in:9081/api/Charger/get/total/trans",
//                            HttpMethod.GET,
//                            entity,
//                            new ParameterizedTypeReference<List<Transaction>>() {
//                            }
//                    );
//
//            return response.getBody();
//
//        } catch (Exception ex) {
//            log.error("WSS API unreachable", ex);
//            return Collections.emptyList();
//        }
//    }
//
//
//    public List<OcppJsonStatus> retrieveTestServerChargerDetails() {
//        try {
//            RestTemplate restTemplate = new RestTemplate(getRequestFactory());
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("STEVE-API-KEY", "Tucker");
//            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
//
//            HttpEntity<Void> entity = new HttpEntity<>(headers);
//
//            ResponseEntity<List<OcppJsonStatus>> response =
//                    restTemplate.exchange(
//                            "https://tuckerio.com:8443/api/Charger/ocppJsonStatus",
//                            HttpMethod.GET,
//                            entity,
//                            new ParameterizedTypeReference<List<OcppJsonStatus>>() {
//                            }
//                    );
//
//            return response.getBody();
//
//        } catch (Exception ex) {
//            log.error("WSS API unreachable", ex);
//            return Collections.emptyList();
//        }
//    }
//
//    private ClientHttpRequestFactory getRequestFactory() {
//        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
//        factory.setConnectTimeout(2000);
//        factory.setReadTimeout(2000);
//        return factory;
//    }


}
