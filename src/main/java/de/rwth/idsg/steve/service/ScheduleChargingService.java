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
/// *
// * SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
// * Copyright (C) 2013-2025 SteVe Community Team
// * All Rights Reserved.
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <https://www.gnu.org/licenses/>.
// */

package de.rwth.idsg.steve.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.rwth.idsg.steve.externalconfig.ScheduledChargingMessages;
import de.rwth.idsg.steve.ocpp.ChargePointService16_InvokerImpl;
import de.rwth.idsg.steve.ocpp.OcppCallback;
import de.rwth.idsg.steve.ocpp.OcppTransport;
import de.rwth.idsg.steve.ocpp.OcppVersion;
import de.rwth.idsg.steve.ocpp.task.RemoteStartTransactionTask;
import de.rwth.idsg.steve.ocpp.ws.data.OcppJsonError;
import de.rwth.idsg.steve.repository.dto.ChargePointSelect;
import de.rwth.idsg.steve.repository.impl.TransactionRepositoryImpl;
import de.rwth.idsg.steve.web.dto.OcppJsonStatus;
import de.rwth.idsg.steve.web.dto.ocpp.RemoteStartTransactionParams;
import jooq.steve.db.tables.records.ScheduleChargingRecord;
import lombok.extern.slf4j.Slf4j;
import ocpp.cp._2015._10.RemoteStopTransactionRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static jooq.steve.db.Tables.SCHEDULE_CHARGING;
import static jooq.steve.db.Tables.TRANSACTION_STOP;

@Slf4j
@Service
public class ScheduleChargingService {

    @Autowired
    private DSLContext dslContext;
    @Autowired
    private ChargePointService16_InvokerImpl chargePointService16Invoker;
    @Autowired
    private TransactionRepositoryImpl transactionRepository;
    @Autowired
    private ManuallyStopTransaction manuallyStopTransaction;
    @Autowired
    private HomeService homeService;

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    private RestTemplate restTemplate;
    private static final String LIVE_API_URL = "http://cms.tuckerio.bigtot.in/flutter/schNotify.php";

    private static final String TEST_API_URL = "https://tuckerio.com/flutter/schNotify.php";

    @Scheduled(fixedRate = 1000)
    public void checkScheduledCharging() {
        Instant nowUtc = Instant.now();
        ZonedDateTime istTime = nowUtc.atZone(ZoneId.of("Asia/Kolkata"));
        LocalDateTime istLocalDateTime = istTime.toLocalDateTime();

        Result<ScheduleChargingRecord> toStart = dslContext
                .selectFrom(SCHEDULE_CHARGING)
                .where("DATE_FORMAT({0}, '%Y-%m-%d %H:%i:%s') = DATE_FORMAT({1}, '%Y-%m-%d %H:%i:%s')",
                        SCHEDULE_CHARGING.START_TIME,
                        Timestamp.valueOf(istLocalDateTime))
                .and(SCHEDULE_CHARGING.IS_START.eq(false)
                        .and(SCHEDULE_CHARGING.IS_ENABLE.eq(true)))
                .fetch();

        Result<ScheduleChargingRecord> toStop = dslContext
                .selectFrom(SCHEDULE_CHARGING)
                .where("DATE_FORMAT({0}, '%Y-%m-%d %H:%i:%s') = DATE_FORMAT({1}, '%Y-%m-%d %H:%i:%s')",
                        SCHEDULE_CHARGING.END_TIME,
                        Timestamp.valueOf(istLocalDateTime))
                .and(SCHEDULE_CHARGING.IS_STOP.eq(false)
                        .and(SCHEDULE_CHARGING.IS_ENABLE.eq(true)))
                .fetch();
        if (!toStart.isEmpty()) {
            for (ScheduleChargingRecord r : toStart) {
                try {
                    if (isRoutine(r.getDay())) {
                        sendRemoteStart(r.getChargeBoxId(), r.getIdtag(), Integer.parseInt(r.getConnectorId()), r.getId(), r.getStartTime(), r.getEndTime());
                    }
                    insertNextSchedules(r.getIdtag(), r.getChargeBoxId(), r.getConnectorId(), r.getDay(), r.getStartTime(), r.getEndTime());
                } catch (NullPointerException e) {

                }
            }
        }
        if (!toStop.isEmpty()) {
            for (ScheduleChargingRecord r : toStop) {
                sendRemoteStop(r.getChargeBoxId(), Integer.parseInt(r.getConnectorId()), r.getId(), r.getIdtag(), r.getEndTime());
            }
        }
        sendNotification(istLocalDateTime);
    }

    private void sendRemoteStart(String chargeBoxId, String idTag, int connectorId, Long id,
                                 DateTime startTime, DateTime endTime) {
        String formattedTime = startTime.toString("HH:mm:ss");

        if (!checkChargePointIsOnline(chargeBoxId)) {
            sendNotification(idTag,
                    "Failed to Start (Charger Offline)",
                    ScheduledChargingMessages.CHARGER_UNAVAILABLE);
            return;
        }
        Integer connectorPk = chargePointService16Invoker.getConnectorPk(chargeBoxId, connectorId);
        String status = chargePointService16Invoker.lastStatusFromConnector(connectorPk);

        if (!"Preparing".equalsIgnoreCase(status) && !"Finishing".equalsIgnoreCase(status)) {
            sendNotification(idTag,
                    "Failed to Start (Connector Busy)",
                    ScheduledChargingMessages.CONNECTOR_BUSY);
            return;
        }
        RemoteStartTransactionParams params = new RemoteStartTransactionParams();
        params.setIdTag(idTag);
        params.setConnectorId(connectorId);

        ChargePointSelect select = new ChargePointSelect(OcppTransport.JSON, chargeBoxId);
        RemoteStartTransactionTask task = new RemoteStartTransactionTask(OcppVersion.V_16, params);
        task.addCallback(new OcppCallback<String>() {
            private final ScheduledFuture<?> timeout = scheduler.schedule(() -> {
                String message = String.format(
                        ScheduledChargingMessages.FAILED_TO_START,
                        startTime
                );
                sendNotification(idTag,
                        "Failed to Start (Timeout)",
                        message);
            }, 10, TimeUnit.SECONDS);

            @Override
            public void success(String cbId, String response) {
                timeout.cancel(false);

                if (response == null) {
                    String message = String.format(
                            ScheduledChargingMessages.FAILED_TO_START,
                            startTime
                    );
                    sendNotification(idTag,
                            "Failed to Start (No Response / Offline)",
                            message);
                    return;
                }

                if ("Accepted".equalsIgnoreCase(response)) {
                    String message = String.format(
                            ScheduledChargingMessages.CHARGING_STARTED,
                            startTime
                    );


                    dslContext.update(SCHEDULE_CHARGING)
                            .set(SCHEDULE_CHARGING.IS_START, true)
                            .where(SCHEDULE_CHARGING.ID.eq(id))
                            .execute();


                    sendNotification(idTag, "Charging Started", message);

                } else if ("Rejected".equalsIgnoreCase(response)) {
                    sendNotification(idTag,
                            "Failed to Start (Rejected by Charger)",
                            ScheduledChargingMessages.FAILED_TO_START);
                }
            }

            @Override
            public void success(String cbId, OcppJsonError error) {
                timeout.cancel(false);
                sendNotification(idTag,
                        "Failed to Start (OCPP Error)",
                        ScheduledChargingMessages.FAILED_TO_START);
            }

            @Override
            public void failed(String cbId, Exception e) {
                timeout.cancel(false);
                sendNotification(idTag,
                        "Failed to Start (Exception: " + e.getMessage() + ")",
                        ScheduledChargingMessages.FAILED_TO_START);
            }
        });
        chargePointService16Invoker.remoteStartTransaction(select, task, idTag, connectorId);
    }

    private void sendRemoteStop(String chargeBoxId, int connectorId, Long id, String idTag, DateTime endTime) {

        Integer transactionId = getActiveTransactionId(chargeBoxId, connectorId);
        if (transactionId != null) {
            RemoteStopTransactionRequest request = new RemoteStopTransactionRequest();
            request.setTransactionId(transactionId);
            manuallyStopTransaction.manuallyStopTransaction(chargeBoxId, transactionId, "Scheduled stop");

            scheduler.schedule(() -> {
                String reason = retrieveStopReason(transactionId);
                if (reason != null) {
                    if (reason.equalsIgnoreCase("Scheduled stop")) {
                        String message = ScheduledChargingMessages.CHARGING_COMPLETED;
                        sendNotification(idTag, "Charging Completed", message);

                        dslContext.update(SCHEDULE_CHARGING)
                                .set(SCHEDULE_CHARGING.IS_STOP, true)
                                .set(SCHEDULE_CHARGING.IS_ENABLE, false)
                                .where(SCHEDULE_CHARGING.ID.eq(id))
                                .execute();
                    }
                } else {
                    sendNotification(idTag,
                            "Charging Not Completed",
                            "ChargeBox is Offline or No StopReason");
                }
            }, 10, TimeUnit.SECONDS);
        } else {
            String message = ScheduledChargingMessages.STOPPED_BY_SERVER;
            sendNotification(idTag,
                    "Charging Session Already Closed Before Scheduled Time new",
                    message);
        }
    }

    private boolean isRoutine(final String dayValues) {
        if (dayValues == null || dayValues.isBlank()) {
            return true;
        }
        String today = DayOfWeek.from(LocalDate.now()).name();
        return Arrays.stream(dayValues.split(","))
                .map(String::trim)
                .anyMatch(d -> d.equalsIgnoreCase(today));
    }

    public void sendNotification(LocalDateTime istLocalDateTime) {
        Result<ScheduleChargingRecord> toNotify = dslContext
                .selectFrom(SCHEDULE_CHARGING)
                .where("DATE_FORMAT({0}, '%Y-%m-%d %H:%i:%s') = DATE_FORMAT({1}, '%Y-%m-%d %H:%i:%s')",
                        SCHEDULE_CHARGING.START_TIME,
                        Timestamp.valueOf(istLocalDateTime.plusMinutes(30)))
                .and(SCHEDULE_CHARGING.IS_NOTIFY_SEND.eq(false))
                .fetch();

        for (ScheduleChargingRecord record : toNotify) {
            sendNotification(record.getIdtag(), "Reminder Before Start ", ScheduledChargingMessages.REMINDER_BEFORE_START);
            dslContext.update(SCHEDULE_CHARGING)
                    .set(SCHEDULE_CHARGING.IS_NOTIFY_SEND, true)
                    .where(SCHEDULE_CHARGING.ID.eq(record.getId()))
                    .execute();
        }
    }

    public boolean checkChargePointIsOnline(final String chargePoint) {
        List<OcppJsonStatus> onlinePoints = this.homeService.onlineChargePoint();
        if (onlinePoints == null || onlinePoints.isEmpty()) {
            return false;
        }
        return onlinePoints.stream()
                .anyMatch(status -> chargePoint.equals(status.getChargeBoxId()));
    }

    private String retrieveStopReason(final Integer transactionId) {
        if (transactionId == null) {
            return null;
        }
        return dslContext
                .select(TRANSACTION_STOP.STOP_REASON)
                .from(TRANSACTION_STOP)
                .where(TRANSACTION_STOP.TRANSACTION_PK.eq(transactionId))
                .fetchOptional(TRANSACTION_STOP.STOP_REASON)
                .orElse(null);
    }

    public void insertNextSchedules(String idTag,
                                    String chargeBoxId,
                                    String connectorId,
                                    String dayValues,
                                    DateTime startTime,
                                    DateTime stopTime) {

        List<Integer> scheduledDays = Arrays.stream(dayValues.split(","))
                .map(String::trim)
                .map(this::dayOfWeekFromString)
                .filter(d -> d != null)
                .sorted()
                .collect(Collectors.toList());
        if (scheduledDays.isEmpty()) {
            return;
        }

        DateTime now = DateTime.now();
        int today = now.getDayOfWeek();
        int daysUntilNext = 0;

        for (int i = 1; i <= 7; i++) {
            int candidateDay = ((today - 1 + i) % 7) + 1;
            if (scheduledDays.contains(candidateDay)) {
                daysUntilNext = i;
                break;
            }
        }

        DateTime nextStart = startTime.plusDays(daysUntilNext);
        long durationMillis = stopTime.getMillis() - startTime.getMillis();
        DateTime nextStop = nextStart.plus(durationMillis);

        try {
            dslContext.insertInto(SCHEDULE_CHARGING)
                    .set(SCHEDULE_CHARGING.IDTAG, idTag)
                    .set(SCHEDULE_CHARGING.CHARGE_BOX_ID, chargeBoxId)
                    .set(SCHEDULE_CHARGING.CONNECTOR_ID, connectorId)
                    .set(SCHEDULE_CHARGING.START_TIME, nextStart)
                    .set(SCHEDULE_CHARGING.END_TIME, nextStop)
                    .set(SCHEDULE_CHARGING.IS_ROUTINE, true)
                    .set(SCHEDULE_CHARGING.IS_ENABLE, true)
                    .set(SCHEDULE_CHARGING.DAY, dayValues)
                    .execute();
        } catch (Exception e) {
            log.error("Schedule Charging Service" + e.getMessage());
        }
    }

    private Integer dayOfWeekFromString(String day) {
        switch (day.toUpperCase()) {
            case "MONDAY":
                return DateTimeConstants.MONDAY;
            case "TUESDAY":
                return DateTimeConstants.TUESDAY;
            case "WEDNESDAY":
                return DateTimeConstants.WEDNESDAY;
            case "THURSDAY":
                return DateTimeConstants.THURSDAY;
            case "FRIDAY":
                return DateTimeConstants.FRIDAY;
            case "SATURDAY":
                return DateTimeConstants.SATURDAY;
            case "SUNDAY":
                return DateTimeConstants.SUNDAY;
            default:
                return null;
        }
    }

    private Integer getActiveTransactionId(String chargeBoxId, int connectorId) {
        List<Integer> activeTxIds = transactionRepository.getActiveTransactionIds(chargeBoxId);
        for (Integer txId : activeTxIds) {
            if (!isTransactionStopped(txId)) {
                return txId;
            }
        }
        return null;
    }

    private boolean isTransactionStopped(Integer txId) {
        return dslContext.fetchExists(
                dslContext.selectOne()
                        .from(TRANSACTION_STOP)
                        .where(TRANSACTION_STOP.TRANSACTION_PK.eq(txId))
        );
    }

    public void sendNotification(String idtag, String title, String message) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("idtag", idtag);
            requestBody.put("message", message);
            requestBody.put("title", title);
            requestBody.put("payload", "Schedule");

            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(requestBody);
            HttpEntity<String> request = new HttpEntity<>(json, headers);

            ResponseEntity<String> response =
                    restTemplate.postForEntity(LIVE_API_URL, request, String.class);

        } catch (Exception e) {
            log.error("SendNotification Error Occur : ");
        }
    }

    @Scheduled(cron = "0 30 17 * * *", zone = "UTC")
    public void runEveryNightIstEquivalent() {
        LocalDateTime start = LocalDateTime.now(ZoneId.of("Asia/Kolkata"))
                .withHour(23).withMinute(0).withSecond(0).withNano(0);
        LocalDateTime end = start.plusDays(1).withHour(6).withMinute(0).withSecond(0).withNano(0);

        Timestamp sqlStart = Timestamp.valueOf(start);
        Timestamp sqlEnd = Timestamp.valueOf(end);

        Result<ScheduleChargingRecord> schedules = dslContext
                .selectFrom(SCHEDULE_CHARGING)
                .where("({0} BETWEEN {1} AND {2})",
                        SCHEDULE_CHARGING.START_TIME, sqlStart, sqlEnd)
                .and(SCHEDULE_CHARGING.IS_ENABLE.eq(true))
                .fetch();


        for (ScheduleChargingRecord record : schedules) {
            String message = String.format(
                    "%s Scheduled to start at %s IST.",
                    ScheduledChargingMessages.REMINDER_BEFORE_START1,
                    record.getStartTime()
            );
            sendNotification(record.getIdtag(), "Overnight Charging Alert ", message);
        }
    }

}




