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

import de.rwth.idsg.steve.service.dto.ChargerStationDTO;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import static jooq.steve.db2.tables.WalletTrackSettlement.WALLET_TRACK_SETTLEMENT;

@Slf4j
@Service
public class WalletTrackSettlementService {

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;

    @Autowired
    @Qualifier("php")
    private DSLContext php;

    private static final Table<?> CHARGE_POINT_VIEW = DSL.table("bigtot_cms.view_charger_station");

    private static final Field<String> CHARGER_ID = DSL.field("charger_id", String.class);
    private static final Field<Integer> CONNECTOR_NO = DSL.field("con_no", Integer.class);
    private static final Field<String> CONNECTOR_ID = DSL.field("con_id", String.class);
    private static final Field<String> CHARGER_QR_CODE = DSL.field("charger_qr_code", String.class);
    private static final Field<String> STATION_ID = DSL.field("station_id", String.class);
    private static final Field<String> STATION_NAME = DSL.field("station_name", String.class);
    private static final Field<String> CPO_ID = DSL.field("cpo_id", String.class);
    private static final Field<String> STATION_CITY = DSL.field("station_city", String.class);
    private static final Field<String> STATION_STATE = DSL.field("station_state", String.class);

   public void updateSettlementService(final Integer transactionId, final double unitFare,
                                 final double lastEnergy,
                                 final double updateConsumedAmount,
                                 final double totalConsumedAmount,
                                 final DateTime lastTimeStamp, final double walletAmount){

       double startEnergy = getStartEnergy(transactionId);
       double consumedEnergy = (lastEnergy - startEnergy) / 1000;
            secondary.update(WALLET_TRACK_SETTLEMENT)
               .set(WALLET_TRACK_SETTLEMENT.LAST_ENERGY, lastEnergy)
               .set(WALLET_TRACK_SETTLEMENT.CONSUMED_ENERGY, consumedEnergy)
               .set(WALLET_TRACK_SETTLEMENT.CONSUMED_AMOUNT, updateConsumedAmount)
               .set(WALLET_TRACK_SETTLEMENT.TOTAL_CONSUMED_AMOUNT, totalConsumedAmount)
               .set(WALLET_TRACK_SETTLEMENT.STOP_TIMESTAMP, lastTimeStamp)
               .where(WALLET_TRACK_SETTLEMENT.TRANSACTION_ID.eq(transactionId))
               .orderBy(WALLET_TRACK_SETTLEMENT.START_TIMESTAMP.desc())
               .limit(1)
               .execute();

   }

    private double getStartEnergy(final Integer transactionId) {
        Double startEnergy = secondary
                .select(WALLET_TRACK_SETTLEMENT.START_ENERGY)
                .from(WALLET_TRACK_SETTLEMENT)
                .where(WALLET_TRACK_SETTLEMENT.TRANSACTION_ID.eq(transactionId))
                .orderBy(WALLET_TRACK_SETTLEMENT.START_TIMESTAMP.desc())
                .limit(1)
                .fetchOneInto(Double.class);

        return startEnergy != null ? startEnergy : 0.0;
    }

    public void insertChargerTariffAmountSettlementService(final Integer transactionId,
                                                                     final double startEnergy,
                                                                     final double lastEnergy,
                                                                     final String idTag,
                                                                     final double tariffAmount,
                                                                     final double gstWithUnitFare,
                                                                     final double walletAmount,
                                                                     final double consumedEnergy,
                                                                     final double consumedAmount,
                                                                     final double totalConsumedAmount,
                                                                     final DateTime startTime,final String chargerId,final Integer connectorNo) {

        try {
            ChargerStationDTO chargerStationDTO = php
                    .select(
                            CHARGER_ID.as("chargerId"),
                            CHARGER_QR_CODE.as("chargerQrCode"),
                            CONNECTOR_NO.as("connectorNo"),
                            STATION_ID,
                            STATION_NAME,
                            CPO_ID,
                            STATION_CITY,
                            STATION_STATE
                    )
                    .from(CHARGE_POINT_VIEW)
                    .where(DSL.upper(CHARGER_ID).eq(chargerId.trim().toUpperCase()))
                    .and(CONNECTOR_NO.eq(connectorNo))
                    .fetchOneInto(ChargerStationDTO.class);

            secondary.insertInto(WALLET_TRACK_SETTLEMENT)
                    .set(WALLET_TRACK_SETTLEMENT.TRANSACTION_ID, transactionId)
                    .set(WALLET_TRACK_SETTLEMENT.ID_TAG, idTag)
                    .set(WALLET_TRACK_SETTLEMENT.START_ENERGY, startEnergy)
                    .set(WALLET_TRACK_SETTLEMENT.LAST_ENERGY, lastEnergy)
                    .set(WALLET_TRACK_SETTLEMENT.CONSUMED_ENERGY, consumedEnergy)
                    .set(WALLET_TRACK_SETTLEMENT.TARIFF_AMOUNT, tariffAmount)
                    .set(WALLET_TRACK_SETTLEMENT.GST_WITH_TARIFF_AMOUNT, gstWithUnitFare)
                    .set(WALLET_TRACK_SETTLEMENT.WALLET_AMOUNT, walletAmount)
                    .set(WALLET_TRACK_SETTLEMENT.CONSUMED_AMOUNT, consumedAmount)
                    .set(WALLET_TRACK_SETTLEMENT.TOTAL_CONSUMED_AMOUNT, totalConsumedAmount)
                    .set(WALLET_TRACK_SETTLEMENT.START_TIMESTAMP, startTime)
                    .set(WALLET_TRACK_SETTLEMENT.STOP_TIMESTAMP, startTime)
                    .set(WALLET_TRACK_SETTLEMENT.STATION_ID, chargerStationDTO.getStationId())
                    .set(WALLET_TRACK_SETTLEMENT.CPO_ID, chargerStationDTO.getCpoId())
                    .set(WALLET_TRACK_SETTLEMENT.STATION_NAME, chargerStationDTO.getStationName())
                    .set(WALLET_TRACK_SETTLEMENT.STATION_CITY, chargerStationDTO.getStationCity())
                    .set(WALLET_TRACK_SETTLEMENT.STATION_STATE, chargerStationDTO.getStationState())
                    .set(WALLET_TRACK_SETTLEMENT.CHARGER_ID, chargerStationDTO.getChargerId())
                    .set(WALLET_TRACK_SETTLEMENT.CHARGER_QR_CODE, chargerStationDTO.getChargerQrCode())
                    .set(WALLET_TRACK_SETTLEMENT.CON_NO, chargerStationDTO.getConnectorNo())
                    .set(WALLET_TRACK_SETTLEMENT.IS_ACTIVE_TRANSACTION, true)
                    .execute();

        } catch (Exception e) {
            log.error("Error inserting tariff record for tx {}: {}", transactionId, e.getMessage(), e);
        }
    }


}
