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

import de.rwth.idsg.steve.service.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

import static jooq.steve.db.Tables.TRANSACTION;
import static jooq.steve.db2.Tables.LIVE_CHARGING_DATA;

@Slf4j
@Service
public class TestChargingData {

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;

    @Autowired
    @Qualifier("php")
    private DSLContext php;

    @Autowired DSLContext ctx;
    @Autowired private LiveChargingRuntimeService runtimeService;

    private static final Table<?> CHARGE_POINT_VIEW = DSL.table("bigtot_cms.view_charger_station");

    private static final Field<String> CHARGER_ID = DSL.field("charger_id", String.class);
    private static final Field<Integer> CONNECTOR_NO = DSL.field("con_no", Integer.class);
    private static final Field<String> CONNECTOR_ID = DSL.field("con_id", String.class);
    private static final Field<String> CHARGER_QR_CODE = DSL.field("charger_qr_code", String.class);
    private static final Field<String> CHARGER_TYPE = DSL.field("charger_type", String.class);
    private static final Field<String> STATION_ID = DSL.field("station_id", String.class);
    private static final Field<String> STATION_NAME = DSL.field("station_name", String.class);
    private static final Field<String> STATION_MOBILE = DSL.field("station_mobile", String.class);
    private static final Field<String> STATION_INVOICE = DSL.field("invoice_address", String.class).as("stationInvoice");
    private static final Field<String> CPO_ID = DSL.field("cpo_id", String.class);
    private static final Field<String> STATION_ADDRESS_ONE = DSL.field("station_address_1", String.class).as("stationAddressOne");
    private static final Field<String> STATION_ADDRESS_TWO = DSL.field("station_address_2", String.class).as("stationAddressTwo");
    private static final Field<String> STATION_PINCODE = DSL.field("station_pincode", String.class).as("stationPincode");
    private static final Field<String> STATION_COUNTRY = DSL.field("station_country", String.class);
    private static final Field<String> STATION_STATE = DSL.field("station_state", String.class);
    private static final Field<String> STATION_LATITUDE = DSL.field("station_latitude", String.class).as("stationLatitude");
    private static final Field<String> STATION_LONGITUDE = DSL.field("station_longitude", String.class).as("stationLongitude");

    private static final Table<?> USER_AND_VEHICLE_DETAILS_VIEW =
            DSL.table("bigtot_cms.view_user_vehicle_show");

    private static final Field<String> IDTAG = DSL.field("idtag", String.class);
    private static final Field<String> NAME = DSL.field("name", String.class);
    private static final Field<String> CMS_ID = DSL.field("cms_id", String.class);
    private static final Field<String> EMAIL = DSL.field("email", String.class);
    private static final Field<String> MOBILE_NUMBER = DSL.field("mobile", String.class);
    private static final Field<Double> WALLET_AMOUNT = DSL.field("wallet_amount", double.class);
    private static final Field<String> ADDRESS_ONE = DSL.field("addressOne", String.class);
    private static final Field<String> ADDRESS_TWO = DSL.field("addressTwo", String.class);
    private static final Field<String> CITY = DSL.field("city", String.class);
    private static final Field<String> STATE = DSL.field("state", String.class);
    private static final Field<String> COUNTRY = DSL.field("country", String.class);
    private static final Field<Boolean> IS_DEFAULT = DSL.field("is_default", Boolean.class);

    private static final Field<String> VEHICLE_NAME = DSL.field("vehicle", String.class).as("number");
    private static final Field<String> VEHICLE_NUMBER = DSL.field("vehicle_name", String.class).as("name");
    private static final Field<String> VEHICLE_MODEL = DSL.field("vehicle_model", String.class).as("model");
    private static final Field<String> VEHICLE_TYPE = DSL.field("vehicle_type", String.class).as("type");
    private static final Field<String> VEHICLE_IMG_URL = DSL.field("vehicle_img_url", String.class).as("imgUrl");
    private static final Field<byte[]> VEHICLE_HUB_IMAGE = DSL.field("user_image", byte[].class).as("hubImage");

    public void liveChargingData(final String chargeBoxId,
                                 final Integer connectorId,
                                 final Integer transactionId,
                                 final String idTag) {

        try {
            // Get transaction count
            Integer transactionCount = ctx
                    .selectCount()
                    .from(TRANSACTION)
                    .where(TRANSACTION.ID_TAG.eq(idTag))
                    .fetchOne(0, Integer.class);

            if (transactionCount == null) transactionCount = 0;
            Integer finalTransactionCount = transactionCount;

            secondary.transaction(conf -> {
                DSLContext tx = DSL.using(conf);

                boolean exists = tx.fetchExists(
                        tx.selectOne()
                                .from(LIVE_CHARGING_DATA)
                                .where(LIVE_CHARGING_DATA.TRANSACTION_ID.eq(transactionId))
                );

                if (exists) {
                    log.info("Live charging row already exists for txId={}", transactionId);
                    return;
                }

                // Use transactionCount here for rank, inserts, etc.
                String rank = runtimeService.resolveUserRank(finalTransactionCount);

                // Fetch ChargePointDTO safely
                ChargePointDTO chargePointDTO = php
                        .select(
                                CHARGER_ID.as("chargerId"), CHARGER_QR_CODE.as("chargerQrCode"), CONNECTOR_NO.as("connectorNo"),
                                CONNECTOR_ID.as("connectorId"),
                                CHARGER_TYPE.as("chargerType"),
                                STATION_ID, STATION_NAME, STATION_MOBILE, CPO_ID,
                                STATION_ADDRESS_ONE, STATION_ADDRESS_TWO, STATION_INVOICE,
                                STATION_PINCODE, STATION_COUNTRY, STATION_STATE,
                                STATION_LATITUDE, STATION_LONGITUDE
                        )
                        .from(CHARGE_POINT_VIEW)
                        .where(DSL.upper(CHARGER_ID).eq(chargeBoxId.trim().toUpperCase()))
                        .and(CONNECTOR_NO.eq(connectorId))
                        .fetchOneInto(ChargePointDTO.class);

                if (chargePointDTO == null) {
                    log.warn("Charge point not found. chargeBoxId={}, connectorId={}",
                            chargeBoxId, connectorId);
                    return;
                }

                runtimeService.putRuntimeData(
                        transactionId,
                        new LiveChargingRuntimeData(
                                transactionId,
                                finalTransactionCount,
                                rank,
                                "Normal",
                                runtimeService.mapChargerType(chargePointDTO.getChargerType()),
                                chargePointDTO.getConnectorNo()
                        )
                );

                /* -------------------------------------------------
                 * Fetch User & Vehicle Details (may be empty)
                 * ------------------------------------------------- */
                List<UserDTO> users = php
                        .select(NAME, CMS_ID, EMAIL, MOBILE_NUMBER, WALLET_AMOUNT,
                                ADDRESS_ONE, ADDRESS_TWO, CITY, STATE, COUNTRY, IS_DEFAULT)
                        .from(USER_AND_VEHICLE_DETAILS_VIEW)
                        .where(IDTAG.eq(idTag))
                        .fetchInto(UserDTO.class);

                List<VehicleDTO> vehicles = php
                        .select(VEHICLE_NAME, VEHICLE_NUMBER, VEHICLE_MODEL,
                                VEHICLE_TYPE, VEHICLE_IMG_URL, VEHICLE_HUB_IMAGE, IS_DEFAULT)
                        .from(USER_AND_VEHICLE_DETAILS_VIEW)
                        .where(IDTAG.eq(idTag))
                        .fetchInto(VehicleDTO.class);

                /* -------------------------------------------------
                 * Resolve User (Guest fallback)
                 * ------------------------------------------------- */
                UserDTO userDTO;
                if (users.isEmpty() || users.get(0).getName() == null) {
                    userDTO = guestUser();
                } else {
                    userDTO = users.stream()
                            .filter(u -> Boolean.TRUE.equals(u.isDefault()))
                            .findFirst()
                            .orElse(users.get(0));
                }

                /* -------------------------------------------------
                 * Resolve Vehicle (Guest fallback)
                 * ------------------------------------------------- */
                VehicleDTO vehicleDTO;
                if (vehicles.isEmpty() || vehicles.get(0).getName() == null) {
                    vehicleDTO = guestVehicle();
                } else {
                    vehicleDTO = vehicles.stream()
                            .filter(v -> Boolean.TRUE.equals(v.isDefault()))
                            .findFirst()
                            .orElse(vehicles.get(0));
                }

                if (users.isEmpty()) {
                    log.info("Guest / UPI charging detected. idTag={}, txId={}", idTag, transactionId);
                }
                try {
                    // ---------------- INSERT OR UPDATE LIVE DATA ----------------
                    secondary.insertInto(LIVE_CHARGING_DATA)
                            .set(LIVE_CHARGING_DATA.CHARGE_BOX_ID, chargeBoxId)
                            .set(LIVE_CHARGING_DATA.CONNECTOR_ID, connectorId)
                            .set(LIVE_CHARGING_DATA.TRANSACTION_ID, transactionId)
                            .set(LIVE_CHARGING_DATA.ID_TAG, idTag)

                            // Charge point info
                            .set(LIVE_CHARGING_DATA.CHARGER_QR_CODE, chargePointDTO.getChargerQrCode())
                            .set(LIVE_CHARGING_DATA.STATION_ID, chargePointDTO.getStationId())
                            .set(LIVE_CHARGING_DATA.STATION_NAME, chargePointDTO.getStationName())
                            .set(LIVE_CHARGING_DATA.STATION_MOBILE, chargePointDTO.getStationMobile())
                            .set(LIVE_CHARGING_DATA.CPO_ID, chargePointDTO.getCpoId())
                            .set(LIVE_CHARGING_DATA.STATION_ADDRESS_ONE, chargePointDTO.getStationAddressOne())
                            .set(LIVE_CHARGING_DATA.STATION_ADDRESS_TWO, chargePointDTO.getStationAddressTwo())
                            .set(LIVE_CHARGING_DATA.STATION_STATE, chargePointDTO.getStationState())
                            .set(LIVE_CHARGING_DATA.STATION_INVOICE, chargePointDTO.getStationInvoice())
                            .set(LIVE_CHARGING_DATA.STATION_COUNTRY, chargePointDTO.getStationCountry())
                            .set(LIVE_CHARGING_DATA.STATION_LATITUDE, chargePointDTO.getStationLatitude())
                            .set(LIVE_CHARGING_DATA.STATION_LONGITUDE, chargePointDTO.getStationLongitude())

                            // User info
                            .set(LIVE_CHARGING_DATA.USER_NAME, userDTO.getName())
                            .set(LIVE_CHARGING_DATA.USER_CMS_ID, userDTO.getCmsId())
                            .set(LIVE_CHARGING_DATA.USER_EMAIL, userDTO.getEmail())
                            .set(LIVE_CHARGING_DATA.USER_MOBILE_NO, userDTO.getMobile())
                            .set(LIVE_CHARGING_DATA.USER_WALLET_AMOUNT, userDTO.getWalletAmount())
                            .set(LIVE_CHARGING_DATA.USER_CITY, userDTO.getCity())
                            .set(LIVE_CHARGING_DATA.USER_STATE, userDTO.getState())
                            .set(LIVE_CHARGING_DATA.USER_COUNTRY, userDTO.getCountry())
                            .set(LIVE_CHARGING_DATA.USER_ADDRESS_ONE, userDTO.getAddressOne())

                            // Vehicle info
                            .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_NAME, vehicleDTO != null ? vehicleDTO.getName() : null)
                            .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_NUMBER, vehicleDTO != null ? vehicleDTO.getNumber() : null)
                            .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_MODEL, vehicleDTO != null ? vehicleDTO.getModel() : null)
                            .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_TYPE, vehicleDTO != null ? vehicleDTO.getType() : null)
                            .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_IMG_URL, vehicleDTO != null ? vehicleDTO.getImgUrl() : null)
                            .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_HUB_IMAGE, vehicleDTO != null ? vehicleDTO.getHubImage() : null)
                            .onDuplicateKeyIgnore()
                            .execute();

                    log.info("Inserted live charging row txId={}", transactionId);
                } catch (Exception e) {
                    log.error("Failed insert for txId=" + transactionId, e);
                }
            });

        } catch (Exception e) {
            System.out.println(e.getMessage());
            log.error("Error in liveChargingData()"+ e.getMessage());
        }
    }

    private UserDTO guestUser() {
        UserDTO u = new UserDTO();
        u.setName("Guest User");
        u.setCmsId(null);
        u.setEmail(null);
        u.setMobile(null);
        u.setWalletAmount(0.0);
        u.setAddressOne(null);
        u.setAddressTwo(null);
        u.setCity(null);
        u.setState(null);
        u.setCountry(null);
        return u;
    }

    private VehicleDTO guestVehicle() {
        VehicleDTO v = new VehicleDTO();
        v.setName("Unknown Vehicle");
        v.setNumber(null);
        v.setModel(null);
        v.setType(null);
        v.setImgUrl(null);
        v.setHubImage(null);
        return v;
    }

}
