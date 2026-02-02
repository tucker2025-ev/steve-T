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

import de.rwth.idsg.steve.service.dto.ChargePointDTO;
import de.rwth.idsg.steve.service.dto.UserDTO;
import de.rwth.idsg.steve.service.dto.VehileDTO;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

import static jooq.steve.db2.Tables.LIVE_CHARGING_DATA;

@Slf4j
@Service
public class LiveChargingData {

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;

    @Autowired
    @Qualifier("php")
    private DSLContext php;

    private static final Table<?> CHARGE_POINT_VIEW =
            DSL.table("bigtot_cms.v_station_charger_details");

    private static final Field<String> CHARGER_ID = DSL.field("charger_id", String.class);
    private static final Field<String> CHARGER_QR_CODE = DSL.field("charger_qr_code", String.class);
    private static final Field<String> STATION_ID = DSL.field("station_id", String.class);
    private static final Field<String> STATION_NAME = DSL.field("station_name", String.class);
    private static final Field<String> STATION_MOBILE = DSL.field("station_mobile", String.class);
    private static final Field<String> CPO_ID = DSL.field("cpo_id", String.class);
    private static final Field<String> STATION_ADDRESS_ONE =
            DSL.field("station_address_1", String.class).as("stationAddressOne");
    private static final Field<String> STATION_ADDRESS_TWO =
            DSL.field("station_address_2", String.class).as("stationAddressTwo");
    private static final Field<String> STATION_PINCODE = DSL.field("station_pincode", String.class);
    private static final Field<String> STATION_COUNTRY = DSL.field("station_country", String.class);
    private static final Field<String> STATION_STATE = DSL.field("station_state", String.class);
    private static final Field<String> STATION_LATITUDE = DSL.field("station_latitude", String.class).as("stationLatitude");
    private static final Field<String> STATION_LONGITUDE = DSL.field("station_longitude", String.class).as("stationLongitude");
    private static final Field<String> CONNECTOR_ID = DSL.field("con_no", String.class);
    private static final Field<String> CHARGER_TYPE = DSL.field("charger_type", String.class);
    private static final Table<?> USER_AND_VEHICLE_DETAILS_VIEW =
            DSL.table("bigtot_cms.view_user_vehicle");

    private static final Field<String> IDTAG = DSL.field("idtag", String.class);
    private static final Field<String> NAME = DSL.field("name", String.class);
    private static final Field<String> CMS_ID = DSL.field("cms_id", String.class);
    private static final Field<String> EMAIL = DSL.field("email", String.class);
    private static final Field<String> MOBILE_NUMBER = DSL.field("mobile", String.class);
    private static final Field<Double> WALLET_AMOUNT = DSL.field("wallet_amount", double.class);
    private static final Field<String> ADDRESS_ONE = DSL.field("address_1", String.class).as("addressOne");
    private static final Field<String> ADDRESS_TWO = DSL.field("address_2", String.class).as("addressTwo");
    private static final Field<String> CITY = DSL.field("city", String.class);
    private static final Field<String> STATE = DSL.field("state", String.class);
    private static final Field<String> COUNTRY = DSL.field("country", String.class);
    private static final Field<Boolean> IS_DEFAULT = DSL.field("is_default", Boolean.class);


    private static final Field<String> VEHICLE_NAME = DSL.field("vname", String.class).as("name");
    private static final Field<String> VEHICLE_NUMBER = DSL.field("vehicle_no", String.class).as("number");
    private static final Field<String> VEHICLE_MODEL = DSL.field("vmodel", String.class).as("model");
    private static final Field<String> VEHICLE_TYPE = DSL.field("v_variant", String.class).as("type");
    private static final Field<String> VEHICLE_IMG_URL = DSL.field("car_img_url", String.class).as("imgUrl");
    private static final Field<byte[]> VEHICLE_HUB_IMAGE = DSL.field("image", byte[].class).as("hubImage");

    public void liveChargingData(final String chargeBoxId,
                                 final Integer connectorId,
                                 final Integer transactionId,
                                 final String idTag) {
        try {

            log.info("LiveChargingData start: chargeBoxId={}, connectorId={}, txId={}, idTag={}",
                    chargeBoxId, connectorId, transactionId, idTag);

            /* ===============================
             * 1️⃣ Charge Point (MANDATORY)
             * =============================== */
            ChargePointDTO chargePointDTO = php
                    .select(
                            CHARGER_QR_CODE, STATION_ID, STATION_NAME, STATION_MOBILE, CPO_ID,
                            STATION_ADDRESS_ONE, STATION_ADDRESS_TWO, STATION_PINCODE,
                            STATION_COUNTRY, STATION_STATE, STATION_LATITUDE, STATION_LONGITUDE
                    )
                    .from(CHARGE_POINT_VIEW)
                    .where(CHARGER_ID.eq(chargeBoxId))
                    .and(CONNECTOR_ID.eq(String.valueOf(connectorId)))
                    .fetchOneInto(ChargePointDTO.class);

            if (chargePointDTO == null) {
                log.error("❌ ChargePoint not found for chargeBoxId={}, connectorId={}",
                        chargeBoxId, connectorId);
                return;
            }

            /* ===============================
             * 2️⃣ Users (OPTIONAL)
             * =============================== */
            List<UserDTO> users = php
                    .select(NAME, CMS_ID, EMAIL, MOBILE_NUMBER, WALLET_AMOUNT,
                            ADDRESS_ONE, ADDRESS_TWO, CITY, STATE, COUNTRY, IS_DEFAULT)
                    .from(USER_AND_VEHICLE_DETAILS_VIEW)
                    .where(IDTAG.eq(idTag))
                    .fetchInto(UserDTO.class);

            UserDTO userDTO = users.stream()
                    .filter(u -> Boolean.TRUE.equals(u.isDefault()))
                    .findFirst()
                    .orElse(users.isEmpty() ? null : users.get(0));

            log.info("User records found: {}", users.size());

            /* ===============================
             * 3️⃣ Vehicles (OPTIONAL)
             * =============================== */
            List<VehileDTO> vehicles = php
                    .select(VEHICLE_NAME, VEHICLE_NUMBER, VEHICLE_MODEL,
                            VEHICLE_TYPE, VEHICLE_IMG_URL, VEHICLE_HUB_IMAGE, IS_DEFAULT)
                    .from(USER_AND_VEHICLE_DETAILS_VIEW)
                    .where(IDTAG.eq(idTag))
                    .fetchInto(VehileDTO.class);

            VehileDTO vehileDTO = vehicles.stream()
                    .filter(v -> Boolean.TRUE.equals(v.isDefault()))
                    .findFirst()
                    .orElse(vehicles.isEmpty() ? null : vehicles.get(0));

            log.info("Vehicle records found: {}", vehicles.size());

            /* ===============================
             * 4️⃣ INSERT (NULL SAFE)
             * =============================== */
            secondary.insertInto(LIVE_CHARGING_DATA)
                    // Core
                    .set(LIVE_CHARGING_DATA.CHARGE_BOX_ID, chargeBoxId)
                    .set(LIVE_CHARGING_DATA.CONNECTOR_ID, connectorId)
                    .set(LIVE_CHARGING_DATA.TRANSACTION_ID, transactionId)
                    .set(LIVE_CHARGING_DATA.ID_TAG, idTag)

                    // Charge point
                    .set(LIVE_CHARGING_DATA.CHARGER_QR_CODE, chargePointDTO.getChargerQrCode())
                    .set(LIVE_CHARGING_DATA.STATION_ID, chargePointDTO.getStationId())
                    .set(LIVE_CHARGING_DATA.STATION_NAME, chargePointDTO.getStationName())
                    .set(LIVE_CHARGING_DATA.STATION_MOBILE, chargePointDTO.getStationMobile())
                    .set(LIVE_CHARGING_DATA.CPO_ID, chargePointDTO.getCpoId())
                    .set(LIVE_CHARGING_DATA.STATION_ADDRESS_ONE, chargePointDTO.getStationAddressOne())
                    .set(LIVE_CHARGING_DATA.STATION_ADDRESS_TWO, chargePointDTO.getStationAddressTwo())
                    .set(LIVE_CHARGING_DATA.STATION_PINCODE, chargePointDTO.getStationPincode())
                    .set(LIVE_CHARGING_DATA.STATION_STATE, chargePointDTO.getStationState())
                    .set(LIVE_CHARGING_DATA.STATION_COUNTRY, chargePointDTO.getStationCountry())
                    .set(LIVE_CHARGING_DATA.STATION_LATITUDE, chargePointDTO.getStationLatitude())
                    .set(LIVE_CHARGING_DATA.STATION_LONGITUDE, chargePointDTO.getStationLongitude())

                    // User (NULL allowed)
                    .set(LIVE_CHARGING_DATA.USER_NAME, userDTO != null ? userDTO.getName() : null)
                    .set(LIVE_CHARGING_DATA.USER_CMS_ID, userDTO != null ? userDTO.getCmsId() : null)
                    .set(LIVE_CHARGING_DATA.USER_EMAIL, userDTO != null ? userDTO.getEmail() : null)
                    .set(LIVE_CHARGING_DATA.USER_MOBILE_NO, userDTO != null ? userDTO.getMobile() : null)
                    .set(LIVE_CHARGING_DATA.USER_WALLET_AMOUNT, userDTO != null ? userDTO.getWalletAmount() : null)
                    .set(LIVE_CHARGING_DATA.USER_ADDRESS_ONE, userDTO != null ? userDTO.getAddressOne() : null)
                    .set(LIVE_CHARGING_DATA.USER_ADDRESS_TWO, userDTO != null ? userDTO.getAddressTwo() : null)
                    .set(LIVE_CHARGING_DATA.USER_CITY, userDTO != null ? userDTO.getCity() : null)
                    .set(LIVE_CHARGING_DATA.USER_STATE, userDTO != null ? userDTO.getState() : null)
                    .set(LIVE_CHARGING_DATA.USER_COUNTRY, userDTO != null ? userDTO.getCountry() : null)

                    // Vehicle (NULL allowed)
                    .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_NAME, vehileDTO != null ? vehileDTO.getName() : null)
                    .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_NUMBER, vehileDTO != null ? vehileDTO.getNumber() : null)
                    .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_MODEL, vehileDTO != null ? vehileDTO.getModel() : null)
                    .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_TYPE, vehileDTO != null ? vehileDTO.getType() : null)
                    .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_IMG_URL, vehileDTO != null ? vehileDTO.getImgUrl() : null)
                    .set(LIVE_CHARGING_DATA.CHARGING_VEHICLE_HUB_IMAGE, vehileDTO != null ? vehileDTO.getHubImage() : null)

                    .execute();

            log.info("✅ LiveChargingData inserted successfully for txId={}", transactionId);

        } catch (Exception e) {
            log.error("❌ Error in liveChargingData()", e);
        }
    }


    public String isType6Charger(final String chargeBoxId, final Integer connectorId) {
        try {
            return php
                    .select(CHARGER_TYPE)
                    .from(CHARGE_POINT_VIEW)
                    .where(CHARGER_ID.eq(chargeBoxId))
                    .and(CONNECTOR_ID.eq(String.valueOf(connectorId)))
                    .fetchOptional(CHARGER_TYPE)
                    .orElse(null);   // No record found
        } catch (Exception ex) {
            log.error("Failed to fetch charger");
            return null;
        }
    }


}
