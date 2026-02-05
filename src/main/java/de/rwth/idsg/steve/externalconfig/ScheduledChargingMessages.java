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
package de.rwth.idsg.steve.externalconfig;

public class ScheduledChargingMessages {

    public static final String REMINDER_BEFORE_START1 = "Heads up! Your vehicle will begin charging automatically ";
    public static final String REMINDER_BEFORE_START =
            " Heads up! Your vehicle will begin charging automatically Within 30 minutes.";
    public static final String CHARGING_STARTED =
            " Your scheduled charging has started at %s. Charging started successfully. You can track progress in the app.";
    public static final String CHARGING_COMPLETED =
            " Your scheduled charging session has successfully completed.";
    public static final String STOPPED_BY_USER =
            " Your scheduled charging was stopped manually you trigger RemoteStop.";
    public static final String STOPPED_BY_SERVER =
            " Your scheduled charging session was stopped before the planned end time. Please visit our History page. ";
    public static final String FAILED_TO_START =
            " Charging could not be started at %s due to a connection issue. Please retry.";
    public static final String INSUFFICIENT_BALANCE =
            " Scheduled charging could not start due to insufficient balance.";
    public static final String CONNECTOR_BUSY =
            " Please plug in your connector to the vehicle.";
    public static final String CHARGER_UNAVAILABLE =
            " Scheduled charging failed as the charger was not available at the scheduled time.";
    public static final String GENERIC_ISSUE =
            " Some error occurred. It might be due to power cut, network issue, or other problem.";

}
