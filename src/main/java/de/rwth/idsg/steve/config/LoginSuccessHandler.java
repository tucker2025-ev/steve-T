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
package de.rwth.idsg.steve.config;

import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;

import static jooq.steve.db.Tables.USER_SESSION_AUDIT;

@Slf4j
@Component
public class LoginSuccessHandler implements AuthenticationSuccessHandler {

    @Autowired
    private DSLContext dslContext;

    private static final Set<String> IGNORED_IPS = Set.of(
            "15.207.37.132",
            "13.233.175.29"
    );

    @Override
    public void onAuthenticationSuccess(
            HttpServletRequest request,
            HttpServletResponse response,
            Authentication authentication
    ) throws IOException {

        String sessionId = request.getSession().getId();
        String ip = getClientIp(request);
        String userAgent = request.getHeader("User-Agent");
        DateTime signInTime = DateTime.now();

        String device = getDeviceType(userAgent);
        String browser = getBrowser(userAgent);
        String os = getOS(userAgent);

        // store for logout usage
        request.getSession().setAttribute("DEVICE_TYPE", device);
        request.getSession().setAttribute("BROWSER", browser);
        request.getSession().setAttribute("OS", os);
        request.getSession().setAttribute("IP_ADDRESS", ip);
        request.getSession().setAttribute("SIGNIN_TIME", signInTime);

        try {
            if (!IGNORED_IPS.contains(ip)){
                dslContext.insertInto(USER_SESSION_AUDIT)
                        .set(USER_SESSION_AUDIT.SESSION_ID, sessionId)
                        .set(USER_SESSION_AUDIT.IP_ADDRESS, ip)
                        .set(USER_SESSION_AUDIT.DEVICE, device)
                        .set(USER_SESSION_AUDIT.OS, os)
                        .set(USER_SESSION_AUDIT.BROWSER, browser)
                        .set(USER_SESSION_AUDIT.SIGNIN_TIME, DateTime.now())
                        .execute();
            }

        } catch (Exception e) {
            log.error("Exception occur : loginSuccessHandler" + e.getMessage());
        }

        response.sendRedirect(request.getContextPath() + "/manager");
    }

    // ---------------- DETECTION METHODS ----------------
    private String getDeviceType(String ua) {
        if (ua == null) return "UNKNOWN";
        ua = ua.toLowerCase();
        if (ua.contains("mobile") || ua.contains("android") || ua.contains("iphone")) return "MOBILE";
        if (ua.contains("ipad") || ua.contains("tablet")) return "TABLET";
        return "DESKTOP";
    }

    private String getBrowser(String ua) {
        if (ua == null) return "UNKNOWN";
        ua = ua.toLowerCase();
        if (ua.contains("edg")) return "EDGE";
        if (ua.contains("chrome")) return "CHROME";
        if (ua.contains("firefox")) return "FIREFOX";
        if (ua.contains("safari") && !ua.contains("chrome")) return "SAFARI";
        return "UNKNOWN";
    }

    private String getOS(String ua) {
        if (ua == null) return "UNKNOWN";
        ua = ua.toLowerCase();
        if (ua.contains("windows")) return "WINDOWS";
        if (ua.contains("android")) return "ANDROID";
        if (ua.contains("iphone") || ua.contains("ios")) return "IOS";
        if (ua.contains("mac os")) return "MAC";
        if (ua.contains("linux")) return "LINUX";
        return "UNKNOWN";
    }

    private String getClientIp(HttpServletRequest request) {
        String xff = request.getHeader("X-Forwarded-For");
        return (xff != null && !xff.isBlank())
                ? xff.split(",")[0].trim()
                : request.getRemoteAddr();
    }
}
