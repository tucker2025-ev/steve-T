/*
 * SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
 * Copyright (C) 2013-2025 SteVe Community Team
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

import de.rwth.idsg.steve.repository.WebSocketLogRepository;
import de.rwth.idsg.steve.service.dto.WebSocketLog;
import de.rwth.idsg.steve.utils.LogFileRetriever;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.LocalDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * @author Sevket Goekay <sevketgokay@gmail.com>
 * @since 15.08.2014
 */
@Slf4j
@Controller
@RequestMapping(value = "/manager")
public class LogController {

    @Autowired
    private WebSocketLogRepository webSocketLogRepository;

    @RequestMapping(value = "/log", method = RequestMethod.GET)
    public void log(HttpServletResponse response) {
        response.setContentType("text/plain");

        try (PrintWriter writer = response.getWriter()) {
            Optional<Path> p = LogFileRetriever.INSTANCE.getPath();
            if (p.isPresent()) {
                Files.lines(p.get(), StandardCharsets.UTF_8)
                        .forEach(writer::println);
            } else {
                writer.write(LogFileRetriever.INSTANCE.getErrorMessage());
            }
        } catch (IOException e) {
            log.error("Exception happened", e);
        }
    }


    @GetMapping("/websocket-logs")
    public String getLogsByDate(
            @RequestParam(value = "date", required = false) String dateStr,
            Model model
    ) {
        LocalDate date;
        try {
            date = (dateStr == null || dateStr.isEmpty())
                    ? LocalDate.now()
                    : LocalDate.parse(dateStr);
        } catch (Exception e) {
            model.addAttribute("error", "Invalid date format");
            return "websocket-logs";
        }

        List<WebSocketLog> logs = webSocketLogRepository.getLogsByDate(date);

        model.addAttribute("logs", logs);
        model.addAttribute("selectedDate", date.toString());
        model.addAttribute("today", LocalDate.now().toString());
        return "websocket-logs";
    }

    public String getLogFilePath() {
        return LogFileRetriever.INSTANCE.getLogFilePathOrErrorMessage();
    }

}
