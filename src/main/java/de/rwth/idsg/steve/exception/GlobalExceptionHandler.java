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
///*
// * SteVe - SteckdosenVerwaltung - https://github.com/steve-community/steve
// * Copyright (C) 2013-2026 SteVe Community Team
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
//package de.rwth.idsg.steve.exception;
//
//import de.rwth.idsg.steve.service.testmobiledto.ResponseDTO;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.ExceptionHandler;
//import org.springframework.web.bind.annotation.RestControllerAdvice;
//
//@RestControllerAdvice
//public class GlobalExceptionHandler {
//
//    @ExceptionHandler(RemoteStartException.class)
//    public ResponseEntity<ResponseDTO> handleRemoteStartException(RemoteStartException ex) {
//        ResponseDTO dto = new ResponseDTO();
//        dto.setStatus(false);
//        dto.setData(ex.getMessage());
//        return ResponseEntity.badRequest().body(dto);
//    }
//
//    @ExceptionHandler(Exception.class)
//    public ResponseEntity<ResponseDTO> handleGenericException(Exception ex) {
//        ResponseDTO dto = new ResponseDTO();
//        dto.setStatus(false);
//        dto.setData("Internal server error");
//        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(dto);
//    }
//
//}
//
