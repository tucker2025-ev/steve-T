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

import de.rwth.idsg.steve.service.dto.RefundAmountAfterTransactionSend;
import de.rwth.idsg.steve.service.dto.RefundAmountAfterTransactionSendResponse;
import de.rwth.idsg.steve.service.dto.RefundAmountDTO;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import static jooq.steve.db.tables.PaymentRequest.PAYMENT_REQUEST;

@Service
@Slf4j
public class RazorpayRefundService {

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DSLContext dslContext;

    @Autowired
    @Qualifier("secondary")
    private DSLContext secondary;

    public void instantRefundPayment(String upiId, int amount, final String rrnId) {
        try {
            String url = "https://star.tuckermotors.com/ev_qr/payout.php";
            RefundAmountDTO request = new RefundAmountDTO();
            request.setUpiId(upiId);
            request.setAmount(amount);
            request.setRrnId(rrnId);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Payout-Api-Key", "Tuckermotors123");
            HttpEntity<RefundAmountDTO> entity =
                    new HttpEntity<>(request, headers);

            ResponseEntity<String> response =
                    restTemplate.postForEntity(url, entity, String.class);
        } catch (Exception e) {
            log.error("Exception occurred RazorpayRefundService in  57 line", e);
        }
    }

    public void refound(final String rrnId, final double chargeAmount, final double balanceAmount) {

        try {
            String url = "https://star.tuckermotors.com/ev_qr/sessiondata.php";
            RefundAmountAfterTransactionSend request = new RefundAmountAfterTransactionSend();
            request.setRrnId(rrnId);
            request.setBalanceAmount(round2(balanceAmount));
            request.setChargeAmount(round2(chargeAmount));

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<RefundAmountAfterTransactionSend> entity =
                    new HttpEntity<>(request, headers);

            ResponseEntity<RefundAmountAfterTransactionSendResponse> response =
                    restTemplate.postForEntity(url, entity, RefundAmountAfterTransactionSendResponse.class);

            RefundAmountAfterTransactionSendResponse body = response.getBody();
            log.info("RazorpayRefundService  PayOut Trigger value : chargeAmount = " + chargeAmount + ",  balanceAmount = " + balanceAmount);
            if (body == null) {
                log.error("Refund API failed: Response body is null");
                return;
            }

            String success = body.getSuccess();
            String message = body.getMessage();

            if ("true".equalsIgnoreCase(success)) {
                log.info("Refund Success: " + message);
            } else {
                log.warn("RazorpayRefundService AfterTransaction PayOut Balance Something Error Occur Check : " + response.getBody());
            }

        } catch (Exception e) {
            log.error("Exception occurred RazorpayRefundService in  101 line", e);
        }

        try {

        } catch (Exception e) {
            log.error("Exception occurred RazorpayRefundService in  111 line : " + e.getMessage());
        }
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

}



