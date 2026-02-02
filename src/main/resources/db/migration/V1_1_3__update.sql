DROP TABLE IF EXISTS `payment_request`;
CREATE TABLE payment_request (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    pay_id          VARCHAR(100) NOT NULL UNIQUE,
    charger_id      VARCHAR(100) NOT NULL,
    connector_id    INT NOT NULL,
    amount          DOUBLE NOT NULL,
    consumed_amount  DOUBLE DEFAULT 0,
    pay_balance      DOUBLE DEFAULT 0,
    invoice_url      VARCHAR(255) DEFAULT NULL,
    is_started      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
