DROP TABLE IF EXISTS schedule_charging;
CREATE TABLE IF NOT EXISTS schedule_charging (
id BIGINT AUTO_INCREMENT PRIMARY KEY, idtag VARCHAR(50) NOT NULL, charge_box_id VARCHAR(50) NOT NULL, connector_id VARCHAR(50) NOT NULL, start_time DATETIME NOT NULL, end_time DATETIME NOT NULL, is_routine TINYINT(1) NOT NULL, day VARCHAR(255) NULL, is_start TINYINT(1) NOT NULL DEFAULT 0, is_stop TINYINT(1) NOT NULL DEFAULT 0 , is_notify_send  TINYINT(1) NOT NULL DEFAULT 0,is_enable  TINYINT(1) NOT NULL DEFAULT 0);

DROP TABLE IF EXISTS charging_fee_exempt_chargebox;
CREATE TABLE IF NOT EXISTS charging_fee_exempt_chargebox (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    idtag VARCHAR(50) NOT NULL,
    chargebox_id VARCHAR(50) NOT NULL UNIQUE,
    FOREIGN KEY (idtag) REFERENCES ocpp_tag(id_tag)
);