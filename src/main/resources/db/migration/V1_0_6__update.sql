DROP TABLE IF EXISTS `transaction_meter_values`;
CREATE TABLE `transaction_meter_values` (
  id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  ocpp_tag_id  VARCHAR(255)  ,
  charge_box_id VARCHAR(255),
  transaction_pk INT(11) UNSIGNED DEFAULT NULL,
  connector_pk INT(11) UNSIGNED DEFAULT NULL,
  `event_timestamp` timestamp(6) NOT NULL DEFAULT current_timestamp(6),
  voltage DOUBLE DEFAULT NULL,
  current DOUBLE DEFAULT NULL,
  power DOUBLE DEFAULT NULL,
  energy DOUBLE DEFAULT NULL,
  soc DOUBLE DEFAULT NULL,
  PRIMARY KEY (id),

  FOREIGN KEY (connector_pk) REFERENCES connector(connector_pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;



DROP TABLE IF EXISTS `websocket_log`;
CREATE TABLE websocket_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    time DATETIME(3),
    charge_box_id VARCHAR(64),
    session_id VARCHAR(100),
    transaction_id VARCHAR(64),
    event VARCHAR(100),
    payload TEXT,
    direction VARCHAR(50)
);