DROP TABLE IF EXISTS `charger_connector_status_log`;
CREATE TABLE `charger_connector_status_log` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `charger_id` VARCHAR(255) NOT NULL,
  `connector_id` INT NOT NULL,
  `status` VARCHAR(255) DEFAULT NULL,
  `error_code` VARCHAR(255) DEFAULT NULL,
  `error_info` VARCHAR(255) DEFAULT NULL,
  `vendor_id` VARCHAR(255) DEFAULT NULL,
  `vendor_error_code` VARCHAR(255) DEFAULT NULL,
  is_connector_error BOOLEAN DEFAULT FALSE,
  `error_occur_timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  error_resolved_timestamp DATETIME,
  PRIMARY KEY (`id`)
);
