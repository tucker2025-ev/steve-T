
DROP TABLE IF EXISTS `charger_status`;
CREATE TABLE charger_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    charge_point VARCHAR(100) NOT NULL,
    connector_id INT NOT NULL,
    connector_last_status VARCHAR(50),
    connector_status_timestamp DATETIME,
    charger_online_timestamp DATETIME,
    charger_offline_timestamp DATETIME,
    is_online BOOLEAN DEFAULT FALSE,
    is_connector_error BOOLEAN DEFAULT FALSE,
    connector_error_resolved_timestamp DATETIME
);
