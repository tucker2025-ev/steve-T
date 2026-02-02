DROP TABLE IF EXISTS `chargebox_status`;
CREATE TABLE chargebox_status (
    s_no INT AUTO_INCREMENT PRIMARY KEY,
    charge_box_id VARCHAR(255) NOT NULL,
    status BOOLEAN NOT NULL,
    connected_timestamp DATETIME NULL,
    disconnected_timestamp DATETIME NULL,
    UNIQUE KEY uk_charge_box_id (charge_box_id)
);
