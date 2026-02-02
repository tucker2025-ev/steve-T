DROP TABLE IF EXISTS `charger_server`;
CREATE TABLE IF NOT EXISTS `charger_server` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `charger_box_id` VARCHAR(255) NOT NULL,
    `server_url` VARCHAR(255) NOT NULL,
    `charger_qr_code` VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_charger_box_id` (`charger_box_id`)
);
