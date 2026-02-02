-- V1_1_7__add_supports_offline_charging_to_charge_box.sql
-- Safe column addition for MySQL

SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'charge_box'
      AND COLUMN_NAME = 'supports_offline_charging'
);

SET @sql := IF(@col_exists = 0,
               'ALTER TABLE charge_box ADD COLUMN supports_offline_charging BOOLEAN NOT NULL DEFAULT FALSE;',
               'SELECT "Column supports_offline_charging already exists";');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
