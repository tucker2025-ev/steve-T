-- V1_1_9__add_start_type_to_transaction_start.sql
-- Safe addition of 'start_type' column

SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'transaction_start'
      AND COLUMN_NAME = 'start_type'
);

SET @sql := IF(@col_exists = 0,
               'ALTER TABLE transaction_start ADD COLUMN start_type VARCHAR(255);',
               'SELECT "Column start_type already exists";');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
