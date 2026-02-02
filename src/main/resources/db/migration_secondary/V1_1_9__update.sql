DROP TABLE IF EXISTS `wallet_track`;
CREATE TABLE wallet_track (
    transaction_id     INT,
    id_tag             VARCHAR(100) NOT NULL,
    start_energy       DOUBLE,
    tariff_amount      DOUBLE,
    last_energy        DOUBLE,
    wallet_amount      DOUBLE,
    consumed_energy    DOUBLE,
    consumed_amount    DOUBLE,
    total_consumed_amount    DOUBLE,
    start_timestamp timestamp(6) ,
    stop_timestamp timestamp(6)
);
