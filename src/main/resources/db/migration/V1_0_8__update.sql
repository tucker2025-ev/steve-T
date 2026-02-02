DROP TABLE IF EXISTS vehicle;

CREATE TABLE vehicle (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_tag VARCHAR(255) COLLATE utf8mb3_unicode_ci NOT NULL,
    vid_number VARCHAR(100) NOT NULL UNIQUE,
    vehicle_number VARCHAR(50),
    is_enable_auto_charging BOOLEAN DEFAULT FALSE,
    vehicle_image LONGBLOB,
    is_enable BOOLEAN NOT NULL DEFAULT 0,
    CONSTRAINT fk_vehicle_id_tag
        FOREIGN KEY (id_tag)
        REFERENCES ocpp_tag(id_tag)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb3
  COLLATE=utf8mb3_unicode_ci;


DROP TABLE IF EXISTS rfid_card;

CREATE TABLE rfid_card (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_tag VARCHAR(255) COLLATE utf8mb3_unicode_ci NOT NULL,
    ac_tag VARCHAR(100) UNIQUE,
    dc_tag VARCHAR(100) UNIQUE,
    hex_tag VARCHAR(255),
    user_name VARCHAR(255),
    user_mobile VARCHAR(30),
    status INT,
    isTrue BOOLEAN DEFAULT FALSE,
    v_mobile VARCHAR(30),
    cms_id VARCHAR(50),
    engrave_id VARCHAR(50),
    CONSTRAINT fk_rfid_card_id_tag
        FOREIGN KEY (id_tag)
        REFERENCES ocpp_tag(id_tag)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb3
  COLLATE=utf8mb3_unicode_ci;
