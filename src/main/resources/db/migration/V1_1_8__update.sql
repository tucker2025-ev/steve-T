CREATE TABLE user_session_audit (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id    VARCHAR(100) NOT NULL,
    ip_address    VARCHAR(45)  NOT NULL,
    device     VARCHAR(45)  NOT NULL,
    os    VARCHAR(45)  NOT NULL,
    browser    VARCHAR(45)  NOT NULL,
    signin_time   TIMESTAMP    NOT NULL,
    signout_time  TIMESTAMP    NULL,

    UNIQUE KEY uk_session_id (session_id)
);
