
DROP VIEW IF EXISTS `v_connector_last_10_status`;
CREATE OR REPLACE VIEW v_connector_last_10_status AS
SELECT
    c.charge_box_id,
    c.connector_id,
    cs.status_timestamp,
    cs.status,
    cs.error_code,
    cs.error_info,
    cs.vendor_id,
    cs.vendor_error_code
FROM (
    SELECT
        cs.*,
        ROW_NUMBER() OVER (PARTITION BY cs.connector_pk ORDER BY cs.status_timestamp DESC) AS rn
    FROM connector_status cs
) cs
JOIN connector c ON c.connector_pk = cs.connector_pk
WHERE cs.rn <= 10
ORDER BY c.charge_box_id, c.connector_id, cs.status_timestamp DESC;
