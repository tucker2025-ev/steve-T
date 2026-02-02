ALTER TABLE wallet_track
ADD COLUMN is_active_transaction BOOLEAN NOT NULL DEFAULT TRUE;
