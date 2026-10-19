-- Выполняется явно перед внедрением; повтор не меняет уже доставленные срезы.
CREATE TABLE IF NOT EXISTS autopilot_hourly_snapshots (
    snapshot_date DATE NOT NULL,
    snapshot_hour SMALLINT NOT NULL CHECK (snapshot_hour BETWEEN 0 AND 23),
    snapshot_at TIMESTAMPTZ NOT NULL,
    orders_sum NUMERIC(18, 2),
    orders_count BIGINT DEFAULT 0,
    adv_spend NUMERIC(18, 2),
    profit_by_cond_orders NUMERIC(18, 2),
    snapshot_status VARCHAR(16) NOT NULL DEFAULT 'captured',
    recovery_note TEXT,
    saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_date, snapshot_hour)
);
ALTER TABLE autopilot_hourly_snapshots
    ADD COLUMN IF NOT EXISTS snapshot_status VARCHAR(16) NOT NULL DEFAULT 'captured',
    ADD COLUMN IF NOT EXISTS recovery_note TEXT,
    ADD COLUMN IF NOT EXISTS orders_count BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS delivery_status VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK (delivery_status IN ('pending', 'synced')),
    ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_delivery_error VARCHAR(64);
ALTER TABLE autopilot_hourly_snapshots
    ALTER COLUMN orders_sum DROP NOT NULL,
    ALTER COLUMN orders_count DROP NOT NULL,
    ALTER COLUMN adv_spend DROP NOT NULL,
    ALTER COLUMN profit_by_cond_orders DROP NOT NULL;
CREATE INDEX IF NOT EXISTS ix_autopilot_snapshots_pending
    ON autopilot_hourly_snapshots (snapshot_date, snapshot_hour)
    WHERE snapshot_status = 'captured' AND delivery_status = 'pending';
