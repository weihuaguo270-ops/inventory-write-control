CREATE TABLE IF NOT EXISTS sku_inventory (
    sku_id        TEXT PRIMARY KEY,
    sellable_qty  BIGINT NOT NULL CHECK (sellable_qty >= 0),
    reserved_qty  BIGINT NOT NULL CHECK (reserved_qty >= 0),
    version       BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id   TEXT PRIMARY KEY,
    order_id         TEXT NOT NULL,
    sku_id           TEXT NOT NULL REFERENCES sku_inventory(sku_id),
    qty              BIGINT NOT NULL CHECK (qty > 0),
    status           TEXT NOT NULL CHECK (status IN ('reserved', 'confirmed', 'released')),
    idempotency_key  TEXT NOT NULL,
    expire_at        TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_reservations_idempotency_key
    ON reservations (idempotency_key);

CREATE UNIQUE INDEX IF NOT EXISTS uq_reservations_order_id
    ON reservations (order_id);

CREATE INDEX IF NOT EXISTS idx_reservations_sku_status
    ON reservations (sku_id, status);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    order_id     TEXT NOT NULL,
    result       TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
