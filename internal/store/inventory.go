package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	ErrInsufficientStock = errors.New("insufficient sellable stock")
	ErrNotFound          = errors.New("not found")
	ErrInvalidState      = errors.New("invalid reservation state")
)

const (
	StatusReserved  = "reserved"
	StatusConfirmed = "confirmed"
	StatusReleased  = "released"
)

type SKU struct {
	SKUID       string `json:"sku_id"`
	SellableQty int64  `json:"sellable_qty"`
	ReservedQty int64  `json:"reserved_qty"`
	Version     int64  `json:"version"`
}

type Reservation struct {
	ReservationID  string    `json:"reservation_id"`
	OrderID        string    `json:"order_id"`
	SKUID          string    `json:"sku_id"`
	Qty            int64     `json:"qty"`
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	ExpireAt       time.Time `json:"expire_at"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Store struct {
	DB  *pgxpool.Pool
	RDB *redis.Client
}

func stockKey(skuID string) string {
	return "stock:sellable:" + skuID
}

func idemKey(key string) string {
	return "idem:reserve:" + key
}

// SyncSellableToRedis loads PG sellable into Redis (seed / restart).
func (s *Store) SyncSellableToRedis(ctx context.Context, skuID string) error {
	var sellable int64
	err := s.DB.QueryRow(ctx, `SELECT sellable_qty FROM sku_inventory WHERE sku_id=$1`, skuID).Scan(&sellable)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return s.RDB.Set(ctx, stockKey(skuID), sellable, 0).Err()
}

func (s *Store) UpsertSKU(ctx context.Context, skuID string, sellable int64) error {
	_, err := s.DB.Exec(ctx, `
		INSERT INTO sku_inventory (sku_id, sellable_qty, reserved_qty, version)
		VALUES ($1, $2, 0, 0)
		ON CONFLICT (sku_id) DO NOTHING`, skuID, sellable)
	if err != nil {
		return err
	}
	return s.SyncSellableToRedis(ctx, skuID)
}

// ResetSKU forces sellable/reserved for demo re-seed.
func (s *Store) ResetSKU(ctx context.Context, skuID string, sellable int64) error {
	_, err := s.DB.Exec(ctx, `
		INSERT INTO sku_inventory (sku_id, sellable_qty, reserved_qty, version)
		VALUES ($1, $2, 0, 0)
		ON CONFLICT (sku_id) DO UPDATE
		SET sellable_qty = EXCLUDED.sellable_qty,
		    reserved_qty = 0,
		    version = sku_inventory.version + 1,
		    updated_at = now()`, skuID, sellable)
	if err != nil {
		return err
	}
	_, _ = s.DB.Exec(ctx, `DELETE FROM reservations WHERE sku_id=$1`, skuID)
	_, _ = s.DB.Exec(ctx, `DELETE FROM processed_events`)
	_ = s.RDB.Del(ctx, stockKey(skuID)).Err()
	return s.RDB.Set(ctx, stockKey(skuID), sellable, 0).Err()
}

func (s *Store) GetSKU(ctx context.Context, skuID string) (SKU, error) {
	var out SKU
	err := s.DB.QueryRow(ctx, `
		SELECT sku_id, sellable_qty, reserved_qty, version
		FROM sku_inventory WHERE sku_id=$1`, skuID).
		Scan(&out.SKUID, &out.SellableQty, &out.ReservedQty, &out.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return SKU{}, ErrNotFound
	}
	return out, err
}

func (s *Store) GetReservation(ctx context.Context, id string) (Reservation, error) {
	return s.scanReservation(ctx, `SELECT reservation_id, order_id, sku_id, qty, status, idempotency_key, expire_at, created_at, updated_at
		FROM reservations WHERE reservation_id=$1`, id)
}

func (s *Store) GetReservationByOrder(ctx context.Context, orderID string) (Reservation, error) {
	return s.scanReservation(ctx, `SELECT reservation_id, order_id, sku_id, qty, status, idempotency_key, expire_at, created_at, updated_at
		FROM reservations WHERE order_id=$1`, orderID)
}

func (s *Store) GetReservationByIdempotency(ctx context.Context, key string) (Reservation, error) {
	return s.scanReservation(ctx, `SELECT reservation_id, order_id, sku_id, qty, status, idempotency_key, expire_at, created_at, updated_at
		FROM reservations WHERE idempotency_key=$1`, key)
}

func (s *Store) scanReservation(ctx context.Context, q string, arg string) (Reservation, error) {
	var r Reservation
	err := s.DB.QueryRow(ctx, q, arg).Scan(
		&r.ReservationID, &r.OrderID, &r.SKUID, &r.Qty, &r.Status,
		&r.IdempotencyKey, &r.ExpireAt, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrNotFound
	}
	return r, err
}

// reserveLua: atomic check-and-decr; returns remaining stock, or -1 if insufficient.
var reserveLua = redis.NewScript(`
local key = KEYS[1]
local qty = tonumber(ARGV[1])
local cur = tonumber(redis.call('GET', key) or '0')
if cur < qty then
  return -1
end
return redis.call('DECRBY', key, qty)
`)

func (s *Store) Reserve(ctx context.Context, r Reservation) (Reservation, bool, error) {
	// Fast idempotent replay via Redis.
	if cached, err := s.RDB.Get(ctx, idemKey(r.IdempotencyKey)).Result(); err == nil && cached != "" {
		existing, err := s.GetReservation(ctx, cached)
		if err == nil {
			return existing, true, nil
		}
	}
	if existing, err := s.GetReservationByIdempotency(ctx, r.IdempotencyKey); err == nil {
		_ = s.RDB.Set(ctx, idemKey(r.IdempotencyKey), existing.ReservationID, 24*time.Hour).Err()
		return existing, true, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Reservation{}, false, err
	}

	remaining, err := reserveLua.Run(ctx, s.RDB, []string{stockKey(r.SKUID)}, r.Qty).Int64()
	if err != nil {
		return Reservation{}, false, err
	}
	if remaining < 0 {
		return Reservation{}, false, ErrInsufficientStock
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		_ = s.compensateRedis(ctx, r.SKUID, r.Qty)
		return Reservation{}, false, err
	}
	defer tx.Rollback(ctx)

	var sellable, reserved int64
	err = tx.QueryRow(ctx, `
		SELECT sellable_qty, reserved_qty FROM sku_inventory
		WHERE sku_id=$1 FOR UPDATE`, r.SKUID).Scan(&sellable, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = s.compensateRedis(ctx, r.SKUID, r.Qty)
		return Reservation{}, false, ErrNotFound
	}
	if err != nil {
		_ = s.compensateRedis(ctx, r.SKUID, r.Qty)
		return Reservation{}, false, err
	}
	if sellable < r.Qty {
		_ = s.compensateRedis(ctx, r.SKUID, r.Qty)
		return Reservation{}, false, ErrInsufficientStock
	}

	_, err = tx.Exec(ctx, `
		UPDATE sku_inventory
		SET sellable_qty = sellable_qty - $2,
		    reserved_qty = reserved_qty + $2,
		    version = version + 1,
		    updated_at = now()
		WHERE sku_id=$1`, r.SKUID, r.Qty)
	if err != nil {
		_ = s.compensateRedis(ctx, r.SKUID, r.Qty)
		return Reservation{}, false, err
	}

	now := time.Now().UTC()
	r.Status = StatusReserved
	r.CreatedAt = now
	r.UpdatedAt = now
	_, err = tx.Exec(ctx, `
		INSERT INTO reservations
		(reservation_id, order_id, sku_id, qty, status, idempotency_key, expire_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		r.ReservationID, r.OrderID, r.SKUID, r.Qty, r.Status, r.IdempotencyKey, r.ExpireAt, r.CreatedAt, r.UpdatedAt)
	if err != nil {
		_ = s.compensateRedis(ctx, r.SKUID, r.Qty)
		// Unique violation on idempotency_key / order_id → treat as replay.
		if existing, e2 := s.GetReservationByIdempotency(ctx, r.IdempotencyKey); e2 == nil {
			return existing, true, nil
		}
		return Reservation{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		_ = s.compensateRedis(ctx, r.SKUID, r.Qty)
		return Reservation{}, false, err
	}

	_ = s.RDB.Set(ctx, idemKey(r.IdempotencyKey), r.ReservationID, 24*time.Hour).Err()
	return r, false, nil
}

func (s *Store) compensateRedis(ctx context.Context, skuID string, qty int64) error {
	return s.RDB.IncrBy(ctx, stockKey(skuID), qty).Err()
}

func (s *Store) Confirm(ctx context.Context, reservationID, eventID, eventType string) (Reservation, bool, error) {
	return s.transition(ctx, reservationID, eventID, eventType, StatusConfirmed, func(tx pgx.Tx, r Reservation) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sku_inventory
			SET reserved_qty = reserved_qty - $2,
			    version = version + 1,
			    updated_at = now()
			WHERE sku_id=$1 AND reserved_qty >= $2`, r.SKUID, r.Qty)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrInvalidState
		}
		return nil
	}, nil)
}

func (s *Store) Release(ctx context.Context, reservationID, eventID, eventType string) (Reservation, bool, error) {
	return s.transition(ctx, reservationID, eventID, eventType, StatusReleased, func(tx pgx.Tx, r Reservation) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sku_inventory
			SET sellable_qty = sellable_qty + $2,
			    reserved_qty = reserved_qty - $2,
			    version = version + 1,
			    updated_at = now()
			WHERE sku_id=$1 AND reserved_qty >= $2`, r.SKUID, r.Qty)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrInvalidState
		}
		return nil
	}, func(ctx context.Context, r Reservation) error {
		return s.RDB.IncrBy(ctx, stockKey(r.SKUID), r.Qty).Err()
	})
}

func (s *Store) transition(
	ctx context.Context,
	reservationID, eventID, eventType, target string,
	apply func(pgx.Tx, Reservation) error,
	afterCommit func(context.Context, Reservation) error,
) (Reservation, bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Reservation{}, false, err
	}
	defer tx.Rollback(ctx)

	if eventID != "" {
		var existingResult string
		err = tx.QueryRow(ctx, `SELECT result FROM processed_events WHERE event_id=$1`, eventID).Scan(&existingResult)
		if err == nil {
			r, e2 := s.GetReservation(ctx, reservationID)
			if e2 != nil {
				return Reservation{}, true, e2
			}
			return r, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return Reservation{}, false, err
		}
	}

	var r Reservation
	err = tx.QueryRow(ctx, `
		SELECT reservation_id, order_id, sku_id, qty, status, idempotency_key, expire_at, created_at, updated_at
		FROM reservations WHERE reservation_id=$1 FOR UPDATE`, reservationID).
		Scan(&r.ReservationID, &r.OrderID, &r.SKUID, &r.Qty, &r.Status, &r.IdempotencyKey, &r.ExpireAt, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, false, ErrNotFound
	}
	if err != nil {
		return Reservation{}, false, err
	}

	if r.Status == target {
		if eventID != "" {
			_, _ = tx.Exec(ctx, `
				INSERT INTO processed_events (event_id, event_type, order_id, result)
				VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, eventID, eventType, r.OrderID, "replay:"+target)
			_ = tx.Commit(ctx)
		}
		return r, true, nil
	}
	if r.Status != StatusReserved {
		return r, false, fmt.Errorf("%w: status=%s want=reserved", ErrInvalidState, r.Status)
	}

	if err := apply(tx, r); err != nil {
		return Reservation{}, false, err
	}
	_, err = tx.Exec(ctx, `
		UPDATE reservations SET status=$2, updated_at=now() WHERE reservation_id=$1`, r.ReservationID, target)
	if err != nil {
		return Reservation{}, false, err
	}
	if eventID != "" {
		_, err = tx.Exec(ctx, `
			INSERT INTO processed_events (event_id, event_type, order_id, result)
			VALUES ($1,$2,$3,$4)`, eventID, eventType, r.OrderID, target)
		if err != nil {
			return Reservation{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Reservation{}, false, err
	}
	if afterCommit != nil {
		if err := afterCommit(ctx, r); err != nil {
			return Reservation{}, false, err
		}
	}
	r.Status = target
	r.UpdatedAt = time.Now().UTC()
	return r, false, nil
}

func (s *Store) EventProcessed(ctx context.Context, eventID string) (bool, error) {
	var n int
	err := s.DB.QueryRow(ctx, `SELECT 1 FROM processed_events WHERE event_id=$1`, eventID).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
