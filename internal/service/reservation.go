package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/store"
)

type ReservationService struct {
	Store *store.Store
	TTL   time.Duration
}

type ReserveRequest struct {
	IdempotencyKey string
	OrderID        string
	SKUID          string
	Qty            int64
}

func (s *ReservationService) Reserve(ctx context.Context, req ReserveRequest) (store.Reservation, bool, error) {
	r := store.Reservation{
		ReservationID:  uuid.NewString(),
		OrderID:        req.OrderID,
		SKUID:          req.SKUID,
		Qty:            req.Qty,
		IdempotencyKey: req.IdempotencyKey,
		ExpireAt:       time.Now().UTC().Add(s.TTL),
	}
	return s.Store.Reserve(ctx, r)
}

func (s *ReservationService) Confirm(ctx context.Context, reservationID, eventID string) (store.Reservation, bool, error) {
	return s.Store.Confirm(ctx, reservationID, eventID, "payment.succeeded")
}

func (s *ReservationService) Release(ctx context.Context, reservationID, eventID string) (store.Reservation, bool, error) {
	return s.Store.Release(ctx, reservationID, eventID, "order.timeout_cancelled")
}

func (s *ReservationService) ConfirmByOrder(ctx context.Context, orderID, eventID string) (store.Reservation, bool, error) {
	r, err := s.Store.GetReservationByOrder(ctx, orderID)
	if err != nil {
		return store.Reservation{}, false, err
	}
	return s.Confirm(ctx, r.ReservationID, eventID)
}

func (s *ReservationService) ReleaseByOrder(ctx context.Context, orderID, eventID string) (store.Reservation, bool, error) {
	r, err := s.Store.GetReservationByOrder(ctx, orderID)
	if err != nil {
		return store.Reservation{}, false, err
	}
	return s.Release(ctx, r.ReservationID, eventID)
}

func (s *ReservationService) GetSKU(ctx context.Context, skuID string) (store.SKU, error) {
	return s.Store.GetSKU(ctx, skuID)
}

func (s *ReservationService) GetReservation(ctx context.Context, id string) (store.Reservation, error) {
	return s.Store.GetReservation(ctx, id)
}
