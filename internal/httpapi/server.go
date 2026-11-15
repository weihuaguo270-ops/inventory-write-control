package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/weihuaguo270-ops/inventory-write-control/internal/service"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/store"
)

type Server struct {
	Svc *service.ReservationService
	Mux *http.ServeMux
}

func New(svc *service.ReservationService) *Server {
	s := &Server{Svc: svc, Mux: http.NewServeMux()}
	s.Mux.HandleFunc("GET /healthz", s.handleHealth)
	s.Mux.HandleFunc("POST /reservations", s.handleReserve)
	s.Mux.HandleFunc("GET /reservations/{id}", s.handleGetReservation)
	s.Mux.HandleFunc("POST /reservations/{id}/confirm", s.handleConfirm)
	s.Mux.HandleFunc("POST /reservations/{id}/release", s.handleRelease)
	s.Mux.HandleFunc("GET /skus/{id}", s.handleGetSKU)
	s.Mux.HandleFunc("POST /events", s.handleEvent) // same path as MQ consumer dispatch
	return s
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type reserveBody struct {
	OrderID string `json:"order_id"`
	SKUID   string `json:"sku_id"`
	Qty     int64  `json:"qty"`
}

func (s *Server) handleReserve(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key required")
		return
	}
	var body reserveBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	if body.OrderID == "" || body.SKUID == "" || body.Qty <= 0 {
		writeErr(w, http.StatusBadRequest, "order_id, sku_id, qty required")
		return
	}
	res, replay, err := s.Svc.Reserve(r.Context(), service.ReserveRequest{
		IdempotencyKey: key,
		OrderID:        body.OrderID,
		SKUID:          body.SKUID,
		Qty:            body.Qty,
	})
	if errors.Is(err, store.ErrInsufficientStock) {
		writeErr(w, http.StatusConflict, "insufficient stock")
		return
	}
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "sku not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	status := http.StatusCreated
	if replay {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"reservation": res,
		"replay":      replay,
	})
}

func (s *Server) handleGetReservation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	res, err := s.Svc.GetReservation(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type actionBody struct {
	EventID string `json:"event_id"`
}

func (s *Server) handleConfirm(w http.ResponseWriter, r *http.Request) {
	s.handleTransition(w, r, true)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	s.handleTransition(w, r, false)
}

func (s *Server) handleTransition(w http.ResponseWriter, r *http.Request, confirm bool) {
	id := r.PathValue("id")
	var body actionBody
	_ = json.NewDecoder(r.Body).Decode(&body)
	var (
		res    store.Reservation
		replay bool
		err    error
	)
	if confirm {
		res, replay, err = s.Svc.Confirm(r.Context(), id, body.EventID)
	} else {
		res, replay, err = s.Svc.Release(r.Context(), id, body.EventID)
	}
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if errors.Is(err, store.ErrInvalidState) {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reservation": res, "replay": replay})
}

func (s *Server) handleGetSKU(w http.ResponseWriter, r *http.Request) {
	sku, err := s.Svc.GetSKU(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sku)
}

type EventBody struct {
	EventID       string `json:"event_id"`
	Type          string `json:"type"`
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
}

func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	var body EventBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	res, replay, err := s.DispatchEvent(r.Context(), body)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if errors.Is(err, store.ErrInvalidState) {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reservation": res, "replay": replay})
}

func (s *Server) DispatchEvent(ctx context.Context, body EventBody) (store.Reservation, bool, error) {
	if body.EventID == "" || body.Type == "" {
		return store.Reservation{}, false, errors.New("event_id and type required")
	}
	switch body.Type {
	case "payment.succeeded":
		if body.ReservationID != "" {
			return s.Svc.Confirm(ctx, body.ReservationID, body.EventID)
		}
		if body.OrderID == "" {
			return store.Reservation{}, false, errors.New("order_id or reservation_id required")
		}
		return s.Svc.ConfirmByOrder(ctx, body.OrderID, body.EventID)
	case "order.timeout_cancelled":
		if body.ReservationID != "" {
			return s.Svc.Release(ctx, body.ReservationID, body.EventID)
		}
		if body.OrderID == "" {
			return store.Reservation{}, false, errors.New("order_id or reservation_id required")
		}
		return s.Svc.ReleaseByOrder(ctx, body.OrderID, body.EventID)
	default:
		return store.Reservation{}, false, errors.New("unknown event type: " + body.Type)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
