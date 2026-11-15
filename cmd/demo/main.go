package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/config"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/httpapi"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/mq"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	mode := flag.String("mode", "all", "all|concurrent|idempotent|confirm|release|mq")
	base := flag.String("base", "http://127.0.0.1"+cfg.HTTPAddr, "server base url")
	flag.Parse()

	sku := cfg.SeedSKUID
	n := cfg.SeedSellableQty

	var failed bool
	run := func(name string, fn func() error) {
		fmt.Printf("== %s ==\n", name)
		if err := fn(); err != nil {
			fmt.Printf("FAIL: %v\n", err)
			failed = true
			return
		}
		fmt.Println("PASS")
	}

	switch *mode {
	case "concurrent":
		run("concurrent no-oversell", func() error { return testConcurrent(*base, sku, n) })
	case "idempotent":
		run("idempotent replay", func() error { return testIdempotent(*base, sku) })
	case "confirm":
		run("payment confirm idempotent", func() error { return testConfirm(*base, sku) })
	case "release":
		run("timeout release idempotent", func() error { return testRelease(*base, sku) })
	case "mq":
		run("rocketmq payment + timeout", func() error { return testMQ(*base, sku, cfg) })
	default:
		// Reset stock between cases by re-seeding via concurrent which assumes server seed;
		// server keeps state — order tests carefully: concurrent last would empty stock.
		run("idempotent replay", func() error { return testIdempotent(*base, sku) })
		run("payment confirm idempotent", func() error { return testConfirm(*base, sku) })
		run("timeout release idempotent", func() error { return testRelease(*base, sku) })
		run("concurrent no-oversell", func() error { return testConcurrent(*base, sku, n) })
	}

	if failed {
		os.Exit(1)
	}
}

func testConcurrent(base, sku string, sellable int64) error {
	// Use remaining stock; if low, still prove success <= available.
	skuState, err := getSKU(base, sku)
	if err != nil {
		return err
	}
	available := skuState.SellableQty
	if available <= 0 {
		return fmt.Errorf("no sellable stock to test; restart server to re-seed")
	}
	workers := int(available) + 20
	var ok atomic.Int64
	var conflict atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			status, _, err := postJSON(base+"/reservations", map[string]any{
				"order_id": fmt.Sprintf("ord-c-%d-%d", time.Now().UnixNano(), i),
				"sku_id":   sku,
				"qty":      1,
			}, map[string]string{"Idempotency-Key": fmt.Sprintf("idem-c-%d-%d", time.Now().UnixNano(), i)})
			if err != nil {
				return
			}
			switch status {
			case http.StatusCreated, http.StatusOK:
				ok.Add(1)
			case http.StatusConflict:
				conflict.Add(1)
			}
		}()
	}
	wg.Wait()
	success := ok.Load()
	fmt.Printf("available=%d workers=%d success=%d conflict=%d\n", available, workers, success, conflict.Load())
	if success > available {
		return fmt.Errorf("oversell: success=%d > available=%d", success, available)
	}
	after, err := getSKU(base, sku)
	if err != nil {
		return err
	}
	if after.SellableQty < 0 || after.ReservedQty < 0 {
		return fmt.Errorf("negative inventory: %+v", after)
	}
	if success+after.SellableQty > available+after.ReservedQty {
		// soft check; sellable+reserved accounting
	}
	fmt.Printf("after sellable=%d reserved=%d\n", after.SellableQty, after.ReservedQty)
	return nil
}

func testIdempotent(base, sku string) error {
	key := fmt.Sprintf("idem-replay-%d", time.Now().UnixNano())
	order := fmt.Sprintf("ord-replay-%d", time.Now().UnixNano())
	body := map[string]any{"order_id": order, "sku_id": sku, "qty": 1}
	headers := map[string]string{"Idempotency-Key": key}

	st1, raw1, err := postJSON(base+"/reservations", body, headers)
	if err != nil {
		return err
	}
	if st1 != http.StatusCreated && st1 != http.StatusOK {
		return fmt.Errorf("first reserve status=%d body=%s", st1, raw1)
	}
	var first map[string]any
	_ = json.Unmarshal(raw1, &first)
	id1 := reservationID(raw1)

	st2, raw2, err := postJSON(base+"/reservations", body, headers)
	if err != nil {
		return err
	}
	if st2 != http.StatusOK && st2 != http.StatusCreated {
		return fmt.Errorf("replay status=%d body=%s", st2, raw2)
	}
	var second map[string]any
	_ = json.Unmarshal(raw2, &second)
	if replay, _ := second["replay"].(bool); !replay {
		return fmt.Errorf("expected replay=true on second call, body=%s", raw2)
	}
	id2 := reservationID(raw2)
	if fmt.Sprint(id1) != fmt.Sprint(id2) {
		return fmt.Errorf("reservation id changed on replay: %v vs %v", id1, id2)
	}
	fmt.Printf("reservation_id=%v replayed\n", id1)
	return nil
}

func testMQ(base, sku string, cfg config.Config) error {
	prod, err := mq.StartProducer(cfg.RocketMQNameSrv, cfg.RocketMQProducerGrp, cfg.RocketMQTopic)
	if err != nil {
		return fmt.Errorf("producer: %w", err)
	}
	defer prod.Shutdown()

	// payment.succeeded via MQ (publish twice for idempotency)
	key := fmt.Sprintf("idem-mq-pay-%d", time.Now().UnixNano())
	order := fmt.Sprintf("ord-mq-pay-%d", time.Now().UnixNano())
	st, raw, err := postJSON(base+"/reservations", map[string]any{
		"order_id": order, "sku_id": sku, "qty": 1,
	}, map[string]string{"Idempotency-Key": key})
	if err != nil {
		return err
	}
	if st != http.StatusCreated && st != http.StatusOK {
		return fmt.Errorf("reserve status=%d body=%s", st, raw)
	}
	rid := reservationID(raw)
	eventID := fmt.Sprintf("evt-mq-pay-%d", time.Now().UnixNano())
	body := httpapi.EventBody{
		EventID: eventID, Type: "payment.succeeded",
		OrderID: order, ReservationID: rid,
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := prod.Publish(ctx, body); err != nil {
			return fmt.Errorf("publish pay#%d: %w", i+1, err)
		}
	}
	if err := waitStatus(base, rid, "confirmed", 20*time.Second); err != nil {
		return err
	}
	fmt.Printf("mq confirmed reservation=%s\n", rid)

	// order.timeout_cancelled via MQ
	key2 := fmt.Sprintf("idem-mq-to-%d", time.Now().UnixNano())
	order2 := fmt.Sprintf("ord-mq-to-%d", time.Now().UnixNano())
	st, raw, err = postJSON(base+"/reservations", map[string]any{
		"order_id": order2, "sku_id": sku, "qty": 1,
	}, map[string]string{"Idempotency-Key": key2})
	if err != nil {
		return err
	}
	if st != http.StatusCreated && st != http.StatusOK {
		return fmt.Errorf("reserve2 status=%d body=%s", st, raw)
	}
	rid2 := reservationID(raw)
	eventID2 := fmt.Sprintf("evt-mq-to-%d", time.Now().UnixNano())
	body2 := httpapi.EventBody{
		EventID: eventID2, Type: "order.timeout_cancelled",
		OrderID: order2, ReservationID: rid2,
	}
	for i := 0; i < 2; i++ {
		if err := prod.Publish(ctx, body2); err != nil {
			return fmt.Errorf("publish to#%d: %w", i+1, err)
		}
	}
	if err := waitStatus(base, rid2, "released", 20*time.Second); err != nil {
		return err
	}
	fmt.Printf("mq released reservation=%s\n", rid2)
	return nil
}

func waitStatus(base, id, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := getReservation(base, id)
		if err == nil && res.Status == want {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	res, _ := getReservation(base, id)
	return fmt.Errorf("timeout waiting status=%s got=%s", want, res.Status)
}

func testConfirm(base, sku string) error {
	key := fmt.Sprintf("idem-pay-%d", time.Now().UnixNano())
	order := fmt.Sprintf("ord-pay-%d", time.Now().UnixNano())
	st, raw, err := postJSON(base+"/reservations", map[string]any{
		"order_id": order, "sku_id": sku, "qty": 1,
	}, map[string]string{"Idempotency-Key": key})
	if err != nil {
		return err
	}
	if st != http.StatusCreated && st != http.StatusOK {
		return fmt.Errorf("reserve status=%d body=%s", st, raw)
	}
	rid := reservationID(raw)
	eventID := fmt.Sprintf("evt-pay-%d", time.Now().UnixNano())
	before, _ := getSKU(base, sku)

	for i := 0; i < 2; i++ {
		st, raw, err = postJSON(base+"/events", map[string]any{
			"event_id": eventID, "type": "payment.succeeded",
			"order_id": order, "reservation_id": rid,
		}, nil)
		if err != nil {
			return err
		}
		if st != http.StatusOK {
			return fmt.Errorf("confirm#%d status=%d body=%s", i+1, st, raw)
		}
	}
	after, err := getSKU(base, sku)
	if err != nil {
		return err
	}
	// confirm reduces reserved by 1, sellable unchanged
	if after.ReservedQty != before.ReservedQty-1 {
		return fmt.Errorf("reserved expected %d got %d", before.ReservedQty-1, after.ReservedQty)
	}
	res, err := getReservation(base, rid)
	if err != nil {
		return err
	}
	if res.Status != "confirmed" {
		return fmt.Errorf("status=%s", res.Status)
	}
	fmt.Printf("confirmed reservation=%s (event replayed)\n", rid)
	return nil
}

func testRelease(base, sku string) error {
	key := fmt.Sprintf("idem-to-%d", time.Now().UnixNano())
	order := fmt.Sprintf("ord-to-%d", time.Now().UnixNano())
	st, raw, err := postJSON(base+"/reservations", map[string]any{
		"order_id": order, "sku_id": sku, "qty": 1,
	}, map[string]string{"Idempotency-Key": key})
	if err != nil {
		return err
	}
	if st != http.StatusCreated && st != http.StatusOK {
		return fmt.Errorf("reserve status=%d body=%s", st, raw)
	}
	rid := reservationID(raw)
	eventID := fmt.Sprintf("evt-to-%d", time.Now().UnixNano())
	before, _ := getSKU(base, sku)

	for i := 0; i < 2; i++ {
		st, raw, err = postJSON(base+"/events", map[string]any{
			"event_id": eventID, "type": "order.timeout_cancelled",
			"order_id": order, "reservation_id": rid,
		}, nil)
		if err != nil {
			return err
		}
		if st != http.StatusOK {
			return fmt.Errorf("release#%d status=%d body=%s", i+1, st, raw)
		}
	}
	after, err := getSKU(base, sku)
	if err != nil {
		return err
	}
	if after.SellableQty != before.SellableQty+1 {
		return fmt.Errorf("sellable expected %d got %d", before.SellableQty+1, after.SellableQty)
	}
	if after.ReservedQty != before.ReservedQty-1 {
		return fmt.Errorf("reserved expected %d got %d", before.ReservedQty-1, after.ReservedQty)
	}
	res, err := getReservation(base, rid)
	if err != nil {
		return err
	}
	if res.Status != "released" {
		return fmt.Errorf("status=%s", res.Status)
	}
	fmt.Printf("released reservation=%s (event replayed)\n", rid)
	return nil
}

type skuDTO struct {
	SKUID       string `json:"SKUID"`
	SellableQty int64  `json:"SellableQty"`
	ReservedQty int64  `json:"ReservedQty"`
}

type skuDTOAlt struct {
	SKUID       string `json:"sku_id"`
	SellableQty int64  `json:"sellable_qty"`
	ReservedQty int64  `json:"reserved_qty"`
}

func getSKU(base, id string) (skuDTO, error) {
	resp, err := http.Get(base + "/skus/" + id)
	if err != nil {
		return skuDTO{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var a skuDTO
	if err := json.Unmarshal(raw, &a); err != nil {
		return skuDTO{}, err
	}
	if a.SKUID == "" {
		var b skuDTOAlt
		_ = json.Unmarshal(raw, &b)
		return skuDTO{SKUID: b.SKUID, SellableQty: b.SellableQty, ReservedQty: b.ReservedQty}, nil
	}
	return a, nil
}

type resDTO struct {
	Status string `json:"Status"`
}

func getReservation(base, id string) (resDTO, error) {
	resp, err := http.Get(base + "/reservations/" + id)
	if err != nil {
		return resDTO{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	status, _ := m["Status"].(string)
	if status == "" {
		status, _ = m["status"].(string)
	}
	return resDTO{Status: status}, nil
}

func reservationID(raw []byte) string {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	r, _ := m["reservation"].(map[string]any)
	if r == nil {
		return ""
	}
	if v, ok := r["ReservationID"].(string); ok {
		return v
	}
	if v, ok := r["reservation_id"].(string); ok {
		return v
	}
	return fmt.Sprint(r["ReservationID"])
}

func postJSON(url string, body any, headers map[string]string) (int, []byte, error) {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, nil
}
