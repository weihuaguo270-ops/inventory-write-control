package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/config"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/httpapi"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/mq"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/service"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/store"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}

	migrationsDir := filepath.Join(findRoot(), "migrations")
	if err := store.Migrate(ctx, pool, migrationsDir); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	st := &store.Store{DB: pool, RDB: rdb}
	if os.Getenv("SEED_RESET") == "1" {
		if err := st.ResetSKU(ctx, cfg.SeedSKUID, cfg.SeedSellableQty); err != nil {
			log.Fatalf("reset sku: %v", err)
		}
		log.Printf("reset sku=%s sellable=%d", cfg.SeedSKUID, cfg.SeedSellableQty)
	} else if err := st.UpsertSKU(ctx, cfg.SeedSKUID, cfg.SeedSellableQty); err != nil {
		log.Fatalf("seed sku: %v", err)
	} else {
		log.Printf("seeded sku=%s sellable=%d", cfg.SeedSKUID, cfg.SeedSellableQty)
	}

	svc := &service.ReservationService{Store: st, TTL: cfg.ReservationTTL}
	api := httpapi.New(svc)

	var consumer *mq.Consumer
	if !cfg.DisableMQ {
		consumer, err = mq.StartConsumer(cfg.RocketMQNameSrv, cfg.RocketMQConsumerGrp, cfg.RocketMQTopic, func(ctx context.Context, body httpapi.EventBody) error {
			_, _, err := api.DispatchEvent(ctx, body)
			return err
		})
		if err != nil {
			log.Printf("mq consumer start failed (HTTP /events still works): %v", err)
		} else {
			log.Printf("mq consumer listening topic=%s", cfg.RocketMQTopic)
			defer consumer.Shutdown()
		}
	} else {
		log.Printf("mq disabled; use POST /events to simulate")
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: api.Mux}
	go func() {
		log.Printf("http listening %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func findRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	dir := wd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "migrations")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return wd
}
