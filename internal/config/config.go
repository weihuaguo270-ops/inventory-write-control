package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr             string
	DatabaseURL          string
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	RocketMQNameSrv      string
	RocketMQTopic        string
	RocketMQConsumerGrp  string
	RocketMQProducerGrp  string
	SeedSKUID            string
	SeedSellableQty      int64
	ReservationTTL       time.Duration
	DisableMQ            bool
}

func Load() Config {
	return Config{
		HTTPAddr:            getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:         getenv("DATABASE_URL", "postgres://inv:inv@localhost:5432/inventory?sslmode=disable"),
		RedisAddr:           getenv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:       getenv("REDIS_PASSWORD", ""),
		RedisDB:             getenvInt("REDIS_DB", 0),
		RocketMQNameSrv:     getenv("ROCKETMQ_NAMESRV", "127.0.0.1:9876"),
		RocketMQTopic:       getenv("ROCKETMQ_TOPIC", "inventory-events"),
		RocketMQConsumerGrp: getenv("ROCKETMQ_CONSUMER_GROUP", "inventory-write-control"),
		RocketMQProducerGrp: getenv("ROCKETMQ_PRODUCER_GROUP", "inventory-write-control-demo"),
		SeedSKUID:           getenv("SEED_SKU_ID", "sku-demo-1"),
		SeedSellableQty:     int64(getenvInt("SEED_SELLABLE_QTY", 10)),
		ReservationTTL:      time.Duration(getenvInt("RESERVATION_TTL_SEC", 900)) * time.Second,
		DisableMQ:           getenv("DISABLE_MQ", "0") == "1",
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
