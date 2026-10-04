package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Order mirrors the payload produced by the publisher.
type Order struct {
	OrderNb      string `json:"orderNb"`
	CustomerName string `json:"customerName"`
	Amount       string `json:"amount"`
	RoutingKey   string `json:"routingKey"`
	Poison       bool   `json:"poison"`
}

func main() {
	db := connectDB()
	defer db.Close()
	ensureSchema(db)

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("⚠️ http server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Reconnect loop: keeps the worker consuming across broker restarts.
	for ctx.Err() == nil {
		if err := consume(ctx, db, hostname); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ consumer stopped: %v (reconnecting in 3s)", err)
			sleepCtx(ctx, 3*time.Second)
		}
	}
	log.Println("👋 Shutting down")
}

// consume connects to RabbitMQ and processes orders.work until the channel closes.
func consume(ctx context.Context, db *sql.DB, worker string) error {
	queue := envOr("RABBITMQ_QUEUE", "orders.work")
	prefetch := envInt("PREFETCH", 1)

	conn, err := amqp.Dial(amqpURL())
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	// Fair dispatch: at most `prefetch` unacknowledged messages per worker.
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	msgs, err := ch.Consume(queue, worker, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}

	log.Printf("👷 Worker %s consuming %s (prefetch=%d, manual ack)", worker, queue, prefetch)

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			handle(db, worker, d)
		}
	}
}

// handle processes a single delivery and acknowledges it only after the DB write.
func handle(db *sql.DB, worker string, d amqp.Delivery) {
	var o Order
	if err := json.Unmarshal(d.Body, &o); err != nil {
		log.Printf("☠️ malformed message rejected -> DLQ: %v", err)
		_ = d.Nack(false, false)
		return
	}
	if o.Poison || o.CustomerName == "POISON" {
		log.Printf("☠️ poison order %s rejected -> DLQ", o.OrderNb)
		_ = d.Nack(false, false)
		return
	}
	if err := insertProcessed(db, worker, o, d.RoutingKey); err != nil {
		log.Printf("❌ db insert failed for %s: %v (requeue)", o.OrderNb, err)
		_ = d.Nack(false, true)
		return
	}
	if err := d.Ack(false); err != nil {
		log.Printf("⚠️ ack failed for %s: %v", o.OrderNb, err)
		return
	}
	log.Printf("✅ processed %s customer=%s routingKey=%s worker=%s", o.OrderNb, o.CustomerName, d.RoutingKey, worker)
}

// connectDB opens PostgreSQL with retries so the worker can start before the DB is ready.
func connectDB() *sql.DB {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		envOr("DB_HOST", "postgres"), envOr("DB_PORT", "5432"),
		envOr("DB_USER", "orders"), envOr("DB_PASSWORD", "orders-demo-change-me"), envOr("DB_NAME", "orders"))

	log.Printf("🔌 Worker connecting to PostgreSQL at %s:%s...", envOr("DB_HOST", "postgres"), envOr("DB_PORT", "5432"))
	var db *sql.DB
	var err error
	for i := 0; i < 20; i++ {
		db, err = sql.Open("postgres", dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				return db
			} else {
				err = pingErr
			}
		}
		log.Printf("⚠️ DB not ready (attempt %d/20): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	log.Fatalf("❌ DB connection failed: %v", err)
	return nil
}

func ensureSchema(db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS processed_orders (
		order_nb     text PRIMARY KEY,
		routing_key  text NOT NULL,
		worker       text NOT NULL,
		processed_at timestamptz NOT NULL DEFAULT now()
	)`)
	if err != nil {
		log.Fatalf("❌ schema creation failed: %v", err)
	}
	log.Println("✅ processed_orders table ready")
}

func insertProcessed(db *sql.DB, worker string, o Order, routingKey string) error {
	_, err := db.Exec(
		`INSERT INTO processed_orders (order_nb, routing_key, worker)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (order_nb) DO NOTHING`,
		o.OrderNb, routingKey, worker)
	return err
}

// amqpURL builds the AMQP connection string from environment variables.
func amqpURL() string {
	u := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(envOr("RABBITMQ_USERNAME", "guest"), envOr("RABBITMQ_PASSWORD", "guest")),
		Host:   net.JoinHostPort(envOr("RABBITMQ_HOST", "localhost"), envOr("RABBITMQ_PORT", "5672")),
		Path:   envOr("RABBITMQ_VHOST", "/"),
	}
	return u.String()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
