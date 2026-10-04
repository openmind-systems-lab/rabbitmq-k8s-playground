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
	mode := envOr("MODE", "audit")
	queue := envOr("RABBITMQ_QUEUE", "orders."+mode)

	db := connectDB()
	defer db.Close()
	ensureSchema(db)

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

	log.Printf("🔔 Fanout consumer started (mode=%s queue=%s)", mode, queue)
	for ctx.Err() == nil {
		if err := consume(ctx, db, mode, queue); err != nil && ctx.Err() == nil {
			log.Printf("⚠️ consumer stopped: %v (reconnecting in 3s)", err)
			sleepCtx(ctx, 3*time.Second)
		}
	}
	log.Println("👋 Shutting down")
}

// consume binds to its own queue and records every broadcast message it receives.
func consume(ctx context.Context, db *sql.DB, mode, queue string) error {
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

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	msgs, err := ch.Consume(queue, mode, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}

	log.Printf("🎧 [%s] consuming %s", mode, queue)
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			var o Order
			if err := json.Unmarshal(d.Body, &o); err != nil {
				log.Printf("⚠️ [%s] malformed message dropped: %v", mode, err)
				_ = d.Ack(false)
				continue
			}
			if err := insertEvent(db, mode, o); err != nil {
				log.Printf("❌ [%s] db insert failed for %s: %v", mode, o.OrderNb, err)
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
			log.Printf("🔔 [%s] received %s customer=%s", mode, o.OrderNb, o.CustomerName)
		}
	}
}

func insertEvent(db *sql.DB, mode string, o Order) error {
	_, err := db.Exec(
		`INSERT INTO fanout_events (mode, order_nb, poison)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (mode, order_nb) DO NOTHING`,
		mode, o.OrderNb, o.Poison)
	return err
}

func connectDB() *sql.DB {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		envOr("DB_HOST", "postgres"), envOr("DB_PORT", "5432"),
		envOr("DB_USER", "orders"), envOr("DB_PASSWORD", "orders-demo-change-me"), envOr("DB_NAME", "orders"))

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
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS fanout_events (
		id          bigserial PRIMARY KEY,
		mode        text NOT NULL,
		order_nb    text NOT NULL,
		poison      boolean NOT NULL DEFAULT false,
		received_at timestamptz NOT NULL DEFAULT now(),
		UNIQUE(mode, order_nb)
	)`)
	if err != nil {
		log.Fatalf("❌ schema creation failed: %v", err)
	}
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

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
