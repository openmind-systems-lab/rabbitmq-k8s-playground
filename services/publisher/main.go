package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Order is the payload published to the topic and fanout exchanges.
type Order struct {
	OrderNb      string `json:"orderNb"`
	CustomerName string `json:"customerName"`
	Amount       string `json:"amount"`
	RoutingKey   string `json:"routingKey"`
	Poison       bool   `json:"poison"`
}

var customers = []string{"Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Heidi"}

// routingKeys are the topic routing keys used to route work messages.
var routingKeys = []string{"orders.created", "orders.shipped", "orders.cancelled"}

// amqpPublisher owns a single AMQP connection and channel with publisher confirms.
type amqpPublisher struct {
	mu             sync.Mutex
	conn           *amqp.Connection
	ch             *amqp.Channel
	url            string
	exchangeTopic  string
	exchangeFanout string
	confirmTimeout time.Duration
}

func main() {
	pub := &amqpPublisher{
		url:            amqpURL(),
		exchangeTopic:  envOr("PUBLISH_EXCHANGE_TOPIC", "orders.topic"),
		exchangeFanout: envOr("PUBLISH_EXCHANGE_FANOUT", "orders.fanout"),
		confirmTimeout: 5 * time.Second,
	}
	if err := pub.ensureChannel(); err != nil {
		log.Fatalf("❌ initial RabbitMQ connection failed: %v", err)
	}

	intervalMS := envInt("PUBLISH_INTERVAL_MS", 2000)
	burst := envInt("PUBLISH_BURST", 10)
	poisonEvery := envInt("POISON_EVERY", 15)

	var counter atomic.Int64

	// HTTP server used by Kubernetes probes and by the test scripts.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if pub.ready() {
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		count := queryInt(r, "count", 1)
		poison := queryInt(r, "poison", 0)
		published := publishBatch(pub, &counter, count, false)
		published += publishBatch(pub, &counter, poison, true)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{
			"requested": count + poison,
			"published": published,
		})
	})
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("⚠️ http server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("🚀 Publisher started (topic=%s fanout=%s interval=%dms poisonEvery=%d)",
		pub.exchangeTopic, pub.exchangeFanout, intervalMS, poisonEvery)

	publishBatch(pub, &counter, burst, false)

	ticker := time.NewTicker(time.Duration(intervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("👋 Shutting down")
			pub.close()
			return
		case <-ticker.C:
			n := counter.Add(1)
			poison := poisonEvery > 0 && n%int64(poisonEvery) == 0
			pub.publishAndLog(newOrder(n, poison))
		}
	}
}

// publishBatch publishes n orders and returns how many were confirmed.
func publishBatch(pub *amqpPublisher, counter *atomic.Int64, n int, poison bool) int {
	published := 0
	for i := 0; i < n; i++ {
		if pub.publishAndLog(newOrder(counter.Add(1), poison)) {
			published++
		}
	}
	if n > 0 {
		log.Printf("📦 Batch published: %d/%d messages (poison=%t)", published, n, poison)
	}
	return published
}

// newOrder builds the next order to publish.
func newOrder(n int64, poison bool) Order {
	customer := customers[int(n)%len(customers)]
	if poison {
		customer = "POISON"
	}
	return Order{
		OrderNb:      fmt.Sprintf("ORD-%05d", n),
		CustomerName: customer,
		Amount:       fmt.Sprintf("%.2f", float64(10+(n%90))+0.5),
		RoutingKey:   routingKeys[int(n)%len(routingKeys)],
		Poison:       poison,
	}
}

// publishAndLog publishes one order to the topic and fanout exchanges.
func (p *amqpPublisher) publishAndLog(o Order) bool {
	if err := p.publish(o); err != nil {
		log.Printf("⚠️ publish failed orderNb=%s: %v", o.OrderNb, err)
		return false
	}
	if o.Poison {
		log.Printf("☠️ published POISON orderNb=%s routingKey=%s", o.OrderNb, o.RoutingKey)
	} else {
		log.Printf("📨 published orderNb=%s customerName=%s routingKey=%s", o.OrderNb, o.CustomerName, o.RoutingKey)
	}
	return true
}

// publish sends the order to both exchanges with publisher confirms, retrying once.
func (p *amqpPublisher) publish(o Order) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if err = p.ensureChannel(); err != nil {
			continue
		}
		if err = p.publishOnce(context.Background(), o); err == nil {
			return nil
		}
		p.close()
	}
	return err
}

func (p *amqpPublisher) publishOnce(ctx context.Context, o Order) error {
	body, err := json.Marshal(o)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    o.OrderNb,
		Timestamp:    time.Now(),
		Body:         body,
	}
	if err := p.confirmPublish(ctx, p.exchangeTopic, o.RoutingKey, msg); err != nil {
		return fmt.Errorf("topic publish: %w", err)
	}
	if err := p.confirmPublish(ctx, p.exchangeFanout, "", msg); err != nil {
		return fmt.Errorf("fanout publish: %w", err)
	}
	return nil
}

// confirmPublish publishes and waits for the broker confirmation.
func (p *amqpPublisher) confirmPublish(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	cctx, cancel := context.WithTimeout(ctx, p.confirmTimeout)
	defer cancel()
	dc, err := p.ch.PublishWithDeferredConfirmWithContext(cctx, exchange, key, false, false, msg)
	if err != nil {
		return err
	}
	select {
	case <-dc.Done():
		if !dc.Acked() {
			return fmt.Errorf("broker nacked the message")
		}
		return nil
	case <-cctx.Done():
		return fmt.Errorf("publish confirm timeout")
	}
}

// ensureChannel (re)opens the connection and channel with confirms enabled.
func (p *amqpPublisher) ensureChannel() error {
	if p.ch != nil && p.conn != nil && !p.conn.IsClosed() {
		return nil
	}
	p.close()
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return err
	}
	p.conn = conn
	p.ch = ch
	return nil
}

func (p *amqpPublisher) ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn != nil && p.ch != nil && !p.conn.IsClosed()
}

func (p *amqpPublisher) close() {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
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

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func queryInt(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}
