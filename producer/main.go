package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	database "producer/db"
	TopicCreater "producer/topic"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Order struct {
	ID       string `json:"id"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

// App struct holds our global dependencies
type App struct {
	DB        *sql.DB
	Transport *kafka.Transport // Holds our secure SSL configuration
}

// The HTTP Handler
func (app *App) createOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var order Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// --- 1. Database Logic ---
	_, err := app.DB.Exec("INSERT INTO orders (id, item, quantity, status) VALUES ($1, $2, $3, 'PENDING')",
		order.ID, order.Item, order.Quantity)

	if err != nil {
		log.Printf("Failed to save order %s to DB: %v", order.ID, err)
		http.Error(w, "Failed to process order (Database Error)", http.StatusInternalServerError)
		return
	}
	fmt.Printf("📦 Order %s saved to database as PENDING.\n", order.ID)

	// --- 2. Secure Kafka Logic ---
	writer := &kafka.Writer{
		Addr:         kafka.TCP(TopicCreater.GetBrokers()...),
		Topic:        "orders-topic",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Transport:    app.Transport, // Inject the secure SSL transport here!
	}
	defer writer.Close()

	orderBytes, _ := json.Marshal(order)

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(order.ID),
			Value: orderBytes,
		},
	)

	if err != nil {
		log.Printf("Failed to write message to Kafka: %v", err)
		http.Error(w, "Order saved, but failed to notify cluster", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Order %s submitted successfully to database and encrypted cluster!\n", order.ID)
}

func main() {
	// ---> 1. LOAD THE .ENV FILE FIRST <---
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️ No .env file found, relying on system environment variables")
	}

	db, err := database.ConnectToDB()
	if err != nil {
		log.Fatal("Database connection failed: ", err)
	}

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS orders (
		id VARCHAR(50) PRIMARY KEY,
		item VARCHAR(100),
		quantity INT,
		status VARCHAR(20) DEFAULT 'PENDING'
	);`

	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	fmt.Println("✅ Orders table is ready.")

	secureTransport := &kafka.Transport{
		TLS: TopicCreater.GetTLSConfig(),
	}

	app := &App{
		DB:        db,
		Transport: secureTransport,
	}

	// Initialize topic over SSL
	TopicCreater.InitKafkaTopic()

	http.HandleFunc("/orders", app.createOrderHandler)
	fmt.Println("🚀 Secure Producer API running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
