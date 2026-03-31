package main

import (
	database "consumer/db"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Order struct {
	ID       string `json:"id"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

func GetBrokers() []string {
	brokerStr := os.Getenv("KAFKA_BROKERS")
	if brokerStr == "" {
		fmt.Println("Running locally without Docker env vars, defaulting to localhost ports")
		return []string{"localhost:29092", "localhost:39092", "localhost:49092"}
	}
	return strings.Split(brokerStr, ",")
}

// getTLSConfig reads our PEM files and packages them into a Go TLS Object
func getTLSConfig() *tls.Config {
	certFile := os.Getenv("SSL_CERT_FILE")
	keyFile := os.Getenv("SSL_KEY_FILE")
	caFile := os.Getenv("SSL_CA_FILE")
	fmt.Println(caFile)
	// If we aren't passing certificates (like testing locally without Docker), return nil
	if certFile == "" || keyFile == "" || caFile == "" {
		log.Println("FATAL!!!: No SSL environment variables found. Defaulting to PLAINTEXT mode.")
		return nil
	}

	// 1. Load the Client's Public and Private Keys
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}

	// 2. Load the Master CA Certificate
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Failed to load CA certificate: %v", err)
	}

	// 3. Put the CA into a "Pool" so Go knows to trust brokers signed by it
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// 4. Build the final TLS configuration
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		// Because we used "localhost" and custom names in our certificates,
		// we skip strict hostname verification, but we STILL verify the CA signature.
		InsecureSkipVerify: true,
	}
}

func main() {
	db, err := database.ConnectToDB()
	if err != nil {
		log.Fatal("Database connection failed: ", err)
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       getTLSConfig(),
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  GetBrokers(),
		GroupID:  "order-processor-group",
		Topic:    "orders-topic",
		MinBytes: 10e3,   // 10KB
		MaxBytes: 10e6,   // 10MB
		Dialer:   dialer, 
	})
	defer reader.Close()

	fmt.Println("🎧 Secure Consumer started. Listening for orders on encrypted cluster...")

	// 3. The Infinite Processing Loop
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var order Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			log.Printf("Failed to parse JSON for message at offset %d: %v", m.Offset, err)
			continue
		}

		fmt.Printf("\n Received Encrypted Order -> Topic: %s | Partition: %d | Key: %s\n",
			m.Topic, m.Partition, string(m.Key))

		// 4. Database Logic: Update the order status
		updateQuery := `UPDATE orders SET status = 'PROCESSED' WHERE id = $1`
		result, err := db.Exec(updateQuery, order.ID)

		if err != nil {
			log.Printf(" Failed to update order %s in DB: %v", order.ID, err)
			continue
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			log.Printf("Order %s not found in DB! (Was it saved by the producer?)", order.ID)
		} else {
			fmt.Printf("Successfully marked Order %s as PROCESSED in PostgreSQL!\n", order.ID)
		}
	}
}
