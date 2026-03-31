package TopicCreater

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func GetBrokers() []string {
	brokerStr := os.Getenv("KAFKA_BROKERS")
	if brokerStr == "" {
		fmt.Println("Running locally without Docker env vars, defaulting to localhost ports")
		return []string{"localhost:29092", "localhost:39092", "localhost:49092"}
	}
	return strings.Split(brokerStr, ",")
}

func GetTLSConfig() *tls.Config {
	certFile := os.Getenv("SSL_CERT_FILE")
	keyFile := os.Getenv("SSL_KEY_FILE")
	caFile := os.Getenv("SSL_CA_FILE")

	if certFile == "" || keyFile == "" || caFile == "" {
		log.Println("No SSL environment variables found. Defaulting to PLAINTEXT mode.")
		return nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Failed to load CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true, 
	}
}

// InitKafkaTopic attempts to securely connect to Kafka and create the topic if it doesn't exist
func InitKafkaTopic() {
	brokers := GetBrokers()
	var conn *kafka.Conn
	var err error

	// 1. Build the Secure Dialer
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       GetTLSConfig(),
	}

	for i := 1; i <= 7; i++ {
		conn, err = dialer.Dial("tcp", brokers[0])
		if err == nil {
			break
		}
		fmt.Printf("Kafka not ready yet... retrying in 3 seconds (Attempt %d/7)\n", i)
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		log.Fatal("Failed to connect to Kafka for topic creation:", err)
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             "orders-topic",
		NumPartitions:     3,
		ReplicationFactor: 3,
	}

	err = conn.CreateTopics(topicConfig)

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			fmt.Println("Kafka topic 'orders-topic' already exists.")
		} else {
			log.Printf("Warning: Failed to create topic: %v\n", err)
		}
	} else {
		fmt.Println("Kafka topic 'orders-topic' auto-created successfully over SSL!")
	}
}