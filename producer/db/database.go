package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func ConnectToDB() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: Error loading .env file, relying on system env variables.")
	}

	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbname := os.Getenv("DB_NAME")

	// Read the exact same SSL files we use for Kafka
	sslCert := os.Getenv("SSL_CERT_FILE")
	sslKey := os.Getenv("SSL_KEY_FILE")
	sslCA := os.Getenv("SSL_CA_FILE")

	var connStr string

	// Dynamically build the connection string
	// Dynamically build the connection string
	if sslCert != "" && sslKey != "" && sslCA != "" {
		// Notice we removed sslcert and sslkey, but kept sslrootcert!
		connStr = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=verify-ca sslrootcert=%s",
			host, port, user, password, dbname, sslCA)
		fmt.Println("🔒 Connecting to PostgreSQL using 1-Way TLS (Encrypted & Verified)...")
	} else {
		connStr = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host, port, user, password, dbname)
		fmt.Println("⚠️ Connecting to PostgreSQL in PLAINTEXT mode...")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database configuration: %w", err)
	}

	maxAttempts := 5
	for i := 1; i <= maxAttempts; i++ {
		err = db.Ping()

		if err == nil {
			fmt.Println("✅ Successfully connected to PostgreSQL!")
			return db, nil
		}

		// Added the exact error message here so you can see if it's a certificate issue!
		log.Printf("⏳ Database connection attempt %d/%d failed: %v", i, maxAttempts, err)

		if i < maxAttempts {
			time.Sleep(1 * time.Second)
		}
	}

	db.Close()
	return nil, fmt.Errorf("could not connect to the database after %d attempts: %w", maxAttempts, err)
}