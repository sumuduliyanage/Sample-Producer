package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type WeatherData struct {
	StationId   int    `json:"stationId"`
	StationName string `json:"stationName"`
	Temperature string `json:"temperature"`
}

func main() {

	keypair, err := tls.LoadX509KeyPair("service.cert", "service.key")
	if err != nil {
		log.Fatalf("Failed to load access key and/or access certificate: %s", err)
	}

	caCert, err := os.ReadFile("ca.pem")
	if err != nil {
		log.Fatalf("Failed to read CA certificate file: %s", err)
	}

	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCert)
	if !ok {
		log.Fatalf("Failed to parse CA certificate file: %s", err)
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			Certificates: []tls.Certificate{keypair},
			RootCAs:      caCertPool,
		},
	}

	serviceURI := os.Getenv("SERVICE_URI")
	if serviceURI == "" {
		fmt.Println("Environment variable 'SERVICE_URI' not set")
	}
	topicName := os.Getenv("TOPIC_NAME")
	if topicName == "" {
		fmt.Println("Environment variable 'TOPIC_NAME' not set")
	}
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{serviceURI},
		Topic:   topicName,
		Dialer:  dialer,
	})
	defer producer.Close()

	// Below endpoint will publish the temperature values of a certain station
	http.HandleFunc("/publish-temperature", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var data WeatherData
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "Invalid request payload", http.StatusBadRequest)
			return
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			http.Error(w, "Failed to marshal JSON data", http.StatusInternalServerError)
			return
		}
		error := producer.WriteMessages(context.Background(), kafka.Message{Value: jsonData})
		if error != nil {
			log.Printf("Failed to send message: %s", error)
			http.Error(w, "Failed to send message", http.StatusInternalServerError)
			return
		}
		log.Printf("Message sent: %s", jsonData)
		w.WriteHeader(http.StatusOK)
	})

	// Start the server on port 9090
	log.Println("Server starting on port 9090...")
	log.Fatal(http.ListenAndServe(":9090", nil))
}
