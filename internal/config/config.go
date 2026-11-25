package config

import "os"

type Config struct {
	Port, DatabaseURL, KafkaBrokers, KafkaTopic string
}

func Load() Config {
	return Config{
		Port: env("PORT", "8080"),
		DatabaseURL: env("DATABASE_URL", "postgres://app:app@localhost:5432/submissions?sslmode=disable"),
		KafkaBrokers: env("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic: env("KAFKA_TOPIC", "submission-events"),
	}
}
func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" { return v }
	return fallback
}
