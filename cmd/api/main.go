package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"submission-service/internal/config"
	"submission-service/internal/db"
	"submission-service/internal/handlers"
	"submission-service/internal/outbox"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	h := handlers.New(pool)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.POST("/submissions", h.CreateSubmission)
	r.GET("/forms/:form_id/submissions", h.ListSubmissions)
	r.GET("/forms/:form_id/aggregations", h.ListAggregations)

	// The outbox publisher retries until Kafka acknowledges the event.
	publisher := outbox.NewPublisher(pool, cfg.KafkaBrokers, cfg.KafkaTopic)
	go publisher.Run(ctx)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("API listening on :%s", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
