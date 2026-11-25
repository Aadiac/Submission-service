package models

import "time"

type Submission struct {
	ID             int64          `json:"id"`
	FormID         string         `json:"form_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	Email          string         `json:"email"`
	Data           map[string]any `json:"data"`
	CreatedAt      time.Time      `json:"created_at"`
}

type CreateSubmissionRequest struct {
	FormID         string         `json:"form_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	Email          string         `json:"email"`
	Data           map[string]any `json:"data"`
}
