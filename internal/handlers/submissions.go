package handlers

import (
	"encoding/json"
	"net/http"
	"net/mail"
	"strconv"
	"strings"
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"submission-service/internal/models"
)

type Handler struct { db *pgxpool.Pool }
func New(pool *pgxpool.Pool) *Handler { return &Handler{db: pool} }

func (h *Handler) CreateSubmission(c *gin.Context) {
	var req models.CreateSubmissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error":"invalid JSON body"}); return
	}
	req.FormID = strings.TrimSpace(req.FormID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.Email = strings.TrimSpace(req.Email)
	if req.FormID == "" { c.JSON(400, gin.H{"error":"form_id is required"}); return }
	if req.IdempotencyKey == "" { c.JSON(400, gin.H{"error":"idempotency_key is required"}); return }
	addr, err := mail.ParseAddress(req.Email)
	if err != nil || addr.Address != req.Email { c.JSON(400, gin.H{"error":"email is invalid"}); return }
	if req.Data == nil { req.Data = map[string]any{} }

	raw, err := json.Marshal(req.Data)
	if err != nil { c.JSON(400, gin.H{"error":"data must be valid JSON"}); return }

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Unique constraint on idempotency_key makes this safe under concurrent requests.
	var s models.Submission
	var dataBytes []byte
	err = h.db.QueryRow(ctx, `
		INSERT INTO submissions(form_id,idempotency_key,email,data)
		VALUES($1,$2,$3,$4)
		ON CONFLICT(idempotency_key) DO NOTHING
		RETURNING id,form_id,idempotency_key,email,data,created_at`,
		req.FormID, req.IdempotencyKey, req.Email, raw,
	).Scan(&s.ID,&s.FormID,&s.IdempotencyKey,&s.Email,&dataBytes,&s.CreatedAt)

	created := err == nil
	if err == pgx.ErrNoRows {
		err = h.db.QueryRow(ctx, `SELECT id,form_id,idempotency_key,email,data,created_at FROM submissions WHERE idempotency_key=$1`, req.IdempotencyKey).
			Scan(&s.ID,&s.FormID,&s.IdempotencyKey,&s.Email,&dataBytes,&s.CreatedAt)
	}
	if err != nil { c.JSON(500, gin.H{"error":"database operation failed"}); return }
	_ = json.Unmarshal(dataBytes, &s.Data)
	status := http.StatusOK
	if created { status = http.StatusCreated }
	c.JSON(status, gin.H{"submission":s,"created":created})
}

func (h *Handler) ListSubmissions(c *gin.Context) {
	formID := strings.TrimSpace(c.Param("form_id"))
	if formID == "" { c.JSON(400, gin.H{"error":"form_id is required"}); return }
	page, _ := strconv.Atoi(c.DefaultQuery("page","1"))
	if page < 1 { page = 1 }
	limit := 10
	offset := (page-1)*limit
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second); defer cancel()
	rows, err := h.db.Query(ctx, `SELECT id,form_id,idempotency_key,email,data,created_at
		FROM submissions WHERE form_id=$1 ORDER BY created_at DESC,id DESC LIMIT $2 OFFSET $3`, formID,limit,offset)
	if err != nil { c.JSON(500, gin.H{"error":"database query failed"}); return }
	defer rows.Close()
	items := make([]models.Submission,0,limit)
	for rows.Next() {
		var s models.Submission; var raw []byte
		if err := rows.Scan(&s.ID,&s.FormID,&s.IdempotencyKey,&s.Email,&raw,&s.CreatedAt); err != nil { c.JSON(500,gin.H{"error":"database scan failed"}); return }
		_ = json.Unmarshal(raw,&s.Data); items=append(items,s)
	}
	var total int64
	if err := h.db.QueryRow(ctx, `SELECT count(*) FROM submissions WHERE form_id=$1`,formID).Scan(&total); err != nil {
		c.JSON(500,gin.H{"error":"count query failed"}); return
	}
	c.JSON(200,gin.H{"form_id":formID,"page":page,"per_page":limit,"total":total,"submissions":items})
}

func (h *Handler) ListAggregations(c *gin.Context) {
	formID := strings.TrimSpace(c.Param("form_id"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
	rows, err := h.db.Query(ctx, `SELECT form_id,hour_start,submission_count FROM hourly_aggregations WHERE form_id=$1 ORDER BY hour_start`,formID)
	if err != nil { c.JSON(500,gin.H{"error":"aggregation query failed"}); return }
	defer rows.Close()
	type row struct { FormID string `json:"form_id"`; Hour time.Time `json:"hour"`; Count int64 `json:"count"` }
	out := []row{}
	for rows.Next() { var x row; if err:=rows.Scan(&x.FormID,&x.Hour,&x.Count); err!=nil { c.JSON(500,gin.H{"error":"scan failed"}); return }; out=append(out,x) }
	c.JSON(200,gin.H{"form_id":formID,"aggregations":out})
}

