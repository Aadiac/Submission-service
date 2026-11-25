package outbox

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Publisher struct { db *pgxpool.Pool; writer *kafka.Writer }
func NewPublisher(pool *pgxpool.Pool, brokers, topic string) *Publisher {
	return &Publisher{db:pool,writer:&kafka.Writer{Addr:kafka.TCP(split(brokers)...),Topic:topic,Balancer:&kafka.Hash{},RequiredAcks:kafka.RequireAll,Async:false}}
}
func split(s string) []string {
	// Broker list is comma-separated.
	var out []string; start:=0
	for i:=0;i<len(s);i++ { if s[i]==',' { if i>start { out=append(out,s[start:i]) }; start=i+1 } }
	if start<len(s) { out=append(out,s[start:]) }; return out
}
func (p *Publisher) Run(ctx context.Context) {
	ticker:=time.NewTicker(300*time.Millisecond); defer ticker.Stop()
	for {
		select { case <-ctx.Done(): return; case <-ticker.C: p.publishBatch(ctx) }
	}
}
func (p *Publisher) publishBatch(ctx context.Context) {
	// Claim a small batch. Marking after publish means a crash can cause replay;
	// Flink deduplicates by submission_id before aggregation.
	rows,err:=p.db.Query(ctx,`SELECT id,submission_id,form_id,created_at FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 200`)
	if err!=nil { log.Printf("outbox read: %v",err); return }
	type event struct { ID int64; SubmissionID int64; FormID string; CreatedAt time.Time }
	batch:=[]event{}
	for rows.Next() { var e event; if err:=rows.Scan(&e.ID,&e.SubmissionID,&e.FormID,&e.CreatedAt); err==nil { batch=append(batch,e) } }
	rows.Close()
	for _,e:=range batch {
		payload,_:=json.Marshal(map[string]any{"submission_id":e.SubmissionID,"form_id":e.FormID,"created_at":e.CreatedAt.UTC().Format(time.RFC3339Nano)})
		if err:=p.writer.WriteMessages(ctx,kafka.Message{Key:[]byte(e.FormID),Value:payload}); err!=nil { log.Printf("kafka publish: %v",err); return }
		if _,err:=p.db.Exec(ctx,`UPDATE outbox SET published_at=now() WHERE id=$1 AND published_at IS NULL`,e.ID); err!=nil { log.Printf("outbox mark: %v",err); return }
	}
}
