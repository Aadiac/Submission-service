CREATE TABLE IF NOT EXISTS submissions (
    id BIGSERIAL PRIMARY KEY,
    form_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_submissions_form_created
    ON submissions(form_id, created_at DESC);

CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    submission_id BIGINT NOT NULL UNIQUE REFERENCES submissions(id),
    form_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS hourly_aggregations (
    form_id TEXT NOT NULL,
    hour_start TIMESTAMPTZ NOT NULL,
    submission_count BIGINT NOT NULL,
    PRIMARY KEY(form_id, hour_start)
);

-- Insert outbox row in the same database transaction as the submission.
CREATE OR REPLACE FUNCTION create_submission_outbox() RETURNS trigger AS $$
BEGIN
    INSERT INTO outbox(submission_id,form_id,created_at)
    VALUES(NEW.id,NEW.form_id,NEW.created_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_submission_outbox ON submissions;
CREATE TRIGGER trg_submission_outbox
AFTER INSERT ON submissions
FOR EACH ROW EXECUTE FUNCTION create_submission_outbox();
