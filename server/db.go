package main

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
)

func newPostgreSQL(uri string) (*sql.DB, error) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, err
	}
	// test the connection
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	query := `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
	CREATE TABLE IF NOT EXISTS workflow (
		account_id UUID NOT NULL,
		id UUID NOT NULL DEFAULT uuid_generate_v4(),
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		name TEXT,
		version TEXT NOT NULL,
		graph JSONB NOT NULL
	);
	CREATE INDEX IF NOT EXISTS workflow_id_aid_rec ON workflow (account_id,id,created_at);
	ALTER TABLE workflow ADD COLUMN IF NOT EXISTS worker TEXT NOT NULL DEFAULT 'workflow-engine0';`
	_, err = db.Query(query)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type dbWorkflow struct {
	ID        uuid.UUID       `json:"id,omitempty"`
	CreatedAt time.Time       `json:"created_at,omitempty"`
	AccountID uuid.UUID       `json:"accound_id,omitempty"`
	Worker    string          `json:"worker,omitempty"`
	Name      string          `json:"name,omitempty"`
	Version   string          `json:"version,omitempty"`
	Graph     json.RawMessage `json:"graph,omitempty"`
}
