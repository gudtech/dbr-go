package ropdb

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// Wrapper type for sql.Rows implementing friendlier error semantics.
type Rows struct {
	rows *sql.Rows
	err  error
}

func WrapRows(rows *sql.Rows, err error) *Rows {
	return &Rows{rows: rows, err: err}
}

func (r *Rows) Scan(dest ...interface{}) bool {
	if r.rows == nil {
		return false
	}

	if r.rows.Next() {
		r.err = r.rows.Scan(dest...)
		if r.err == nil {
			return true
		}

		r.Close()
		return false
	}

	r.err = r.rows.Err()
	_ = r.rows.Close()
	return false
}

func (r *Rows) Close() {
	if r.rows != nil {
		_ = r.rows.Close()
		r.rows = nil
	}
}

func (r *Rows) Err() error {
	return r.err
}

func QueryContext(ctx context.Context, db *sql.DB, dbErr error, query string, args ...interface{}) *Rows {
	if dbErr != nil {
		return &Rows{err: dbErr}
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &Rows{err: err}
	}
	return &Rows{rows: rows}
}

type Row struct {
	row *sql.Row
	err error
}

func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	return r.row.Scan(dest...)
}
