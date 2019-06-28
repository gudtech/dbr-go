package ropdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type Account struct {
	pool *Common

	id int

	active    bool
	name      string
	clientKey string

	// Underyling database connection
	db    *sql.DB
	dbErr error

	// Synchronization object for opening the database
	dbLock sync.Once

	// Database/schema name
	config *DbrInstance
	ops    *DbrInstance

	accountLock sync.RWMutex
}

func (a *Account) Common() *Common {
	a.accountLock.RLock()
	defer a.accountLock.RUnlock()
	return a.pool
}

func (a *Account) ID() int {
	a.accountLock.RLock()
	defer a.accountLock.RUnlock()
	return a.id
}

func (a *Account) Active() bool {
	a.accountLock.RLock()
	defer a.accountLock.RUnlock()
	return a.active
}

func (a *Account) SetActive(active bool) {
	a.accountLock.Lock()
	defer a.accountLock.Unlock()
	a.active = active
}

func (a *Account) Name() string {
	a.accountLock.RLock()
	defer a.accountLock.RUnlock()
	return a.name
}

func (a *Account) ClientKey() string {
	a.accountLock.RLock()
	defer a.accountLock.RUnlock()
	return a.clientKey
}

func (a *Account) setInfo(name, clientKey string) {
	a.accountLock.Lock()
	defer a.accountLock.Unlock()
	a.name = name
	a.clientKey = clientKey
}

// Config returns a copy of the `config` dbr instance.
func (a *Account) DbrConfig() *DbrInstance {
	a.accountLock.RLock()
	defer a.accountLock.RUnlock()
	copy := a.config.Copy()
	return copy
}

func (a *Account) setDbrConfig(config *DbrInstance) {
	a.accountLock.Lock()
	defer a.accountLock.Unlock()
	a.config = config
}

// Ops returns a copy of the `ops` dbr instance.
func (a *Account) DbrOps() *DbrInstance {
	a.accountLock.RLock()
	defer a.accountLock.RUnlock()
	copy := a.ops.Copy()
	return copy
}

func (a *Account) setDbrOps(ops *DbrInstance) {
	a.accountLock.Lock()
	defer a.accountLock.Unlock()
	a.ops = ops
}

func (a *Account) NameAndID() string {
	return fmt.Sprintf("%s (%d)", a.Name(), a.ID())
}

func (a *Account) DB() (*sql.DB, error) {
	a.accountLock.Lock()
	defer a.accountLock.Unlock()
	a.dbLock.Do(func() {
		a.db, a.dbErr = a.pool.OpenInstance(a.config)
	})
	return a.db, a.dbErr
}

func (a *Account) QueryPrep(query string) string {
	return strings.Replace(strings.Replace(query, "{config}", a.config.dbname, -1), "{ops}", a.ops.dbname, -1)
}

func (a *Account) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	db, dbErr := a.DB()
	if dbErr != nil {
		return nil, dbErr
	}
	return db.ExecContext(ctx, a.QueryPrep(query), args...)
}

func (a *Account) QueryContext(ctx context.Context, query string, args ...interface{}) *Rows {
	db, dbErr := a.DB()
	return QueryContext(ctx, db, dbErr, a.QueryPrep(query), args...)
}

func (a *Account) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	db, dbErr := a.DB()
	if dbErr != nil {
		return &Row{err: dbErr}
	}
	return &Row{row: db.QueryRowContext(ctx, a.QueryPrep(query), args...)}
}
