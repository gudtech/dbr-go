package ropdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type Common struct {
	// Database reference named by DBR_gt.conf
	bootDB          *sql.DB
	commonDbrConfig DbrInstance
	accounts        map[int]*Account
	accountsLock    sync.RWMutex
	accountDB       map[string]*sql.DB
	accountDBLock   sync.Mutex
	closing         bool

	enum       map[string]int
	enumHandle map[int]string
	enumName   map[int]string
	enumLock   sync.RWMutex
}

func Open(ctx context.Context, confPath string, dbrExpand bool) (*Common, error) {
	conftext, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %s", confPath, err)
	}

	// Load the bootstrap instance from the DBR_gt.conf

	var dbrconf DbrInstance
	p := new(Common)
	p.accounts = make(map[int]*Account)
	p.accountDB = make(map[string]*sql.DB)
	p.enum = make(map[string]int)
	p.enumName = make(map[int]string)
	p.enumHandle = make(map[int]string)

	for _, line := range strings.Split(string(conftext), "\n") {
		nocomment := strings.TrimSpace(strings.Split(line, "#")[0])
		if strings.HasPrefix(nocomment, "---") {
			return nil, errors.New("Multi-section DBR configs not supported")
		}

		for _, part := range strings.Split(nocomment, ";") {
			if eq := strings.IndexRune(part, '='); eq >= 0 {
				key := strings.TrimSpace(part[0:eq])
				value := strings.TrimSpace(part[eq+1:])
				switch key {
				case "dbname", "database":
					dbrconf.dbname = value
				case "handle", "name":
					dbrconf.handle = value
				case "password":
					dbrconf.connectionPassword = value
					dbrconf.dbrPassword = value
				case "username", "user":
					dbrconf.connectionUsername = value
					dbrconf.dbrUsername = value
				case "hostname", "host":
					dbrconf.host = value
				case "module", "type":
					dbrconf.module = value
				}
			}
		}
	}

	p.commonDbrConfig = dbrconf

	p.bootDB, err = p.OpenInstance(&dbrconf)
	if err != nil {
		return nil, err
	}

	err = p.dbrEnums(ctx)
	if err != nil {
		_ = p.bootDB.Close()
		return nil, fmt.Errorf("failed to load DBR enum information: %s", err)
	}

	if dbrExpand {
		err := p.ReadAccounts(ctx)
		if err != nil {
			_ = p.bootDB.Close()
			return nil, fmt.Errorf("failed to load DBR instances: %s", err)
		}

	}

	return p, nil
}

func ReadGtutilConfig(filename string) (map[string]string, error) {
	text, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %s", filename, err)
	}

	kv := make(map[string]string)
	for _, line := range strings.Split(string(text), "\n") {
		nocomment := strings.TrimSpace(strings.Split(line, "#")[0])
		if nocomment == "" {
			continue
		}
		pair := strings.SplitN(nocomment, "=", 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf("config line in %s has no = separator", filename)
		}
		key := strings.TrimSpace(pair[0])
		value := strings.TrimSpace(pair[1])
		_, exists := kv[key]
		if exists {
			return nil, fmt.Errorf("config line in %s has dup key %s", filename, key)
		}
		kv[key] = value
	}
	return kv, nil
}

func (p *Common) dbrEnums(ctx context.Context) error {
	p.enumLock.Lock()
	defer p.enumLock.Unlock()

	erows := QueryContext(ctx, p.bootDB, nil, "SELECT enum_id, name, handle FROM dbr.enum")
	defer erows.Close()
	var id int
	var name, handle string
	for erows.Scan(&id, &name, &handle) {
		//log.Println("enum:", id, name, handle)
		p.enum[handle] = id
		p.enumName[id] = name
		p.enumHandle[id] = handle
	}

	if erows.Err() != nil {
		return erows.Err()
	}

	return nil
}

func (p *Common) OpenInstance(i *DbrInstance) (*sql.DB, error) {
	var module, dsn string
	switch i.module {
	case "Mysql":
		module = "mysql"
		if strings.HasPrefix(i.host, "/") {
			dsn = fmt.Sprintf("%s:%s@unix(%s)/?collation=utf8mb4_unicode_ci", i.connectionUsername, i.connectionPassword, i.host)
		} else {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/?collation=utf8mb4_unicode_ci", i.connectionUsername, i.connectionPassword, i.host, 3306)
		}
	default:
		return nil, fmt.Errorf("unhandled module %s", i.module)
	}

	p.accountDBLock.Lock()
	defer p.accountDBLock.Unlock()

	if p.closing {
		return nil, errors.New("Pool being closed")
	}

	if db := p.accountDB[dsn]; db != nil {
		return db, nil
	}

	db, err := sql.Open(module, dsn)
	if err != nil {
		return nil, err
	}

	p.accountDB[dsn] = db
	return db, nil
}

func (p *Common) Enum(handle string) (int, bool) {
	p.enumLock.RLock()
	defer p.enumLock.RUnlock()

	value, ok := p.enum[handle]
	return value, ok
}

func (p *Common) EnumName(id int) (string, bool) {
	p.enumLock.RLock()
	defer p.enumLock.RUnlock()

	value, ok := p.enumName[id]
	return value, ok
}

func (p *Common) EnumHandle(id int) (string, bool) {
	p.enumLock.RLock()
	defer p.enumLock.RUnlock()

	value, ok := p.enumHandle[id]
	return value, ok
}

func (p *Common) DB() *sql.DB {
	return p.bootDB
}

func (p *Common) setAccount(id int, acct *Account) {
	p.accountsLock.Lock()
	defer p.accountsLock.Unlock()
	p.accounts[id] = acct
}

func (p *Common) Account(id int) *Account {
	p.accountsLock.RLock()
	defer p.accountsLock.RUnlock()
	a := p.accounts[id]

	if a == nil {
		return nil
	} else {
		return a
	}
}

func (p *Common) RetryAccount(ctx context.Context, id int) (*Account, error) {
	a := p.Account(id)
	if a == nil {
		err := p.ReadAccounts(ctx)
		if err != nil {
			return nil, err
		}

		a = p.Account(id)
	}

	return a, nil
}

func (p *Common) deleteAccount(id int) {
	p.accountsLock.Lock()
	defer p.accountsLock.Unlock()
	delete(p.accounts, id)
}

func (p *Common) Accounts() []*Account {
	p.accountsLock.RLock()
	defer p.accountsLock.RUnlock()

	var copy []*Account
	for _, acct := range p.accounts {
		copy = append(copy, acct)
	}

	return copy
}

func (p *Common) Close() error {
	// May be called on an incompletely opened pool
	var someErr error
	p.accountDBLock.Lock()
	defer p.accountDBLock.Unlock()

	p.closing = true
	for _, db := range p.accountDB {
		if err := db.Close(); err != nil {
			someErr = err
		}
	}
	return someErr
}

func (p *Common) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return p.bootDB.ExecContext(ctx, query, args...)
}

func (p *Common) QueryContext(ctx context.Context, query string, args ...interface{}) *Rows {
	return QueryContext(ctx, p.bootDB, nil, query, args...)
}

func (p *Common) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	return &Row{row: p.bootDB.QueryRowContext(ctx, query, args...)}
}

func (p *Common) BeginTx(ctx context.Context, options *sql.TxOptions) (*sql.Tx, error) {
	return p.bootDB.BeginTx(ctx, options)
}

func (p *Common) ReadAccounts(ctx context.Context) error {
	// Load additional instance(s) from dbr.dbr_instances
	irows := QueryContext(ctx, p.bootDB, nil, "SELECT handle, username, password, dbname, dbfile, host, module, tag FROM dbr.dbr_instances")
	defer irows.Close()

	var i DbrInstance
	i.connectionUsername = p.commonDbrConfig.connectionUsername
	i.connectionPassword = p.commonDbrConfig.connectionPassword

	for irows.Scan(&i.handle, &i.dbrUsername, &i.dbrPassword, &i.dbname, &i.dbfile, &i.host, &i.module, &i.tag) {
		// this assumes all common databases are colocated w/DBR and all per-client databases are colocated
		if i.tag == nil {
			continue
		}

		id, err := strconv.Atoi((*i.tag)[1:])
		if err != nil || id == 0 {
			continue
		}

		acct := p.Account(id)
		if acct == nil {
			acct = &Account{id: id, pool: p}
			p.setAccount(id, acct)
		}

		icopy := i
		switch i.handle {
		case "config":
			acct.setDbrConfig(&icopy)
		case "ops":
			acct.setDbrOps(&icopy)
		}
	}
	if irows.Err() != nil {
		return irows.Err()
	}

	var id int
	var name, clientkey string
	crows := QueryContext(ctx, p.bootDB, nil, "SELECT id, name, clientkey FROM directory.client")
	defer crows.Close()

	for crows.Scan(&id, &name, &clientkey) {
		acct := p.Account(id)

		if acct != nil && acct.DbrConfig() != nil && acct.DbrOps() != nil {
			acct.SetActive(true)
			acct.setInfo(name, clientkey)
		}
	}

	if crows.Err() != nil {
		return crows.Err()
	}

	for _, acct := range p.Accounts() {
		if !acct.Active() {
			p.deleteAccount(acct.id)
		}
	}

	return nil
}

func (common *Common) InstanceByTag(ctx context.Context, tag string, handle string) (*DbrInstance, error) {
	var instance DbrInstance
	instance.connectionUsername = common.commonDbrConfig.connectionUsername
	instance.connectionPassword = common.commonDbrConfig.connectionPassword

	row := common.bootDB.QueryRowContext(ctx, "SELECT handle, username, password, dbname, dbfile, host, module, tag FROM dbr.dbr_instances WHERE tag = ? AND handle = ? LIMIT 1", tag, handle)
	err := row.Scan(&instance.handle, &instance.dbrUsername, &instance.dbrPassword, &instance.dbname, &instance.dbfile, &instance.host, &instance.module, &instance.tag)
	if err != nil {
		return nil, err
	}

	return &instance, nil
}

func (common *Common) PrivByName(ctx context.Context, name string) (int, error) {
	var id int
	err := common.bootDB.QueryRowContext(ctx, "SELECT id FROM directory.acl_priv WHERE name = ?", name).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("no priv named `%s` found", name)
		}

		return 0, err
	}

	return id, nil
}
