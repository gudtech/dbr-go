package ropdb

type DbrInstance struct {
	handle             string
	dbname             string
	connectionUsername string
	connectionPassword string
	dbrUsername        string
	dbrPassword        string
	host               string
	dbfile             *string
	module             string
	tag                *string
}

func (dbr *DbrInstance) Copy() *DbrInstance {
	if dbr == nil {
		return nil
	}

	copy := *dbr

	// Make sure the pointers to strings aren't copied.
	copy.dbfile = nil
	if dbr.dbfile != nil {
		dbfileCopy := *dbr.dbfile
		copy.dbfile = &dbfileCopy
	}

	copy.tag = nil
	if dbr.tag != nil {
		tagCopy := *dbr.tag
		copy.tag = &tagCopy
	}

	return &copy
}

// Same loosely checks if the dbr instances are using the same database,
// if they aren't the same then it returns what was different.
func (dbr *DbrInstance) SameDatabase(other *DbrInstance) (bool, []string) {
	var differences []string
	if dbr.DatabaseName() != other.DatabaseName() {
		differences = append(differences, "database name")
	}

	if dbr.Host() != other.Host() {
		differences = append(differences, "host")
	}

	if len(differences) > 0 {
		return false, differences
	}

	return true, []string{}
}

func (dbr *DbrInstance) Handle() string {
	return dbr.handle
}

func (dbr *DbrInstance) DatabaseName() string {
	return dbr.dbname
}

func (dbr *DbrInstance) ConnectionUsername() string {
	return dbr.connectionUsername
}

func (dbr *DbrInstance) ConnectionPassword() string {
	return dbr.connectionPassword
}

func (dbr *DbrInstance) DbrUsername() string {
	return dbr.dbrUsername
}

func (dbr *DbrInstance) DbrPassword() string {
	return dbr.dbrPassword
}

func (dbr *DbrInstance) Host() string {
	return dbr.host
}

func (dbr *DbrInstance) DatabaseFile() *string {
	return dbr.dbfile
}

func (dbr *DbrInstance) Module() string {
	return dbr.module
}

func (dbr *DbrInstance) Tag() *string {
	return dbr.tag
}
