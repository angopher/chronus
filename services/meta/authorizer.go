package meta

import (
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

type Authorizer struct {
}

func (a *Authorizer) AuthorizeQuery(u meta.User, query *influxql.Query, database string) error {
	return nil
}

func (a *Authorizer) AuthorizeWrite(username, database string) error {
	return nil
}

func (a *Authorizer) AuthorizeDatabase(u meta.User, priv influxql.Privilege, database string) error {
	return nil
}
