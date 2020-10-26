package txdriver

import (
	"context"
	"database/sql/driver"
)

type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (d *dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return d.driver.Open(d.dsn)
}

func (d *dsnConnector) Driver() driver.Driver {
	return d.driver
}
