package txdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// TransactionDBCreator creates DB instances that are scoped to a transaction.
type TransactionDBCreator struct {
	c      <-chan *connection
	driver driver.Driver
}

// FromDriver creates a TransactionDBCreator from a driver.Driver
func FromDriver(ctx context.Context, d driver.Driver, dsn string) (*TransactionDBCreator, error) {
	var connector driver.Connector
	var err error
	if dc, ok := d.(driver.DriverContext); ok {
		connector, err = dc.OpenConnector(dsn)
		if err != nil {
			return nil, err
		}
	} else {
		connector = &dsnConnector{dsn, d}
	}

	return FromConnector(ctx, connector)
}

// FromConnector creates a TransactionDBCreator from a Connector
func FromConnector(ctx context.Context, connector driver.Connector) (*TransactionDBCreator, error) {
	driverConn, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	// Convert to Connection that implements the interfaces we want
	conn, ok := driverConn.(driverConnection)
	if !ok {
		driverConn.Close()
		return nil, errIncompatibleDriver
	}

	driver := connector.Driver()
	c := make(chan *connection, 1)
	txDriverConn := &connection{
		driverConnection: conn,
		c:                c,
		cleanup:          cleanupFunctionForDriver(driver),
	}

	// setup will insert the conn into the channel
	if err = txDriverConn.setup(); err != nil {
		return nil, err
	}

	creator := &TransactionDBCreator{
		c:      c,
		driver: driver,
	}
	return creator, nil
}

// DB creates an *sql.DB that uses the transaction connection.
// Only one DB is created at any time, this is to ensure that only one
// test is interacting with the database at any moment.
func (t *TransactionDBCreator) DB(ctx context.Context) (*sql.DB, error) {
	var conn *connection
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn = <-t.c:
	}

	connector := &txConnector{
		conn:   conn,
		driver: t.driver,
	}
	db := sql.OpenDB(connector)

	// We need to open a connection here, to ensure that
	// the underlying connection is closed in case the DB is never used.
	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// return to DB pool
	if err = c.Close(); err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	return db, nil
}
