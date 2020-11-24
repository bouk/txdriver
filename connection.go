package txdriver

import (
	"context"
	"database/sql/driver"
	"errors"
)

var (
	errConnInUse                = errors.New("txdriver: connection already in use")
	errConnCannotBeClosed       = errors.New("txdriver: connection should not be closed")
	errIncompatibleDriver       = errors.New("txdriver: incompatible driver")
	errAlreadyInsideTransaction = errors.New("txdriver: already inside transaction")
	errNotInsideTransaction     = errors.New("txdriver: not inside transaction")
)

type txConnector struct {
	conn   *connection
	driver driver.Driver
}

func (t *txConnector) Driver() driver.Driver {
	return t.driver
}

func (t *txConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if t.conn == nil {
		panic(errConnInUse)
	}
	conn := t.conn
	t.conn = nil
	return conn, nil
}

// driverConnection is the interface that drivers need to implement
type driverConnection interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ExecerContext
	driver.Pinger
	driver.QueryerContext
}

type connection struct {
	driverConnection

	// cleanup is called when recycling the connection
	cleanup func(driverConnection) error

	// insideTransaction specifies whether we have a savepoint
	insideTransaction bool

	// c is the channel to return the connection to on Close
	c chan<- *connection

	// tx is the Real transaction
	tx driver.Tx
}

func (c *connection) setup() (err error) {
	c.tx, err = c.driverConnection.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil {
		c.c <- c
	}
	return
}

// Close is here to receive the Close() call on *sql.DB. If Close is called on
// an *sql.Conn the txConnector won't be able to dispense another connection and
// the *sql.DB that is using it will stop working.
func (c *connection) Close() error {
	// Roll back the Real transaction
	if err := c.tx.Rollback(); err != nil {
		return err
	}

	if c.cleanup != nil {
		if err := c.cleanup(c.driverConnection); err != nil {
			return err
		}
	}

	// Start another transaction
	return c.setup()
}

func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.insideTransaction {
		// The sql package should make sure this doesn't happen
		return nil, errAlreadyInsideTransaction
	}
	c.insideTransaction = true

	_, err := c.ExecContext(ctx, "SAVEPOINT txdriver_transaction", nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *connection) Commit() error {
	if !c.insideTransaction {
		return errNotInsideTransaction
	}
	c.insideTransaction = false

	_, err := c.ExecContext(context.Background(), "RELEASE SAVEPOINT txdriver_transaction", nil)
	return err
}

func (c *connection) Rollback() error {
	if !c.insideTransaction {
		return errNotInsideTransaction
	}
	c.insideTransaction = false

	_, err := c.ExecContext(context.Background(), "ROLLBACK TO txdriver_transaction", nil)
	return err
}
