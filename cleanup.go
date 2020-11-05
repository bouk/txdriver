package txdriver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
)

func cleanupFunctionForDriver(d driver.Driver) func(Connection) error {
	name := reflect.TypeOf(d).Elem().Name()
	switch name {
	case "MySQLDriver":
		return mysqlCleanup
	default:
		return nil
	}
}

func mysqlCleanup(c Connection) error {
	// Reset AUTO_INCREMENT for all the tables that were changed
	rows, err := c.Query(`SELECT TABLE_NAME
	FROM information_schema.tables
	WHERE TABLE_SCHEMA=DATABASE() AND AUTO_INCREMENT>1`, nil)
	if err != nil {
		return err
	}

	// This looks weird because we don't have an *sql.Conn, but rather a driver.Conn
	names := make([]string, 0)
	values := make([]driver.Value, len(rows.Columns()))
	for {
		err := rows.Next(values)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		names = append(names, fmt.Sprintf("%s", values[0]))
	}

	for _, name := range names {
		if _, err := c.Exec(fmt.Sprintf("ALTER TABLE `%s` AUTO_INCREMENT = 1;", name), nil); err != nil {
			return err
		}
	}
	return nil
}
