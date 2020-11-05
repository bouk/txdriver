package txdriver

import (
	"database/sql/driver"
	"fmt"
	"io"
)

func cleanupFunctionForDriver(d driver.Driver) func(driverConnection) error {
	switch fmt.Sprintf("%T", d) {
	case "*mysql.MySQLDriver":
		return mysqlCleanup
	case "*pq.Driver":
		return pqCleanup
	default:
		return nil
	}
}

func pqCleanup(c driverConnection) error {
	// Reset all sequences to 1 https://www.postgresql.org/docs/8.2/functions-sequence.html
	// TODO: need to take into account maximum ids that wre inserted before the transaction
	rows, err := c.Query(`
SELECT SETVAL(c.oid, 1, false)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace 
WHERE c.relkind = 'S' AND n.nspname = 'public'
`, nil)
	if err != nil {
		return err
	}
	return rows.Close()
}

func mysqlCleanup(c driverConnection) error {
	// Reset AUTO_INCREMENT for all the tables that were changed
	rows, err := c.Query(`
SELECT TABLE_NAME
FROM information_schema.tables
WHERE TABLE_SCHEMA=DATABASE() AND AUTO_INCREMENT>1`, nil)
	if err != nil {
		return err
	}
	defer rows.Close()

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
