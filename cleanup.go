package txdriver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
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
	// This beauty is adapted from over here: https://gist.github.com/tbarbugli/5495200
	// And also inspired from here: https://stackoverflow.com/questions/41102908/how-to-reset-all-sequences-to-1-before-database-migration-in-postgresql
	rows, err := c.Query(`
SELECT 'SELECT SETVAL(' ||quote_literal(S.relname)|| ', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 0) + 1, false) FROM ' ||quote_ident(T.relname)|| ';'
FROM pg_class AS S, pg_depend AS D, pg_class AS T, pg_attribute AS C
WHERE S.relkind = 'S'
AND S.oid = D.objid
AND D.refobjid = T.oid
AND D.refobjid = C.attrelid
AND D.refobjsubid = C.attnum
ORDER BY S.relname
`, nil)
	if err != nil {
		return err
	}

	// This looks weird because we don't have an *sql.Conn, but rather a driver.Conn
	queries := make([]string, 0)
	values := make([]driver.Value, len(rows.Columns()))
	for {
		err := rows.Next(values)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		queries = append(queries, fmt.Sprintf("%s", values[0]))
	}
	if len(queries) == 0 {
		return nil
	}
	r, err := c.Query(strings.Join(queries, ";"), nil)
	if err != nil {
		return err
	}
	return r.Close()
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
