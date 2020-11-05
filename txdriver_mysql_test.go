package txdriver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func createMySQLTestDB(t testing.TB) *TransactionDBCreator {
	testDatabase := fmt.Sprintf("test_db_%d", rand.Int63())

	conf := mysql.NewConfig()
	conf.Addr = "127.0.0.1:14123"
	conf.User = "root"
	conf.MultiStatements = true

	ctx := context.Background()

	createConn, err := mysql.NewConnector(conf)
	if err != nil {
		t.Fatal(err)
	}
	createDB := sql.OpenDB(createConn)
	statement := fmt.Sprintf(`CREATE DATABASE %s`, testDatabase)
	_, err = createDB.ExecContext(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}
	createDB.Close()
	conf.DBName = testDatabase

	conn, err := mysql.NewConnector(conf)
	if err != nil {
		t.Fatal(err)
	}

	createDB = sql.OpenDB(conn)
	if _, err = createDB.ExecContext(ctx, `CREATE TABLE users (id BIGINT PRIMARY KEY AUTO_INCREMENT)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := createDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s", testDatabase)); err != nil {
			t.Error(err)
		}
	})

	txdb, err := FromConnector(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	return txdb
}

func TestMySQL(t *testing.T) {
	ctx := context.TODO()
	txdb := createMySQLTestDB(t)
	for i := 0; i < 10; i++ {
		db, err := txdb.DB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := db.ExecContext(ctx, "INSERT INTO users VALUES ()")
		if err != nil {
			t.Fatal(err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Error(err)
		}
		if id != 1 {
			t.Errorf("unexpected insert ID: %d!=1", id)
		}
		db.Close()
	}
}
