package txdriver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func ExampleFromConnector() {
	ctx := context.TODO()
	testDatabase := fmt.Sprintf("test_db_%d", rand.Int63())

	conf := mysql.NewConfig()
	conf.Addr = "127.0.0.1:14123"
	conf.User = "root"
	conf.MultiStatements = true

	createConn, _ := mysql.NewConnector(conf)
	createDB := sql.OpenDB(createConn)
	statement := fmt.Sprintf(`CREATE DATABASE %s`, testDatabase)
	createDB.ExecContext(ctx, statement)
	createDB.Close()

	conf.DBName = testDatabase
	conn, _ := mysql.NewConnector(conf)

	createDB = sql.OpenDB(conn)
	createDB.ExecContext(ctx, `CREATE TABLE users (id BIGINT PRIMARY KEY AUTO_INCREMENT)`)
	defer func() {
		defer createDB.Close()
		createDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s", testDatabase))
	}()

	txdb, _ := FromConnector(ctx, conn)

	db, _ := txdb.DB(ctx)
	db.Exec("INSERT INTO users VALUES ()")

	var n int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&n)
	fmt.Println(n)
	db.Close()

	db, _ = txdb.DB(ctx)
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&n)
	fmt.Println(n)
	db.Close()

	// Output:
	// 1
	// 0
}

func createTestDB(t testing.TB) *TransactionDBCreator {
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
	txdb := createTestDB(t)
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
