package txdriver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
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
